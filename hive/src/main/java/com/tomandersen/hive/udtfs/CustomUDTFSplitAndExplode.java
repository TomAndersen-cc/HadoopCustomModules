package com.tomandersen.hive.udtfs;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFJSONTuple;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFPosExplode;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

/**
 * <h3>Hive自定义表生成函数UDTF的实现Demo</h3>
 * 功能: 将字符串按照指定分隔符分割成独立的单词,并返回多行.
 * <p>
 * UDTF输入: 待分割字符串,分隔符正则表达式
 * UDTF输出: 分割后产生的单词列
 * <p>
 * 参考: {@link GenericUDTFJSONTuple},{@link GenericUDTFExplode},{@link GenericUDTFPosExplode}
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/5/21
 */

// 用于生成Function document.可以使用desc function <function_name>命令查看函数使用文档.
@Description(
        name = "split_explode",
        value = "_FUNC_(str,regex) - Splits str around occurrences that match regex and " +
                "separates the elements into multiple rows."
)
public class CustomUDTFSplitAndExplode extends GenericUDTF {

    // 保存所有输入字段对应的 ObjectInspector
    private transient ObjectInspector[] inputFieldIOs;
    // 保存输出列个数
    private int numCols;
    // 保存输出列
    private transient Text[] retCols;
    // 保存空输出列,用于在特殊情况输出
    private transient Object[] nullCols;
    // 保存输入参数中的正则表达式
    private String regex;
    // 保存正则表达式regex是否已经获取
    private boolean regexParsed = false;


    /**
     * 此方法一般用于检查输入参数是否合法,每个实例只会调用一次.
     * 一般用于用于检查UDTF函数的输入参数,以及生成一个用于查看新生成字段数据的StructObjectInspector.
     *
     * @param argOIs {@link StructObjectInspector},传入的是所有输入参数字段的查看器
     * @return {@link StructObjectInspector},返回的是新生成字段的查看器
     * @date 2020/5/22
     */
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 1.输入参数检查
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();// 获取所有的输入参数字段
        int argsLength = inputFields.size();

        inputFieldIOs = new ObjectInspector[argsLength];// 获取所有输入参数字段对应的检查器(Inspector)
        for (int i = 0; i < argsLength; i++) {
            inputFieldIOs[i] = inputFields.get(i).getFieldObjectInspector();
        }

        if (argsLength != 2) {// 检查参数个数
            throw new UDFArgumentException("split_explode(str,regex) takes only two arguments: " +
                    "the string and regular expression");
        }

        for (int i = 0; i < argsLength; ++i) {// 检查参数类型
            if (inputFieldIOs[i].getCategory() != ObjectInspector.Category.PRIMITIVE ||
                    !inputFieldIOs[i].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
                throw new UDFArgumentException("split_explode(str,regex)'s arguments have to be string type");
            }
        }

        // 2.成员变量赋值
        numCols = 1;
        retCols = new Text[numCols];
        nullCols = new Object[numCols];
        for (int i = 0; i < numCols; i++) {
            retCols[i] = new Text();
            nullCols[i] = null;
        }

        // 3.定义输出字段的列名(字段名)
        // 此处设置的各个列名很可能会在用户执行Hive SQL查询时,会被其指定的列别名所覆盖,如果查询时未指定别名,则显示此列名.
        // 此自定义表生成函数UDTF只输出单列,所以只添加了单个列的列名
        ArrayList<String> fieldNames = new ArrayList<>();
        fieldNames.add("split_explode_col");

        // 4.定义输出字段对应字段类型的ObjectInspector(对象检查器),一般用于查看/转换/操控该字段的内存对象
        // 只是查看器,并非是真正的字段对象
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>();
        // 由于输出字段的数据类型都是String类型
        // 因此使用检查器PrimitiveObjectInspectorFactory.javaStringObjectInspector
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        // 5.生成并返回输出对象的对象检查器
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // 此方法为核心处理方法,声明主要的处理过程,如果是对表使用函数,则会对表中每一行数据通过同一个实例调用一次此方法.
    // 输入参数为待处理的字符串以及分隔符正则表达式,每次处理完后直接使用forward方法将数据传给收集器.
    @Override
    public void process(Object[] args) throws HiveException {
        // 1.通过 ObjectInspector 获取待处理字符串
        String str = ((StringObjectInspector) inputFieldIOs[0]).getPrimitiveJavaObject(args[0]);
        if (str == null || str.length() == 0) {// 如果对象为null则直接输出空行
            forward(nullCols);
            return;
        }
        // 2.获取分割符正则表达式
        if (!regexParsed) {
            regex = ((StringObjectInspector) inputFieldIOs[1]).getPrimitiveJavaObject(args[1]);
            regexParsed = true;
        }
        // 3.按照正则表达式分割字符串
        String[] words = str.split(regex);
        // 4.输出数据
        // 将输出行通过forward方法传给收集器,每调用一次forward则代表在新生成字段中生成一行数据
        for (String word : words) {
            // 将输出数据放入输出数组的指定位置(每次写入时为覆盖写入),然后输出.本UDTF固定输出单列.
            retCols[0].set(word);
            forward(retCols);
        }
    }

    // 释放相关资源,一般不需要
    @Override
    public void close() throws HiveException {

    }

    // 返回此UDTF对应的函数名
    @Override
    public String toString() {
        return "split_explode";
    }
}
