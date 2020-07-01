package com.tomandersen.hive.udtfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.UDFJson;
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
 * <h3>此UDTF主要用于处理启动日志start-log</h3>
 * 将输入的单个字符串按照启动日志start-log的JSON格式进行解析,然后按照表的格式输出JSON对象中的
 * 各个字段,类似于hive中内建的 json_tuple 函数.
 * <p>
 * UDTF输入: 代表启动日志的Json字符串,以及需要获取的Json Value对应的Json Key
 * UDTF输出: 启动日志中与输入的Json Key对应的Json Value
 * <p>
 * 参考:
 * {@link GenericUDTFJSONTuple},{@link GenericUDTFExplode},{@link GenericUDTFPosExplode},
 * {@link UDFJson},{@link CustomUDTFSplitAndExplode}
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/6/13
 */
// 用于生成Function document.可以使用desc function <function_name>命令查看函数使用文档.
@Description(
        name = "start_log_handler",
        value = "_FUNC_(jsonStr,key1,key2...) - return all values of JSON object corresponding to the keys."
)
public class CustomUDTFStartLogHandler extends GenericUDTF {

    // 保存所有输入字段对应的 ObjectInspector
    private transient ObjectInspector[] inputFieldIOs;
    // 保存输出列个数
    private int numCols;
    // 保存输出列
    private transient Text[] retCols;
    // 保存空输出列,用于在特殊情况输出
    private transient Object[] nullCols;
    // 保存输入参数中所有的key字符串
    private String[] jsonKeys;
    // 保存Key是否已经解析
    private boolean keyParsed = false;

    /**
     * 此方法一般用于检查UDTF函数的输入参数是否合法,以及生成一个用于查看输出字段数据的StructObjectInspector.
     *
     * @param argOIs {@link StructObjectInspector},传入的是所有输入参数字段的查看器
     * @return {@link StructObjectInspector},返回的是新生成字段结构化对象的查看器
     * @date 2020/5/22
     */
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 1.获取所有输入参数字段
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
        int argsLength = inputFields.size(); // 参数长度(首个参数为JSONStr)
        // PS:参数统称为字段field,可以看做是被查询表的某一列

        numCols = argsLength - 1; // 输出列的个数,有多少个Key输入则输出多少列
        inputFieldIOs = new ObjectInspector[argsLength]; // 获取所有输入参数字段对应的检查器(Inspector)
        for (int i = 0; i < argsLength; i++) {
            inputFieldIOs[i] = inputFields.get(i).getFieldObjectInspector();
        }
        // PS:任何字段对象都需通过对应的检查器来查看,各类检查器(Inspector)统一了查看对应字段所用的API,且每类检查器全局都是由工厂Factory生成的单例

        // 2.检查参数个数
        if (argsLength < 2) {// 本UDTF至少需要2个参数,即JSONStr和key
            throw new UDFArgumentException
                    ("start_log_handler(JSONStr,key1...) takes two argument at least");
        }
        // 3.检查参数类型
        for (int i = 0; i < argsLength; ++i) {// 本UDTF的所有参数必须皆为原始类型,且必须为String类型
            if (inputFieldIOs[i].getCategory() != ObjectInspector.Category.PRIMITIVE ||
                    !inputFieldIOs[i].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
                throw new UDFArgumentException
                        ("start_log_handler(...)'s arguments have to be string type");
            }
        }
        /*原意是在此仇判断首个参数是否能够解析成JSON对象,如果不能则抛出异常
         throw new UDFArgumentException("start_log_handler(...)'s first argument have to be a JSON string.")
         逻辑错误:无法在此方法内获取具体的对象,因为在此方法中需要进行参数校验以及生成并返回各个参数字段对应的查看器
         此时并不能获取具体的对象,因此无法判断首个参数是否符合JSON格式.*/

        // 4.成员变量赋值
        retCols = new Text[numCols]; // 生成返回列,用于保存返回的每行对象
        nullCols = new Object[numCols]; // 生成空返回列,用于在某些特殊情况返回
        jsonKeys = new String[numCols]; // 创建输入参数中的JSON Key字符串对象
        for (int i = 0; i < numCols; i++) {
            retCols[i] = new Text();
            nullCols[i] = null;
        }

        // 5.设置输出字段名,以及对应的 ObjectInspector,创建并返回输出对象的 ObjectInspector
        ArrayList<String> fieldNames = new ArrayList<>(numCols);
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>(numCols);
        for (int i = 0; i < numCols; i++) {
            fieldNames.add("c" + i);// 设置输出列的列名
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);// 设置输出字段的查看器
        }
        // 将所有的输出对象视为整个结构化对象,生成并返回此结构化对象的查看器 Inspector
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);

    }

    // 处理传入的参数对象,使用对应的 ObjectInspector 访问对象内容
    // 此方法会根据传入列的行数通过同一个实例重复调用多次
    @Override
    public void process(Object[] o) throws HiveException {
        // 排除特殊情况,如果首个对象为null,则直接返回空行
        if (o[0] == null) {
            forward(nullCols);
            return;
        }

        // 将首个输入参数对象视为JSON字符串单独进行转换
        String startLogStr = ((StringObjectInspector) inputFieldIOs[0]).getPrimitiveJavaObject(o[0]);
        // 排除特殊情况,如果不能转换成JSON对象,则直接返回空行
        if (!JSON.isValidObject(startLogStr)) {
            forward(nullCols);
            return;
        }

        // 使用StringObjectInspector将输入参数对象Key转换为String对象
        // 由于对每行调用函数时,key都相同,因此Key只需要解析一次
        if (!keyParsed) {
            for (int i = 0; i < numCols; i++) {
                jsonKeys[i] = ((StringObjectInspector) inputFieldIOs[i + 1]).getPrimitiveJavaObject(o[i + 1]);
            }
            keyParsed = true;
        }

        // 获取Json Key对应的Json Value
        JSONObject startLogJsonObj = JSON.parseObject(startLogStr);// 将首个参数转换成JSON对象
        for (int i = 0; i < numCols; i++) {// 遍历参数中的Key,获取JSON对象中对应的Value,将其保存在返回列中,然后传给收集器
            String strValue = startLogJsonObj.getString(jsonKeys[i].trim());
            if (strValue == null) {// 如果没有对应的JSON value,则会返回null,若返回null则将其设置为空字符串
                strValue = "";
            }
            retCols[i].set(strValue);
        }
        forward(retCols);
    }

    // 查询结束时调用一次,用于释放相关资源
    @Override
    public void close() throws HiveException {

    }

    // 返回此UDTF对应的函数名
    @Override
    public String toString() {
        return "start_log_handler";
    }

    // 测试
    public static void main(String[] args) {
    }
}
