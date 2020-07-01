package com.tomandersen.hive.udtfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFJSONTuple;
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
 * <h3>此UDTF主要用于处理事件日志event_log的公共字段部分</h3>
 * <p>
 * UDTF输入: 公共字段Json字符串,以及需要获取对应Json Value的Json Key.
 * <p>
 * UDTF输出: 输入的Json Key对应的公共字段中的Json Value.
 * <p>
 * 参考:{@link GenericUDTFJSONTuple},{@link CustomUDTFEventLogBaseHander},
 * {@link CustomUDTFEventLogEventHandler},{@link CustomUDTFEventLogHandler}
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/6/18
 */
// 用于生成Function document.可以使用desc function <function_name>命令查看函数使用文档.
@Description(
        name = "event_log_common_handler",
        value = "_FUNC_(jsonStr,key1,...) - return the json value" +
                "corresponding to the json keys in json strings."
)
public class CustomUDTFEventLogCommonHandler extends GenericUDTF {

    // 输出列个数
    private int numCols;
    // 所有输入字段对应的 ObjectInspector
    private transient ObjectInspector[] inputFieldIOs;
    // 输入参数中所有的 Json key
    private String[] jsonKeys;
    // JSON key是否已经解析
    private boolean keyParsed = false;
    // 输出列的某一行
    private transient Text[] retCols;
    // 空输出列,用于在特殊情况输出
    private transient Object[] nullCols;

    // 检查输入参数合法性,获取输入参数信息,创建并返回输出对象的 ObjectInspector
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 1.输入参数个数检查
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();// 获取所有输入字段
        int argsLength = inputFields.size();// 接收的参数个数
        if (argsLength < 2) {// 至少需要2个参数
            throw new UDFArgumentException
                    ("event_log_common_handler(event_log,key1...) takes two argument at least");
        }


        // 2.输入参数类型检查
        inputFieldIOs = new ObjectInspector[argsLength];
        for (int i = 0; i < argsLength; i++) {// 获取所有输入参数字段对应的 ObjectInspector
            inputFieldIOs[i] = inputFields.get(i).getFieldObjectInspector();
        }
        for (int i = 0; i < argsLength; ++i) {// 本UDTF的所有参数类型必须为Java String类型
            if (inputFieldIOs[i].getCategory() != ObjectInspector.Category.PRIMITIVE ||
                    !inputFieldIOs[i].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
                throw new UDFArgumentException
                        ("event_log_common_handler()'s arguments have to be string type");
            }
        }


        // 3.成员变量赋值
        numCols = argsLength - 1;// 计算返回列个数(输入参数中有一个为jsonStr,其他为Json Key,故返回列个数等于argsLength -1)
        retCols = new Text[numCols];// 生成返回列数组,用于保存返回的每行输出对象
        jsonKeys = new String[numCols];// 生成Json Key数组,用于保存输入的Json keys
        nullCols = new Object[numCols];// 生成空返回列数组,用于在某些特殊情况输出
        for (int i = 0; i < numCols; i++) {// 填充返回列
            retCols[i] = new Text();
            nullCols[i] = null;
        }


        // 4.设置输出字段名,以及对应的 ObjectInspector,创建并返回输出对象的 ObjectInspector
        ArrayList<String> fieldNames = new ArrayList<>(numCols); // 输出字段名
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>(numCols); // 输出字段检查器 ObjectInspector
        for (int i = 0; i < numCols; i++) {
            fieldNames.add("c" + i);
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }
        // 将所有的输出对象视为整个结构化对象,生成并返回此结构化对象的查看器 ObjectInspector
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // 对于输入的每行数据,同一个实例会多次调用此方法进行处理
    @Override
    public void process(Object[] args) throws HiveException {
        // 排除特殊情况
        if (args == null || args[0] == null) {
            forward(nullCols);
            return;
        }

        // 1.获取输入参数(获取输入的公共字段的Json字符串和Json Key)
        String commonFieldStr = ((StringObjectInspector) inputFieldIOs[0]).getPrimitiveJavaObject(args[0]);// common fields
        if (!keyParsed) {// 每个实例的Json Key都相同,重复调用Process时,Json Key不重复解析
            for (int i = 0; i < numCols; i++) {
                jsonKeys[i] = ((StringObjectInspector) inputFieldIOs[i + 1]).getPrimitiveJavaObject(args[i + 1]);
            }
            keyParsed = true;
        }

        // 2.处理输入参数
        if (!JSON.isValidObject(commonFieldStr)) {// 如果输入的公共字段字符串不满足Json对象格式,则直接输出空行并返回
            forward(nullCols);
            return;
        }
        JSONObject commonFieldJsonObj = JSON.parseObject(commonFieldStr);// 将公共字段字符串转换成Json对象

        // 3.构建并输出返回行
        for (int i = 0; i < numCols; i++) {
            String jsonValue = commonFieldJsonObj.getString(jsonKeys[i].trim());// 取出对应的JSON Value
            if (jsonValue == null) {// 如果没有对应的JSON value,则会返回null,若返回null则将其设置为空字符串
                jsonValue = "";
            }
            retCols[i].set(jsonValue);
        }
        forward(retCols);// 输出返回行
    }

    @Override
    public void close() throws HiveException {

    }

    // 返回此UDTF对应的函数名
    @Override
    public String toString() {
        return "event_log_common_handler";
    }
}
