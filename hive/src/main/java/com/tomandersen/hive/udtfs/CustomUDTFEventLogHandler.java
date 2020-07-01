package com.tomandersen.hive.udtfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
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
 * <h3>此UDTF主要用于处理事件日志event_log,直接生成所有所需字段</h3>
 *
 * <p>
 * UDTF输入：event log 字符串,以及公共字段"cm"中需要输出的Json Value对应的Json Key
 * UDTF输出: 与输入的Json Key对应的公共字段的Json Value,server_time,event_name,event_json
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/6/16
 */
// 用于生成Function document.可以使用desc function <function_name>命令查看函数使用文档.
@Description(
        name = "event_log_handler",
        value = "_FUNC_(str,key1...) - handler the event log string, then " +
                "return the JSON value to the keys in common fields, " +
                "server_time,event_name,and event_json."
)
public class CustomUDTFEventLogHandler extends GenericUDTF {
    // 输出列个数
    private int numCols;
    // 所有输入字段对应的 ObjectInspector
    private transient ObjectInspector[] inputFieldIOs;
    // 输入参数中所有的JSON key
    private String[] jsonKeys;
    // 是否已经解析过jsonKey
    private boolean keyParsed;
    // 输出列的某一行
    private transient Text[] retCols;
    // 空输出列,用于在特殊情况输出
    private transient Object[] nullCols;

    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs)
            throws UDFArgumentException {
        // 1.输入参数个数检查
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();
        int argsLength = inputFields.size();
        if (argsLength < 1) {
            throw new UDFArgumentException
                    ("event_log_handler() takes at least one argument");
        }

        // 2.输入参数类型检查
        inputFieldIOs = new ObjectInspector[argsLength];// 获取输入字段查看器
        for (int i = 0; i < argsLength; i++) {
            inputFieldIOs[i] = inputFields.get(i).getFieldObjectInspector();
        }
        for (int i = 0; i < argsLength; ++i) {// 本UDTF的所有参数类型必须为Java String类型
            if (inputFieldIOs[i].getCategory() != ObjectInspector.Category.PRIMITIVE ||
                    !inputFieldIOs[i].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
                throw new UDFArgumentException
                        ("event_log_handler()'s arguments have to be string type");
            }
        }

        // 3.成员变量赋值
        numCols = argsLength + 2;// 计算返回列个数
        retCols = new Text[numCols];// 生成返回列数组,用于保存返回的每行输出对象
        nullCols = new Object[numCols];// 生成空返回列数组,用于返回空行
        jsonKeys = new String[argsLength - 1];// 生成jsonKey数组,用于保存输入的jsonKey
        for (int i = 0; i < numCols; i++) {// 填充返回列
            retCols[i] = new Text();
            nullCols[i] = null;
        }

        // 4.返回输出对象的查看器
        ArrayList<String> fieldNames = new ArrayList<>(numCols); // 输出字段名
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>(numCols); // 输出字段检查器 ObjectInspector
        for (int i = 0; i < numCols - 3; i++) {
            fieldNames.add("c" + i);
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }
        fieldNames.add("server_time");// 设置输出列的列名 server_time
        fieldNames.add("event_name");// 设置输出列的列名 event_name
        fieldNames.add("event_json");// 设置输出列的列名 event_json
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector); // 设置输出字段 server_time 的查看器
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector); // 设置输出字段 event_name 的查看器
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector); // 设置输出字段 event_json 的查看器
        // 将所有的输出对象视为整个结构化对象,生成并返回此结构化对象的查看器 ObjectInspector
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    // 用于接收每行的参数,对于同一张表会通过同一个实例调用多次此方法
    @Override
    public void process(Object[] args) throws HiveException {
        // 排除特殊情况
        if (args == null || args[0] == null) {
            forward(nullCols);
            return;
        }

        // 1.获取输入参数
        String eventLog = ((StringObjectInspector) inputFieldIOs[0]).getPrimitiveJavaObject(args[0]);// 获取输入的事件日志event log字符串
        if (!keyParsed) {// 获取输入的JSON key
            for (int i = 0; i < numCols - 3; i++) {
                jsonKeys[i] = ((StringObjectInspector) inputFieldIOs[i + 1]).getPrimitiveJavaObject(args[i + 1]);
            }
            keyParsed = true;
        }


        // 2.处理输入参数
        String[] strs = eventLog.split("\\|");
        if (strs.length != 2) {// 特殊情况,输出空行
            forward(nullCols);
            return;
        }
        // 获取当前事件日志的 server_time,事件日志本体
        String serverTime = strs[0];// server_time
        String eventLogStr = strs[1];// event log
        if (!JSON.isValidObject(eventLogStr)) {// 如果输入的事件日志本体不能转换成JSON对象,则直接输出空行
            forward(nullCols);
            return;
        }
        JSONObject eventLogJsonObj = JSON.parseObject(eventLogStr);// 将事件日志转换成JSON对象
        JSONObject commonFieldsJsonObj = eventLogJsonObj.getJSONObject("cm");// 获取事件日志中的公共字段JSON对象
        if (commonFieldsJsonObj == null) {// 排除特殊情况,若公共字段为空,则输出空行
            forward(nullCols);
            return;
        }
        JSONArray eventsJsonArray = eventLogJsonObj.getJSONArray("et");// 获取时间日志中的具体事件JSON数组


        // 3.构建并输出返回行
        // 遍历JSON key,取出 common fields 中对应的JSON Value,然后放入返回列数组中
        for (int i = 0; i < numCols - 3; i++) {
            String jsonValue = commonFieldsJsonObj.getString(jsonKeys[i].trim());// 取出对应的JSON Value
            if (jsonValue == null) {// 如果没有对应的JSON value,则会返回null,若返回null则将其设置为空字符串
                jsonValue = "";
            }
            retCols[i].set(jsonValue);
        }
        retCols[numCols - 3].set(serverTime);// 将server_time也加入到返回行中
        // 如果事件JSON数组为空,则最后两列 event_name 和 event_json 设置为空,输出并返回
        if (eventsJsonArray == null || eventsJsonArray.size() == 0) {
            retCols[numCols - 2].set("");// set event_name
            retCols[numCols - 1].set("");// set event_json
            forward(retCols);
            return;
        }

        // 遍历事件JSON数组,获取其中的所有JSON值(即JSON对象字符串),并将其转换成JSON对象
        // 获取每个JSON对象的"en"值对应的JSON value,并输出
        for (Object event : eventsJsonArray) {// 遍历JSON数组中的每个JSON值
            String eventStr = event.toString();// 单个事件字符串
            if (JSON.isValidObject(eventStr)) {// 如果JSON值为JSON对象,则将其转换成JSONObject,并输出返回行
                JSONObject eventJsonObj = JSON.parseObject(eventStr);
                String eventName = eventJsonObj.getString("en");// 获取当前事件名
                if (eventName == null) {// 如果没有对应的JSON value,则会返回null,若返回null则将其设置为空字符串
                    eventName = "";
                }
                retCols[numCols - 2].set(eventName);// set event_name
                retCols[numCols - 1].set(eventStr);// set event_json
                forward(retCols);
            } else {// 如果JSON值不是JSON对象,是其他的JSON值(即数字,字符串,布尔值,null,JSON数组),则最后两列设置为空字符串
                retCols[numCols - 2].set("");
                retCols[numCols - 1].set("");
                forward(retCols);
            }
        }

    }

    // 查询结束时调用一次,用于释放相关资源
    @Override
    public void close() throws HiveException {

    }

    // 返回此UDTF对应的函数名
    @Override
    public String toString() {
        return "event_log_handler";
    }

    // 用于测试
    public static void main(String[] args) {

    }
}
