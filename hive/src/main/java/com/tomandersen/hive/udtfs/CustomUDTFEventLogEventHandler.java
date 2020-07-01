package com.tomandersen.hive.udtfs;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
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
 * <h3>此UDTF主要用于处理事件日志event_log的事件部分</h3>
 * 事件日志的事件部分为JSON数组,其中会有多个JSON对象.
 * <p>
 * UDTF输入: 事件日志JSON数组字符串.
 * <p>
 * UDTF输出: 事件日志数组中的所有事件的事件名,以及事件本体的Json字符串.
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
        name = "event_log_event_handler",
        value = "_FUNC_(jsonStr) - return the event_name and envet_body in jsonStr which represents a" +
                "event log."
)
public class CustomUDTFEventLogEventHandler extends GenericUDTF {

    // 输出列个数
    private int numCols;
    // 所有输入字段对应的 ObjectInspector
    private transient ObjectInspector[] inputFieldIOs;
    // 输出列的某一行
    private transient Text[] retCols;
    // 空输出列,用于在特殊情况输出
    private transient Object[] nullCols;

    // 检查输入参数合法性,获取输入参数信息,创建并返回输出对象的 ObjectInspector
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        // 1.输入参数检查
        List<? extends StructField> inputFields = argOIs.getAllStructFieldRefs();// 获取所有输入字段
        int argsLength = inputFields.size();// 获实际接收的参数个数
        if (argsLength != 1) {// 参数个数检查
            throw new UDFArgumentException
                    ("event_log_event_handler(jsonArray) takes only one argument which represents a json array");
        }
        inputFieldIOs = new ObjectInspector[argsLength]; // 获取所有输入参数字段对应的 ObjectInspector
        for (int i = 0; i < argsLength; i++) {
            inputFieldIOs[i] = inputFields.get(i).getFieldObjectInspector();
        }

        for (int i = 0; i < argsLength; ++i) {// 参数类型检查
            if (inputFieldIOs[i].getCategory() != ObjectInspector.Category.PRIMITIVE ||
                    !inputFieldIOs[i].getTypeName().equals(serdeConstants.STRING_TYPE_NAME)) {
                throw new UDFArgumentException
                        ("event_log_event_handler()'s arguments have to be string type");
            }
        }

        // 2.成员变量赋值
        numCols = 2;// 实际输出的列个数,只输出event_name,event_body
        retCols = new Text[numCols]; // 生成返回列数组,用于保存返回的每行输出对象
        nullCols = new Object[numCols]; // 生成空返回列数组,用于在某些特殊情况输出
        for (int i = 0; i < numCols; i++) {// 填充返回列
            retCols[i] = new Text();
            nullCols[i] = null;
        }

        // 3.设置输出字段的列名,以及对应的查看器 Inspector
        ArrayList<String> fieldNames = new ArrayList<>(numCols); // 输出字段名
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>(numCols); // 输出字段对象检查器 ObjectInspector
        fieldNames.add("event_name"); // 设置输出列的列名
        fieldNames.add("envet_json"); // 设置输出列的列名
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector); // 设置输出字段的查看器
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector); // 设置输出字段的查看器
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

        // 获取输入参数,即JSON数组字符串,并将其转换成JSONArray对象
        String eventsJsonArrayStr = ((StringObjectInspector) inputFieldIOs[0]).getPrimitiveJavaObject(args[0]);
        if (!JSON.isValidArray(eventsJsonArrayStr)) {// 如果输入参数不为JSON数组,则直接输出空行
            forward(nullCols);
            return;
        }

        // 遍历事件JSON数组,获取其中的所有JSON值(即JSON对象字符串),并将其转换成JSON对象
        // 获取每个JSON对象的"en"值对应的JSON value,并输出此JSON value和对应的JSON对象字符串
        JSONArray eventsJsonArray = JSON.parseArray(eventsJsonArrayStr);
        for (Object eventObj : eventsJsonArray) {// 遍历JSON数组中的每个JSON值
            String eventStr = eventObj.toString();// event string
            if (JSON.isValidObject(eventStr)) {// 如果JSON值为JSON对象,则将其转换成JSONObject,并输出en对应的JSON值和JSON字符串
                JSONObject eventJsonObj = JSON.parseObject(eventStr);
                String eventName = eventJsonObj.getString("en");
                if (eventName == null) eventName = "";// 如果没有对应的JSON value,则会返回null,若返回null则将其设置为空字符串
                retCols[0].set(eventName);// set event_name
                retCols[1].set(eventStr);// set event_json
                forward(retCols);
            } else {// 如果JSON值不为JSON对象,是其他的JSON值(即数字,字符串,布尔值,null,JSON数组),则直接返回空行
                forward(nullCols);
            }
        }
    }

    // 查询结束时调用一次,用于释放相关资源
    @Override
    public void close() {

    }

    // 返回此UDTF对应的函数名
    @Override
    public String toString() {
        return "event_log_event_handler";
    }
}
