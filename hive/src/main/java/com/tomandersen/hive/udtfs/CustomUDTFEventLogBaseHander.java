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
 * <h3>此UDTF主要用于处理事件日志event_log,分离开头的时间戳</h3>
 * 事件日志输入时带有时间戳前缀,与日志本体JSON字符串之间使用"|"分割.
 *
 * <p>
 * UDTF输入: 事件日志字符串,事件日志本体中Json Key.
 * <p>
 * UDTF输出: 事件日志时间戳,事件日志本体中与输入Json Key对应的Json Value.
 *
 * <p>
 * 参考:
 * {@link GenericUDTFJSONTuple},{@link GenericUDTFExplode},{@link GenericUDTFPosExplode},
 * {@link UDFJson},{@link CustomUDTFSplitAndExplode}
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/6/16
 */
// 用于生成Function document.可以使用desc function <function_name>命令查看函数使用文档.
@Description(
        name = "event_log_base_handler",
        value = "_FUNC_(str,key1,...) - return the json value " +
                "corresponding to the json keys in event log body and the server_time."
)
public class CustomUDTFEventLogBaseHander extends GenericUDTF {
    // 输出列个数
    private int numCols;
    // 所有输入字段对应的 ObjectInspector
    private transient ObjectInspector[] inputFieldIOs;
    // 输入参数中所有的JSON key
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
                    ("event_log_base_handler(event_log,key1...) takes two argument at least");
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
                        ("event_log_base_handler()'s arguments have to be string type");
            }
        }

        // 3.成员变量赋值
        numCols = argsLength;// 计算实际输出的列数(返回列个数与输入参数个数相同)
        retCols = new Text[numCols];// 生成返回列数组,用于保存返回的每行输出对象
        jsonKeys = new String[numCols - 1];// 生成jsonKey数组,保存输入的Json Key,等于返回列个数-1,因为最后一列固定为server_time
        nullCols = new Object[numCols];// 生成空返回列数组,用于在某些特殊情况输出
        for (int i = 0; i < numCols; i++) {// 填充返回列
            retCols[i] = new Text();
            nullCols[i] = null;
        }

        // 4.设置输出字段名,以及对应的ObjectInspector,创建并返回输出对象的ObjectInspector
        ArrayList<String> fieldNames = new ArrayList<>(numCols); // 输出字段名
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<>(numCols); // 输出字段检查器 ObjectInspector
        for (int i = 0; i < numCols - 1; i++) {
            fieldNames.add("c" + i);
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }
        fieldNames.add("server_time"); // 设置输出列的列名server_time
        fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector); // 设置输出字段server_time的查看器
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
        String eventLog = ((StringObjectInspector) inputFieldIOs[0]).getPrimitiveJavaObject(args[0]);// event log 字符串
        if (!keyParsed) {// Json Key
            for (int i = 0; i < numCols - 1; i++) {
                jsonKeys[i] = ((StringObjectInspector) inputFieldIOs[i + 1]).getPrimitiveJavaObject(args[i + 1]);
            }
            keyParsed = true;
        }

        // 2.处理输入参数
        String[] strs = eventLog.split("\\|");// 分割事件日志的时间戳和日志本体
        if (strs.length != 2) {// 排除特殊情况,输出空行
            forward(nullCols);
            return;
        }
        String serverTime = strs[0];// 时间戳
        String eventLogJsonStr = strs[1];// 事件日志本体
        if (!JSON.isValidObject(eventLogJsonStr)) {// 如果事件日志本体不为JSON对象,则直接输出空行
            forward(nullCols);
            return;
        }
        JSONObject eventLogJsonObj = JSON.parseObject(eventLogJsonStr);// 将事件日志转换成JSON对象

        // 3.构建并输出返回行
        // 遍历输入的Json Key,取出event log中对应的JSON Value,然后放入返回列数组中
        for (int i = 0; i < numCols - 1; i++) {
            String jsonValue = eventLogJsonObj.getString(jsonKeys[i].trim());// 取出对应的JSON Value
            if (jsonValue == null) {// 如果没有对应的JSON value,则会返回null,若返回null则将其设置为空字符串
                jsonValue = "";
            }
            retCols[i].set(jsonValue);
        }
        retCols[numCols - 1].set(serverTime);// 将server_time也加入到返回队列中进行返回
        forward(retCols);// 输出返回行
    }

    // 查询结束时调用一次,用于释放相关资源
    @Override
    public void close() {

    }

    // 返回此UDTF对应的函数名
    @Override
    public String toString() {
        return "event_log_base_handler";
    }

    // 测试
    public static void main(String[] args) {
    }
}
