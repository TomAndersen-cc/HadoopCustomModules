package com.tomandersen.hive.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * <h3>Hive自定义函数UDF的实现Demo</h3>
 * 功能: 将输入的字符串转换成小写形式并返回.
 *
 * <p>
 * (此处为Class详细描述).
 *
 * @author TomAndersen
 * @version 1.0
 * @date 2020/5/20
 */
public class CustomUDFLower extends UDF {

    // 将输入的单个字符串转换成小写形式返回
    public Text evaluate(final Text s) {
        if (s == null) return null;
        return new Text(s.toString().toLowerCase());
    }

    // 将输入的多个字符串转换成小写形式,并组成单个字符串返回
    public Text evaluate(final Text... s) {
        StringBuilder sb = new StringBuilder();
        for (Text text : s) {
            if (text != null) sb.append(text.toString());
        }
        return new Text(sb.toString().toLowerCase());
    }
}
