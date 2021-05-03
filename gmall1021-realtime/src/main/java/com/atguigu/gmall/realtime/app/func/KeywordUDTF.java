package com.atguigu.gmall.realtime.app.func;

import com.atguigu.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 自定义UDTF函数
 */
@FunctionHint(output = @DataTypeHint("ROW<word STING>"))
public class KeywordUDTF extends TableFunction<Row> {
    public void eval(String text) {
        for (Object keyword : KeywordUtil.analyze(text)) {
            // 输出
            collect(Row.of(keyword));
        }
    }
}
