package com.huashao.gmall.realtime.app.func;

import com.huashao.gmall.realtime.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.util.List;

/**
 * Author: huashao
 * Date: 2021/8/11
 * Desc:  自定义UDTF函数实现分词操作；底层调用分词的工具类
 * 对字符串分词，就是一行输入，多行输出，所以要自定义表函数，一进多出；
 *
 */

//加注解，row里的泛型是string
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
//TableFunction的泛型是<Row>
public class KeywordUDTF extends TableFunction<Row> {

    //这个方法不是重写，是约定好的同名函数
    public void eval(String value) {
        //使用工具类对字符串进行分词
        List<String> keywordList = KeywordUtil.analyze(value);
        for (String keyword : keywordList) {
            //collect(Row.of(keyword));
            //创建行row，行中只有一列，就是分好的词
            Row row = new Row(1);
            //设置一行中的列值，一行只有一个值，就是分好的词
            row.setField(0,keyword);
            //使用发射器发送数据
            collect(row);
        }
    }
}
