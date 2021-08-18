package com.huashao.gmall.realtime.utils;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * Author: huashao
 * Date: 2021/8/11
 * Desc: IK分词器分词工具类
 */
public class KeywordUtil {
    //分词    将字符串进行分词，将分词之后的结果放到一个List集合中返回，传入的参数是要分词的string
    public static List<String> analyze(String text){
        List<String> wordList = new ArrayList<>();
        //将要分词的字符串转换为字符输入流
        StringReader sr = new StringReader(text);
        //创建分词器对象，第二个参数是智能分词
        IKSegmenter ikSegmenter = new IKSegmenter(sr, true);
        // Lexeme  是分词后的一个单词对象
        Lexeme lexeme = null;
        //通过循环，获取分词后的数据
        while(true){
            try {
                //获取一个单词
                if((lexeme = ikSegmenter.next())!=null){
                    String word = lexeme.getLexemeText();
                    //将分好的每个词，存放入List
                    wordList.add(word);
                }else{
                    break;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return wordList;
    }

    public static void main(String[] args) {
        String text = "尚硅谷大数据数仓";
        System.out.println(KeywordUtil.analyze(text));
    }
}
