package com.huashao.gmall.realtime.bean;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Author: huashao
 * Date: 2021/8/9
 * Desc: 用该注解标记的属性，不需要插入到ClickHouse
 */
//注解是用于属性上
@Target(FIELD)
// 注解的作用范围或生命周期是runtime运行时。
@Retention(RUNTIME)
public @interface TransientSink {
}
