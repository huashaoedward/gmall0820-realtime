package com.huashao.gmall.realtime.utils;

import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Author: huashao
 * Date: 2021/8/5
 * Desc:  创建单例的线程池对象的工具类（双重检测）
 */
public class ThreadPoolUtil {
    private static ThreadPoolExecutor pool;

    /**
     *      单例对象模式，获取线程
     *     corePoolSize:指定了线程池中的线程数量，它的数量决定了添加的任务是开辟新的线程去执行，还是放到workQueue任务队列中去；
     *     maximumPoolSize:指定了线程池中的最大线程数量，这个参数会根据你使用的workQueue任务队列的类型，决定线程池会开辟的最大线程数量；
     *     keepAliveTime:当线程池中空闲线程数量超过corePoolSize时，多余的线程会在多长时间内被销毁；
     *     unit:keepAliveTime的单位
     *     workQueue:任务队列，被添加到线程池中，但尚未被执行的任务
     * @return
     */
    public static ThreadPoolExecutor getInstance(){
        //单例模式的双重校验，两次判空
        if(pool == null){
            //同步，对本类加锁；
            //synchronized 锁的对象是方法的调用者！静态方法可以直接使用，就是基于类；而普通方法通过对象来调用；
            synchronized (ThreadPoolUtil.class){
                if(pool == null){

                    //ThreadPoolExecutor的4个构造参数：初始线程数量，最多的线程数量容量，有郊时间，时间单位，任务队列
                    /*LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)，
                    队列存放的是Runnable线程，队列长度是Integer.MAX_VALUE*/
                    pool = new ThreadPoolExecutor(
                        4,20,300, TimeUnit.SECONDS,new LinkedBlockingDeque<Runnable>(Integer.MAX_VALUE)
                    );
                }
            }
        }
        return pool;
    }
}
