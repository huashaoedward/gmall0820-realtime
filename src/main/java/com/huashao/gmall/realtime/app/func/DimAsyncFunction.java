package com.huashao.gmall.realtime.app.func;

import com.alibaba.fastjson.JSONObject;
import com.huashao.gmall.realtime.bean.OrderWide;
import com.huashao.gmall.realtime.utils.DimUtil;
import com.huashao.gmall.realtime.utils.ThreadPoolUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.hadoop.hbase.TableName;

import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * 泛型是IN, OUT,输入和输出是一样的
 * Author: huashao
 * Date: 2021/8/5
 * Desc:  自定义维度异步查询的函数
 *      模板方法设计模式
 *      在父类中只定义方法的声明，让整个流程跑通
 *      具体的实现延迟到子类中实现
 */
//这里是抽象类，是因为类里有抽象方法; 还要实现 维度关联接口DimJoinFunction
public abstract class DimAsyncFunction<T> extends RichAsyncFunction<T,T> implements DimJoinFunction<T>{

    //线程池对象的 父接口声明（多态）
    private ExecutorService executorService;

    //要关联查询的维度的表名
    private String tableName;

    //有参构造，参数为要关联查询的维度的表名
    public DimAsyncFunction(String tableName) {
        this.tableName = tableName;
    }

    //在open里初始化
    @Override
    public void open(Configuration parameters) throws Exception {
        //调用工具类，初始化线程池对象
        System.out.println("初始化线程池对象");
        executorService = ThreadPoolUtil.getInstance();
    }

    /**
     * 发送异步请求的方法
     * @param obj     流中的事实数据
     * @param resultFuture      异步处理结束之后，返回结果
     * @throws Exception
     */
    @Override
    public void asyncInvoke(T obj, ResultFuture<T> resultFuture) throws Exception {
        //通过线程池对象的submit方法（类似Thread类的start方法），创建新线程，执行里面的方法：一是查询，二是关联
        executorService.submit(
                //多线程实现
            new Runnable() {
                @Override
                public void run() {
                    try {
                        //发送异步请求
                        long start = System.currentTimeMillis();
                        //从流中事实数据获取key
                        String key = getKey(obj);

                        //根据维度的主键到维度表中进行查询
                        JSONObject dimInfoJsonObj = DimUtil.getDimInfo(tableName, key);
                        //System.out.println("维度数据Json格式：" + dimInfoJsonObj);

                        if(dimInfoJsonObj != null){
                            //重写父接口的方法Join,维度关联  流中的事实数据和查询出来的维度数据进行关联，封装入obj
                            join(obj,dimInfoJsonObj);
                        }
                        //System.out.println("维度关联后的对象:" + obj);
                        long end = System.currentTimeMillis();
                        System.out.println("异步维度查询耗时" +(end -start)+"毫秒");

                        //将关联后的数据数据obj继续向下传递，关联到底层的函数的结果，最终被发射出来
                        resultFuture.complete(Arrays.asList(obj));

                    } catch (Exception e) {
                        e.printStackTrace();
                        throw new RuntimeException(tableName + "维度异步查询失败");
                    }
                }
            }
        );
    }
}
