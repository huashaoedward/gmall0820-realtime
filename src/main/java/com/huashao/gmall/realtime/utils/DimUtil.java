package com.huashao.gmall.realtime.utils;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.java.tuple.Tuple2;
import redis.clients.jedis.Jedis;

import java.util.List;

/**
 * Author: huashao
 * Date: 2021/8/3
 * Desc: 用于维度查询的工具类  底层调用的是PhoenixUtil；
 * 因为PhoenixUtil是从Phoenix中查询数据，将查询到每一条记录封装成指定的bean对象，再存入List中；返回值是List;
 * 本工具类是再封装，将查到的结果封装成JsonObj；而且结合了Redis缓存方法提高查询效率
 *
 * select * from dim_base_trademark where id=10 and name=zs;
 */
public class DimUtil {

    /**
     * (一)从Phoenix中查询数据，没有使用缓存;
     * @param tableName  表名
     * @param cloNameAndValue  使用可变参数的Tuple来接收多个的列名和列值
     * @return  返回值是jsonObj
     */
    public static JSONObject getDimInfoNoCache(String tableName, Tuple2<String, String>... cloNameAndValue) {
        //拼接查询条件
        String whereSql = " where ";
        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> tuple2 = cloNameAndValue[i];
            //元组的第一个元素
            String filedName = tuple2.f0;
            String fieldValue = tuple2.f1;
            //当有多个条件时，用and连接
            if (i > 0) {
                whereSql += " and ";
            }
            //第一个条件，拼接where
            whereSql += filedName + "='" + fieldValue + "'";
        }

        String sql = "select * from " + tableName + whereSql;
        System.out.println("查询维度的SQL:" + sql);

        //把sql传入queryList()中，结果要转为JSONObject
        List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
        JSONObject dimJsonObj = null;
        //对于维度查询来讲，一般都是根据主键进行查询，不可能返回多条记录，只会有一条
        if (dimList != null && dimList.size() > 0) {
            //只有一条记录，所以取第一条
            dimJsonObj = dimList.get(0);
        } else {
            System.out.println("维度数据没有找到:" + sql);
            System.out.println();
        }
        return dimJsonObj;
    }

    //再做一层查询方法封装，在做维度关联的时候，大部分场景都是通过id进行关联，所以提供一个方法，只需要将id的值作为参数传进来即可
    public static JSONObject getDimInfo(String tableName, String id) {
        return getDimInfo(tableName, Tuple2.of("id", id));
    }


    /*
        (二)优化：从Phoenix中查询数据，加入了旁路缓存
             先从缓存查询，如果缓存没有查到数据，再到Phoenix查询，并将查询结果放到缓存中

        redis
            类型：    string
            Key:     dim:表名:值       例如：dim:DIM_BASE_TRADEMARK:10_xxx
            上面的值，是查询条件中属性的值。
            value：  通过PhoenixUtil到维度表中查询数据，取出第一条，格式是jsonObj,并将其转换为json字符串
            失效时间:  24*3600(1天)

        //"DIM_BASE_TRADEMARK", Tuple2.of("id", "13"),Tuple2.of("tm_name","zz"))

        redisKey= "dim:dim_base_trademark:"
        where id='13'  and tm_name='zz'


        dim:dim_base_trademark:13_zz ----->Json

        dim:dim_base_trademark:13_zz
    */
    public static JSONObject getDimInfo(String tableName, Tuple2<String, String>... cloNameAndValue) {
        //拼接查询条件key
        String whereSql = " where ";

        //拼接redisKey和where条件
        //注意！！！key的格式，dim:表名:值，这里只拼接查询条件的属性的值，不拼接属性名
        String redisKey = "dim:" + tableName.toLowerCase() + ":";
        for (int i = 0; i < cloNameAndValue.length; i++) {
            Tuple2<String, String> tuple2 = cloNameAndValue[i];
            String filedName = tuple2.f0;
            String fieldValue = tuple2.f1;
            //当有多个条件时，用and连接
            if (i > 0) {
                whereSql += " and ";
                //如果传入的是多个tuple，也就是查询条件中有多个值，要用_拼接，
                redisKey += "_";
            }
            whereSql += filedName + "='" + fieldValue + "'";
            redisKey += fieldValue;
        }

        //从Redis中获取数据
        Jedis jedis = null;
        //维度数据查询结果的json字符串形式
        String dimJsonStr = null;
        //维度数据查询结果的json对象形式
        JSONObject dimJsonObj = null;
        try {
            //调用工具类，获取jedis客户端
            jedis = RedisUtil.getJedis();
            //根据key到Redis中查询，结果返回是string
            dimJsonStr = jedis.get(redisKey);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("从redis中查询维度失败");
        }

        //判断是否从Redis中查询到了数据
        if (dimJsonStr != null && dimJsonStr.length() > 0) {
            //把查询到的结果String，转为JsonObj
            dimJsonObj = JSON.parseObject(dimJsonStr);
        } else {
            //如果在Redis中没有查到数据，需要到Phoenix中查询
            String sql = "select * from " + tableName + whereSql;
            System.out.println("查询维度的SQL:" + sql);
            List<JSONObject> dimList = PhoenixUtil.queryList(sql, JSONObject.class);
            //对于维度查询来讲，一般都是根据主键进行查询，不可能返回多条记录，只会有一条
            if (dimList != null && dimList.size() > 0) {
                //只有一条记录，所以取第一条
                dimJsonObj = dimList.get(0);
                //将查询出来的数据放到Redis中缓存起来
                if (jedis != null) {
                    //setex(key,有效时间1天，value)，value要转成String格式
                    jedis.setex(redisKey, 3600 * 24, dimJsonObj.toJSONString());
                }
            } else {
                System.out.println("维度数据没有找到:" + sql);
            }
        }

        //关闭Jedis
        if (jedis != null) {
            jedis.close();
        }

        return dimJsonObj;
    }

    /*根据key让Redis中的缓存失效; 主要是在DimSink中如果操作类型是update，就调用这个方法。
    * 需要传入的是表名和id
    * */
    public static void deleteCached(String tableName, String id) {
        //key的格式，dim:表名:值，这里只拼接查询条件的属性的值，不拼接属性名
        String key = "dim:" + tableName.toLowerCase() + ":" + id;
        try {
            Jedis jedis = RedisUtil.getJedis();
            // 通过key清除缓存
            jedis.del(key);
            jedis.close();
        } catch (Exception e) {
            System.out.println("缓存异常！");
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {
        //System.out.println(PhoenixUtil.queryList("select * from DIM_BASE_TRADEMARK", JSONObject.class));
        //JSONObject dimInfo = DimUtil.getDimInfoNoCache("DIM_BASE_TRADEMARK", Tuple2.of("id", "14"));

        JSONObject dimInfo = DimUtil.getDimInfo("DIM_BASE_TRADEMARK", "14");

        System.out.println(dimInfo);
    }
}
