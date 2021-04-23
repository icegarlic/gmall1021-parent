package com.atguigu.gmall.realtime.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {
    private static JedisPool jedisPool;

    public static Jedis getJedis(){
        if(jedisPool == null){
            JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
            jedisPoolConfig.setMinIdle(5);
            jedisPoolConfig.setMaxIdle(5);
            jedisPoolConfig.setMaxWaitMillis(5000);
            jedisPoolConfig.setBlockWhenExhausted(true);
            jedisPoolConfig.setTestOnBorrow(true);
            jedisPoolConfig.setMaxTotal(20);
            jedisPool = new JedisPool(jedisPoolConfig,"hadoop102",6379,10000);
        }
        System.out.println("开辟连接池");
        return jedisPool.getResource();
    }

    public static void main(String[] args) {
        Jedis jedis = getJedis();
        String msg = jedis.ping();
        System.out.println(msg);
    }
}
