package com.memdb.redis;

import redis.clients.jedis.Jedis;

public class connectToredis {
	public static void main(String[] args) {
		
		Jedis jedis = new Jedis("192.168.99.103",6379);
//		jedis.auth("foobared");
		System.out.println(jedis.hget("calldesc", "acd"));
		jedis.close();

	}
}
