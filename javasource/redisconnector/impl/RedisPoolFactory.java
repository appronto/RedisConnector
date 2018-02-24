package redisconnector.impl;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class RedisPoolFactory {
	private static JedisPool pool; 
	
	protected RedisPoolFactory(){}
	
	public static JedisPool getJedisPool(){
		if(pool == null) {
			pool = (redisconnector.proxies.constants.Constants.getRedisAuth().trim().length() > 0
					? new JedisPool(new JedisPoolConfig(), redisconnector.proxies.constants.Constants.getRedisEndpoint().trim(),
							Integer.valueOf(redisconnector.proxies.constants.Constants.getRedisPort().trim()),
							Protocol.DEFAULT_TIMEOUT, redisconnector.proxies.constants.Constants.getRedisAuth().trim())
					: new JedisPool(new JedisPoolConfig(), redisconnector.proxies.constants.Constants.getRedisEndpoint().trim(),
							Integer.valueOf(redisconnector.proxies.constants.Constants.getRedisPort().trim()),
							Protocol.DEFAULT_TIMEOUT));
		}
		return pool;
	}
}
