package redisconnector.impl;

import java.math.BigDecimal;
import java.math.MathContext;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import com.mendix.core.Core;
import com.mendix.core.objectmanagement.member.*;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.IMendixObjectMember;
import com.mendix.systemwideinterfaces.core.ISession;

import redisconnector.proxies.*;
import redis.clients.jedis.GeoCoordinate;
import redis.clients.jedis.GeoRadiusResponse;
import redis.clients.jedis.GeoUnit;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Tuple;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.params.geo.GeoRadiusParam;

public class RedisConnector 
{ 
	private Jedis redis = null;
	private ILogNode _logNode = Core.getLogger("Redis");
	
	private static JedisPool pool =  (redisconnector.proxies.constants.Constants.getRedisAuth().trim().length() > 0 
			? new JedisPool(new JedisPoolConfig(), redisconnector.proxies.constants.Constants.getRedisEndpoint().trim()
					,Integer.valueOf(redisconnector.proxies.constants.Constants.getRedisPort().trim())
					, Protocol.DEFAULT_TIMEOUT,redisconnector.proxies.constants.Constants.getRedisAuth().trim()) 
						: new JedisPool(new JedisPoolConfig(), redisconnector.proxies.constants.Constants.getRedisEndpoint().trim()
							,Integer.valueOf(redisconnector.proxies.constants.Constants.getRedisPort().trim())
							, Protocol.DEFAULT_TIMEOUT)) ;
	
	private Jedis subscriberRedis = null;
	private static MendixRedisPubSub pubSub = new MendixRedisPubSub();
	
	public RedisConnector() {		
		try {
			Long.valueOf( redisconnector.proxies.constants.Constants.getRedisDatabaseIndex().trim());
		}
		catch (NumberFormatException e){
			_logNode.error("Please configure constant RedisDatabaseIndex with value 0 or higher, current value: "+ redisconnector.proxies.constants.Constants.getRedisDatabaseIndex().trim(), e); 
			throw new IllegalArgumentException("Please configure constant RedisDatabaseIndex with value 0 or higher, current value: "+ redisconnector.proxies.constants.Constants.getRedisDatabaseIndex().trim());
		}
	}
	
	public void destroy(){
		pool.destroy();
	}
	
	//https://redis.io/commands/expire
	public long expire(String Key, int Seconds){
		try {
			redis = pool.getResource();
			setDatabase();
			_logNode.debug("expire " + Key + " Seconds " + Seconds); 
			return redis.expire(Key, Seconds); 
		} 
		catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        }
	        throw e;
	        
	    }
		finally {
		  if (redis != null){
			  redis.close();
		  }
		}
	}
	
	private void setDatabase() {
		if ( redis.getDB() != Long.valueOf( redisconnector.proxies.constants.Constants.getRedisDatabaseIndex().trim() ) )
			redis.select(Integer.valueOf( redisconnector.proxies.constants.Constants.getRedisDatabaseIndex().trim() ) );
	}
	
	private GeoUnit GetGeoUnitByEnum(redisconnector.proxies.Enum_GeoUnit Unit)
	{
		switch (Unit) {
	        case FT: return GeoUnit.FT;
	        case MI: return GeoUnit.MI;
	        case KM: return GeoUnit.KM;
	        default: return GeoUnit.M;
		}
	}
		
	//https://redis.io/commands/georadius
		public java.util.List<IMendixObject> georadius(IContext context,String Key, double Latitude, double Longitude, double Radius, redisconnector.proxies.Enum_GeoUnit Unit, int Max) {
			try {
				redis = pool.getResource();
				setDatabase();
				_logNode.debug("georadius " + Key +  "," + Longitude +"," +  "," + Latitude +"," + Radius +","  + Unit); 
				List<GeoRadiusResponse> results;
				GeoRadiusParam param = GeoRadiusParam.geoRadiusParam().sortAscending().withCoord().withDist();
				
				if (Max > 0 ){ param.count(Max); } 
				
				results = redis.georadius(Key, Longitude, Latitude, Radius, GetGeoUnitByEnum(Unit), param); 

				ArrayList<IMendixObject> resultList = new ArrayList<IMendixObject>();
			   
				for( GeoRadiusResponse object : results ) 
				{
					GeoPosition row = new GeoPosition(context);
					row.setName( object.getMemberByString());	
					row.setLatitude( new BigDecimal( object.getCoordinate().getLatitude() , MathContext.DECIMAL64) );
					row.setLongitude( new BigDecimal( object.getCoordinate().getLongitude() , MathContext.DECIMAL64) );
					row.setDistance( new BigDecimal( object.getDistance() , MathContext.DECIMAL64) );
					resultList.add( row.getMendixObject() );
				 }
				 
				 return resultList;
			} 
			catch (JedisConnectionException e)
		    {
		        if (redis != null)
		        {
		        	redis.close();
		        }
		        throw e;
		    }
			finally {
			  if (redis != null){
				  redis.close();
			  }
			}
		}

		
	//https://redis.io/commands/publish
	public void publish(String Channel, String Message) {
		try {
			redis = pool.getResource();
			setDatabase();
			_logNode.debug("publish " + Channel + " message " + Message); 
			redis.publish(Channel, Message); 
		} 
		catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        	_logNode.debug("publish " + Channel + " message " + Message); 
	        }
	        throw e;
	        
	    }
		finally {
		  if (redis != null){
			  redis.close();
		  }
		}
	}

	//https://redis.io/commands/psubscribe
	public void subscribe(final String Channel) {
		try {
			_logNode.debug("psubscribe " + Channel); 
			
			new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						subscriberRedis = pool.getResource();
						subscriberRedis.select( Integer.valueOf( redisconnector.proxies.constants.Constants.getRedisDatabaseIndex().trim() ) );
						subscriberRedis.psubscribe(pubSub, Channel); 
						subscriberRedis.close();
					} catch (Exception e) {
						_logNode.error("psubscribe " + Channel,e); 
					}
				}
			}, "subscriberThread").start();
		} 
		catch (JedisConnectionException e)
	    {
	        if (subscriberRedis != null)
	        {
	        	subscriberRedis.close();
	        }
	        throw e;
	    }
	}
	
	//https://redis.io/commands/punsubscribe
	public void unsubscribe(String Channel) {
		try {
			_logNode.debug("punsubscribe " + Channel); 
			if (pubSub != null)
			{
				pubSub.punsubscribe(Channel);
			}
		} 
		catch (JedisConnectionException e)
	    {
	        throw e;
	    }
	}
	
	
	//https://redis.io/commands/zrange
	public java.util.List<IMendixObject> zrange(IContext context,String Key, long Start, long Stop, redisconnector.proxies.Enum_Sort Sort) {
		try {
			redis = pool.getResource();
			setDatabase();
			_logNode.debug("zrange " + Key +  "," + Start +"," + Stop); 
			Set<String> results;
			if (Sort != null && Sort == Enum_Sort.Ascending)
			{
				results = redis.zrevrange(Key, Start, Stop);
			}
			else
			{
				results = redis.zrange(Key, Start, Stop); 
			}
			ArrayList<IMendixObject> resultList = new ArrayList<IMendixObject>();
		      
			int count = 1;
			for(Object object : results) 
			{
				 ResultRow row = new ResultRow(context);
				 row.setKey(String.valueOf(count));
				 row.setValue((String) object);
				 resultList.add(row.getMendixObject());
				 count++;
			 }
			 
			 return resultList;
		} 
		catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        }
	        throw e;
	    }
		finally {
		  if (redis != null){
			  redis.close();
		  }
		}
	}
	
	//https://redis.io/commands/zrange
	public java.util.List<IMendixObject> zrangeWithScore(IContext context,String Key, long Start, long Stop, redisconnector.proxies.Enum_Sort Sort) {
		try {
			redis = pool.getResource();
			setDatabase();
			 _logNode.debug("zrange " + Key +  "," + Start +"," + Stop); 
			 Set<Tuple> results;
			 if (Sort != null && Sort == Enum_Sort.Ascending)
			 {
				 results = redis.zrangeWithScores(Key, Start, Stop);
			 }
			 else
			 {
				 results = redis.zrevrangeWithScores(Key, Start, Stop);
			 }
			 
			 ArrayList<IMendixObject> resultList = new ArrayList<IMendixObject>();
		      
			 int count = 1;
			 for(Tuple object : results) 
			 {
				 ResultRow row = new ResultRow(context);
				 row.setKey( String.valueOf(count) );
				 row.setValue( object.getElement() ); 
				 row.setScore( new BigDecimal(object.getScore(), MathContext.DECIMAL64) );
				 resultList.add( row.getMendixObject() );
				 count++;
			 }
			 
			 return resultList;
		} 
		catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        }
	        throw e;
	    }
		finally {
		  if (redis != null){
			  redis.close();
		  }
		}
	}
	
	//http://redis.io/commands/zadd
	public long zadd(String Key, double Score, String Member) 
	{
		try
	    {
	        redis = pool.getResource();
	        setDatabase();
	        _logNode.debug("zadd " + Key +  " + member " + Member+  " + score " + Score); 
			 return redis.zadd(Key,Score,Member); 
	    }
	    catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        }
	        throw e;
	    }
	    finally
	    {
	        if (redis != null)
	        {
	        	 redis.close();
	        }
	    }
		
	}
	
	//http://redis.io/commands/zadd
	public long zrank(String Key, String Member, redisconnector.proxies.Enum_Sort Sort) 
	{
		try {
			redis = pool.getResource();
			setDatabase();
			 _logNode.debug("zrank " + Key +  ", Member " + Member +", Sort:" + Sort); 

			 if (Sort != null && Sort == Enum_Sort.Ascending)
			 {
				 return redis.zrank(Key, Member);
			 }
			 else
			 {
				 return redis.zrevrank(Key, Member);
			 }
		} 
		catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        }
	        throw e;
	    }
		finally {
		  if (redis != null){
			  redis.close();
		  }
		}
		
	}
	
	//https://redis.io/commands/llen
	public long llen(String Key) 
	{
		try
	    {
	        redis = pool.getResource();
	        setDatabase();
	        _logNode.debug("LLEN " + Key); 
			 return redis.llen(Key); 
	    }
	    catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        }
	        throw e;
	    }
	    finally
	    {
	        if (redis != null)
	        {
	        	 redis.close();
	        }
	    }
		
	}

	
	//https://redis.io/commands/geohash
	public String geohash(String Key, String Member) {
		try {
			 _logNode.debug("DEL " + Key + " Member:" + Member); 
			 redis = pool.getResource();
		     setDatabase();
			 List<String> geohashes = redis.geohash(Key, Member);
			 if (geohashes.size() == 1)
			 {
				 return geohashes.get(0);
			 }
			 else
			 {
				 return null;
			 }
		} 
		catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        }
	        throw e;
	    }
		finally {
		  if (redis != null){
			  redis.close();
		  }
		}
	}
	
	//https://redis.io/commands/zcard
	public long zcard(String Key) {
		try {
			 _logNode.debug("zcard " + Key ); 
			 redis = pool.getResource();
		     setDatabase();
			 return redis.zcard(Key); 
		} 
		catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        }
	        throw e;
	    }
		finally {
		  if (redis != null){
			  redis.close();
		  }
		}
	}
	
	//https://redis.io/commands/zrem
	public long zrem(String Key, String Member) {
		try {
			 _logNode.debug("DEL " + Key + " Member:" + Member); 
			 redis = pool.getResource();
		     setDatabase();
			 return redis.zrem(Key, Member); 
		} 
		catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        }
	        throw e;
	    }
		finally {
		  if (redis != null){
			  redis.close();
		  }
		}
	}
	
	
	//http://redis.io/commands/geopos
	public IMendixObject geopos(IContext context, String Key, String Member)  throws Exception {
		try {
			_logNode.debug("GEOPOS " + Key +  " Member: "+ Member); 
			redis = pool.getResource();
	        setDatabase();
			
				 List<GeoCoordinate> result = redis.geopos(Key, Member); 

				 if (result.size() > 0)
				 {
					 GeoPosition geoPosition = new GeoPosition(context);
					 geoPosition.setLatitude(new BigDecimal(result.get(0).getLatitude(), MathContext.DECIMAL64) );
					 geoPosition.setLongitude(new BigDecimal(result.get(0).getLongitude(), MathContext.DECIMAL64) );      
					 return geoPosition.getMendixObject();
				 }
				 else return null;
			} 
			catch (JedisConnectionException e)
		    {
		        if (redis != null)
		        {
		        	redis.close();
		        }
		        throw e;
		    }
			finally {
			  if (redis != null){
				  redis.close();
			  }
			}
	}

	
	//http://redis.io/commands/geoadd
	public long geoadd(String Key, double Latitude, double Longitude, String Member) 
	{	
		try
	    {
	        redis = pool.getResource();
	        setDatabase();
	        _logNode.debug("GEOADD " + Key +  " + " + Latitude +  " + " + Longitude +  " + " + Member);
	       	        
			return redis.geoadd(Key, Longitude, Latitude, Member); 
	    }
	    catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        }
	        throw e;
	    }
	    finally
	    {
	        if (redis != null)
	        {
	        	 redis.close();
	        }
	    }
		
	}
	
	
	//http://redis.io/commands/lpush
	public long lpush(String Key, String Value) 
	{
		try
	    {
	        redis = pool.getResource();
	        setDatabase();
	        _logNode.debug("LPUSH " + Key +  " + " + Value); 
			 return redis.lpush(Key,Value); 
	    }
	    catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        }
	        throw e;
	    }
	    finally
	    {
	        if (redis != null)
	        {
	        	 redis.close();
	        }
	    }
		
	}
	
	//see http://redis.io/commands/rpush
	public long rpush(String Key, String Value) {
		try {
			 _logNode.debug("RPUSH " + Key +  " + " + Value); 
			 redis = pool.getResource();
		        setDatabase();
			 return redis.rpush(Key,Value); 
		} 
		catch (JedisConnectionException e)
	    {
	        if (redis != null)
	        {
	        	redis.close();
	        }
	        throw e;
	    }
		finally {
		  if (redis != null){
			  redis.close();
		  }
		}
	}
	
	//see http://redis.io/commands/lrange
		public java.util.List<IMendixObject> lrange(IContext context,String Key, long Start, long Stop) {
			try {
				redis = pool.getResource();
		        setDatabase();
				 _logNode.debug("LRANGE " + Key +  "," + Start +"," + Stop); 
				 List<String> results = redis.lrange(Key, Start, Stop); 
				
				 ArrayList<IMendixObject> resultList = new ArrayList<IMendixObject>();
			      
				 for (int i=0; i < results.size(); i++){
					 ResultRow row = new ResultRow(context);
			        	row.setKey(String.valueOf(i));
			        	row.setValue(results.get(i));
			        	resultList.add(row.getMendixObject());         
				 }
				 return resultList;
			} 
			catch (JedisConnectionException e)
		    {
		        if (redis != null)
		        {
		        	redis.close();
		        }
		        throw e;
		    }
			finally {
			  if (redis != null){
				  redis.close();
			  }
			}
		}
		

		//see http://redis.io/commands/hset
		public long hset(String Key,  String Field,  String Value) {
			try {
				 _logNode.debug("HSET " + Key +  " + " + Field + " + " + Value); 
				 redis = pool.getResource();
			     setDatabase();
				 return redis.hset(Key,Field,Value); 
			} 
			catch (JedisConnectionException e)
		    {
		        if (redis != null)
		        {
		        	redis.close();
		        }
		        throw e;
		    }
			finally {
			  if (redis != null){
				  redis.close();
			  }
			}
		}
		
		//see http://redis.io/commands/hmset
		public String hmset(IContext context, String Key, IMendixObject HashMapObject) 
		{
			try 
			{
				_logNode.debug("HMSET " + Key); 
				redis = pool.getResource();
		        setDatabase();
				Map<String, String> hashMap = new HashMap<String, String>();			    
				Map<String, ? extends IMendixObjectMember<?>> members = HashMapObject.getMembers(context);
				
				for( String key : members.keySet() ) 
				{ 
					IMendixObjectMember<?> m = members.get(key);
					if (m.isVirtual() || m instanceof MendixAutoNumber || HashMapObject.getValue(context, key) == null)
						continue;
					if (m instanceof MendixDateTime){
						hashMap.put(key, processDate(context, (Date) HashMapObject.getValue(context, key),((MendixDateTime) m).shouldLocalize()));
						continue;
					}
					if (m instanceof MendixInteger){
						hashMap.put(key, Integer.toString((int) (HashMapObject.getValue(context, key))));
						continue;
					}
					if (m instanceof MendixDecimal){
						hashMap.put(key, ((BigDecimal) (HashMapObject.getValue(context, key))).toString());
						continue;
					}
					if (m instanceof MendixEnum){
						hashMap.put(key, HashMapObject.getValue(context, key).toString());
						continue;
					}
					if (m instanceof MendixBoolean){
						hashMap.put(key, Boolean.toString((boolean) (HashMapObject.getValue(context, key))));
						continue;
					}
					if (m instanceof MendixLong){
						hashMap.put(key, Long.toString((long) (HashMapObject.getValue(context, key))));
						continue;
					}
					if (((!(m instanceof MendixObjectReference) && !(m instanceof MendixObjectReferenceSet)))){
					 	hashMap.put(key, HashMapObject.getValue(context, key).toString());	
					}
				}
			 	return redis.hmset(Key,hashMap); 
			}
			catch (JedisConnectionException e)
		    {
		        if (redis != null){
		        	redis.close();
		        }
		        throw e;
		    }
			finally {
			  if (redis != null){
				  redis.close();
			  }
			}
		}
		
		//see http://redis.io/commands/del
		public long del(String Key) {
			try {
				 _logNode.debug("DEL " + Key); 
				 redis = pool.getResource();
			     setDatabase();
				 return redis.del(Key); 
			} 
			catch (JedisConnectionException e)
		    {
		        if (redis != null)
		        {
		        	redis.close();
		        }
		        throw e;
		    }
			finally {
			  if (redis != null){
				  redis.close();
			  }
			}
		}
		
		//see http://redis.io/commands/hmget
		public IMendixObject hmget(IContext context, String Key, IMendixObject ObjectToReturn) throws Exception {
			try {
				_logNode.debug("HMGET " + Key +  " + object: "  + ObjectToReturn.toString()); 
				redis = pool.getResource();
		        setDatabase();
				
				Map<String, ? extends IMendixObjectMember<?>> members;				    
				members = ObjectToReturn.getMembers(context);
				String[] fields = new String[members.size()];
				int i = 0;
				for(String key : members.keySet()) { 
					fields[i]= key;
					i++;
				}
				
				 List<String> resultList;			 
				 resultList = redis.hmget(Key, fields); 
					
				 i = 0;
				 
				 for (String result : resultList)
				 {
						IMendixObjectMember<?> m = members.get(fields[i]);
						if (m.isVirtual() || m instanceof MendixAutoNumber ||result == null){
							i++; continue;
						}
						if (m instanceof MendixDateTime){
							ObjectToReturn.setValue(context, fields[i], processDateString(context, result, ((MendixDateTime) m).shouldLocalize()) );
							i++;
							continue;
						}
						if (m instanceof MendixInteger){
							ObjectToReturn.setValue(context, fields[i], Integer.valueOf(result));
							i++;
							continue;
						}
						if (m instanceof MendixDecimal){
							ObjectToReturn.setValue(context, fields[i], new BigDecimal(result , MathContext.DECIMAL64) );
							i++;
							continue;
						}
						if (m instanceof MendixBoolean){
							ObjectToReturn.setValue(context, fields[i], Boolean.valueOf(result));
							i++;
							continue;
						}
						if (m instanceof MendixLong){
							ObjectToReturn.setValue(context, fields[i], Long.valueOf(result));
							i++;
							continue;
						}
						if (((!(m instanceof MendixObjectReference) && !(m instanceof MendixObjectReferenceSet)))){
							ObjectToReturn.setValue(context, fields[i], result);
							i++;
						}	
						
				 }
				 return ObjectToReturn;
						  
			} 
			catch (JedisConnectionException e)
		    {
		        if (redis != null)
		        {
		        	redis.close();
		        }
		        throw e;
		    }
			finally {
			  if (redis != null){
				  redis.close();
			  }
			}
		}

		
		 private static Date processDateString(IContext context, String value, boolean shouldLocalize ) {
				Locale userLocale = Core.getLocale(context);
				
				DateFormat dateFormat = DateFormat.getDateTimeInstance(
                        DateFormat.SHORT, 
                        DateFormat.DEFAULT, 
                        userLocale);

				if(shouldLocalize){
					dateFormat.setTimeZone(getSessionTimeZone(context));
				}
				else
					dateFormat.setTimeZone(getUTCTimeZone());
				
				try {
					return dateFormat.parse(value);
				} 
				catch (ParseException e) {
					e.printStackTrace();
				}
				return null;
		}
		 
		 private static String processDate(IContext context, Date value, boolean shouldLocalize ) {
				Locale userLocale = Core.getLocale(context);
				
				DateFormat dateFormat = DateFormat.getDateTimeInstance(
                     DateFormat.SHORT, 
                     DateFormat.DEFAULT, 
                     userLocale);

				if(shouldLocalize){
					dateFormat.setTimeZone(getSessionTimeZone(context));
				}
				else
					dateFormat.setTimeZone(getUTCTimeZone());
				
				return dateFormat.format(value);

		}
		 
	    public static TimeZone getSessionTimeZone(IContext context) {
			ISession session = context.getSession();
			if (session != null) {
				TimeZone timeZone = session.getTimeZone();
				if (timeZone != null)
					return timeZone;
				return getUTCTimeZone();
			}
			return getUTCTimeZone();
		}
	    
		public static TimeZone getUTCTimeZone() {
			return TimeZone.getTimeZone("UTC");
		}		
}