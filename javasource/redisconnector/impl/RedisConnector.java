package redisconnector.impl;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;

import com.mendix.core.Core;
import com.mendix.core.objectmanagement.member.*;
import com.mendix.logging.ILogNode;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.systemwideinterfaces.core.IMendixObject;
import com.mendix.systemwideinterfaces.core.IMendixObjectMember;
import com.mendix.systemwideinterfaces.core.ISession;

import redisconnector.proxies.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisConnectionException;

public class RedisConnector 
{ 
	private Jedis redis = null;
	private ILogNode _logNode = Core.getLogger("Redis");
	
	private static JedisPool pool =  (redisconnector.proxies.constants.Constants.getRedisAuth().trim().length() > 0 
			? new JedisPool(new JedisPoolConfig(), redisconnector.proxies.constants.Constants.getRedisEndpoint()
					,Integer.valueOf(redisconnector.proxies.constants.Constants.getRedisPort())
					, Protocol.DEFAULT_TIMEOUT,redisconnector.proxies.constants.Constants.getRedisAuth()) 
						: new JedisPool(new JedisPoolConfig(), redisconnector.proxies.constants.Constants.getRedisEndpoint()
							,Integer.valueOf(redisconnector.proxies.constants.Constants.getRedisPort())
							, Protocol.DEFAULT_TIMEOUT)) ;
		
	public RedisConnector() {		
	}
	
	public void destroy(){
		pool.destroy();
	}
	
	//http://redis.io/commands/lpush
	public long lpush(String Key, String Value) {
		
		try
	    {
	        redis = pool.getResource();
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
		public String hmset(IContext context, String Key, java.util.List<IMendixObject> ListOfObjects) {
			try {
				_logNode.debug("HMSET " + Key +  " + hashes: "  + ListOfObjects.size()); 
				redis = pool.getResource();
				Map<String, String> hashMap = new HashMap<String, String>();
						    
				for (IMendixObject object : ListOfObjects){
					
					Map<String, ? extends IMendixObjectMember<?>> members = object.getMembers(context);
					for(String key : members.keySet()) { 
						IMendixObjectMember<?> m = members.get(key);
						if (m.isVirtual() || m instanceof MendixAutoNumber)
							continue;
						if (m instanceof MendixDateTime){
							hashMap.put(key, processDate(context, object.getValue(context, key),((MendixDateTime) m).shouldLocalize()));
							continue;
						}
						if (m instanceof MendixInteger){
							hashMap.put(key, Integer.toString((object.getValue(context, key))));
							continue;
						}
						if (m instanceof MendixDecimal){
							hashMap.put(key, Double.toString((object.getValue(context, key))));
							continue;
						}
						if (m instanceof MendixEnum){
							hashMap.put(key, object.getValue(context, key).toString());
							continue;
						}
						if (m instanceof MendixBoolean){
							hashMap.put(key, Boolean.toString((object.getValue(context, key))));
							continue;
						}
						if (m instanceof MendixLong){
							hashMap.put(key, Long.toString((object.getValue(context, key))));
							continue;
						}
						if (((!(m instanceof MendixObjectReference) && !(m instanceof MendixObjectReferenceSet)))){
						 	hashMap.put(key, object.getValue(context, key).toString());	
						}
					}
					 
				}
				 			 
				 return redis.hmset(Key,hashMap); 
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
		
		//see http://redis.io/commands/del
		public long del(String Key) {
			try {
				 _logNode.debug("DEL " + Key); 
				 redis = pool.getResource();
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
						if (m.isVirtual() || m instanceof MendixAutoNumber){
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
							ObjectToReturn.setValue(context, fields[i], Double.valueOf(result));
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