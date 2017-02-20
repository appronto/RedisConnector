package redisconnector.impl;

import java.util.HashMap;

import com.mendix.core.Core;
import com.mendix.core.CoreException;
import com.mendix.systemwideinterfaces.core.IContext;
import redisconnector.proxies.constants.*;

import redis.clients.jedis.JedisPubSub;

public final class MendixRedisPubSub extends JedisPubSub {
	public void onMessage(String channel, String message) {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("Channel", channel);
		parameters.put("Pattern", null);
		parameters.put("Message", message);
		IContext systemContext = Core.createSystemContext();
		try {
			Core.getLogger("RedisConnector").debug("onMessage: Channel " + channel + " message: " + message);
			Core.execute(systemContext, Constants.getOnReceiveMessageMicroflow(), parameters);
		} catch (CoreException e) {
			Core.getLogger("RedisConnector").error("Error onMessage: Channel " + channel + " Message: " + message, e);
		}
	}

	public void onSubscribe(String channel, int subscribedChannels) {
		Core.getLogger("RedisConnector")
				.debug("onSubscribe: Channel " + channel + " subscribedChannels: " + subscribedChannels);
	}

	public void onUnsubscribe(String channel, int subscribedChannels) {
		Core.getLogger("RedisConnector")
				.debug("onUnsubscribe: Channel " + channel + " subscribedChannels: " + subscribedChannels);
	}

	public void onPSubscribe(String pattern, int subscribedChannels) {
		Core.getLogger("RedisConnector")
				.debug("onPSubscribe: Pattern " + pattern + " subscribedChannels: " + subscribedChannels);
	}

	public void onPUnsubscribe(String pattern, int subscribedChannels) {
		Core.getLogger("RedisConnector")
				.debug("onPUnsubscribe: Pattern " + pattern + " subscribedChannels: " + subscribedChannels);
	}

	public void onPMessage(String pattern, String channel, String message) {
		HashMap<String, Object> parameters = new HashMap<String, Object>();
		parameters.put("Channel", channel);
		parameters.put("Pattern", pattern);
		parameters.put("Message", message);
		IContext systemContext = Core.createSystemContext();
		try {
			Core.getLogger("RedisConnector").debug("onMessage: Channel " + channel + " message: " + message);
			Core.execute(systemContext, redisconnector.proxies.constants.Constants.getOnReceiveMessageMicroflow(),
					parameters);
		} catch (CoreException e) {
			Core.getLogger("RedisConnector")
					.error("Error onPMessage: Channel " + channel + " Pattern " + pattern + " Message: " + message, e);
		}
	}
}
