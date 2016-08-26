# Redis Connector

The **Redis connector** can be used to seamlessly connect to Redis (http://redis.io/). Redis is an open source (BSD licensed), in-memory data structure store, used as database, cache and message broker. 
It supports data structures such as strings, hashes, lists, sets, sorted sets with range queries, bitmaps, hyperloglogs and geospatial indexes with radius queries. 
Redis has built-in replication, Lua scripting, LRU eviction, transactions and different levels of on-disk persistence, and provides high availability via Redis Sentinel and automatic partitioning with Redis Cluster.

# Dependencies
* [Jedis](https://github.com/xetorthio/jedis), A blazingly small and sane redis java client.

# Installation
### Prerequisities
* An **endpoint** that points to your Redis instance.
* The **authkey** (optional)

## Installation
* Import the module **Redis connector** in your project (from the Mendix AppStore or by downloading and exporting the module from this project)
* Add Microflow BeforeShutDownRedisConnector to you Before shutdown setting for cleaning up your Redis pool after shutdown  app.


# Getting Started
* You can have 2 options for testing with Redis. Local and in the cloud. 
	* Download Redis (ported for Windows): https://github.com/MSOpenTech/redis/releases 
	* Start with a free account at redislabs: https://redislabs.com/pricing?service=redis (note: your performance is limited! better try local installation first)
	* check the unit tests for example implementations
Once you have imported the RedisConnector module in your mendix application, you will have *Redis connector* available in the Toolbox. 

It supports now 8 actions *LPUSH*, *RPUSH*, *LRANGE*, *DEL*, *HSET*, *HMGETALL*, *HMGET*, *HMSET*  and more to come!
In order to use any of these in your Mendix application, you can just drag and drop them to your microflow.
Next step would be to provide all the arguments to the selected action and choose the output result name. 

### COMMANDS
See http://redis.io/commands/


# Remarks
* Avoid fetching large amounts of data as it can lead to memory issues because all the ResultRow data is being loaded into memory at once.
* pretty amazing performance on local machine 
	`2016-08-16 23:53:59.828	Redis	Start storing 2.000.000 strings in redis`
	`2016-08-16 23:55:05.144	Redis	End storing strings in redis, start retrieve`
	`2016-08-16 23:55:06.894	Redis	Retrieved ended for list, Start sum`
	`2016-08-16 23:55:06.957	Redis	Sum: 1999999000000`


# License
Licensed under the Apache license.

# Developers notes
* Open the RedisConnector.mpr in the Mendix Modeler.
* Use *Deploy for Eclipse* option (F6) and you can then import this module as an Eclipse project to the Eclipse IDE.

# Version history
0.1 first commands for Redis implemented
