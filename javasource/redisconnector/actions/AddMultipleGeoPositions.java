// This file was generated by Mendix Modeler.
//
// WARNING: Only the following code will be retained when actions are regenerated:
// - the import list
// - the code between BEGIN USER CODE and END USER CODE
// - the code between BEGIN EXTRA CODE and END EXTRA CODE
// Other code you write will be lost the next time you deploy the project.
// Special characters, e.g., é, ö, à, etc. are supported in comments.

package redisconnector.actions;

import java.util.HashMap;
import java.util.Map;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.webui.CustomJavaAction;
import redis.clients.jedis.GeoCoordinate;
import redisconnector.impl.RedisConnector;
import com.mendix.systemwideinterfaces.core.IMendixObject;

/**
 * GEOADD key longitude latitude member [longitude latitude member ...]
 * 
 * Available since 3.2.0.
 * Time complexity: O(log(N)) for each item added, where N is the number of elements in the sorted set.
 * Adds the specified geospatial items (latitude, longitude, name) to the specified key. Data is stored into the key as a sorted set, in a way that makes it possible to later retrieve items using a query by radius with the GEORADIUS or GEORADIUSBYMEMBER commands.
 * The command takes arguments in the standard format x,y so the longitude must be specified before the latitude. There are limits to the coordinates that can be indexed: areas very near to the poles are not indexable. The exact limits, as specified by EPSG:900913 / EPSG:3785 / OSGEO:41001 are the following:
 * Valid longitudes are from -180 to 180 degrees.
 * Valid latitudes are from -85.05112878 to 85.05112878 degrees.
 * The command will report an error when the user attempts to index coordinates outside the specified ranges.
 * Note: there is no GEODEL command because you can use ZREM in order to remove elements. The Geo index structure is just a sorted set.
 */
public class AddMultipleGeoPositions extends CustomJavaAction<Boolean>
{
	private String key;
	private java.util.List<IMendixObject> __geoPositions;
	private java.util.List<redisconnector.proxies.GeoPosition> geoPositions;

	public AddMultipleGeoPositions(IContext context, String key, java.util.List<IMendixObject> geoPositions)
	{
		super(context);
		this.key = key;
		this.__geoPositions = geoPositions;
	}

	@Override
	public Boolean executeAction() throws Exception
	{
		this.geoPositions = new java.util.ArrayList<redisconnector.proxies.GeoPosition>();
		if (__geoPositions != null)
			for (IMendixObject __geoPositionsElement : __geoPositions)
				this.geoPositions.add(redisconnector.proxies.GeoPosition.initialize(getContext(), __geoPositionsElement));

		// BEGIN USER CODE
		
		Map<String, GeoCoordinate> coordinateMap = new HashMap<String, GeoCoordinate>();
		for (redisconnector.proxies.GeoPosition geoPosition : geoPositions)
	    {
	    	coordinateMap.put(geoPosition.getName(), new GeoCoordinate(geoPosition.getLongitude().doubleValue(), geoPosition.getLatitude().doubleValue()));
	    }
	    		
		RedisConnector redisconnector = new RedisConnector();        
		redisconnector.geoadd(key, coordinateMap);
		return true;
		// END USER CODE
	}

	/**
	 * Returns a string representation of this action
	 */
	@Override
	public String toString()
	{
		return "AddMultipleGeoPositions";
	}

	// BEGIN EXTRA CODE
	// END EXTRA CODE
}
