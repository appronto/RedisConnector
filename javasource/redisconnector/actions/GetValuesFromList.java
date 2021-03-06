// This file was generated by Mendix Modeler.
//
// WARNING: Only the following code will be retained when actions are regenerated:
// - the import list
// - the code between BEGIN USER CODE and END USER CODE
// - the code between BEGIN EXTRA CODE and END EXTRA CODE
// Other code you write will be lost the next time you deploy the project.
// Special characters, e.g., é, ö, à, etc. are supported in comments.

package redisconnector.actions;

import redisconnector.impl.RedisConnector;
import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.webui.CustomJavaAction;
import com.mendix.systemwideinterfaces.core.IMendixObject;

/**
 * LRANGE key start stop
 * 
 * Returns the specified elements of the list stored at key. The offsets start and stop are zero-based indexes, with 0 being the first element of the list (the head of the list), 1 being the next element and so on.
 * These offsets can also be negative numbers indicating offsets starting at the end of the list. For example, -1 is the last element of the list, -2 the penultimate, and so on.
 * Consistency with range functions in various programming languages
 * 
 * Note that if you have a list of numbers from 0 to 100, LRANGE list 0 10 will return 11 elements, that is, the rightmost item is included. This may or may not be consistent with behavior of range-related functions in your programming language of choice (think Ruby's Range.new, Array#slice or Python's range() function).
 * 
 * Out-of-range indexes
 * Out of range indexes will not produce an error. If start is larger than the end of the list, an empty list is returned. If stop is larger than the actual end of the list, Redis will treat it like the last element of the list.
 * 
 * Return value
 * Array reply: list of elements in the specified range.
 * 
 * Examples
 * redis> RPUSH mylist "one"
 * (integer) 1
 * redis> RPUSH mylist "two"
 * (integer) 2
 * redis> RPUSH mylist "three"
 * (integer) 3
 * redis> LRANGE mylist 0 0
 * 1) "one"
 * redis> LRANGE mylist -3 2
 * 1) "one"
 * 2) "two"
 * 3) "three"
 * redis> LRANGE mylist -100 100
 * 1) "one"
 * 2) "two"
 * 3) "three"
 * redis> LRANGE mylist 5 10
 * (empty list or set)
 * redis> 
 */
public class GetValuesFromList extends CustomJavaAction<java.util.List<IMendixObject>>
{
	private String key;
	private Long start;
	private Long stop;

	public GetValuesFromList(IContext context, String key, Long start, Long stop)
	{
		super(context);
		this.key = key;
		this.start = start;
		this.stop = stop;
	}

	@Override
	public java.util.List<IMendixObject> executeAction() throws Exception
	{
		// BEGIN USER CODE
		RedisConnector redisconnector = new RedisConnector(); 
		return redisconnector.lrange(this.getContext(), key, start, stop);
		// END USER CODE
	}

	/**
	 * Returns a string representation of this action
	 */
	@Override
	public String toString()
	{
		return "GetValuesFromList";
	}

	// BEGIN EXTRA CODE
	// END EXTRA CODE
}
