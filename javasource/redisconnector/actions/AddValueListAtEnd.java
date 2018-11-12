// This file was generated by Mendix Modeler.
//
// WARNING: Only the following code will be retained when actions are regenerated:
// - the import list
// - the code between BEGIN USER CODE and END USER CODE
// - the code between BEGIN EXTRA CODE and END EXTRA CODE
// Other code you write will be lost the next time you deploy the project.
// Special characters, e.g., é, ö, à, etc. are supported in comments.

package redisconnector.actions;

import com.mendix.systemwideinterfaces.core.IContext;
import com.mendix.webui.CustomJavaAction;
import redisconnector.impl.RedisConnector;
import com.mendix.systemwideinterfaces.core.IMendixObject;

/**
 * RPUSH key value [value ...]
 * 
 * Insert all the specified values at the tail of the list stored at key. If key does not exist, it is created as empty list before performing the push operation. When key holds a value that is not a list, an error is returned.
 * It is possible to push multiple elements using a single command call just specifying multiple arguments at the end of the command. Elements are inserted one after the other to the tail of the list, from the leftmost element to the rightmost element. So for instance the command RPUSH mylist a b c will result into a list containing a as first element, b as second element and c as third element.
 * 
 * Return value
 * Integer reply: the length of the list after the push operation.
 * 
 * Examples
 * redis> RPUSH mylist "hello"
 * (integer) 1
 * redis> RPUSH mylist "world"
 * (integer) 2
 * redis> LRANGE mylist 0 -1
 * 1) "hello"
 * 2) "world"
 * redis> 
 */
public class AddValueListAtEnd extends CustomJavaAction<Long>
{
	private String key;
	private java.util.List<IMendixObject> __valueList;
	private java.util.List<redisconnector.proxies.InputRow> valueList;

	public AddValueListAtEnd(IContext context, String key, java.util.List<IMendixObject> valueList)
	{
		super(context);
		this.key = key;
		this.__valueList = valueList;
	}

	@Override
	public Long executeAction() throws Exception
	{
		this.valueList = new java.util.ArrayList<redisconnector.proxies.InputRow>();
		if (__valueList != null)
			for (IMendixObject __valueListElement : __valueList)
				this.valueList.add(redisconnector.proxies.InputRow.initialize(getContext(), __valueListElement));

		// BEGIN USER CODE
		RedisConnector redisconnector = new RedisConnector(); 
		return redisconnector.rpush(key,valueList);
		// END USER CODE
	}

	/**
	 * Returns a string representation of this action
	 */
	@Override
	public String toString()
	{
		return "AddValueListAtEnd";
	}

	// BEGIN EXTRA CODE
	// END EXTRA CODE
}
