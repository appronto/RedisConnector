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

/**
 * Removes the first count occurrences of elements equal to value from the list stored at key. The count argument influences the operation in the following ways:
 * count > 0: Remove elements equal to value moving from head to tail.
 * count < 0: Remove elements equal to value moving from tail to head.
 * count = 0: Remove all elements equal to value.
 * For example, LREM list -2 "hello" will remove the last two occurrences of "hello" in the list stored at list.
 * Note that non-existing keys are treated like empty lists, so when key does not exist, the command will always return 0.
 * Return value
 * Integer reply: the number of removed elements.
 */
public class DeleteValueFromList extends CustomJavaAction<Long>
{
	private String key;
	private Long count;
	private String value;

	public DeleteValueFromList(IContext context, String key, Long count, String value)
	{
		super(context);
		this.key = key;
		this.count = count;
		this.value = value;
	}

	@Override
	public Long executeAction() throws Exception
	{
		// BEGIN USER CODE
		RedisConnector redisconnector = new RedisConnector(); 
		return redisconnector.lrem(key,count, value);
		// END USER CODE
	}

	/**
	 * Returns a string representation of this action
	 */
	@Override
	public String toString()
	{
		return "DeleteValueFromList";
	}

	// BEGIN EXTRA CODE
	// END EXTRA CODE
}
