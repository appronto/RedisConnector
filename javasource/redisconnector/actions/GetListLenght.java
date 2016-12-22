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

public class GetListLenght extends CustomJavaAction<Long>
{
	private String Key;

	public GetListLenght(IContext context, String Key)
	{
		super(context);
		this.Key = Key;
	}

	@Override
	public Long executeAction() throws Exception
	{
		// BEGIN USER CODE
		RedisConnector redisconnector = new RedisConnector(); 
		return redisconnector.llen(Key);
		// END USER CODE
	}

	/**
	 * Returns a string representation of this action
	 */
	@Override
	public String toString()
	{
		return "GetListLenght";
	}

	// BEGIN EXTRA CODE
	// END EXTRA CODE
}