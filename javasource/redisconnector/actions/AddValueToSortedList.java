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
 * Adds all the specified members with the specified scores to the sorted set stored at key. It is possible to specify multiple score / member pairs. If a specified member is already a member of the sorted set, the score is updated and the element reinserted at the right position to ensure the correct ordering.
 * If key does not exist, a new sorted set with the specified members as sole members is created, like if the sorted set was empty. If the key exists but does not hold a sorted set, an error is returned.
 * The score values should be the string representation of a double precision floating point number. +inf and -inf values are valid values as well.
 */
public class AddValueToSortedList extends CustomJavaAction<Long>
{
	private String key;
	private java.math.BigDecimal score;
	private String member;

	public AddValueToSortedList(IContext context, String key, java.math.BigDecimal score, String member)
	{
		super(context);
		this.key = key;
		this.score = score;
		this.member = member;
	}

	@Override
	public Long executeAction() throws Exception
	{
		// BEGIN USER CODE
		RedisConnector redisconnector = new RedisConnector(); 
	    double ScoreConverted = score.doubleValue();
        
		return redisconnector.zadd(key, ScoreConverted, member);
		// END USER CODE
	}

	/**
	 * Returns a string representation of this action
	 */
	@Override
	public String toString()
	{
		return "AddValueToSortedList";
	}

	// BEGIN EXTRA CODE
	// END EXTRA CODE
}
