/**
 * 
 */
package com.local.bits;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;

/**
 * @author gandrala
 * Fire queries asynchronously.
 *  method getUninterruptibly waits for the query to return.
 *  This approach here is to submit all the different queries as quickly as possible so that they can 
 *  process in the background, when we want to get the results use getUninterruptibly or getUninterruptibly(with timeout)
 *  depending on the use case.
 *  
 *   An another better approach is to use listener, for this approach check the example
 *
 */
public class AsyncQuery
{
	public void queryData(Session session)
	{
		Statement stmt = QueryBuilder.select().all().from("local","rk");
		ResultSetFuture rsFuture = session.executeAsync(stmt);
		for (Row row : rsFuture.getUninterruptibly())
		{
			System.out.printf("id:%s;tx_dt:%tc;name:%s",row.getString("id"),row.getDate("tx_dt"),row.getString("name"));
		}
	}
		
	
}
