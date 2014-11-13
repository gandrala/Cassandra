/**
 * 
 */
package com.local.bits;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;

/**
 * @author gandrala Firing a query with IN clause is going to load up the
 *         coordinator node to do lot of heavy lifting. To avoid overloading
 *         coordinator node break up the IN clause and fire individual queries.
 *         Guava class Futures.successfulAsList does the work of returning all
 *         the result sets.
 *
 */
public class AsyncProcessingCallbackMultiPartQuery
{
	private final static String GET_ROWS_RK = "SELECT * FROM LOCAL.RK where id=?";

	private List<ResultSetFuture> submitQueries(Session session, List<String> keys)
	{
		List<ResultSetFuture> futures = Lists.newArrayList();
		for (String key : keys)
		{
			futures.add(session.executeAsync(GET_ROWS_RK, key));
		}

		return futures;
	}

	private Future<List<ResultSet>> getFullResultSet(Session session, List<String> keys)
	{
		return Futures.successfulAsList(submitQueries(session, keys));
	}

	// Sample code on how to assemble and get the details.
/*	public static void main(String[] args) 
	{ 
		AsyncProcessingCallbackMultiPartQuery apq = new AsyncProcessingCallbackMultiPartQuery(); 
		Future<List<ResultSet>> futureList = apq.getFullResultSet(CassandraConnector.INSTANCE.getSession(),
	  Lists.newArrayList("1", "3","5")); 
		try 
		{ 
			for ( ResultSet rs :futureList.get()) 
			{ 
				for (Row row : rs) 
				{
					System.out.println("id="+row.getString("id")+";name="+row.getString("name")+";tx_dt="+row.getDate("tx_dt")); 
				}
			} 
			} 
			catch (InterruptedException e) 
			{
				e.printStackTrace(); 
			} 
		    catch (ExecutionException e) 
			{
		    	e.printStackTrace(); 
		    } 
			finally 
			{
				CassandraConnector.INSTANCE.shutdown(); 
			}
	  }*/
}