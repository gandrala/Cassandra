/**
 * 
 */
package com.local.bits;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;

/**
 * @author gandrala
 * pass async query to callback
 * implement callback so that callback code can handle the results in success and failure block.
 * Main thread is not going to get blocked.
 *
 */
public class AsyncProcessingWithCallback
{
	private final static String GET_ROWS_RK="SELECT * FROM LOCAL.RK";
	public void queryData(Session session)
	{		
		ResultSetFuture rsFuture = session.executeAsync(GET_ROWS_RK);
		Futures.addCallback(rsFuture, new FutureCallback<ResultSet>()
		{

			public void onFailure(Throwable arg0)
			{
				// TODO Auto-generated method stub
				arg0.printStackTrace();
				
			}

			public void onSuccess(ResultSet arg0)
			{
				for (Row row : arg0)
				{
					System.out.println("id="+row.getString("id")+";"+"tx_dt="+row.getDate("tx_dt")+";"+"name="+row.getString("name"));
				}
				
			}
		});
		
	}
	public static void main(String[] args)
	{
		AsyncProcessingWithCallback ap = new AsyncProcessingWithCallback();
		ap.queryData(CassandraConnector.INSTANCE.getSession());
		CassandraConnector.INSTANCE.shutdown();
	}
}
