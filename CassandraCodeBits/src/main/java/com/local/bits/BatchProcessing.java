package com.local.bits;

import java.text.SimpleDateFormat;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

/**
 * @author gandrala
 * 
 *CREATE TABLE local.rk (
    id text,
    tx_dt timestamp,
    name text,
    PRIMARY KEY (id));
 * use PreparedStatement to build the CQL.
 * use BatchStatement to queue up the batch data
 * use session.execute to run the batch
 * 
 */

public class BatchProcessing
{
	private final static String INSERT_ROW_RK="INSERT INTO LOCAL.RK(id,tx_dt,name) values (?,?,?)";
	
	
	// Pass the session by calling CassandraConnector.INSTANCE.getSession()
	public void loadData(Session session)
	{
		PreparedStatement loadRKDataPreparedStatement = session.prepare(INSERT_ROW_RK);
		//Make the batch
		BatchStatement batch = new BatchStatement();
		//bind the values and queue the statement
		batch.add(loadRKDataPreparedStatement.bind("1",new java.util.Date(),"First"));
		batch.add(loadRKDataPreparedStatement.bind("2",new java.util.Date(),"Second"));
		batch.add(loadRKDataPreparedStatement.bind("3",new java.util.Date(),"Third"));
		batch.add(loadRKDataPreparedStatement.bind("4",new java.util.Date(),"Fourth"));
		batch.add(loadRKDataPreparedStatement.bind("5",new java.util.Date(),"Five"));
		session.execute(batch);
		
	}
	
}
