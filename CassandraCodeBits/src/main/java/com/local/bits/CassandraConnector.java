/**
 * 
 */
package com.local.bits;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * @author gandrala
 * enum class to maintain only one session object.
 * For webapps, Load this enum during contextInitialized method and call shutdown in contextDestroyed method
 * For non webapps, load this enum at the start of the app and call shutdown in app exit method.  
 * Session is thread safe and can be reused it through out the application.
 */
public enum CassandraConnector
{
	INSTANCE;
	private Cluster cluster;
	private Session session;
	private static final Logger log = LoggerFactory.getLogger(CassandraConnector.class);
	private CassandraConnector()
	{
		// In production, get the contact points from a property file		
		cluster = Cluster.builder().addContactPoint("127.0.0.1").build();
		session = cluster.connect();
	}
	public Session getSession()
	{
		if (session==null)
		{
			log.info("session object is null");
			throw new IllegalStateException("Connection is not initialized");
		}
		return session;
	}
	public void shutdown()
	{	
		log.info("Shutting down started");
		if (session!=null)
			session.close();
		if (cluster!=null)
			cluster.close();
		log.info("Shutting down ended");
	}
	public static void main(String[] agrs)
	{
		CassandraConnector.INSTANCE.getSession();
		CassandraConnector.INSTANCE.shutdown();
		
	}
}
