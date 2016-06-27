package by.training.filmstore.dao.pool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ResourceBundle;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PoolConnection {
	
	private static Logger logger = LogManager.getLogger(PoolConnection.class);
	private BlockingQueue<Connection> availableConnections;
	private BlockingQueue<Connection> usedConnections;
	private static PoolConnection poolConnection;

	private void initPoolConnection(String driver,String url,String user,String password,int minSize) throws PoolConnectionException
	{
		try {
			Class.forName(driver);
			availableConnections = new ArrayBlockingQueue<Connection>(minSize);
			usedConnections = new ArrayBlockingQueue<Connection>(minSize);
			for(int i = 0;i<minSize;i++)
			{
				Connection connection = DriverManager.getConnection(url,user,password);
				availableConnections.add(connection);
			}
		} catch (ClassNotFoundException e) {
			logger.error("Can't find database driver class");
			throw new PoolConnectionException("Can't find database driver class");
		} catch (SQLException e) {
			logger.error("Error creating database connection");
			throw new PoolConnectionException("Error creating database connection");
		}
	}
	
	public static synchronized PoolConnection getInstance() throws PoolConnectionException {
		if (poolConnection == null) {
			poolConnection = new PoolConnection();
		}
		return poolConnection;
	}

	public Connection takeConnection() throws PoolConnectionException
	{
		Connection connection = null;
		try {
			connection = availableConnections.take();
			usedConnections.offer(connection);
		} catch (InterruptedException e) {
			logger.error("Can't take connection(InterruptedException)");
			throw new PoolConnectionException(e);
		}
		return connection;
	}
	
	public boolean putbackConnection(Connection connection) throws SQLException {
		boolean success = false;
		if (connection != null) {
	        if (connection.isClosed()) {
	            throw new SQLException("Attempting to close closed connection");
	        }
	        if (connection.isReadOnly()) {
	            connection.setReadOnly(false);
	        }
			if(!connection.getAutoCommit()){
				connection.commit();
			}
			if (!usedConnections.remove(connection)){
				throw new SQLException("Error deleting connection from the given away connections pool");
			}
			if(!availableConnections.offer(connection)){
				throw new SQLException("Error allocating connection to the pool");
			}
			success = true;
		 }
		return success;
	}

	public void disposePoolConnection()
	{
		closeConnections(availableConnections);
		closeConnections(usedConnections);
		availableConnections.clear();
		usedConnections.clear();
	}

	private PoolConnection() throws PoolConnectionException
	{
			ResourceBundle bundle = ResourceBundle.getBundle(PoolConnectionParamName.RESOURCEPATH);
			String driver = bundle.getString(PoolConnectionParamName.DBDRIVER);
			String url = bundle.getString(PoolConnectionParamName.DBURL);
			String user = bundle.getString(PoolConnectionParamName.DBUSER);
			String password = bundle.getString(PoolConnectionParamName.DBPASSWORD);
			int minSize = Integer.parseInt(bundle.getString(PoolConnectionParamName.POOLSIZE));
			initPoolConnection(driver, url, user, password, minSize);
	}
	
	private void closeConnections(BlockingQueue<Connection> connections)
	{
		Connection connection = null;
		while((connection = connections.poll())!=null)
		{
			try {
				if(!connection.getAutoCommit())
				{
					connection.commit();
				}
				connection.close();
			} catch (SQLException e) {
				logger.error("Error closing database connection");
			}
		}
		
	}

	private final class PoolConnectionParamName {
		static final String DBDRIVER="db.driver";
	    static final String DBURL="db.url";
	    static final String DBUSER = "db.user";
		static final String DBPASSWORD = "db.password";
		static final String POOLSIZE = "db.poolsize";
	    static final String RESOURCEPATH = "db";
	}

}
