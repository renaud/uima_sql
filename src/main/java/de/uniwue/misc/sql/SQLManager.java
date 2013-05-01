package de.uniwue.misc.sql;
import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


/**
 * The SQLManager manages the connection to a SQL database. The coordinates which database to access
 * with which kind of configuration is determined be SQLConfig object which have to be used to create
 * SQLManager object via the static getSQLManager(SQLConfig) method. If the config parameter is omitted
 * the default config is used (only if has been set beforehand). To set the standard config use the
 * static method setStdConfig. To get noticed for a change of the stdConfig a listener can be enlisted
 *
 * A manager instance uses a single connection which is used during all database operations.
 * Every time a manager is requested with a certain config the same manager instance is used.
 * For connections with another config a new manager instance is created.
 * The manager currently supports MSSQL and MySQL
 * @author Georg Fette
 */
public class SQLManager {

	private static Map<SQLConfig, SQLManager> singeltons = new HashMap<SQLConfig, SQLManager>();
	private static SQLConfig stdConfig;

	private Connection con;
	public SQLConfig config;

	public static interface DefaultSQLConfigChangedListener { void defaultSQLConfigChanged(); }
	public static Set<DefaultSQLConfigChangedListener> defaultSQLConfigChangedListeners =
			new HashSet<DefaultSQLConfigChangedListener>();
	private static void callDefaultSQLConfigChanged() {
		for (DefaultSQLConfigChangedListener aListener : defaultSQLConfigChangedListeners) {
			aListener.defaultSQLConfigChanged();
		}
	}


	private SQLManager(SQLConfig aConfig) throws SQLException {
	  config = aConfig;
	  initializeSQL();
	}


	public static SQLManager getSQLManager() throws SQLException {
		SQLConfig config;
		config = getStdConfig();
		return getSQLManager(config);
	}


	public static SQLManager getSQLManager(SQLConfig aConfig) throws SQLException {
		SQLManager result = null;
		if (singeltons.containsKey(aConfig)) {
			result = singeltons.get(aConfig);
		} else {
			try {
				result = new SQLManager(aConfig);
				singeltons.put(aConfig, result);
			} catch (SQLException e) {
				throw new SQLException("Could not create SQLManager with parameters: \n" +
						"Server: '" + aConfig.sqlServer + "'\n" +
						"User: '" + aConfig.user + "'\n" +
						"Password: '" + aConfig.password + "'\n" +
						"Database: '" + aConfig.database + "'\n" +
						e.getMessage());
			}
		}
		return result;
	}


	public static SQLConfig getStdConfig() throws SQLException {
		if (stdConfig == null) {
			File stdFile;
			try {
				stdFile = new File("config.txt").getCanonicalFile();
				if (stdFile.exists()) {
					stdConfig = SQLConfig.read(stdFile);
					stdConfig.dbType = SQLConfig.decideDBType();
				}
			} catch (IOException e) {
				throw new SQLException(e);
			}
		}
		return stdConfig;
	}


	public static void setStdConfig(String aUser, String aDatabase,
			String aPassword, String anSQLServer) throws SQLException {
		setStdConfig(new SQLConfig(aUser, aDatabase, aPassword, anSQLServer));
	}


	public static void setStdConfig(SQLConfig aConfig) throws SQLException {
		if ((stdConfig == null) || !stdConfig.equals(aConfig)) {
			stdConfig = aConfig;
			// test the new connection
			SQLManager.getSQLManager(aConfig);
			callDefaultSQLConfigChanged();
		}
	}


	public void dropTable(String tableName) throws SQLException {
		Statement st;
		String command;

		st = createStatement();
		if (microsoftSQL()) {
			command = "IF EXISTS (SELECT * FROM information_schema.tables WHERE table_name='" +
					tableName + "') DROP TABLE " + tableName;
		} else {
			command = "DROP TABLE IF EXISTS " + tableName;
		}
		st.execute(command);
		st.close();
		commit();
	}


	private void reestablishConnection() throws SQLException {
		try {
			// try to close
			con.close();
		} catch (SQLException e1) {
		}
		// recreate the connection
		createConnection();
	}


	public PreparedStatement createPreparedStatement(String sql) throws SQLException {
		try {
			return con.prepareStatement(sql);
		} catch (SQLException e) {
			// if first call fails try to reestablish connection and try again
			reestablishConnection();
			return con.prepareStatement(sql);
		}
	}


	public PreparedStatement createPreparedStatementReturnGeneratedKey(
			String sql) throws SQLException {
		try {
			return con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		} catch (SQLException e) {
			// if first call fails try to reestablish connection and try again
			reestablishConnection();
			return con.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
		}
	}


	public Statement createStatement() throws SQLException {
		try {
			return con.createStatement();
		} catch (SQLException e) {
			// if first call fails try to reestablish connection and try again
			reestablishConnection();
			return con.createStatement();
		}
	}


	public boolean microsoftSQL() {
		return config.dbType == DBType.MSSQL;
	}


  public void dispose() {
    try {
    	// try to close the connection. If it fails, who cares...
      con.close();
    } catch (SQLException e) {
    }
  }


  private void registerDriverClass() throws SQLException  {
  	try {
  		if (microsoftSQL()) {
  			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver").newInstance();
  		} else {
  			Class.forName("com.mysql.jdbc.Driver").newInstance();
  		}
  	} catch (Exception e) {
  		throw new SQLException(e);
  	}
  }


  public void commit() throws SQLException {
  	con.commit();
  }


  private void createConnection() throws SQLException {
  	String host;
  	Properties props;

  	if (config.useJDBUrl) {
    	con = DriverManager.getConnection(config.jdbcURL);
  	} else {
  		if (microsoftSQL()) {
  			host = "jdbc:sqlserver://" + config.sqlServer + ";databaseName=" + config.database;
  		} else {
  			host = "jdbc:mysql://" + config.sqlServer + "/" + config.database;
  		}
    	props = new Properties();
    	props.put("user", config.user);
    	props.put("password", config.password);
    	con = DriverManager.getConnection(host, props);
  	}
  	con.setAutoCommit(false);
  }


  private void initializeSQL() throws SQLException {
  	registerDriverClass();
  	createConnection();
  }


  public boolean tableExists(String tableName) throws SQLException {
  	Statement st;
  	String command;
  	boolean result = false;

  	if (tableName != null) {
  		st = createStatement();
  		if (microsoftSQL()) {
  			command = "SELECT * FROM sysobjects WHERE name='" + tableName + "'";
  		} else {
  			command = "SELECT table_name from INFORMATION_SCHEMA.TABLES " +
  					"WHERE TABLE_SCHEMA='" + config.database + "' AND table_name='" +
  					tableName + "'";
  		}
  		ResultSet set = st.executeQuery(command);
  		result = set.next();
  		set.close();
  		st.close();
  	}
  	return result;
  }


}
