package de.uniwue.misc.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import de.uniwue.misc.util.LogManagerUniWue;


/**
 * A DatabaseManager is a helper class which stores and loads task specific data.
 * The manager uses only one table whose name has to be returned by getTableName().
 * The user has to implement the methods
 * - getTableName: to determine the name of the table the manager is working on
 * - readResult(ResultSet resultSet): this method is called when readTables is called on the manager.
 * 		the read command reads all columns (*) from the table if not given otherwise by
 * 		overriding getReadTablesCommand()
 * - getCreatTableString(): returns the SQLCode to create the table the manager administrates
 *
 * The manager's database connection works via a SQLManager object which has to be given in the
 * constructor.
 * @author Georg Fette
 */
public abstract class DatabaseManager {

	public SQLManager sqlManager;

	public abstract String getTableName();
	protected abstract void readResult(ResultSet resultSet) throws SQLException;
	protected abstract String getCreateTableString();


	public DatabaseManager(SQLManager aSqlManager) {
		sqlManager = aSqlManager;
	}


	protected String getCreateTableAdditionalColumns() {
		return "";
	}


	protected String getBooleanString(boolean aValue) {
		if (aValue) {
			return "1";
		} else {
			return "0";
		}
	}


	protected String getCreateTableStub() {
		String command = "CREATE TABLE " + getTableName() + " (\n";
		return command;
	}


	public SQLManager getSQLManager() {
		return sqlManager;
	}


	private boolean tableExists() throws SQLException {
		return sqlManager.tableExists(getTableName());
	}


	protected void createSQLTables() throws SQLException {
		Statement st;
		String command;

		if ((getTableName() != null) && !tableExists()) {
			st = sqlManager.createStatement();
			command = getCreateTableString();
			st.execute(command);
			st.close();
			sqlManager.commit();
		}
	}


	protected String getRestrictionString() {
		return "";
	}


	protected Long getGeneratedLongKey(PreparedStatement st) {
		ResultSet result;
		long genID = 0;
		try {
			result = st.getGeneratedKeys();
			if (result.next()) {
				genID = result.getInt(1);
			}
		} catch (SQLException e) {
		}
		return genID;
	}


	protected Integer getGeneratedIntKey(PreparedStatement st) {
		ResultSet result;
		Integer genID = 0;
		try {
			result = st.getGeneratedKeys();
			if (result.next()) {
				genID = result.getInt(1);
			}
		} catch (SQLException e) {
		}
		return genID;
	}


	protected String getReadTablesCommand() {
		String result = "select * from " + getTableName();
		String restriction = getRestrictionString();
		if (!restriction.isEmpty()) {
			result += " where " + restriction;
		}
		return result;
	}


	public void readTables() throws SQLException {
		readTables(false);
	}


	protected void preProcessResultSet(ResultSet resultSet) throws SQLException {
	}


	public void readTables(boolean verbose) throws SQLException {
		Statement st;
		ResultSet resultSet;
		int counter = 0;

		if (getTableName() == null) {
			return;
		}
		st = sqlManager.createStatement();
		if (verbose) {
			LogManagerUniWue.info("Started reading table '" + getTableName() + "'");
		}
		String command = getReadTablesCommand();
		resultSet = st.executeQuery(command);
		if (verbose) {
			LogManagerUniWue.info("Finished command for reading table '" + getTableName() + "'");
		}
		preProcessResultSet(resultSet);
		while (resultSet.next()) {
			readResult(resultSet);
			counter++;
			if (counter % 500000 == 0) {
				if (verbose) {
					LogManagerUniWue.info("Loaded " + counter + " entries");
				}
			}
			if (resultSet.isClosed()) {
				break;
			}
		}
		if (verbose) {
			LogManagerUniWue.info("Finished reading table '" + getTableName() + "'");
		}
		st.close();
	}


	protected String addAndIfNecessary(String restriction) {
		if (restriction.isEmpty()) {
			return restriction;
		} else {
			return restriction + " and ";
		}
	}


	public void commit() throws SQLException {
		sqlManager.commit();
	}


	protected void dropTable() throws SQLException {
		Statement st;
		String command;

		if (getTableName() != null) {
			st = sqlManager.createStatement();
			command = "drop table " + getTableName();
			st.execute(command);
			st.close();
			sqlManager.commit();
		}
	}


	protected Set<String> columnNames;
	protected Set<String> getColumnNames() throws SQLException {
		if (columnNames == null) {
			String command = getReadTablesCommand();
			Statement st;
			st = sqlManager.createStatement();
			ResultSet resultSet = st.executeQuery(command);
			columnNames = getColumnNames(resultSet);
			resultSet.close();
			st.close();
		}
		return columnNames;
	}


	protected Set<String> getColumnNames(ResultSet resultSet) throws SQLException {
		Set<String> result = new HashSet<String>();
		ResultSetMetaData metaData = resultSet.getMetaData();
		int columnCount = metaData.getColumnCount();
		for (int i = 1; i <= columnCount; i++) {
			String columnName = metaData.getColumnName(i);
			result.add(columnName);
		}
		return result;
	}



}
