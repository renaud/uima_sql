package de.uniwue.uima.persist.dbManager;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.uima.persist.PersistException;
import de.uniwue.uima.persist.PersistenceManager;

public class TmpTypeManager extends DatabaseManager {

	private String tableName;

	public TmpTypeManager(SQLManager sqlManager, String[] annotTypesToLoad,
			PersistenceManager persManager) throws SQLException {
		super(sqlManager);
		tableName = "tmpTypeTable" + UUID.randomUUID().toString().replaceAll("-", "_");
		if (sqlManager.microsoftSQL()) {
			tableName = "#" + tableName;
		}
		createSQLTables();
		for (String aTypeName : annotTypesToLoad) {
			try {
				long typeID = persManager.annotTypeManager.getAnnotTypeID(aTypeName, false);
				insertID(typeID);
			} catch (PersistException e) {
				e.printStackTrace();
			}
		}
	}


	@Override public String getTableName() {
		return tableName;
	}


	@Override protected void readResult(ResultSet resultSet) {
	}


	public void insertIDs(List<Long> ids) {
	}


	public void insertID(Long id) {
  	Statement st;
  	String command = "";

  	try {
  		if (sqlManager.microsoftSQL()) {
    		command += "IF NOT EXISTS (SELECT * FROM " + getTableName() +
    		" WHERE typeID=" + id + ") INSERT ";
  		} else {
  			command += "INSERT IGNORE ";
  		}
  		command += "INTO " + getTableName() +
  				" (typeID) VALUES (" + id + ")";
  		st = sqlManager.createStatement();
  		st.execute(command);
  		st.close();
  	} catch (SQLException e) {
  		e.printStackTrace();
  	}
	}


	@Override protected String getCreateTableString() {
		String command;
		String tableName = getTableName();
		if (sqlManager.microsoftSQL()) {
			command = "CREATE TABLE " + tableName + " (\n";
		} else {
			command = "CREATE TEMPORARY TABLE " + tableName + " (\n";
		}
		command += "typeID BIGINT NOT NULL PRIMARY KEY)";
		return command;
	}

}
