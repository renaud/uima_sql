package de.uniwue.uima.persist.dbManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.uima.persist.PersistConfig;
import de.uniwue.uima.persist.PersistException;
import de.uniwue.uima.persist.data.LoadConfig;

public class DocManager extends DatabaseManager {

	private PersistConfig persistConfig;


  public DocManager(SQLManager aSqlManager,
  		PersistConfig aPersistConfig) throws SQLException {
  	super(aSqlManager);
  	persistConfig = aPersistConfig;
  	createSQLTables();
  }


  @Override public String getTableName() {
		return persistConfig.getDocTableName();
	}


	@Override protected void readResult(ResultSet resultSet) {
	}


	@Override protected String getCreateTableString() {
		String command;
		String tableName = getTableName();
		command = "CREATE TABLE " + tableName + "(\n";
		if (sqlManager.microsoftSQL()) {
			command += "doc_ID BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY, \n";
		} else {
			command += "doc_ID BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY, \n";
		}
		if (sqlManager.microsoftSQL()) {
			command += "text VARCHAR(MAX), \n";
		} else {
			command += "text MEDIUMTEXT, \n";
		}
		command += "extID VARCHAR(200) NOT NULL, \n" +
				"col_ID VARCHAR(200) NOT NULL ";
		if (sqlManager.microsoftSQL()) {
			command +=
				")\n " +
				"CREATE INDEX Index_" + tableName + "_extID_colID ON " +
				tableName + " (extID, col_ID); \n" +
				"CREATE INDEX Index_" + tableName + "_colID ON " +
				tableName + " (col_ID); \n" +
				"ALTER TABLE " + tableName + " " +
				"ADD CONSTRAINT " + tableName + "_extID__colID_unique UNIQUE (extID, col_ID); \n";
		} else {
			command += ", \n" +
			"INDEX Index_" + tableName + "_extID_colID (extID, col_ID), \n" +
			"INDEX Index_" + tableName + "_colID (col_ID), \n" +
			"CONSTRAINT " + tableName + "_extID__colID_unique UNIQUE (extID, col_ID) \n" +
			")";
		}
		return command;
	}


	public long getDocID(String extID, String colID,
			boolean complainIfNotExists) throws PersistException {
    PreparedStatement st;
    String command;
    ResultSet resultSet;
    long result;

    try {
      command = "SELECT doc_ID FROM " + getTableName() + " WHERE extID=? AND col_ID=?";
      st = sqlManager.createPreparedStatement(command);
      st.setString(1, extID);
      st.setString(2, colID);
      resultSet = st.executeQuery();
      if (resultSet.next()) {
      	result = resultSet.getLong("doc_ID");
      } else {
      	if (complainIfNotExists) {
      		throw new PersistException("Doc with extID '" + extID +
      				"' does not exist in collection '" + colID + "'.");
      	} else {
      		result = 0;
      	}
      }
      resultSet.close();
      st.close();
    } catch (SQLException e) {
    	throw new PersistException(e);
    }
    return result;
	}


	public List<String> getExtDocIDs(LoadConfig loadConfig) throws PersistException {
    PreparedStatement st;
    String command = "";
    ResultSet resultSet;
    List<String> result = new ArrayList<String>();

    try {
    	command = "SELECT extID FROM " + getTableName() + " WHERE col_ID=? ORDER BY extID";
      st = sqlManager.createPreparedStatement(command);
      st.setString(1, loadConfig.colID);
      resultSet = st.executeQuery();
      while (resultSet.next()) {
      	String extID = resultSet.getString("extID");
      	result.add(extID);
      }
      resultSet.close();
      st.close();
    } catch (SQLException e) {
    	throw new PersistException(e);
    }
    return result;
	}


	public List<String> getExtDocIDs(String colID) throws PersistException {
		LoadConfig config = new LoadConfig();
		config.colID = colID;
		return getExtDocIDs(config);
	}


	public List<String> getColIDs() throws PersistException {
    PreparedStatement st;
    String command;
    ResultSet resultSet;
    List<String> result = new ArrayList<String>();

    try {
      command = "SELECT col_ID FROM " + getTableName();
      st = sqlManager.createPreparedStatement(command);
      resultSet = st.executeQuery();
      while (resultSet.next()) {
      	String colID = resultSet.getString("col_ID");
      	result.add(colID);
      }
      resultSet.close();
      st.close();
    } catch (SQLException e) {
    	throw new PersistException(e);
    }
  	return result;
	}


	public List<Long> getDocIDs(String colID) throws PersistException {
    PreparedStatement st;
    String command;
    ResultSet resultSet;
    List<Long> result = new ArrayList<Long>();

    try {
      command = "SELECT doc_ID FROM " + getTableName() + " WHERE col_ID=?";
      st = sqlManager.createPreparedStatement(command);
      st.setString(1, colID);
      resultSet = st.executeQuery();
      while (resultSet.next()) {
      	Long ID = resultSet.getLong("doc_ID");
      	result.add(ID);
      }
      st.close();
    } catch (SQLException e) {
    	throw new PersistException(e);
    }
    return result;
	}


	public String getDocText(String extID, String colID) throws PersistException {
    PreparedStatement st;
    String command;
    ResultSet resultSet;
    String result;

    try {
      command = "SELECT text FROM " + getTableName() + " WHERE extID=? AND col_ID=?";
      st = sqlManager.createPreparedStatement(command);
      st.setString(1, extID);
      st.setString(2, colID);
      resultSet = st.executeQuery();
      if (resultSet.next()) {
      	result = resultSet.getString("text");
      } else {
      	throw new PersistException("Doc with extID '" + extID +
      			"' does not exist in collection '" + colID + "'.");
      }
      resultSet.close();
      st.close();
    } catch (SQLException e) {
    	throw new PersistException(e);
    }
    return result;
	}


	public String getDocText(long anID) throws PersistException {
    PreparedStatement st;
    String command;
    ResultSet resultSet;
    String result = null;

    try {
      command = "SELECT text FROM " + getTableName() + " WHERE doc_ID=?";
      st = sqlManager.createPreparedStatement(command);
      st.setLong(1, anID);
      resultSet = st.executeQuery();
      if (resultSet.next()) {
      	result = resultSet.getString("text");
      } else {
      	throw new PersistException("Doc with ID '" + anID + "' does not exist.");
      }
      resultSet.close();
      st.close();
    } catch (SQLException e) {
    	throw new PersistException(e);
    }
    return result;
	}


	public long insertDoc(String docText, String extID, String colID) throws PersistException {
  	PreparedStatement st;
  	String command = "";

  	try {
  		if (sqlManager.microsoftSQL()) {
    		command += "IF NOT EXISTS (SELECT * FROM " + getTableName() +
    		" WHERE extID='" + extID.replaceAll("'", "''") + "'" +
    		" AND col_ID='" + colID.replaceAll("'", "''") + "') INSERT ";
  		} else {
  			command += "INSERT IGNORE ";
  		}
  		command += "INTO " + getTableName() + " (text, extID, col_ID) VALUES (?, ?, ?)";
  		st = sqlManager.createPreparedStatementReturnGeneratedKey(command);
  		st.setString(1, docText);
  		st.setString(2, extID);
  		st.setString(3, colID);
  		st.execute();
  		long genID;
  		ResultSet result = st.getGeneratedKeys();
  		if (result.next()) {
  			genID = result.getInt(1);
  		} else {
  			genID = getDocID(extID, colID, true);
  		}
  		result.close();
  		st.close();
  		return genID;
  	} catch (SQLException e) {
    	throw new PersistException(e);
  	}
	}


	public void deleteDoc(String extID, String colID) throws PersistException {
    PreparedStatement st;
    String command;

    try {
      command = "DELETE FROM " + getTableName() + " WHERE extID=? AND col_ID=?";
      st = sqlManager.createPreparedStatement(command);
      st.setString(1, extID);
      st.setString(2, colID);
      st.execute();
      st.close();
    } catch (SQLException e) {
    	throw new PersistException(e);
    }
	}


	public void deleteCol(String colID) throws PersistException {
    PreparedStatement st;
    String command;

    try {
      command = "DELETE FROM " + getTableName() + " WHERE col_ID=?";
      st = sqlManager.createPreparedStatement(command);
      st.setString(1, colID);
      st.execute();
      st.close();
    } catch (SQLException e) {
    	throw new PersistException(e);
    }
	}


}
