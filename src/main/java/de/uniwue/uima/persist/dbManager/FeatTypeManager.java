package de.uniwue.uima.persist.dbManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.uima.resource.metadata.TypeDescription;

import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.util.StringUtilsUniWue;
import de.uniwue.uima.persist.PersistConfig;
import de.uniwue.uima.persist.PersistException;

public class FeatTypeManager extends DatabaseManager {

	private AnnotTypeManager annotTypeManager;
	private Map<String, HashMap<Long, Long>> featNameAndAnnotType2FeatID =
			new HashMap<String, HashMap<Long, Long>>();
	private Map<Long, String> featID2Name = new HashMap<Long, String>();
	private PersistConfig persistConfig;
	private Map<Long, List<String>> annotTypeID2featureTypes =
			new HashMap<Long, List<String>>();

	public FeatTypeManager(SQLManager aSqlManager,
			PersistConfig aPersistConfig) throws SQLException {
		super(aSqlManager);
  	persistConfig = aPersistConfig;
		createSQLTables();
	}


	public void setAnnotTypeManager(AnnotTypeManager aManager) {
		annotTypeManager = aManager;
	}


	@Override public String getTableName() {
		return persistConfig.getFeatTypeTableName();
	}


	@Override protected void readResult(ResultSet resultSet) {
	}


	@Override protected String getCreateTableString() {
		String command;
		String tableName = getTableName();
		command = "CREATE TABLE " + tableName + "(\n";
		if (sqlManager.microsoftSQL()) {
			command += "feat_type_ID BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY, \n";
		} else {
			command += "feat_type_ID BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY, \n";
		}
		command += "annot_type_ID BIGINT, \n" +
		"name varchar(200), \n" +
		"feat_type varchar(200) ";
		if (sqlManager.microsoftSQL()) {
			command +=
				")\n " +
				"ALTER TABLE " + tableName + " " +
				"ADD CONSTRAINT " + tableName + "_name_annotID_unique UNIQUE (name, annot_type_ID); \n" +
				"CREATE INDEX Index_" + tableName + "_feat_type_ID ON " +
				tableName + " (feat_type_ID); \n" +
				"CREATE INDEX Index_" + tableName + "_name ON " +
				tableName + " (name); \n";
		} else {
			command += ", \n" +
			"CONSTRAINT " + tableName + "_name_annotID_unique UNIQUE (name, annot_type_ID), \n" +
			"INDEX Index_" + tableName + "_type_ID (annot_type_ID), \n" +
			"INDEX Index_" + tableName + "_name (name) \n" +
			")";
		}
		return command;
	}


	public String getFeatureTypeName(long aFeatTypeID) throws PersistException {
    PreparedStatement st;
    String command = "";
    ResultSet resultSet;
    String result = null;

    if (featID2Name.containsKey(aFeatTypeID)) {
    	return featID2Name.get(aFeatTypeID);
    }
    try {
      command += "SELECT name FROM " + getTableName() + " WHERE feat_type_ID=?";
      st = sqlManager.createPreparedStatement(command);
      st.setLong(1, aFeatTypeID);
      resultSet = st.executeQuery();
      if (resultSet.next()) {
      	result = resultSet.getString("name");
      	featID2Name.put(aFeatTypeID, result);
      } else {
        st.close();
      	throw new PersistException("FeatureType with aFeatTypeID " +
      			aFeatTypeID + " does not exist.");
      }
      st.close();
    } catch (SQLException e) {
			throw new PersistException(e);
    }
    return result;
  }


	public long getFeatureTypeID(String aFeatName, long annot_type_ID) throws PersistException {
    PreparedStatement st;
    String command = "";
    ResultSet resultSet;
    long result = Long.MIN_VALUE;

    if (featNameAndAnnotType2FeatID.containsKey(aFeatName) &&
    		featNameAndAnnotType2FeatID.get(aFeatName).containsKey(annot_type_ID)) {
    	return featNameAndAnnotType2FeatID.get(aFeatName).get(annot_type_ID);
    }
    try {
    	List<Long> parentIDs = annotTypeManager.getParentsIncludingMe(annot_type_ID);
    	String attrTypeIDs = StringUtilsUniWue.concatLongs(parentIDs, ", ");
      command += "select feat_type_ID from " + getTableName() +
      " where name='" + aFeatName.replaceAll("'", "''") + "' and annot_type_ID in (" + attrTypeIDs + ")";
      st = sqlManager.createPreparedStatement(command);
      resultSet = st.executeQuery();
      if (resultSet.next()) {
      	result = resultSet.getLong("feat_type_ID");
      	if (!featNameAndAnnotType2FeatID.containsKey(aFeatName)) {
      		featNameAndAnnotType2FeatID.put(aFeatName, new HashMap<Long, Long>());
      	}
      	featNameAndAnnotType2FeatID.get(aFeatName).put(annot_type_ID, result);
      } else {
        st.close();
      	throw new PersistException("FeatureType with name " +
      			aFeatName + " for annot_type_id " + annot_type_ID + " does not exist.");
      }
      st.close();
    } catch (SQLException e) {
			throw new PersistException(e);
    }
    return result;
  }


	public void insertFeatureType(String featureName, long annotTypeID,
			String type) throws PersistException {
  	PreparedStatement st;
  	String command = "";

  	try {
  		if (sqlManager.microsoftSQL()) {
    		command +=
    			"IF NOT EXISTS (SELECT * FROM " + getTableName() +
    			" WHERE name='" + featureName.replaceAll("'", "''") + "' AND " +
    			"annot_type_ID=" + annotTypeID + ") INSERT ";
  		} else {
  			command += "INSERT IGNORE ";
  		}
  		command += "INTO " + getTableName() + " " +
  				"(name, annot_type_ID, feat_type) VALUES (?, ?, ?)";
  		st = sqlManager.createPreparedStatement(command);
  		st.setString(1, featureName);
  		st.setLong(2, annotTypeID);
  		st.setString(3, type);
  		st.execute();
  		st.close();
  	} catch (SQLException e) {
			throw new PersistException(e);
  	}
	}


	public void createFeatureType(TypeDescription type, long annotTypeID) throws PersistException {
    PreparedStatement st;
    String command = "";
    ResultSet resultSet;

    try {
      command = "SELECT name, feat_type FROM " +
      	getTableName() + " WHERE annot_type_id=?";
      st = sqlManager.createPreparedStatement(command);
      st.setLong(1, annotTypeID);
      resultSet = st.executeQuery();
      while (resultSet.next()) {
      	String name = resultSet.getString("name");
      	String feat_type = resultSet.getString("feat_type");
      	if (feat_type.contains("[]")) {
      		String elementType = feat_type.replaceAll("\\[\\]", "");
      		type.addFeature(name, "", "uima.cas.FSArray", elementType, false);
      	} else {
      		type.addFeature(name, "", feat_type);
      	}
      }
      st.close();
    } catch (SQLException e) {
			throw new PersistException(e);
    }
	}


	public List<String> getFeatureTypes(long annotTypeID) throws PersistException {
    PreparedStatement st;
    String command = "";
    ResultSet resultSet;
    List<String> result = new ArrayList<String>();

    if (annotTypeID2featureTypes.containsKey(annotTypeID)) {
    	return annotTypeID2featureTypes.get(annotTypeID);
    }
    try {
      command = "SELECT feat_type FROM " +
      	getTableName() + " WHERE annot_type_id=?";
      st = sqlManager.createPreparedStatement(command);
      st.setLong(1, annotTypeID);
      resultSet = st.executeQuery();
      while (resultSet.next()) {
      	String feat_type = resultSet.getString("feat_type");
      	result.add(feat_type);
      }
      annotTypeID2featureTypes.put(annotTypeID, result);
      st.close();
      return result;
    } catch (SQLException e) {
			throw new PersistException(e);
    }
	}



}
