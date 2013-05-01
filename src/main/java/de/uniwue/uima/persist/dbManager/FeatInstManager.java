package de.uniwue.uima.persist.dbManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.text.AnnotationFS;

import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.util.StringUtilsUniWue;
import de.uniwue.uima.persist.PersistConfig;
import de.uniwue.uima.persist.PersistException;

public class FeatInstManager extends DatabaseManager {


	private DocManager docManager;
	private AnnotInstManager annotInstManager;
	private FeatTypeManager featTypeManager;
	private AnnotTypeManager annotTypeManager;
	private PersistConfig persistConfig;


  public FeatInstManager(SQLManager aSqlManager,
  		FeatTypeManager aFeatTypeManager,
  		DocManager aDocManager, PersistConfig aPersistConfig) throws SQLException {
  	super(aSqlManager);
  	persistConfig = aPersistConfig;
  	featTypeManager = aFeatTypeManager;
  	docManager = aDocManager;
  	createSQLTables();
  }


  public void setAnnotInstManager(AnnotInstManager anAnnotInstManager, AnnotTypeManager anAnnotTypeManager) {
  	annotInstManager = anAnnotInstManager;
  	annotTypeManager = anAnnotTypeManager;
  }


	@Override public String getTableName() {
		return persistConfig.getFeatInstTableName();
	}


	@Override protected void readResult(ResultSet resultSet) {
	}


	@Override protected String getCreateTableString() {
		String command;
		String tableName = getTableName();
		command = "CREATE TABLE " + tableName + "(\n";
		if (sqlManager.microsoftSQL()) {
			command += "feat_inst_ID BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY, \n";
		} else {
			command += "feat_inst_ID BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY, \n";
		}
		command += "annot_inst_ID BIGINT, \n" +
		"feat_type_ID BIGINT, \n" +
		"value varchar(1000) \n";
		if (sqlManager.microsoftSQL()) {
			command += ")\n " +
			"ALTER TABLE " + tableName + " " +
			"ADD CONSTRAINT " + tableName + "_annot_inst_feat_type_unique UNIQUE (feat_type_ID, annot_inst_ID); \n" +
			"CREATE INDEX Index_" + tableName + "_annot_inst_ID ON " +
			tableName + " (annot_inst_ID); \n" +
			"CREATE INDEX Index_" + tableName + "_feat_type_ID ON " +
			tableName + " (feat_type_ID); \n" +
			"CREATE INDEX Index_" + tableName + "_value ON " +
			tableName + " (value); ";
		} else {
			command += ", \n" +
			"CONSTRAINT " + tableName + "_annot_inst_feat_type_unique UNIQUE (feat_type_ID, annot_inst_ID), \n" +
			"INDEX Index_" + tableName + "_annot_inst_ID (annot_inst_ID),\n" +
			"INDEX Index_" + tableName + "_feat_type_ID (feat_type_ID),\n" +
			"INDEX Index_" + tableName + "_value (value)\n" +
			")";
		}
		return command;
	}


	public void insertFeature(long annot_inst_ID, long feat_type_ID, String value) throws PersistException {
  	PreparedStatement st;
  	String command = "";

  	try {
  		if (sqlManager.microsoftSQL()) {
    		command +=
    			"IF NOT EXISTS (SELECT * FROM " + getTableName() + " WHERE" +
    			" annot_inst_ID=" + annot_inst_ID + " AND feat_type_ID=" + feat_type_ID +
    			") INSERT ";
  		} else {
  			command += "INSERT IGNORE ";
  		}
  		command += "INTO " + getTableName() +
  			" (feat_type_ID, value, annot_inst_ID) VALUES (?, ?, ?)";
  		st = sqlManager.createPreparedStatement(command);
  		st.setLong(1, feat_type_ID);
  		st.setString(2, value);
  		st.setLong(3, annot_inst_ID);
  		st.execute();
  		st.close();
  	} catch (SQLException e) {
			throw new PersistException(e);
  	}
	}


	private void readCreatedFeatures(ResultSet resultSet, CAS cas,
			HashMap<Long, AnnotationFS> createdAnnots) throws PersistException {
		try {
			while (resultSet.next()) {
				long annotInstID = resultSet.getLong("annot_inst_ID");
				long featTypeID = resultSet.getLong("feat_type_ID");
				String value = resultSet.getString("value");
				AnnotationFS anAnnot = createdAnnots.get(annotInstID);
				if (anAnnot == null) {
					// das ist gefährlich !
					// es könnte auf einen Fehler hinweisen
					// es könnte jedoch auch sein, dass nach der Erstellung eines Readers
					// neue Annotationen hinzugefügt wurde, dessen Typen der Reader für die Annotationen noch nicht kannte
					continue;
				}
				String featName = featTypeManager.getFeatureTypeName(featTypeID);
				String fullFeatName = anAnnot.getType().getName() + ":" + featName;
				Feature aFeature = cas.getTypeSystem().getFeatureByFullName(fullFeatName);
				if (aFeature == null) {
					continue;
				}
				String name = aFeature.getRange().getName();
				if (name.equals("uima.cas.String")) {
					anAnnot.setStringValue(aFeature, value);
				} else if (name.equals("uima.cas.Boolean")) {
					anAnnot.setBooleanValue(aFeature, Boolean.parseBoolean(value));
				} else if (name.equals("uima.cas.Integer")) {
					anAnnot.setIntValue(aFeature, Integer.parseInt(value));
				} else if (name.equals("uima.cas.Double")) {
					anAnnot.setDoubleValue(aFeature, Double.parseDouble(value));
				} else if (name.equals("uima.cas.Long")) {
					anAnnot.setLongValue(aFeature, Long.parseLong(value));
				} else if (annotTypeManager.getAnnotTypeNames().contains(name)) {
					long anInstID = Long.parseLong(value);
					AnnotationFS referencesAnnotInst = createdAnnots.get(anInstID);
					anAnnot.setFeatureValue(aFeature, referencesAnnotInst);
				} else {
					throw new PersistException("unknown type '" + name + "'");
				}
			}
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void createFeatures(CAS cas, HashMap<Long, AnnotationFS> createdAnnots,
			String extDocID, TmpTypeManager tmpTypeManager) throws PersistException {
		PreparedStatement st;
		String command;
		ResultSet resultSet;

		try {
			command =
				"SELECT value, " +
				getTableName() + ".annot_inst_ID, feat_type_ID" +
				" FROM " +
				getTableName() + ", " +
				annotInstManager.getTableName() + ", " +
				docManager.getTableName() +
				" WHERE extID=? AND " +
				getTableName() + ".annot_inst_ID=" +
				annotInstManager.getTableName() + ".annot_inst_ID AND " +
				annotInstManager.getTableName() + ".doc_ID=" +
				docManager.getTableName() + ".doc_ID";
			if (tmpTypeManager != null) {
				command += " AND annot_type_ID IN (SELECT * FROM " + tmpTypeManager.getTableName() + ")";
			}
			st = sqlManager.createPreparedStatement(command);
			st.setString(1, extDocID);
			resultSet = st.executeQuery();
			readCreatedFeatures(resultSet, cas, createdAnnots);
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void createFeatures(CAS cas,
			HashMap<Long, AnnotationFS> createdAnnots, Long aDocID) throws PersistException {
		PreparedStatement st;
		String command;
		ResultSet resultSet;

		try {
			command =
				"SELECT value, " + getTableName() + ".annot_inst_ID, feat_type_ID" +
				" FROM " + getTableName() + ", " + annotInstManager.getTableName() +
				" WHERE doc_ID=? AND " + getTableName() + ".annot_inst_ID=" +
				annotInstManager.getTableName() + ".annot_inst_ID";
			st = sqlManager.createPreparedStatement(command);
			st.setLong(1, aDocID);
			resultSet = st.executeQuery();
			readCreatedFeatures(resultSet, cas, createdAnnots);
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void deleteFeatsForAnnotsForType(long annot_type_ID) throws PersistException {
    PreparedStatement st;
    String command;

    try {
      command =
      	"DELETE FROM " + getTableName() + " WHERE feat_inst_ID in (" +
      	"SELECT feat_inst_ID from " + getTableName() + ", " +
      	annotInstManager.getTableName() + " WHERE " +
      	getTableName() + ".annot_inst_ID=" +
      	annotInstManager.getTableName() + ".annot_inst_ID AND annot_type_ID=?)";
      st = sqlManager.createPreparedStatement(command);
      st.setLong(1, annot_type_ID);
      st.execute();
      st.close();
    } catch (SQLException e) {
			throw new PersistException(e);
    }
	}


	public void deleteFeatsForDoc(String extID, String colID) throws PersistException {
		PreparedStatement st;
		String command;

		try {
			if (sqlManager.microsoftSQL()) {
				command = "delete from " + getTableName() + " where feat_inst_ID IN";
			} else {
				command = "delete from " + getTableName() + " using " + getTableName()  + " JOIN";
			}
			command += " (\n" +
					"select feat_inst_ID from " + getTableName() + ", " +
					docManager.getTableName() + "," +
					annotInstManager.getTableName() + " where " +
					annotInstManager.getTableName() + ".doc_ID=" +
					docManager.getTableName() + ".doc_ID and " +
					annotInstManager.getTableName() + ".annot_inst_ID=" +
					getTableName() + ".annot_inst_ID and extID=? and col_ID=?)";
			if (!sqlManager.microsoftSQL()) {
				command += " s on " + getTableName() + ".feat_inst_ID=s.feat_inst_ID";
			}
			st = sqlManager.createPreparedStatement(command);
			st.setString(1, extID);
			st.setString(2, colID);
			st.execute();
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void deleteFeatsForCol(String colID) throws PersistException {
		PreparedStatement st;
		String command;

		try {
			if (sqlManager.microsoftSQL()) {
				command = "DELETE FROM " + getTableName() + " WHERE feat_inst_ID IN";
			} else {
				command = "DELETE FROM " + getTableName() + " USING " + getTableName()  + " JOIN";
			}
			command += " (\n" +
					"SELECT feat_inst_ID FROM " + getTableName() + ", " +
					docManager.getTableName() + "," +
					annotInstManager.getTableName() + " WHERE \n" +
					annotInstManager.getTableName() + ".doc_ID=" +
					docManager.getTableName() + ".doc_ID AND " +
					annotInstManager.getTableName() + ".annot_inst_ID=" +
					getTableName() + ".annot_inst_ID AND col_ID=? \n" +
					")";
			if (!sqlManager.microsoftSQL()) {
				command += " s ON " + getTableName() + ".feat_inst_ID=s.feat_inst_ID";
			}
			st = sqlManager.createPreparedStatement(command);
			st.setString(1, colID);
			st.execute();
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void deleteFeatsForDoc(String extID, String colID, long annot_type_ID) throws PersistException {
		PreparedStatement st;
		String command;

		try {
			if (sqlManager.microsoftSQL()) {
				command = "DELETE FROM " + getTableName() + " WHERE feat_inst_ID IN";
			} else {
				command = "DELETE FROM " + getTableName() + " USING " + getTableName()  + " JOIN";
			}
			command += " (\n" +
					"SELECT feat_inst_ID FROM " + getTableName() + ", " +
					docManager.getTableName() + "," +
					annotInstManager.getTableName() + " WHERE \n" +
					annotInstManager.getTableName() + ".doc_ID=" +
					docManager.getTableName() + ".doc_ID AND " +
					annotInstManager.getTableName() + ".annot_inst_ID=" +
					getTableName() + ".annot_inst_ID AND extID=? AND col_ID=? AND annot_type_ID=?" +
					")";
			if (!sqlManager.microsoftSQL()) {
				command += " s ON " + getTableName() + ".feat_inst_ID=s.feat_inst_ID";
			}
			st = sqlManager.createPreparedStatement(command);
			st.setString(1, extID);
			st.setString(2, colID);
			st.setLong(3, annot_type_ID);
			st.execute();
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void deleteFeatsForDoc(String extID, String colID,
			List<Long> annot_type_IDs) throws PersistException {
		PreparedStatement st;
		String command;

		try {
			String listString = StringUtilsUniWue.concatLongs(annot_type_IDs, ",");
			if (sqlManager.microsoftSQL()) {
				command = "DELETE FROM " + getTableName() + " WHERE feat_inst_ID IN";
			} else {
				command = "DELETE FROM " + getTableName() + " USING " + getTableName()  + " JOIN";
			}
			command += " (\n" +
					"SELECT feat_inst_ID FROM " + getTableName() + ", " +
					docManager.getTableName() + "," +
					annotInstManager.getTableName() + " WHERE \n" +
					annotInstManager.getTableName() + ".doc_ID=" +
					docManager.getTableName() + ".doc_ID AND " +
					annotInstManager.getTableName() + ".annot_inst_ID=" +
					getTableName() + ".annot_inst_ID AND extID=? AND col_ID=? and " +
					"annot_type_ID IN (" + listString + ")" +
					")";
			if (!sqlManager.microsoftSQL()) {
				command += " s ON " + getTableName() + ".feat_inst_ID=s.feat_inst_ID";
			}
			st = sqlManager.createPreparedStatement(command);
			st.setString(1, extID);
			st.setString(2, colID);
			st.execute();
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}




}
