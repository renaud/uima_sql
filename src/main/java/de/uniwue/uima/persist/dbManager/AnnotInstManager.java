package de.uniwue.uima.persist.dbManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.text.AnnotationFS;

import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.util.StringUtilsUniWue;
import de.uniwue.uima.persist.PersistConfig;
import de.uniwue.uima.persist.PersistException;

public class AnnotInstManager extends DatabaseManager {

	private AnnotTypeManager annotTypeManager;
	private FeatInstManager featInstManager;
	private DocManager docManager;
	private PersistConfig persistConfig;


	public AnnotInstManager(SQLManager aSqlManager,
			AnnotTypeManager anAnnotTypeManager,
			FeatInstManager aFeatInstManager,
			DocManager aDocManager, PersistConfig aPersistConfig) throws SQLException {
		super(aSqlManager);
  	persistConfig = aPersistConfig;
		docManager = aDocManager;
		annotTypeManager = anAnnotTypeManager;
		featInstManager = aFeatInstManager;
		createSQLTables();
	}


	@Override public String getTableName() {
		return persistConfig.getAnnotInstTableName();
	}


	@Override protected void readResult(ResultSet resultSet) {
	}


	@Override protected String getCreateTableString() {
		String command;
		String tableName = getTableName();
		command = "CREATE TABLE " + tableName + "(\n";
		if (sqlManager.microsoftSQL()) {
			command += "annot_inst_ID BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY, \n";
		} else {
			command += "annot_inst_ID BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY, \n";
		}
		command += "beginOffset INT, \n" +
				"endOffset INT, \n" +
				"annot_type_ID BIGINT, \n" +
				"doc_ID BIGINT, \n" +
				"covered varchar(1000) \n";
		if (sqlManager.microsoftSQL()) {
			command +=
					")\n " +
							"CREATE INDEX Index_" + tableName + "_beginOffset ON " +
							tableName + " (beginOffset); \n" +
							"CREATE INDEX Index_" + tableName + "_annot_type_ID ON " +
							tableName + " (annot_type_ID); \n" +
							"CREATE INDEX Index_" + tableName + "_doc_ID ON " +
							tableName + " (doc_ID); \n" +
							"CREATE INDEX Index_" + tableName + "_covered ON " +
							tableName + " (covered); \n";
		} else {
			command += ", \n" +
					"INDEX Index_" + tableName + "_type_begin_end_doc (annot_type_ID, beginOffset, endOffset, doc_ID),\n" +
					"INDEX Index_" + tableName + "_beginOffset (beginOffset),\n" +
					"INDEX Index_" + tableName + "_endOffset (endOffset),\n" +
					"INDEX Index_" + tableName + "_annot_type_ID (annot_type_ID),\n" +
					"INDEX Index_" + tableName + "_doc_ID (doc_ID),\n" +
					"INDEX Index_" + tableName + "_covered (covered)\n" +
					")";
		}
		return command;
	}


	public long insertAnnotInst(long annot_type_ID, int begin, int end,
			long docID, String coveredText) throws PersistException {
		Statement st;
		String command = "";

		try {
			command += "INSERT INTO " + getTableName() + " " +
					"(beginOffset, endOffset, annot_type_ID, doc_ID, covered) VALUES (" +
					begin + ", " +
					end + ", " +
					annot_type_ID + ", " +
					docID + ", " +
					"'" + coveredText.replaceAll("'", "''") + "'" +
					")";
			st = sqlManager.createStatement();
			st.execute(command, Statement.RETURN_GENERATED_KEYS);
			ResultSet result = st.getGeneratedKeys();
			result.next();
			long genID = result.getInt(1);
			result.close();
			st.close();
			return genID;
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public long getAnnotInstID(Long annot_type_ID, int begin, int end,
			long docID) throws PersistException {
		PreparedStatement st;
		String command;
		ResultSet resultSet;
		long result;

		try {
			command = "SELECT annot_inst_ID FROM " + getTableName() + " WHERE " +
					"annot_type_ID=? AND beginOffset=? AND endOffset=? AND doc_ID=?";
			st = sqlManager.createPreparedStatement(command);
			st.setLong(1, annot_type_ID);
			st.setInt(2, begin);
			st.setInt(3, end);
			st.setLong(4, docID);
			resultSet = st.executeQuery();
			if (resultSet.next()) {
				result = resultSet.getLong("annot_inst_ID");
			} else {
				result = Long.MIN_VALUE;
				throw new PersistException("AnnotInst does not exist");
			}
			st.close();
			return result;
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	private HashMap<Long, AnnotationFS> readCreatedAnnots(
			ResultSet resultSet, CAS cas) throws PersistException {
		HashMap<Long, AnnotationFS> createdAnnots = new HashMap<Long, AnnotationFS>();

		try {
			while (resultSet.next()) {
				long instID = resultSet.getLong("annot_inst_ID");
				int begin = resultSet.getInt("beginOffset");
				int end = resultSet.getInt("endOffset");
				long typeID = resultSet.getLong("annot_type_ID");
				String typeName = annotTypeManager.getAnnotTypeName(typeID);
				Type aType = cas.getTypeSystem().getType(typeName);
				if (aType == null) {
					// das ist gefährlich !
					// wenn der Typ nicht im TypSystem vorhanden ist, kann es auf einen Fehler hinweisen
					// es könnte z.B. jedoch auch sein, dass Annotationen dieses Typs erst nach der Erstellung eines
					// Readers der jetzt laden will hinzugefügt wurden
					continue;
				}
				AnnotationFS newAnnot = cas.createAnnotation(aType, begin, end);
				cas.addFsToIndexes(newAnnot);
				createdAnnots.put(instID, newAnnot);
			}
			return createdAnnots;
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void createAnnots(String extDocID, String colID, CAS cas,
			TmpTypeManager tmpTypeManager) throws PersistException {
		PreparedStatement st;
		String command;
		ResultSet resultSet;
		HashMap<Long, AnnotationFS> createdAnnots;

		try {
			command = "SELECT annot_inst_ID, beginOffset, endOffset, annot_type_ID" +
					" FROM " + getTableName() +
					", " + docManager.getTableName() +
					" WHERE extID=? AND col_ID=? AND " + getTableName() +
					".doc_ID=" + docManager.getTableName() + ".doc_ID";
			if (tmpTypeManager != null) {
				command += " AND annot_type_ID IN (SELECT * FROM " + tmpTypeManager.getTableName() + ")";
			}
			st = sqlManager.createPreparedStatement(command);
			st.setString(1, extDocID);
			st.setString(2, colID);
			resultSet = st.executeQuery();
			createdAnnots = readCreatedAnnots(resultSet, cas);
			featInstManager.createFeatures(cas, createdAnnots, extDocID, tmpTypeManager);
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void createAnnots(long aDocID, CAS cas) throws PersistException {
		PreparedStatement st;
		String command;
		ResultSet resultSet;
		HashMap<Long, AnnotationFS> createdAnnots;

		try {
			command = "SELECT annot_inst_ID, beginOffset, endOffset, annot_type_ID" +
					" FROM " + getTableName() + " WHERE doc_ID=?";
			st = sqlManager.createPreparedStatement(command);
			st.setLong(1, aDocID);
			resultSet = st.executeQuery();
			createdAnnots = readCreatedAnnots(resultSet, cas);
			featInstManager.createFeatures(cas, createdAnnots, aDocID);
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void deleteAnnotsForType(long annot_type_ID) throws PersistException {
		PreparedStatement st;
		String command;

		try {
			command = "DELETE FROM " + getTableName() + " WHERE annot_type_ID=?";
			st = sqlManager.createPreparedStatement(command);
			st.setLong(1, annot_type_ID);
			st.execute();
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void deleteAnnotsForDoc(String extID, String colID) throws PersistException {
		PreparedStatement st;
		String command;

		try {
			if (sqlManager.microsoftSQL()) {
				command = "DELETE FROM " + getTableName() + " WHERE annot_inst_ID IN";
			} else {
				command = "DELETE FROM " + getTableName() + " USING " + getTableName()  + " JOIN";
			}
			command += " (\n" +
					"SELECT annot_inst_ID FROM " + getTableName() + ", " +
					docManager.getTableName() + " WHERE " +
					getTableName() + ".doc_ID=" +
					docManager.getTableName() + ".doc_ID and extID=? AND col_ID=?)";
			if (!sqlManager.microsoftSQL()) {
				command += " s ON " + getTableName() + ".annot_inst_ID=s.annot_inst_ID";
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


	public void deleteAnnotsForCol(String colID) throws PersistException {
		PreparedStatement st;
		String command;

		try {
			if (sqlManager.microsoftSQL()) {
				command = "DELETE FROM " + getTableName() + " WHERE annot_inst_ID IN";
			} else {
				command = "DELETE FROM " + getTableName() + " USING " + getTableName()  + " JOIN";
			}
			command += " (\n" +
					"SELECT annot_inst_ID FROM " + getTableName() + ", " +
					docManager.getTableName() + " WHERE " +
					getTableName() + ".doc_ID=" +
					docManager.getTableName() + ".doc_ID AND col_ID=?\n" +
					")";
			if (!sqlManager.microsoftSQL()) {
				command += " s ON " + getTableName() + ".annot_inst_ID=s.annot_inst_ID";
			}
			st = sqlManager.createPreparedStatement(command);
			st.setString(1, colID);
			st.execute();
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void deleteAnnotsForDoc(String extID, String colID,
			long annot_type_ID) throws PersistException {
		PreparedStatement st;
		String command;

		try {
			if (sqlManager.microsoftSQL()) {
				command = "DELETE FROM " + getTableName() + " WHERE annot_inst_ID IN";
			} else {
				command = "DELETE FROM " + getTableName() + " USING " + getTableName()  + " JOIN";
			}
			command += " (\n" +
					"SELECT annot_inst_ID FROM " + getTableName() + ", " +
					docManager.getTableName() + " WHERE " +
					getTableName() + ".doc_ID=" +
					docManager.getTableName() + ".doc_ID AND extID=? AND col_ID=? AND " +
					"annot_type_ID=?" +
					")";
			if (!sqlManager.microsoftSQL()) {
				command += " s ON " + getTableName() + ".annot_inst_ID=s.annot_inst_ID";
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


	public void deleteAnnotsForDoc(String extID, String colID,
			List<Long> annot_type_IDs) throws PersistException {
		PreparedStatement st;
		String command;

		try {
			String listString = StringUtilsUniWue.concatLongs(annot_type_IDs, ",");
			if (sqlManager.microsoftSQL()) {
				command = "DELETE FROM " + getTableName() + " WHERE annot_inst_ID IN";
			} else {
				command = "DELETE FROM " + getTableName() + " USING " + getTableName()  + " JOIN";
			}
			command += " (\n" +
					"SELECT annot_inst_ID FROM " + getTableName() + ", " +
					docManager.getTableName() + " WHERE " +
					getTableName() + ".doc_ID=" +
					docManager.getTableName() + ".doc_ID AND extID=? AND col_ID=? AND " +
					"annot_type_ID IN (" + listString + ")" +
					")";
			if (!sqlManager.microsoftSQL()) {
				command += " s ON " + getTableName() + ".annot_inst_ID=s.annot_inst_ID";
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
