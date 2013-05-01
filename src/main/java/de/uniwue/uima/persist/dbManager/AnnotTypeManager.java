package de.uniwue.uima.persist.dbManager;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.uima.resource.metadata.TypeDescription;
import org.apache.uima.resource.metadata.TypeSystemDescription;

import de.uniwue.misc.sql.DatabaseManager;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.uima.persist.PersistConfig;
import de.uniwue.uima.persist.PersistException;

public class AnnotTypeManager extends DatabaseManager {

	public static String annotationTypeName = "uima.tcas.Annotation";
	public static String documentAnnotationTypeName = "uima.tcas.DocumentAnnotation";

	private Map<String, Long> name2TypeID = new HashMap<String, Long>();
	private Map<Long, String> typeID2Name = new HashMap<Long, String>();
	private Map<Long, Long> parents = new HashMap<Long, Long>();
	private Map<Long, Set<Long>> children = new HashMap<Long, Set<Long>>();
	private Map<Long, Boolean> isDocumentAnnotSiblingMap = new HashMap<Long, Boolean>();
	private Set<String> cachedAnnotTypeNames;
	private FeatTypeManager featTypeManager;
	private DocManager docManager;
	private AnnotInstManager annotInstManager;
	private PersistConfig persistConfig;


	public AnnotTypeManager(SQLManager aSqlManager, FeatTypeManager aFeatTypeManager,
			DocManager aDocManager, PersistConfig aPersistConfig) throws SQLException {
		super(aSqlManager);
  	persistConfig = aPersistConfig;
		featTypeManager = aFeatTypeManager;
		docManager = aDocManager;
		createSQLTables();
		addDocumentAnnotType();
	}


	private void addDocumentAnnotType() {
		try {
			insertAnnotType(documentAnnotationTypeName, 0);
		} catch (PersistException e) {
			e.printStackTrace();
		}
	}


	@Override public String getTableName() {
		return persistConfig.getAnnotTypeTableName();
	}


	@Override protected void readResult(ResultSet resultSet) {
	}


	public void setAnnotInstManager(AnnotInstManager anAnnotInstManager) {
		annotInstManager = anAnnotInstManager;
	}


	@Override protected String getCreateTableString() {
		String command;
		String tableName = getTableName();
		command = "CREATE TABLE " + tableName + "(\n";
		if (sqlManager.microsoftSQL()) {
			command += "annot_type_ID BIGINT IDENTITY(1, 1) NOT NULL PRIMARY KEY, \n";
		} else {
			command += "annot_type_ID BIGINT AUTO_INCREMENT NOT NULL PRIMARY KEY, \n";
		}
		command += "name varchar(200) NOT NULL, \n" +
				"parent_ID bigint ";
		if (sqlManager.microsoftSQL()) {
			command +=
					")\n " +
							"ALTER TABLE " + tableName + " " +
							"ADD CONSTRAINT " + tableName + "_name_unique UNIQUE (name); \n" +
							"CREATE INDEX Index_" + tableName + "_parent_ID ON " +
							tableName + " (parent_ID); \n" +
							"CREATE INDEX Index_" + tableName + "_name ON " +
							tableName + " (name); \n";
		} else {
			command += ", \n" +
					"CONSTRAINT " + tableName + "_name_unique UNIQUE (name), \n" +
					"INDEX Index_" + tableName + "_parent_ID (parent_ID), \n" +
					"INDEX Index_" + tableName + "_name (name) \n" +
					")";
		}
		return command;
	}


	public long insertAnnotType(String name, long parentID) throws PersistException {
		PreparedStatement st;
		String command = "";

		try {
			if (sqlManager.microsoftSQL()) {
				command += "IF NOT EXISTS (SELECT * FROM " + getTableName() +
						" WHERE name='" + name.replaceAll("'", "''") + "') INSERT ";
			} else {
				command += "INSERT IGNORE ";
			}
			command += "INTO " + getTableName() + " (name, parent_ID) VALUES (?, ?)";
			st = sqlManager.createPreparedStatementReturnGeneratedKey(command);
			st.setString(1, name);
			st.setLong(2, parentID);
			st.execute();
			long genID = getGeneratedLongKey(st);
			if (genID == 0) {
				genID = getAnnotTypeID(name);
			} else {
				if (cachedAnnotTypeNames != null) {
					cachedAnnotTypeNames.add(name);
				}
			}
			st.close();
			return genID;
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public long getAnnotTypeID(String name) throws PersistException {
		return getAnnotTypeID(name, true);
	}


	public long getAnnotTypeID(String name, boolean complainIfNotExists)
			throws PersistException {
		PreparedStatement st;
		String command = "";
		ResultSet resultSet;
		long result;

		if (name2TypeID.containsKey(name)) {
			return name2TypeID.get(name);
		}
		try {
			command += "SELECT annot_type_ID FROM " + getTableName() + " WHERE name=?";
			st = sqlManager.createPreparedStatement(command);
			st.setString(1, name);
			resultSet = st.executeQuery();
			if (resultSet.next()) {
				result = resultSet.getLong("annot_type_ID");
				name2TypeID.put(name, result);
			} else {
				if (complainIfNotExists) {
					resultSet.close();
					st.close();
					throw new PersistException("AnnotType with name '" + name + "' does not exist.");
				} else {
					result = 0;
				}
			}
			resultSet.close();
			st.close();
			return result;
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public List<Long> getParentsIncludingMe(long anAttrID) throws PersistException {
		List<Long> result = new ArrayList<Long>();
		long currentParent = anAttrID;
		while (currentParent != 0) {
			result.add(currentParent);
			currentParent = getAnnotParentID(currentParent);
		}
		return result;
	}


	public Set<Long> getSiblingsIncludingMe(long anAttrID) throws PersistException {
		Set<Long> children = getAnnotChildIDs(anAttrID);
		Set<Long> result = new HashSet<Long>(children);

		for (Long anID : children) {
			Set<Long> subChildren = getSiblingsIncludingMe(anID);
			result.addAll(subChildren);
		}
		result.add(anAttrID);
		return result;
	}


	private long getAnnotParentID(long anAttrID) throws PersistException {
		PreparedStatement st;
		String command = "";
		ResultSet resultSet;
		long result;

		if (parents.containsKey(anAttrID)) {
			return parents.get(anAttrID);
		}
		try {
			command += "SELECT parent_ID FROM " + getTableName() + " WHERE annot_type_ID=?";
			st = sqlManager.createPreparedStatement(command);
			st.setLong(1, anAttrID);
			resultSet = st.executeQuery();
			if (resultSet.next()) {
				result = resultSet.getLong("parent_ID");
				parents.put(anAttrID, result);
			} else {
				throw new PersistException("AnnotType with ID " + anAttrID + " does not exist.");
			}
			st.close();
			return result;
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	private Set<Long> getAnnotChildIDs(long anAttrID) throws PersistException {
		PreparedStatement st;
		String command = "";
		ResultSet resultSet;
		Set<Long> result = new HashSet<Long>();

		if (children.containsKey(anAttrID)) {
			return children.get(anAttrID);
		}
		try {
			command += "SELECT annot_type_ID FROM " + getTableName() + " WHERE parent_ID=?";
			st = sqlManager.createPreparedStatement(command);
			st.setLong(1, anAttrID);
			resultSet = st.executeQuery();
			while (resultSet.next()) {
				Long anID = resultSet.getLong("annot_type_ID");
				result.add(anID);
			}
			children.put(anAttrID, result);
			st.close();
			return result;
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public String getAnnotTypeName(long anID) throws PersistException {
		PreparedStatement st;
		String command = "";
		ResultSet resultSet;
		String result = null;

		if (typeID2Name.containsKey(anID)) {
			return typeID2Name.get(anID);
		}
		try {
			command += "SELECT name FROM " + getTableName() + " WHERE annot_type_ID =?";
			st = sqlManager.createPreparedStatement(command);
			st.setLong(1, anID);
			resultSet = st.executeQuery();
			if (resultSet.next()) {
				result = resultSet.getString("name");
				typeID2Name.put(anID, result);
			} else {
				try {
					throw new PersistException("AnnotType with ID '" + anID + "' does not exist.");
				} catch (PersistException e) {
					e.printStackTrace();
				}
			}
			st.close();
			return result;
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public Set<String> getAnnotTypeNames() throws PersistException {
		PreparedStatement st;
		String command = "";
		ResultSet resultSet;
		HashSet<String> result = new HashSet<String>();

		if (cachedAnnotTypeNames != null) {
			return cachedAnnotTypeNames;
		}
		try {
			command += "select name from " + getTableName();
			st = sqlManager.createPreparedStatement(command);
			resultSet = st.executeQuery();
			while (resultSet.next()) {
				String aName = resultSet.getString("name");
				result.add(aName);
			}
			st.close();
			cachedAnnotTypeNames = result;
			return result;
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void createAnnotType(String annotName, TypeSystemDescription tsDescr) throws PersistException {
		PreparedStatement st;
		String command = "";
		ResultSet resultSet;

		try {
			command = "SELECT annot_type_ID, parent_ID FROM " +
					getTableName() + " WHERE name=?";
			st = sqlManager.createPreparedStatement(command);
			st.setString(1, annotName);
			resultSet = st.executeQuery();
			if (resultSet.next()) {
				long parentID = resultSet.getLong("parent_ID");
				long annotTypeID = resultSet.getLong("annot_type_ID");
				String parentName;
				if (parentID == 0) {
					parentName = annotationTypeName;
				} else {
					parentName = getAnnotTypeName(parentID);
				}
				TypeDescription type = tsDescr.addType(annotName, annotName, parentName);
				featTypeManager.createFeatureType(type, annotTypeID);
			} else {
				try {
					throw new PersistException("AnnotType with name '" + annotName + "' does not exist.");
				} catch (PersistException e) {
					e.printStackTrace();
				}
			}
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
	}


	public void resolveFeaturesReferencingYetUnknownTypes(
			Set<String> annotTypeNames) throws PersistException {
		boolean somethingChanged;
		do {
			somethingChanged = false;
			for (String anAnnotTypeName : annotTypeNames.toArray(new String[0])) {
				long annotTypeID = getAnnotTypeID(anAnnotTypeName);
				List<String> featureTypes = featTypeManager.getFeatureTypes(annotTypeID);
				for (String aFeatureType : featureTypes) {
					if (getAnnotTypeNames().contains(aFeatureType) &&
							!annotTypeNames.contains(aFeatureType) && !aFeatureType.equals(annotationTypeName)) {
						somethingChanged = true;
						annotTypeNames.add(aFeatureType);
					}
				}
			}
		} while (somethingChanged);
	}


	public void resolveParentAnnotTypes(Set<String> typeNames) throws PersistException {
		for (String aTypeName : typeNames.toArray(new String[0])) {
			long annotTypeID = getAnnotTypeID(aTypeName);
			List<Long> parentsAndMe = getParentsIncludingMe(annotTypeID);
			for (Long anAttrTypeID : parentsAndMe) {
				if (anAttrTypeID != annotTypeID) {
					String parentName = getAnnotTypeName(anAttrTypeID);
					if (!parentName.equals(annotationTypeName) && !typeNames.contains(parentName)) {
						typeNames.add(parentName);
					}
				}
			}
		}
	}


	public Set<String> getAnnotTypeNames(List<String> extDocIDs, String colID) throws PersistException {
		Set<String> result = new HashSet<String>();
		PreparedStatement st;
		String command;
		ResultSet resultSet;

		try {
			String extDocIDString = "";
			for (String anID : extDocIDs) {
				extDocIDString += anID.replaceAll("'", "''") + ", ";
			}
			if (extDocIDString.length() > 0) {
				extDocIDString = extDocIDString.replaceAll(", $", "");
			}
			command = "SELECT name FROM " + annotInstManager.getTableName() +
					", " + docManager.getTableName() + "," + getTableName() +
					" WHERE extID IN (" + extDocIDString + ") AND " +
					getTableName() + ".doc_ID=" + docManager.getTableName() + ".doc_ID AND " +
					docManager.getTableName() + ".colID=? AND " +
					getTableName() + ".annot_type_ID=" + getTableName() + ".annot_type_ID";
			st = sqlManager.createPreparedStatement(command);
			st.setString(1, colID);
			resultSet = st.executeQuery();
			while (resultSet.next()) {
				String typeName = resultSet.getString("name");
				result.add(typeName);
			}
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
		resolveFeaturesReferencingYetUnknownTypes(result);
		resolveParentAnnotTypes(result);
		return result;
	}


	public Set<String> getAnnotTypeNamesByCol(String colID) throws PersistException {
		Set<String> result = new HashSet<String>();
		PreparedStatement st;
		String command;
		ResultSet resultSet;

		try {
			command = "SELECT DISTINCT(name) FROM " + annotInstManager.getTableName() +
					", " + docManager.getTableName() + "," + getTableName() +
					" WHERE col_ID=? AND " +
					annotInstManager.getTableName() + ".doc_ID=" +
					docManager.getTableName() + ".doc_ID AND " +
					annotInstManager.getTableName() + ".annot_type_ID=" +
					getTableName() + ".annot_type_ID";
			st = sqlManager.createPreparedStatement(command);
			st.setString(1, colID);
			resultSet = st.executeQuery();
			while (resultSet.next()) {
				String typeName = resultSet.getString("name");
				result.add(typeName);
			}
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
		resolveFeaturesReferencingYetUnknownTypes(result);
		resolveParentAnnotTypes(result);
		return result;
	}



	public Set<String> getAnnotTypeNamesByNamespaceRange(String range) throws PersistException {
		Set<String> result = new HashSet<String>();
		PreparedStatement st;
		String command;
		ResultSet resultSet;

		try {
			range = range.replaceAll("\\*", "%");
			command = "SELECT DISTINCT(name) FROM " + getTableName() +
					" WHERE name LIKE '" + range.replaceAll("'", "''") + "'";
			st = sqlManager.createPreparedStatement(command);
			resultSet = st.executeQuery();
			while (resultSet.next()) {
				String typeName = resultSet.getString("name");
				result.add(typeName);
			}
			st.close();
		} catch (SQLException e) {
			throw new PersistException(e);
		}
		resolveFeaturesReferencingYetUnknownTypes(result);
		resolveParentAnnotTypes(result);
		return result;
	}


	public boolean isDocumentAnnotSibling(Long anAttrID) throws PersistException {
		if (isDocumentAnnotSiblingMap.containsKey(anAttrID)) {
			return isDocumentAnnotSiblingMap.get(anAttrID);
		} else {
			List<Long> parents = getParentsIncludingMe(anAttrID);
			long docAnnTypeID = getAnnotTypeID(documentAnnotationTypeName);
			boolean result = parents.contains(docAnnTypeID);
			isDocumentAnnotSiblingMap.put(anAttrID, result);
			return result;
		}
	}

}
