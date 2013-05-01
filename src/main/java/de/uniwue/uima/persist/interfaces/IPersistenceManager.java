package de.uniwue.uima.persist.interfaces;

import java.util.List;
import java.util.Set;

import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.resource.metadata.TypeSystemDescription;

import de.uniwue.uima.persist.PersistException;
import de.uniwue.uima.persist.data.LoadConfig;
import de.uniwue.uima.persist.data.SaveConfig;

/**
 * A PersistenceManager is a class which can load and save UIMA CASes, their
 * type systems (and respective type system descriptions) in a persistent data layer like a
 * SQL database.
 * The CASes are organized in collections which are sets of documents with an identifying
 * collection ID.
 *
 * @author Georg Fette
 */
public interface IPersistenceManager {

	/**
	 * As the underlying persistent data layer can be a database which is transaction based
	 * certain operations on the persistence manager do not immediately commit the performed
	 * operation on the underlying layer but instead waits until this commit method is explicitly
	 * called. This can improve performance significantly.
	 * @throws PersistException
	 */
	void commit() throws PersistException;

	/**
	 * A new document entry is saved.
	 * This operation does no immediate commit, so the commit has to be performed manually
	 * @param aDocText: the text of the new document
	 * @param extID: the external ID with which the document is identifies. E.g. the filename.
	 * @param colID: the document collection's ID this document is belonging to
	 * @throws PersistException
	 */
	void saveCAS(String aDocText, String extID, String colID) throws PersistException;


	 /** The CAS is stored. The extDocID and colID are obtained from the CAS itself and have to be
	 * contained in the CAS. If those identifiers need to be changed before storage use the persistence
	 * manager's name provider (getNameProvider)
	 * This operation does no immediate commit, so the commit has to be performed manually
	 * @param cas
	 * @param saveConfig: a configuration to determine if all or which UIMA types have to be stored.
	 * @throws PersistException
	 */
	void saveCAS(CAS cas, SaveConfig saveConfig) throws PersistException;

	/** The CAS is stored. As no saveConfig is given, all UIMA types of the given CAS are stored.
	 * This operation does no immediate commit, so the commit has to be performed manually
	 * @param cas
	 * @throws PersistException
	 */
	void saveCAS(CAS cas) throws PersistException;

	/**
	 * Creates an analysis engine which can store CASes.
	 * @param saveConfig: a configuration to determine if all or if not then which UIMA types have to be stored.
	 * @return
	 * @throws PersistException
	 */
	AnalysisEngineDescription createWriterDescription(SaveConfig saveConfig) throws PersistException;

	/**
	 * Creates an analysis engine which can store CASes. When used in a pipeline the analysis engine
	 * stores only those UIMA types which are contained in the given type system description
	 * @param tsd: a description which declares which UIMA types should be stored by the
	 * analysis engine when used in a pipeline.
	 * @return
	 * @throws PersistException
	 */
	AnalysisEngineDescription createWriterDescription(TypeSystemDescription tsd) throws PersistException;


	/**
	 * Loads data into the given CAS determined by the given documents identifiers.
	 * @param extDocID
	 * @param colID
	 * @param cas
	 * @throws PersistException
	 */
	void loadCAS(String extDocID, String colID, CAS cas) throws PersistException;

	/**
	 * Creates a collection reader for the given documents of the given collection ID.
	 * The type system used for loading is automatically created. It consists of all types
	 * of the annotations which are currently stored in the given documents.
	 * @param extDocIDs
	 * @param colID
	 * @return
	 * @throws PersistException
	 */
	CollectionReaderDescription loadCASByExtID(List<String> extDocIDs, String colID) throws PersistException;

	/**
	 * Creates a collection reader for all documents of the given collection ID.
	 * The type system used for loading is automatically created. It consists of all types
	 * of the annotations which are currently stored in the determined documents.
	 * @param colID
	 * @return
	 * @throws PersistException
	 */
	CollectionReaderDescription loadCASByColID(String colID) throws PersistException;

	/**
	 * Creates a collection reader for all documents of the given collection ID.
	 * The type system used for loading is automatically created. It consists of all types
	 * of the annotations which are currently stored in the determined documents.
	 * @param loadConfig: a configuration object to determine what should be loaded.
	 * The config includes the collection id which should be loaded. If set only certain
	 * documents determined by their IDs are loaded. It can (but doesn't need to )
	 * include a type system description to tell what UIMA types should be loaded.
	 * @return
	 * @throws PersistException
	 */
	CollectionReaderDescription loadCAS(LoadConfig loadConfig) throws PersistException;

	/**
	 * @param colID
	 * @return The external IDs of all documents stored for this collection
	 * @throws PersistException
	 */
	String[] getExtDocIDs(String colID) throws PersistException;

	/**
	 * @param extDocIDs
	 * @param colID
	 * @return The list of document texts for the given identifiers
	 * @throws PersistException
	 */
	List<String> getDocText(List<String> extDocIDs, String colID) throws PersistException;

	/**
	 * @param extDocID
	 * @param colID
	 * @return The document text of the document with the given ID from the given collection
	 * @throws PersistException
	 */
	String getDocText(String extDocID, String colID) throws PersistException;


	/**
	 * @return Returns the name provider the persistence provider is using.
	 * The name provider handles the access to the CASes external document IDs and
	 * collection IDs
	 */
	INameProvider getNameProvider();


	/**
	 * @param colID
	 * @throws PersistException
	 */
	void deleteCol(String colID) throws PersistException;

	/**
	 * This operation does no immediate commit, so the commit has to be performed manually
	 * @param extID
	 * @param colID
	 * @throws PersistException
	 */
	void deleteCAS(String extID, String colID) throws PersistException;

	/**
	 * @param extID
	 * @param colID
	 * @throws PersistException
	 */
	void deleteAnnotsForDoc(String extID, String colID) throws PersistException;

	/**
	 * This operation does no immediate commit, so the commit has to be performed manually
	 * @param extID
	 * @param colID
	 * @param typeName
	 * @throws PersistException
	 */
	void deleteAnnotsForDoc(String extID, String colID, String typeName) throws PersistException;

	/**
	 * This operation does no immediate commit, so the commit has to be performed manually
	 * @param typeName
	 * @throws PersistException
	 */
	void deleteAnnotsForType(String typeName) throws PersistException;

	/**
	 * This operation does no immediate commit, so the commit has to be performed manually
	 * @param colID
	 * @throws PersistException
	 */
	void deleteAnnotsForCol(String colID) throws PersistException;


	/** Stores the given type system
	 * @param ts
	 * @throws PersistException
	 */
	void saveTypesystem(TypeSystem ts) throws PersistException;

	/** Stores the given type system description
	 * @param tsDesc
	 * @throws PersistException
	 */
	void saveTypesystem(TypeSystemDescription tsDesc) throws PersistException;

	/** Loads a type system description based on the given load configuration. For specific
	 * information on the load behavior the interested reader is referred to the respective
	 * 'loadTypesystem' method that resembles the given configuration.
	 * @param loadConfig
	 * @return
	 * @throws PersistException
	 */
	TypeSystemDescription loadTypesystem(LoadConfig loadConfig) throws PersistException;

	/** Loads a type system description based on the given type names. Any type that does
	 * not exist in the stores type tables is ignored.
	 * @param typeNames
	 * @return
	 * @throws PersistException
	 */
	TypeSystemDescription loadTypesystem(Set<String> typeNames) throws PersistException;

	/** Loads a type system description based on the given
	 * @param extDocIDs
	 * @param colID
	 * @return
	 * @throws PersistException
	 */
	TypeSystemDescription loadTypesystemByDocIDs(List<String> extDocIDs, String colID) throws PersistException;

	/**
	 * @param colID
	 * @return
	 * @throws PersistException
	 */
	TypeSystemDescription loadTypesystemByColID(String colID) throws PersistException;

	/** Returns a type system description which includes all types which's names
	 * match the given regular expression.
	 * @param namespaceRangeRegex
	 * @return
	 * @throws PersistException
	 */
	TypeSystemDescription loadTypesystemByNamespace(String namespaceRegex) throws PersistException;

	/** Loads the type system description determined by the given load config and adds
	 * alls loaded types to the given type system
	 * @param loadConfig
	 * @param tsd: the type system which is enriched by this loading operation
	 * @throws PersistException
	 */
	void enrichTypesystem(LoadConfig loadConfig, TypeSystemDescription tsd) throws PersistException;

}
