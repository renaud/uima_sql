package de.uniwue.uima.persist;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.FeatureStructure;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.collection.CollectionReaderDescription;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.Capability;
import org.apache.uima.resource.metadata.TypeDescription;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.uimafit.factory.AnalysisEngineFactory;
import org.uimafit.factory.CollectionReaderFactory;

import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.uima.persist.data.LoadConfig;
import de.uniwue.uima.persist.data.SaveConfig;
import de.uniwue.uima.persist.dbManager.AnnotInstManager;
import de.uniwue.uima.persist.dbManager.AnnotTypeManager;
import de.uniwue.uima.persist.dbManager.DocManager;
import de.uniwue.uima.persist.dbManager.FeatInstManager;
import de.uniwue.uima.persist.dbManager.FeatTypeManager;
import de.uniwue.uima.persist.dbManager.TmpTypeManager;
import de.uniwue.uima.persist.interfaces.INameProvider;
import de.uniwue.uima.persist.interfaces.IPersistenceManager;

public class PersistenceManager implements IPersistenceManager {

    public DocManager docManager;
    public AnnotInstManager annotInstManager;
    public FeatInstManager featInstManager;
    public SQLManager sqlManager;
    public AnnotTypeManager annotTypeManager;
    public FeatTypeManager featTypeManager;
    public PersistConfig persistConfig;
    public INameProvider nameProvider;
    public TypeSystemLoadManager typeSystemManager;

    public PersistenceManager(SQLConfig anSQLConfig) throws SQLException {
        this(SQLManager.getSQLManager(anSQLConfig), new PersistConfig(),
                new DKProNameProvider());
    }

    public PersistenceManager(SQLManager anSQLManager) throws SQLException {
        this(anSQLManager, new PersistConfig(), new DKProNameProvider());
    }

    public PersistenceManager(SQLManager anSQLManager,
            PersistConfig persistConfig) throws SQLException {
        this(anSQLManager, persistConfig, new DKProNameProvider());
    }

    public PersistenceManager(SQLManager anSQLManager,
            PersistConfig aPersistConfig, INameProvider aNameProvider)
            throws SQLException {
        sqlManager = anSQLManager;
        persistConfig = aPersistConfig;
        nameProvider = aNameProvider;
        docManager = new DocManager(sqlManager, persistConfig);
        featTypeManager = new FeatTypeManager(sqlManager, persistConfig);
        annotTypeManager = new AnnotTypeManager(sqlManager, featTypeManager,
                docManager, persistConfig);
        featTypeManager.setAnnotTypeManager(annotTypeManager);
        featInstManager = new FeatInstManager(sqlManager, featTypeManager,
                docManager, persistConfig);
        annotInstManager = new AnnotInstManager(sqlManager, annotTypeManager,
                featInstManager, docManager, persistConfig);
        featInstManager.setAnnotInstManager(annotInstManager, annotTypeManager);
        annotTypeManager.setAnnotInstManager(annotInstManager);
        typeSystemManager = new TypeSystemLoadManager(this);
    }

    public void saveCAS(String aDocText, String extID, String colID)
            throws PersistException {
        long docID = docManager.getDocID(extID, colID, false);
        if (docID != 0) {
            // if the document already exists then delete it and all its
            // annotations
            deleteAnnotsForDoc(extID, colID);
            docManager.deleteDoc(extID, colID);
        }
        docManager.insertDoc(aDocText, extID, colID);
    }

    public void saveCAS(CAS cas, SaveConfig saveConfig) throws PersistException {
        String extID = nameProvider.getExtDocID(cas);
        String colID = nameProvider.getCollectionID(cas);
        // if the documents already exists then take its old docID, otherwise
        // store it
        long docID = docManager.getDocID(extID, colID, false);
        if (docID == 0) {
            docID = docManager.insertDoc(cas.getDocumentText(), extID, colID);
        } else {
            // if the document already exists then delete all its currently
            // existing
            // annotations which will then be taken from the CAS and saved again
            if (saveConfig.saveRestrictedAnnotTypeAmount()) {
                deleteAnnotsForDoc(extID, colID,
                        saveConfig.getAnnotTypesToSave());
            } else {
                deleteAnnotsForDoc(extID, colID);
            }
        }
        HashMap<AnnotationFS, Long> savedAnnots = new HashMap<AnnotationFS, Long>();
        List<AnnotationFS> annotsToSave = new ArrayList<AnnotationFS>();
        for (AnnotationFS anAnnot : cas.getAnnotationIndex()) {
            // collect only annotations which have to be saved and are not a
            // DocumentAnnotation
            if (!saveConfig.saveRestrictedAnnotTypeAmount()
                    || saveConfig.getAnnotTypesToSave().contains(
                            anAnnot.getType().getName())) {
                if (!anAnnot.getType().getName()
                        .equals(AnnotTypeManager.documentAnnotationTypeName)) {
                    annotsToSave.add(anAnnot);
                }
            }
        }
        // store the collected annotation
        for (AnnotationFS anAnnot : annotsToSave) {
            Long newAnnotIsntID = saveAnnot(anAnnot, docID);
            savedAnnots.put(anAnnot, newAnnotIsntID);
        }
        // store potential feature references
        for (AnnotationFS anAnnot : annotsToSave) {
            saveReferences(savedAnnots, anAnnot);
        }
    }

    public void saveCAS(CAS cas) throws PersistException {
        SaveConfig saveConfig = new SaveConfig();
        saveCAS(cas, saveConfig);
    }

    private void saveReferences(HashMap<AnnotationFS, Long> savedAnnots,
            AnnotationFS anAnnot) throws PersistException {
        long annotInstID = savedAnnots.get(anAnnot);
        String annotTypeName = anAnnot.getType().getName();
        long annot_type_ID = annotTypeManager.getAnnotTypeID(annotTypeName);
        for (Feature aFeat : anAnnot.getType().getFeatures()) {
            Set<String> annotTypeNames = annotTypeManager.getAnnotTypeNames();
            String aFeatRangeName = aFeat.getRange().getName();
            if (!annotTypeNames.contains(aFeatRangeName)) {
                // if the feature is no known annotation type skip it
                continue;
            }
            FeatureStructure aFS = anAnnot.getFeatureValue(aFeat);
            if (aFS == null) {
                // can it happen that an existing feature has no value ?
                // nevertheless it will not be stored
                continue;
            }
            if (savedAnnots.containsKey(aFS)) {
                // if the feature is one of the previously saved annotations
                // then save its "annot_inst_id"
                long refAnnotID = savedAnnots.get(aFS);
                long feat_type_ID = featTypeManager.getFeatureTypeID(
                        aFeat.getShortName(), annot_type_ID);
                featInstManager.insertFeature(annotInstID, feat_type_ID,
                        Long.toString(refAnnotID));
            }
        }
    }

    private long saveAnnot(AnnotationFS anAnnot, long docID)
            throws PersistException {
        String annotTypeName = anAnnot.getType().getName();
        long annot_type_ID = annotTypeManager.getAnnotTypeID(annotTypeName);
        String coveredText = "";
        // DocumentAnnotations always cover the whole document and thus do not
        // need a coveredText
        if (!annotTypeManager.isDocumentAnnotSibling(annot_type_ID)) {
            coveredText = anAnnot.getCoveredText();
        }
        long instID = annotInstManager.insertAnnotInst(annot_type_ID,
                anAnnot.getBegin(), anAnnot.getEnd(), docID, coveredText);
        for (Feature aFeat : anAnnot.getType().getFeatures()) {
            if (aFeat.getShortName().equals("sofa")
                    || aFeat.getShortName().equals("begin")
                    || aFeat.getShortName().equals("end")) {
                // sofa, begin and end do not need to be saved
                continue;
            }
            if (!aFeat.getRange().getName().matches("^uima\\.cas\\..*")) {
                // if the range is not UIMA base type then skip here
                // references to other annotations in the same CAS care saved in
                // "saveReferences" later
                continue;
            }
            if (aFeat.getRange().getName().equals("uima.cas.StringArray")) {
                // TODO: Arrays speicherbar machen
                continue;
            }
            String value = anAnnot.getFeatureValueAsString(aFeat);
            if (value != null) {
                long feat_type_ID = featTypeManager.getFeatureTypeID(
                        aFeat.getShortName(), annot_type_ID);
                featInstManager.insertFeature(instID, feat_type_ID, value);
            }
        }
        return instID;
    }

    public void loadCAS(String extDocID, String colID, CAS cas,
            TmpTypeManager tmpTypeManager) throws PersistException {
        cas.reset();
        String docText = docManager.getDocText(extDocID, colID);
        cas.setDocumentText(docText);
        annotInstManager.createAnnots(extDocID, colID, cas, tmpTypeManager);
        nameProvider.setMetaData(cas, extDocID, colID);
    }

    public void loadCAS(String extDocID, String colID, CAS cas)
            throws PersistException {
        loadCAS(extDocID, colID, cas, null);
    }

    public void deleteCAS(String extID, String colID) throws PersistException {
        deleteAnnotsForDoc(extID, colID);
        docManager.deleteDoc(extID, colID);
    }

    public void deleteAnnotsForDoc(String extID, String colID)
            throws PersistException {
        featInstManager.deleteFeatsForDoc(extID, colID);
        annotInstManager.deleteAnnotsForDoc(extID, colID);
    }

    public void deleteAnnotsForDoc(String extID, String colID,
            String annotTypeName) throws PersistException {
        long annot_type_ID = annotTypeManager.getAnnotTypeID(annotTypeName);
        featInstManager.deleteFeatsForDoc(extID, colID, annot_type_ID);
        annotInstManager.deleteAnnotsForDoc(extID, colID, annot_type_ID);
    }

    public void deleteAnnotsForDoc(String extID, String colID,
            Set<String> annotTypeNames) throws PersistException {
        if (annotTypeNames.isEmpty()) {
            return;
        }
        List<Long> annotTypeIDsToDelete = new ArrayList<Long>();
        for (String aTypeName : annotTypeNames) {
            long aTypeIDtoDelete = annotTypeManager.getAnnotTypeID(aTypeName);
            annotTypeIDsToDelete.add(aTypeIDtoDelete);
        }
        featInstManager.deleteFeatsForDoc(extID, colID, annotTypeIDsToDelete);
        annotInstManager.deleteAnnotsForDoc(extID, colID, annotTypeIDsToDelete);
    }

    public void deleteAnnotsForType(String annotTypeName)
            throws PersistException {
        long annot_type_ID = annotTypeManager.getAnnotTypeID(annotTypeName);
        featInstManager.deleteFeatsForAnnotsForType(annot_type_ID);
        annotInstManager.deleteAnnotsForType(annot_type_ID);
    }

    public void deleteAnnotsForCol(String colID) throws PersistException {
        featInstManager.deleteFeatsForCol(colID);
        annotInstManager.deleteAnnotsForCol(colID);
        commit();
    }

    public CollectionReaderDescription loadCAS(LoadConfig loadConfig)
            throws PersistException {
        if (loadConfig.tsd == null) {
            // if the type system is not given in the loadConfig the type system
            // has
            // to be computed by the documents in the database
            if ((loadConfig.extDocIDs != null)
                    && loadConfig.extDocIDs.isEmpty()) {
                loadConfig.tsd = loadTypesystemByDocIDs(loadConfig.extDocIDs,
                        loadConfig.colID);
            } else {
                loadConfig.tsd = loadTypesystemByColID(loadConfig.colID);
            }
        } else {
            // if a type system is given it has to be saved so that loading
            // currently unknown types does not make problems later
            nameProvider.addMetaDataTypes(loadConfig.tsd);
            saveTypesystem(loadConfig.tsd);
        }
        if (!loadConfig.loadRestrictAnnotTypesAmount()) {
            // the annotation types to load are either already defined or all
            // types in the given type system are taken
            List<TypeDescription> annotSiblingTypes = typeSystemManager
                    .getAnnotTypesFromTypeSystem(loadConfig.tsd);
            for (TypeDescription aType : annotSiblingTypes) {
                loadConfig.addAnnotTypeToLoad(aType.getName());
            }
        }
        if (loadConfig.extDocIDs == null) {
            loadConfig.extDocIDs = docManager.getExtDocIDs(loadConfig);
        }
        String[] extDocIDsArray = loadConfig.extDocIDs.toArray(new String[0]);
        try {
            CollectionReaderDescription reader = CollectionReaderFactory
                    .createDescription(
                            PersistCASCollectionReader.class,
                            loadConfig.tsd,
                            PersistCASCollectionReader.COL_ID_PARAMNAME,
                            loadConfig.colID,
                            PersistCASCollectionReader.EXTDOCID_PARAMNAME,
                            loadConfig.extDocIDs.toArray(new String[0]),
                            PersistCASCollectionReader.SQL_DATABASE_PARAMNAME,
                            sqlManager.config.database,
                            PersistCASCollectionReader.SQL_SERVER_PARAMNAME,
                            sqlManager.config.sqlServer,
                            PersistCASCollectionReader.SQL_PASSWORD_PARAMNAME,
                            sqlManager.config.password,
                            PersistCASCollectionReader.SQL_DBTYPE_PARAMNAME,
                            sqlManager.config.dbType.toString(),
                            PersistCASCollectionReader.SQL_USER_PARAMNAME,
                            sqlManager.config.user,
                            PersistCASCollectionReader.EXTDOCID_PARAMNAME,
                            extDocIDsArray,
                            PersistCASCollectionReader.ANNOT_TYPES_TO_LOAD_PARAMNAME,
                            loadConfig.getAnnotTypesToLoad());
            return reader;
        } catch (ResourceInitializationException e) {
            throw new PersistException(e);
        }
    }

    public CollectionReaderDescription loadCASByColID(String colID)
            throws PersistException {
        LoadConfig loadConfig = new LoadConfig();
        loadConfig.colID = colID;
        return loadCAS(loadConfig);
    }

    public CollectionReaderDescription loadCASByExtID(List<String> extDocIDs,
            String colID) throws PersistException {
        LoadConfig loadConfig = new LoadConfig();
        loadConfig.colID = colID;
        loadConfig.extDocIDs = extDocIDs;
        return loadCAS(loadConfig);
    }

    public AnalysisEngineDescription createWriterDescription(
            SaveConfig saveConfig) throws PersistException {
        try {
            List<Capability> capList = new ArrayList<Capability>();
            // if a restricted set of annotation types is given in the save
            // config, these are
            // the capabilities of this engine. If no restriction is given the
            // capabilities stay
            // empty and the engine saves all types
            if (saveConfig.saveRestrictedAnnotTypeAmount()) {
                Capability cap = UIMAFramework.getResourceSpecifierFactory()
                        .createCapability();
                for (String anAnnotName : saveConfig.getAnnotTypesToSave()) {
                    cap.addOutputType(anAnnotName, true);
                }
                capList.add(cap);
            }
            // the engine does not need a type system. Its capabilities are
            // sufficient
            AnalysisEngineDescription writer = AnalysisEngineFactory
                    .createPrimitiveDescription(
                            PersistCASCollectionWriter.class, null, null, null,
                            capList.toArray(new Capability[0]),
                            PersistCASCollectionWriter.SQL_DATABASE_PARAMNAME,
                            sqlManager.config.database,
                            PersistCASCollectionWriter.SQL_SERVER_PARAMNAME,
                            sqlManager.config.sqlServer,
                            PersistCASCollectionWriter.SQL_PASSWORD_PARAMNAME,
                            sqlManager.config.password,
                            PersistCASCollectionWriter.SQL_DBTYPE_PARAMNAME,
                            sqlManager.config.dbType.toString(),
                            PersistCASCollectionWriter.SQL_USER_PARAMNAME,
                            sqlManager.config.user);
            return writer;
        } catch (ResourceInitializationException e) {
            throw new PersistException(e);
        }
    }

    public AnalysisEngineDescription createWriterDescription(
            TypeSystemDescription tsd) throws PersistException {
        saveTypesystem(tsd);
        SaveConfig saveConfig = new SaveConfig();
        for (TypeDescription aType : tsd.getTypes()) {
            saveConfig.addAnnotTypeToSave(aType.getName());
        }
        return createWriterDescription(saveConfig);
    }

    public void deleteCol(String colID) throws PersistException {
        featInstManager.deleteFeatsForCol(colID);
        annotInstManager.deleteAnnotsForCol(colID);
        docManager.deleteCol(colID);
        commit();
    }

    public String[] getExtDocIDs(String colID) throws PersistException {
        return docManager.getExtDocIDs(colID).toArray(new String[0]);
    }

    public INameProvider getNameProvider() {
        return nameProvider;
    }

    public void commit() throws PersistException {
        try {
            sqlManager.commit();
        } catch (SQLException e) {
            throw new PersistException(e);
        }
    }

    public TypeSystemDescription loadTypesystem(LoadConfig loadConfig)
            throws PersistException {
        return typeSystemManager.loadTypesystem(loadConfig);
    }

    public TypeSystemDescription loadTypesystem(Set<String> typeNames)
            throws PersistException {
        return typeSystemManager.loadTypesystem(typeNames);
    }

    public TypeSystemDescription loadTypesystemByDocIDs(List<String> extIDs,
            String colID) throws PersistException {
        return typeSystemManager.loadTypesystemByDocIDs(extIDs, colID);
    }

    public TypeSystemDescription loadTypesystemByColID(String colID)
            throws PersistException {
        return typeSystemManager.loadTypesystemByColID(colID);
    }

    public TypeSystemDescription loadTypesystemByNamespace(
            String namespaceRangeRegex) throws PersistException {
        return typeSystemManager.loadTypesystemByNamespace(namespaceRangeRegex);
    }

    public void enrichTypesystem(LoadConfig loadConfig,
            TypeSystemDescription tsd) throws PersistException {
        typeSystemManager.loadTypesystem(loadConfig, tsd);
    }

    public void saveTypesystem(TypeSystemDescription tsDesc)
            throws PersistException {
        typeSystemManager.saveTypesystem(tsDesc);
    }

    public void saveTypesystem(TypeSystem ts) throws PersistException {
        typeSystemManager.saveTypesystem(ts);
    }

    public List<String> getDocText(List<String> extDocIDs, String colID)
            throws PersistException {
        List<String> result = new ArrayList<String>();
        for (String extDocID : extDocIDs) {
            String aText = getDocText(extDocID, colID);
            result.add(aText);
        }
        return result;
    }

    public String getDocText(String extDocID, String colID)
            throws PersistException {
        return docManager.getDocText(extDocID, colID);
    }

}
