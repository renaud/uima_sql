package de.uniwue.uima.persist;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.uima.UimaContext;
import org.apache.uima.cas.CAS;
import org.apache.uima.collection.CollectionException;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.util.Progress;
import org.uimafit.component.JCasCollectionReader_ImplBase;
import org.uimafit.descriptor.ConfigurationParameter;

import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.uima.persist.dbManager.TmpTypeManager;

public class PersistCASCollectionReader extends JCasCollectionReader_ImplBase {

    public static final String EXTDOCID_PARAMNAME = "ExtDocIDs";
    @ConfigurationParameter(name = EXTDOCID_PARAMNAME, mandatory = false, description = "List of external IDs which should be loaded from the database")
    private String[] extDocIDs;

    public static final String ANNOT_TYPES_TO_LOAD_PARAMNAME = "AnnotTypesToLoad";
    @ConfigurationParameter(name = ANNOT_TYPES_TO_LOAD_PARAMNAME, mandatory = false, description = "If set, only those annotation types are loaded")
    private String[] annotTypesToLoad;

    public static final String COL_ID_PARAMNAME = "CollectionID";
    @ConfigurationParameter(name = COL_ID_PARAMNAME, mandatory = false, description = "Collection with docs which should be loaded from the database")
    protected String colID;

    public static final String SQL_DATABASE_PARAMNAME = "SQLDatabase";
    @ConfigurationParameter(name = SQL_DATABASE_PARAMNAME, mandatory = true, description = "database for db access")
    private String sqlDatabase;
    public static final String SQL_SERVER_PARAMNAME = "SQLServer";
    @ConfigurationParameter(name = SQL_SERVER_PARAMNAME, mandatory = true, description = "server for db access")
    private String sqlServer;
    public static final String SQL_PASSWORD_PARAMNAME = "SQLPassword";
    @ConfigurationParameter(name = SQL_PASSWORD_PARAMNAME, mandatory = true, description = "password for db access")
    private String sqlPassword;
    public static final String SQL_DBTYPE_PARAMNAME = "SQLDBType";
    @ConfigurationParameter(name = SQL_DBTYPE_PARAMNAME, mandatory = true, description = "dbType for db access")
    private String sqlDBType;
    public static final String SQL_USER_PARAMNAME = "SQLUser";
    @ConfigurationParameter(name = SQL_USER_PARAMNAME, mandatory = true, description = "user for db access")
    private String sqlUser;

    protected PersistenceManager persManager;
    private List<String> extIDsToProcess = new ArrayList<String>();
    private TmpTypeManager tmpTypeManager;
    private SQLManager sqlManager;

    public void initialize(UimaContext context)
            throws ResourceInitializationException {
        super.initialize(context);
        SQLConfig sqlConfig = new SQLConfig(sqlUser, sqlDatabase, sqlPassword,
                sqlServer, DBType.valueOf(sqlDBType));
        try {
            sqlManager = SQLManager.getSQLManager(sqlConfig);
            persManager = new PersistenceManager(sqlManager);
        } catch (SQLException e) {
            throw new ResourceInitializationException(e);
        }
        if (extDocIDs == null) {
            // get all documents of this collection
            try {
                extIDsToProcess.addAll(persManager.docManager
                        .getExtDocIDs(colID));
            } catch (PersistException e) {
                throw new ResourceInitializationException(e);
            }
        } else {
            // only get the given list of docs
            for (String anID : extDocIDs) {
                extIDsToProcess.add(anID);
            }
        }
    }

    public Progress[] getProgress() {
        return null;
    }

    public boolean hasNext() throws IOException, CollectionException {
        return !extIDsToProcess.isEmpty();
    }

    private TmpTypeManager getTmpTypeManager() throws PersistException {
        // creates a manager for a temporary table which makes loading a lot
        // faster.
        // the temporary table contains the IDs of the annotation types to load
        if ((annotTypesToLoad != null) && (tmpTypeManager == null)) {
            Set<String> typesToLoad = new HashSet<String>();
            for (String anAnnotTypeToLoad : annotTypesToLoad) {
                long anID = persManager.annotTypeManager
                        .getAnnotTypeID(anAnnotTypeToLoad);
                Set<Long> siblingIDs = persManager.annotTypeManager
                        .getSiblingsIncludingMe(anID);
                for (Long aSubID : siblingIDs) {
                    String siblingName = persManager.annotTypeManager
                            .getAnnotTypeName(aSubID);
                    typesToLoad.add(siblingName);
                }
            }
            try {
                tmpTypeManager = new TmpTypeManager(sqlManager,
                        typesToLoad.toArray(new String[0]), persManager);
            } catch (SQLException e) {
                throw new PersistException(e);
            }
        }
        return tmpTypeManager;
    }

    public void getNext(JCas arg0) throws IOException, CollectionException {
        String extID;

        extID = extIDsToProcess.get(0);
        extIDsToProcess.remove(0);
        CAS cas = arg0.getCas();
        try {
            if (getTmpTypeManager() != null) {
                persManager.loadCAS(extID, colID, cas, getTmpTypeManager());
            } else {
                persManager.loadCAS(extID, colID, cas);
            }
        } catch (PersistException e) {
            throw new CollectionException(e);
        }
    }

}
