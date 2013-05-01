package de.uniwue.uima.persist;

import java.sql.SQLException;
import java.util.Iterator;
import java.util.Set;

import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.analysis_engine.ResultSpecification;
import org.apache.uima.analysis_engine.TypeOrFeature;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.uimafit.component.JCasConsumer_ImplBase;
import org.uimafit.descriptor.ConfigurationParameter;

import de.uniwue.misc.sql.DBType;
import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.uima.persist.data.SaveConfig;

public class PersistCASCollectionWriter extends JCasConsumer_ImplBase {

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

    private SaveConfig config;
    private PersistenceManager persManager;
    private int docsProcessed = 0;

    @Override
    public void initialize(UimaContext context)
            throws ResourceInitializationException {
        super.initialize(context);
        SQLConfig sqlConfig = new SQLConfig(sqlUser, sqlDatabase, sqlPassword,
                sqlServer, DBType.valueOf(sqlDBType));
        try {
            SQLManager sqlManager = SQLManager.getSQLManager(sqlConfig);
            persManager = new PersistenceManager(sqlManager);
        } catch (SQLException e) {
            throw new ResourceInitializationException(e);
        }
    }

    private boolean persistentTypesEnsured = false;

    private void ensurePersistentTypes(TypeSystem ts) throws PersistException {
        // only save type system once
        if (!persistentTypesEnsured) {
            Set<String> typeNames = persManager.annotTypeManager
                    .getAnnotTypeNames();
            Iterator<Type> iter = ts.getTypeIterator();
            while (iter.hasNext()) {
                Type aType = iter.next();
                if (!typeNames.contains(aType.getName())) {
                    // only save the whole typesystem if there is at least one
                    // missing type
                    // which does not yet exist in the database
                    persManager.saveTypesystem(ts);
                    break;
                }
            }
            persistentTypesEnsured = true;
        }
    }

    private SaveConfig getSaveConfig(CAS cas) throws PersistException {
        config = new SaveConfig();
        ResultSpecification resultSpec = getResultSpecification();
        TypeOrFeature[] toSave = resultSpec.getResultTypesAndFeatures();
        // store only the types which are defined in the ResultSpecification
        // the specs have to be set in the creation of the writer or
        // in the call for process(cas, specs)
        for (TypeOrFeature aTOrF : toSave) {
            if (aTOrF.isType()) {
                long anAttrID = persManager.annotTypeManager
                        .getAnnotTypeID(aTOrF.getName());
                Set<Long> siblingIDs = persManager.annotTypeManager
                        .getSiblingsIncludingMe(anAttrID);
                for (Long anID : siblingIDs) {
                    String name = persManager.annotTypeManager
                            .getAnnotTypeName(anID);
                    config.addAnnotTypeToSave(name);
                }
            }
        }
        return config;
    }

    @Override
    public void process(JCas jCas) throws AnalysisEngineProcessException {
        CAS cas = jCas.getCas();
        try {
            ensurePersistentTypes(cas.getTypeSystem());
            SaveConfig config = getSaveConfig(cas);
            persManager.saveCAS(cas, config);
            docsProcessed++;
            if (docsProcessed % 1000 == 0) {
                persManager.commit();
            }
        } catch (PersistException e) {
            throw new AnalysisEngineProcessException(e);
        }
    }

    @Override
    public void batchProcessComplete() throws AnalysisEngineProcessException {
        super.batchProcessComplete();
        try {
            persManager.commit();
        } catch (PersistException e) {
            throw new AnalysisEngineProcessException(e);
        }
    }

}
