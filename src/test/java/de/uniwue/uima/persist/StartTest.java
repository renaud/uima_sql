package de.uniwue.uima.persist;

import java.io.File;
import java.io.FileInputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.resource.metadata.TypeDescription;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.junit.Test;

import de.uniwue.misc.sql.SQLConfig;
import de.uniwue.misc.sql.SQLManager;
import de.uniwue.misc.util.UIMAUtilUniWue;
import de.uniwue.uima.persist.data.SaveConfig;
import de.uniwue.uima.persist.interfaces.INameProvider;

public class StartTest {

    @Test
    public void test() throws Exception {

        SQLConfig sqlConfig = new SQLConfig("root", "uima_sql", "", "localhost");
        SQLManager.setStdConfig(sqlConfig);

        SQLManager sqlManager = SQLManager.getSQLManager();
        dropTables(sqlManager);

        PersistenceManager persManager = new PersistenceManager(sqlManager);
        testLoadingAndSavingTypeSystems(persManager);
        testLoadingAndSavingCASes(persManager);
    }

    private static void testLoadingAndSavingTypeSystems(
            PersistenceManager persManager) throws Exception {

        // FIXME do not hardcode paths
        File tsdInFile = new File("src/test/resources/DKProTypeSystem.xml");
        File tsdOutFile = new File("src/test/resources/DKProTypeSystemOut.xml");
        TypeSystemDescription tsdIn = UIMAUtilUniWue.loadTypesystem(tsdInFile);
        tsdIn.resolveImports();
        List<String> tdListIn = new ArrayList<String>();
        for (TypeDescription aTD : tsdIn.getTypes()) {
            tdListIn.add(aTD.getName());
        }
        Collections.sort(tdListIn);
        persManager.saveTypesystem(tsdIn);
        TypeSystemDescription tsdOut = persManager
                .loadTypesystemByNamespace("*");
        UIMAUtilUniWue.saveTypesystem(tsdOut, tsdOutFile);
        List<String> tdListOut = new ArrayList<String>();
        for (TypeDescription aTD : tsdOut.getTypes()) {
            tdListOut.add(aTD.getName());
        }
        for (TypeDescription aTD : tsdOut.getTypes()) {
            if (!tdListIn.contains(aTD.getName())) {
                System.out
                        .println("The input type system does not contain the type '"
                                + aTD.getName()
                                + "' but the recovered type system does.");
            }
        }
        for (TypeDescription aTD : tsdIn.getTypes()) {
            if (!tdListOut.contains(aTD.getName())) {
                System.out
                        .println("The recovered type system does not contain the type '"
                                + aTD.getName()
                                + "' but the input type system does.");
            }
        }
        Collections.sort(tdListOut);
    }

    private static void testLoadingAndSavingCASes(PersistenceManager persManager)
            throws Exception {

        File tsdInFile = new File("src/test/resources/DKProTypeSystem.xml");
        TypeSystemDescription tsd = UIMAUtilUniWue.loadTypesystem(tsdInFile);
        persManager.saveTypesystem(tsd);
        CAS inputCas = CasCreationUtils.createCas(tsd, null, null);
        CAS outputCas = CasCreationUtils.createCas(tsd, null, null);
        String extDocID = "sofie_0-20.xmi";
        File xmiFile = new File("src/test/resources", extDocID);
        FileInputStream inputStream = new FileInputStream(xmiFile);
        XmiCasDeserializer.deserialize(inputStream, inputCas, true);
        SaveConfig config = new SaveConfig();
        INameProvider nameProvider = persManager.getNameProvider();
        String colID = "testCollection";
        nameProvider.setMetaData(inputCas, extDocID, colID);
        persManager.saveCAS(inputCas, config);
        persManager.commit();
        persManager.loadCAS(extDocID, colID, outputCas);
        File xmiOutFile = new File("src/test/resources/testDocOut.xmi");
        UIMAUtilUniWue.saveCAS(outputCas, xmiOutFile);
        for (AnnotationFS anAnnot : inputCas.getAnnotationIndex()) {
            boolean textContained = false;
            for (AnnotationFS anOutAnnot : outputCas.getAnnotationIndex(anAnnot
                    .getType())) {
                if (anAnnot.getCoveredText()
                        .equals(anOutAnnot.getCoveredText())) {
                    textContained = true;
                    boolean featuresContained = false;
                    for (Feature aFeat : anAnnot.getType().getFeatures()) {
                        if (aFeat.getRange().getName()
                                .equals("uima.cas.String")) {
                            String featValue = anAnnot
                                    .getFeatureValueAsString(aFeat);
                            String outFeatValue = anOutAnnot
                                    .getFeatureValueAsString(aFeat);
                            if (((featValue == null) && (outFeatValue == null))
                                    || featValue.equals(outFeatValue)) {
                                featuresContained = true;
                            }
                        } else {
                            featuresContained = true;
                        }
                    }
                    if (!featuresContained) {
                        System.out
                                .println("The recovered CAS is missing a feature.");
                    }
                }
            }
            if (!textContained) {
                System.out
                        .println("The recovered CAS is missing an annotation.");
            }
        }
    }

    private static void dropTables(SQLManager sqlManager) throws SQLException {

        PersistenceManager persManager = new PersistenceManager(sqlManager);

        persManager.sqlManager.dropTable(persManager.persistConfig
                .getAnnotInstTableName());
        persManager.sqlManager.dropTable(persManager.persistConfig
                .getAnnotTypeTableName());
        persManager.sqlManager.dropTable(persManager.persistConfig
                .getFeatInstTableName());
        persManager.sqlManager.dropTable(persManager.persistConfig
                .getFeatTypeTableName());
        persManager.sqlManager.dropTable(persManager.persistConfig
                .getDocTableName());
    }
}
