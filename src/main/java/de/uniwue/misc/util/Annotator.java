package de.uniwue.misc.util;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.FSIterator;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.cas.text.AnnotationIndex;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.metadata.FsIndexDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.InvalidXMLException;
import org.xml.sax.SAXException;

/**
 * An Annotator is helper class for doing certain UIMA tasks in an shorter way.
 * 
 * @author Georg Fette
 */
public class Annotator {

    public AnalysisEngine ae;
    public TypeSystem typeSystem;
    protected CAS reusableCas;

    public Annotator(CAS cas) {
        typeSystem = cas.getTypeSystem();
    }

    public Annotator(File engineFile) throws InvalidXMLException,
            ResourceInitializationException, IOException {
        LogManagerUniWue.info("Reading typeSystem from file '"
                + engineFile.getAbsolutePath() + "'");
        ae = UIMAUtilUniWue.getAE(engineFile);
        reusableCas = createCAS();
        typeSystem = reusableCas.getTypeSystem();
    }

    public Annotator(File typeSystemFile, boolean isTypeSystemFile)
            throws ResourceInitializationException, InvalidXMLException,
            IOException {
        reusableCas = UIMAUtilUniWue
                .createCASFromTypeSystemFile(typeSystemFile);
        typeSystem = reusableCas.getTypeSystem();
    }

    public CAS getCAS() {
        if (reusableCas == null) {
            reusableCas = createCAS();
        } else {
            reusableCas.reset();
        }
        return reusableCas;
    }

    /**
     * This method can be overriden by subclasses
     * 
     * @param cas
     */
    public void cleanCAS(CAS cas) {
    }

    public void createXmis(File untaggedDir, File untaggedXmiDir)
            throws SAXException, IOException {
        CAS cas;
        int counter = 0;
        if (!untaggedXmiDir.exists()) {
            untaggedXmiDir.mkdir();
        }
        System.out.println("Starting creating XMIs");
        cas = createCAS();
        for (File aFile : untaggedDir.listFiles()) {
            UIMAUtilUniWue.fillCAS(aFile, cas);
            File newFile = new File(untaggedXmiDir, aFile.getName() + ".xmi");
            UIMAUtilUniWue.saveCAS(cas, newFile);
            counter++;
            if (counter % 500 == 0) {
                System.out.println("Finished " + counter + " files");
            }
        }
        System.out.println("Finished creating XMIs");
    }

    public CAS createCAS() {
        try {
            if (ae != null) {
                return ae.newCAS();
            } else {
                return CasCreationUtils.createCas(typeSystem, null,
                        new FsIndexDescription[0], null);
            }
        } catch (ResourceInitializationException e) {
            e.printStackTrace();
        }
        return null;
    }

    public CAS getCAS(File file) throws SAXException, IOException {
        CAS cas = getCAS();
        UIMAUtilUniWue.fillCAS(file, cas);
        return cas;
    }

    public void deleteAnnots(CAS casOut, Collection<String> typeNames) {
        for (String aTypeName : typeNames) {
            Type infoType = typeSystem.getType(aTypeName);
            final AnnotationIndex<AnnotationFS> infoIndex = casOut
                    .getAnnotationIndex(infoType);
            if (infoIndex == null)
                continue;
            FSIterator<AnnotationFS> infoIterator = infoIndex.iterator();
            List<AnnotationFS> toRemove = new LinkedList<AnnotationFS>();
            while (infoIterator.isValid()) {
                toRemove.add(infoIterator.get());
                infoIterator.moveToNext();
            }
            for (AnnotationFS rem : toRemove) {
                casOut.removeFsFromIndexes(rem);
            }
        }
    }

    public CAS annotate(File aFile) throws SAXException, IOException {
        CAS cas;

        cas = getCAS(aFile);
        cas = annotate(cas);
        return cas;
    }

    public CAS annotate(CAS cas) {
        try {
            ae.process(cas);
            return cas;
        } catch (AnalysisEngineProcessException e) {
            LogManagerUniWue.warning(e.getMessage());
            e.printStackTrace();
        }
        return null;
    }

}
