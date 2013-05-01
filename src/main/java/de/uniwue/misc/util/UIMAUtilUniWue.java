package de.uniwue.misc.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.uima.UIMAFramework;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.cas.CAS;
import org.apache.uima.cas.impl.XmiCasDeserializer;
import org.apache.uima.cas.impl.XmiCasSerializer;
import org.apache.uima.resource.ResourceInitializationException;
import org.apache.uima.resource.ResourceSpecifier;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.CasCreationUtils;
import org.apache.uima.util.FileUtils;
import org.apache.uima.util.InvalidXMLException;
import org.apache.uima.util.XMLInputSource;
import org.apache.uima.util.XMLParser;
import org.xml.sax.SAXException;

public class UIMAUtilUniWue {

    public static void saveCAS(CAS cas, File file) throws SAXException,
            FileNotFoundException {
        FileOutputStream ounputStream = new FileOutputStream(file);
        XmiCasSerializer.serialize(cas, ounputStream);
    }

    public static TypeSystemDescription loadTypesystem(File aTSFile)
            throws InvalidXMLException, IOException {
        TypeSystemDescription tsd = null;

        XMLParser xmlParser = UIMAFramework.getXMLParser();
        tsd = xmlParser.parseTypeSystemDescription(new XMLInputSource(aTSFile));
        tsd.resolveImports();
        return tsd;
    }

    public static void saveTypesystem(TypeSystemDescription tsd, File aTSFile)
            throws SAXException, IOException {
        OutputStream outStream = new FileOutputStream(aTSFile);
        tsd.toXML(outStream);
        outStream.close();
    }

    public static void fillCAS(String aText, CAS cas) {
        cas.reset();
        cas.setDocumentText(aText);
    }

    public static void fillCAS(File file, CAS cas) throws SAXException,
            IOException {
        FileInputStream inputStream;

        inputStream = new FileInputStream(file);
        if (file.getName().contains(".xmi")) {
            XmiCasDeserializer.deserialize(inputStream, cas, true);
        } else {
            String fileText = FileUtils.file2String(file, "utf-8");
            fillCAS(fileText, cas);
        }
    }

    public static TypeSystemDescription createEmptyTypeSystem() {
        InputStream anInStream = UIMAUtilUniWue.class
                .getResourceAsStream("/emptyTypeSystem.xml");
        TypeSystemDescription result = null;
        try {
            result = loadTypesystem(anInStream);
        } catch (InvalidXMLException e) {
            e.printStackTrace();
        }
        return result;
    }

    public static TypeSystemDescription loadTypesystem(InputStream anInStream)
            throws InvalidXMLException {
        TypeSystemDescription result = null;
        XMLParser xmlParser = UIMAFramework.getXMLParser();
        result = xmlParser.parseTypeSystemDescription(new XMLInputSource(
                anInStream, null));
        return result;
    }

    public static TypeSystemDescription mergeTypesystem(
            TypeSystemDescription tsd, TypeSystemDescription tsd2)
            throws ResourceInitializationException {
        List<TypeSystemDescription> typeSystems = new ArrayList<TypeSystemDescription>();
        typeSystems.add(tsd);
        typeSystems.add(tsd2);
        TypeSystemDescription result;
        result = CasCreationUtils.mergeTypeSystems(typeSystems);
        return result;
    }

    public static TypeSystemDescription loadTypesystem(File aTSFile,
            TypeSystemDescription tsd) throws ResourceInitializationException,
            InvalidXMLException, IOException {
        TypeSystemDescription result = null;
        XMLParser xmlParser = UIMAFramework.getXMLParser();
        TypeSystemDescription tsd2 = xmlParser
                .parseTypeSystemDescription(new XMLInputSource(aTSFile));
        tsd2.resolveImports();
        List<TypeSystemDescription> typeSystems = new ArrayList<TypeSystemDescription>();
        typeSystems.add(tsd);
        typeSystems.add(tsd2);
        result = CasCreationUtils.mergeTypeSystems(typeSystems);
        return result;
    }

    public static CAS createCASFromTypeSystemFile(File typeSystemFile)
            throws ResourceInitializationException, InvalidXMLException,
            IOException {
        XMLParser xmlParser = UIMAFramework.getXMLParser();
        TypeSystemDescription tsDesc;
        tsDesc = xmlParser.parseTypeSystemDescription(new XMLInputSource(
                typeSystemFile));
        return CasCreationUtils.createCas(tsDesc, null, null);
    }

    public static AnalysisEngine getAE(File anEngineFile) throws IOException,
            InvalidXMLException, ResourceInitializationException {
        AnalysisEngine result = null;
        XMLInputSource in;
        ResourceSpecifier specifier;

        in = new XMLInputSource(anEngineFile);
        specifier = UIMAFramework.getXMLParser().parseResourceSpecifier(in);
        result = UIMAFramework.produceAnalysisEngine(specifier);
        return result;
    }

    public static File[] getXMIFiles(File fileFolder) {
        File[] files = fileFolder.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return name.endsWith("xmi");
            }
        });
        return files;
    }
}
