package de.uniwue.uima.persist;

import org.apache.uima.cas.CAS;
import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.text.AnnotationFS;
import org.apache.uima.cas.text.AnnotationIndex;
import org.apache.uima.resource.metadata.TypeDescription;
import org.apache.uima.resource.metadata.TypeSystemDescription;

import de.uniwue.uima.persist.interfaces.INameProvider;

public class DKProNameProvider implements INameProvider {

    private String getMetaDataTypeName() {
        return "de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData";
    }

    private String getExtDocIDFeatureName() {
        return "documentId";
    }

    private String getColextionIDFeatureName() {
        return "collectionId";
    }

    public String getExtDocID(CAS cas) {
        String extID = null;
        Type metaType = cas.getTypeSystem().getType(getMetaDataTypeName());
        Feature feature = metaType
                .getFeatureByBaseName(getExtDocIDFeatureName());
        AnnotationIndex<AnnotationFS> index = cas.getAnnotationIndex(metaType);
        for (AnnotationFS anFS : index) {
            extID = anFS.getFeatureValueAsString(feature);
        }
        return extID;
    }

    public String getCollectionID(CAS cas) {
        String colID = null;
        Type metaType = cas.getTypeSystem().getType(getMetaDataTypeName());
        Feature feature = metaType
                .getFeatureByBaseName(getColextionIDFeatureName());
        AnnotationIndex<AnnotationFS> index = cas.getAnnotationIndex(metaType);
        for (AnnotationFS anFS : index) {
            colID = anFS.getFeatureValueAsString(feature);
        }
        return colID;
    }

    public void setMetaData(CAS cas, String anExtID, String aColID) {
        Type metaType = cas.getTypeSystem().getType(getMetaDataTypeName());
        Feature featureExtID = metaType
                .getFeatureByBaseName(getExtDocIDFeatureName());
        Feature featureColID = metaType
                .getFeatureByBaseName(getColextionIDFeatureName());
        AnnotationIndex<AnnotationFS> index = cas.getAnnotationIndex(metaType);
        AnnotationFS newAnnot;
        if (index.size() == 0) {
            newAnnot = cas.createAnnotation(metaType, 0, cas.getDocumentText()
                    .length());
        } else {
            newAnnot = index.iterator().next();
            cas.removeFsFromIndexes(newAnnot);
        }
        newAnnot.setFeatureValueFromString(featureExtID, anExtID);
        newAnnot.setFeatureValueFromString(featureColID, aColID);
        cas.addFsToIndexes(newAnnot);
    }

    public void addMetaDataTypes(TypeSystemDescription tsd) {
        if (tsd.getType(getMetaDataTypeName()) == null) {
            TypeDescription type = tsd.addType(getMetaDataTypeName(),
                    getMetaDataTypeName(), "uima.tcas.DocumentAnnotation");
            type.addFeature(getColextionIDFeatureName(), "", "uima.cas.String");
            type.addFeature(getExtDocIDFeatureName(), "", "uima.cas.String");
        }
    }

}
