package de.uniwue.uima.persist.interfaces;

import org.apache.uima.cas.CAS;
import org.apache.uima.resource.metadata.TypeSystemDescription;

/**
 * A name provider handles the access to the meta data of documents represented as CASes.
 * The data is stored in the CAS and can be accessed via the name provider
 * @author Georg Fette
 */
public interface INameProvider {

	public String getExtDocID(CAS cas);
	String getCollectionID(CAS cas);
	void addMetaDataTypes(TypeSystemDescription tsd);
	void setMetaData(CAS cas, String anExtID, String aColID);

}
