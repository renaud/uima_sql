package de.uniwue.uima.persist.data;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.uima.resource.metadata.TypeSystemDescription;

/**
 * The load configuration is used for loading CASes as well as type system descriptions.
 * For the loading of CASes it determined which documents from which collection are retrieved
 * and for which annotation types instances are loaded during load.
 * For the loading of type systems it determines which types should be included in the loaded
 * type system description
 * @author Georg Fette
 */
public class LoadConfig {

	private Set<String> annotTypesToLoad;
	public List<String> extDocIDs;

	public TypeSystemDescription tsd;
	public String namespaceRangeRegex;

	public String colID;


	public void addAnnotTypeToLoad(String aTypeName) {
		if (annotTypesToLoad == null) {
			annotTypesToLoad = new HashSet<String>();
		}
		annotTypesToLoad.add(aTypeName);
	}


	public void addAnnotTypesToLoad(Set<String> typeNames) {
		if (annotTypesToLoad == null) {
			annotTypesToLoad = new HashSet<String>();
		}
		annotTypesToLoad.addAll(typeNames);
	}


	public String[] getAnnotTypesToLoad() {
		return annotTypesToLoad.toArray(new String[0]);
	}


	public boolean loadRestrictAnnotTypesAmount() {
		return (annotTypesToLoad != null);
	}

}
