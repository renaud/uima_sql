package de.uniwue.uima.persist.data;

import java.util.HashSet;
import java.util.Set;

/**
 * The save config determines how CASes are saved.
 * It can be defined if only a subset of annotation types are stored.
 * @author Georg Fette
 */
public class SaveConfig {

	private Set<String> annotTypesToSave;


	public Set<String> getAnnotTypesToSave() {
		return annotTypesToSave;
	}


	public void addAnnotTypeToSave(String aType) {
		if (annotTypesToSave == null) {
			annotTypesToSave = new HashSet<String>();
		}
		annotTypesToSave.add(aType);
	}


	public boolean saveRestrictedAnnotTypeAmount() {
		return (annotTypesToSave != null);
	}


}
