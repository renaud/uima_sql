package de.uniwue.uima.persist;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.uima.cas.Feature;
import org.apache.uima.cas.Type;
import org.apache.uima.cas.TypeSystem;
import org.apache.uima.resource.metadata.FeatureDescription;
import org.apache.uima.resource.metadata.TypeDescription;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.apache.uima.util.InvalidXMLException;

import de.uniwue.misc.util.UIMAUtilUniWue;
import de.uniwue.uima.persist.data.LoadConfig;
import de.uniwue.uima.persist.dbManager.AnnotTypeManager;

public class TypeSystemLoadManager {

	private PersistenceManager persManager;


	public TypeSystemLoadManager(PersistenceManager aPersManager) {
		persManager = aPersManager;
	}


	public TypeSystemDescription loadTypesystem(Set<String> typeNames) throws PersistException {
		TypeSystemDescription tsd;
		tsd = UIMAUtilUniWue.createEmptyTypeSystem();
		loadTypesystem(typeNames, tsd);
		return tsd;
	}


	public TypeSystemDescription loadTypesystemByDocIDs(List<String> extIDs, String colID) throws PersistException {
		TypeSystemDescription tsd;
		tsd = UIMAUtilUniWue.createEmptyTypeSystem();
		loadTypesystemByDocIDs(extIDs, colID, tsd);
		return tsd;
	}


	public TypeSystemDescription loadTypesystemByColID(String domain) throws PersistException {
		TypeSystemDescription tsd;
		tsd = UIMAUtilUniWue.createEmptyTypeSystem();
		loadTypesystemByDomain(domain, tsd);
		return tsd;
	}


	public TypeSystemDescription loadTypesystemByNamespace(String namespaceRangeRegex)
			throws PersistException {
		TypeSystemDescription tsd;
		tsd = UIMAUtilUniWue.createEmptyTypeSystem();
		loadTypesystemByNamespace(namespaceRangeRegex, tsd);
		return tsd;
	}


	public void loadTypesystemByDomain(String domain,
			TypeSystemDescription tsd) throws PersistException {
		Set<String> typeNames = persManager.annotTypeManager.getAnnotTypeNamesByCol(domain);
		loadTypesystem(typeNames, tsd);
	}


	public void loadTypesystemByNamespace(String namespaceRangeRegex,
			TypeSystemDescription tsd) throws PersistException {
		LoadConfig loadConfig = new LoadConfig();
		Set<String> typeNames =
				persManager.annotTypeManager.getAnnotTypeNamesByNamespaceRange(namespaceRangeRegex);
		loadConfig.addAnnotTypesToLoad(typeNames);
		loadTypesystem(loadConfig, tsd);
	}


	public void loadTypesystem(Set<String> typeNames,
			TypeSystemDescription tsd) throws PersistException {
		LoadConfig loadConfig = new LoadConfig();
		loadConfig.addAnnotTypesToLoad(typeNames);
		loadTypesystem(loadConfig, tsd);
	}


	public TypeSystemDescription loadTypesystemByDocIDs(List<String> extIDs, String colID,
			TypeSystemDescription tsd) throws PersistException {
		Set<String> typeNames = persManager.annotTypeManager.getAnnotTypeNames(extIDs, colID);
		loadTypesystem(typeNames, tsd);
		return tsd;
	}


	public TypeSystemDescription loadTypesystem(LoadConfig loadConfig) throws PersistException {
		TypeSystemDescription tsd;
		tsd = UIMAUtilUniWue.createEmptyTypeSystem();
		loadTypesystem(loadConfig, tsd);
		return tsd;
	}


	public void loadTypesystem(LoadConfig loadConfig, TypeSystemDescription tsd) throws PersistException {
		if (loadConfig.namespaceRangeRegex != null) {
			loadTypesystemByNamespace(loadConfig.namespaceRangeRegex, tsd);
		} else if (loadConfig.loadRestrictAnnotTypesAmount()) {
			loadTypesystemInternal(loadConfig.getAnnotTypesToLoad(), tsd);
		} else if (loadConfig.extDocIDs != null) {
			loadTypesystemByDocIDs(loadConfig.extDocIDs, loadConfig.colID, tsd);
		} else {
			loadTypesystemByDomain(loadConfig.colID, tsd);
		}
	}


	private void loadTypesystemInternal(String[] typeNames,
			TypeSystemDescription tsd) throws PersistException {
  	try {
  		Set<String> existingTypes = new HashSet<String>();
  		for (TypeDescription aType : tsd.getTypes()) {
  			existingTypes.add(aType.getName());
  		}
  		for (String aTypeName : typeNames) {
  			if (!existingTypes.contains(aTypeName)) {
  				persManager.annotTypeManager.createAnnotType(aTypeName, tsd);
  			}
  		}
  		ensureMetaDataTypes(tsd);
			tsd.resolveImports();
		} catch (InvalidXMLException e) {
			throw new PersistException(e);
		}
	}


	public void ensureMetaDataTypes(TypeSystemDescription tsd) throws PersistException {
		// add the meta data types to the type system
		persManager.nameProvider.addMetaDataTypes(tsd);
		// ensure that the meta data types are saved in the database
		TypeSystemDescription metaTSD;
		metaTSD = UIMAUtilUniWue.createEmptyTypeSystem();
		persManager.nameProvider.addMetaDataTypes(metaTSD);
		persManager.saveTypesystem(metaTSD);
	}


	private void sortTypeDescriptions(List<TypeDescription> tds) {
		List<TypeDescription> result = new ArrayList<TypeDescription>();
		Set<String> collectedTypes = new HashSet<String>();

		while (!tds.isEmpty()) {
			List<TypeDescription> currentCollectedTypes = new ArrayList<TypeDescription>();
			for (TypeDescription aTD : tds) {
				if (aTD.getSupertypeName().equals(AnnotTypeManager.annotationTypeName) ||
						aTD.getSupertypeName().equals(AnnotTypeManager.documentAnnotationTypeName) ||
						collectedTypes.contains(aTD.getSupertypeName())) {
					currentCollectedTypes.add(aTD);
					collectedTypes.add(aTD.getName());
				}
			}
			tds.removeAll(currentCollectedTypes);
			result.addAll(currentCollectedTypes);
		}
		tds.clear();
		tds.addAll(result);
	}


	private void sortTypes(TypeSystem ts, List<Type> tds) {
		List<Type> result = new ArrayList<Type>();
		Set<String> collectedTypes = new HashSet<String>();

		while (!tds.isEmpty()) {
			List<Type> currentCollectedTypes = new ArrayList<Type>();
			for (Type aTD : tds) {
				String superTypeName = ts.getParent(aTD).getName();
				if (superTypeName.equals(AnnotTypeManager.annotationTypeName) ||
						superTypeName.equals(AnnotTypeManager.documentAnnotationTypeName) ||
						collectedTypes.contains(superTypeName)) {
					currentCollectedTypes.add(aTD);
					collectedTypes.add(aTD.getName());
				}
			}
			tds.removeAll(currentCollectedTypes);
			result.addAll(currentCollectedTypes);
		}
		tds.clear();
		tds.addAll(result);
	}


	public void saveTypesystem(TypeSystem ts) throws PersistException {
		List<Type> annotSiblingTypes = getAnnotTypesFromTypeSystem(ts);
		// as super types have to be stored before their siblings the list of types has to get sorted
		sortTypes(ts, annotSiblingTypes);
		for (Type td : annotSiblingTypes) {
			String parentName = ts.getParent(td).getName();
			long parentID = 0;
			if (!parentName.equals(AnnotTypeManager.annotationTypeName)) {
				parentID = persManager.annotTypeManager.getAnnotTypeID(parentName);
			}
			long annotTypeID = persManager.annotTypeManager.insertAnnotType(td.getName(), parentID);
			for (Feature fd : td.getFeatures()) {
				if (fd.getRange().getName().matches("^uima\\.cas\\..*Array")) {
					// TODO: arrays are not yet supported
					continue;
				}
				// store for me only those features that I define myself and not those inherited from my parent types
				if (fd.getDomain().equals(td)) {
					persManager.featTypeManager.insertFeatureType(fd.getShortName(), annotTypeID, fd.getRange().getName());
				}
			}
		}
		persManager.commit();
	}


	public void saveTypesystem(TypeSystemDescription tsDesc) throws PersistException {
		try {
			tsDesc.resolveImports();
		} catch (InvalidXMLException e) {
			throw new PersistException(e);
		}
		List<TypeDescription> annotSiblingTypes = getAnnotTypesFromTypeSystem(tsDesc);
		// as super types have to be stored before their siblings the list of types has to get sorted
		sortTypeDescriptions(annotSiblingTypes);
		for (TypeDescription td : annotSiblingTypes) {
			long annotTypeID;
			// store only types which are not yet known
			if (!persManager.annotTypeManager.getAnnotTypeNames().contains(td.getName())) {
				String parentName = td.getSupertypeName();
				long parentID = 0;
				if (!parentName.equals(AnnotTypeManager.annotationTypeName)) {
					parentID = persManager.annotTypeManager.getAnnotTypeID(parentName);
				}
				annotTypeID = persManager.annotTypeManager.insertAnnotType(td.getName(), parentID);
			} else {
				annotTypeID = persManager.annotTypeManager.getAnnotTypeID(td.getName());
			}
			for (FeatureDescription fd : td.getFeatures()) {
				if (fd.getRangeTypeName().matches("^uima\\.cas\\..*Array")) {
					// TODO: arrays are not yet supported
					continue;
				}
				persManager.featTypeManager.insertFeatureType(fd.getName(), annotTypeID, fd.getRangeTypeName());
			}
		}
		persManager.commit();
	}


	public List<Type> getAnnotTypesFromTypeSystem(TypeSystem ts) {
		Iterator<Type> typeIter = ts.getTypeIterator();
		List<Type> annotSiblingTypes = new ArrayList<Type>();
		while (typeIter.hasNext()) {
			Type type = typeIter.next();
			if (isSiblingOf(ts, type, AnnotTypeManager.annotationTypeName)) {
				annotSiblingTypes.add(type);
			}
		}
		return annotSiblingTypes;
	}


	public List<TypeDescription> getAnnotTypesFromTypeSystem(TypeSystemDescription tsd) {
		List<TypeDescription> annotSiblingTypes = new ArrayList<TypeDescription>();
		for (TypeDescription td : tsd.getTypes()) {
			// as there is no real inheritance of annotType to docannotType
			// in the type system descriptions I have to check on the possible inheritance of both types
			if (isSiblingOf(tsd, td, AnnotTypeManager.annotationTypeName) ||
					isSiblingOf(tsd, td, AnnotTypeManager.documentAnnotationTypeName)) {
				annotSiblingTypes.add(td);
			}
		}
		return annotSiblingTypes;
	}


	public boolean isSiblingOf(TypeSystem aTs, Type aType1, String aType2Name) {
		Type currentType = aType1;

		if (aType1.getName().equals(aType2Name)) {
			return false;
		}
		while ((currentType != null) && !currentType.getName().equals(AnnotTypeManager.annotationTypeName)) {
			if (currentType.getName().equals(aType2Name)) {
				return true;
			}
			currentType = aTs.getParent(currentType);
		}
		if ((currentType != null) && currentType.getName().equals(aType2Name)) {
			return true;
		}
		return false;
	}


	public boolean isSiblingOf(TypeSystemDescription tsd, TypeDescription aType1, String aType2Name) {
		TypeDescription currentType = aType1;

		while (currentType != null) {
			if (currentType.getSupertypeName().equals(aType2Name)) {
				return true;
			}
			currentType = tsd.getType(currentType.getSupertypeName());
		}
		return false;
	}


}
