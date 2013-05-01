package de.uniwue.uima.persist;

public class PersistConfig {

	private String docTableName;
	private String annotInstTableName;
	private String annotTypeTableName;
	private String featInstTableName;
	private String featTypeTableName;
	private String tablePrefix = "CASStore";


	public PersistConfig() {
		initializeTableNames();
	}


	public PersistConfig(String aTablePrefix) {
		tablePrefix = aTablePrefix;
		initializeTableNames();
	}


	private void initializeTableNames() {
		docTableName = "CASDocument";
		annotInstTableName = "Annot_Inst";
		annotTypeTableName = "Annot_Type";
		featInstTableName = "Feat_Inst";
		featTypeTableName = "Feat_Type";
		if (!tablePrefix.isEmpty()) {
			docTableName = tablePrefix + "_" + docTableName;
			annotInstTableName = tablePrefix + "_" + annotInstTableName;
			annotTypeTableName = tablePrefix + "_" + annotTypeTableName;
			featInstTableName = tablePrefix + "_" + featInstTableName;
			featTypeTableName = tablePrefix + "_" + featTypeTableName;
		}
	}


	public String getDocTableName() {
		return docTableName;
	}
	public void setDocTableName(String docTableName) {
		this.docTableName = docTableName;
	}


	public String getAnnotInstTableName() {
		return annotInstTableName;
	}
	public void setAnnotInstTableName(String annotInstTableName) {
		this.annotInstTableName = annotInstTableName;
	}


	public String getAnnotTypeTableName() {
		return annotTypeTableName;
	}
	public void setAnnotTypeTableName(String annotTypeTableName) {
		this.annotTypeTableName = annotTypeTableName;
	}


	public String getFeatInstTableName() {
		return featInstTableName;
	}
	public void setFeatInstTableName(String featInstTableName) {
		this.featInstTableName = featInstTableName;
	}


	public String getFeatTypeTableName() {
		return featTypeTableName;
	}
	public void setFeatTypeTableName(String featTypeTableName) {
		this.featTypeTableName = featTypeTableName;
	}


}
