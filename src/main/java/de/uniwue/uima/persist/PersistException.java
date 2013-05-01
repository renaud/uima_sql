package de.uniwue.uima.persist;

public class PersistException extends Exception {

	private static final long serialVersionUID = 1L;


	public PersistException(String message) {
		super(message);
	}


	public PersistException(Exception exp) {
		super(exp);
	}

}
