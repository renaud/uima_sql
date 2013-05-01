package de.uniwue.misc.util;

/**
 * This interface has the purpose to wrap an eclipse ProgressMonitor and to use the
 * wrapped instance in another project without having the surrounding project to
 * reference the Eclipse packages
 * @author Georg Fette
 */
public interface IProgressMonitor {

	void beginTask(String name, int totalWork);
	void done();
	void subTask(String name);
	void worked(int totalWork);
	boolean isCanceled();

}
