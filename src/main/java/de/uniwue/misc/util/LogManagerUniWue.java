package de.uniwue.misc.util;
import java.io.File;
import java.io.IOException;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;


public class LogManagerUniWue {


	private static Logger logger;
	public static boolean logToFileSystemSet = false;
  public static File logDir = new File(".", "log");


	public static void setLogToFileSystem() {
		if (!logToFileSystemSet) {
		  logger = getLogger();
	  	if (!logDir.exists()) {
	  		logDir.mkdir();
	  	}
			addHandler(new File(logDir, "std.log"), logger);
			logToFileSystemSet = true;
		}
	}


	public static void info(String aMessage) {
		getLogger().info(aMessage);
	}


	public static void warning(String aMessage) {
		getLogger().warning(aMessage);
	}


	public static void warning(String aMessage, Exception exp) {
		getLogger().log(Level.SEVERE, aMessage, exp);
	}


	public static Logger getLogger() {
		return getLogger("UniWue");
	}


	public static Logger getLogger(String name) {
		if (logger == null) {
		  logger = Logger.getLogger(name);
		}
		return logger;
	}


  public static void addHandler(File outputFile, Logger logger) {
    try {
      FileHandler handler = new FileHandler(outputFile.getAbsolutePath(), 1024 * 1024, 10);
      handler.setFormatter(new Formatter() {
        @Override
        public String format(LogRecord arg0) {
          return arg0.getMessage() + "\n";
        }
      });
      logger.addHandler(handler);
    } catch (SecurityException e) {
      e.printStackTrace();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }




}
