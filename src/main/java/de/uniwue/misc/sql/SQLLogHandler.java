package de.uniwue.misc.sql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * A handler which stores logged data in a sql database table.
 *
 * @author Georg Fette
 */
public class SQLLogHandler extends DatabaseManager {

	private int logged = 0;
	private static List<SQLLogHandler> handlers = new ArrayList<SQLLogHandler>();


	private SQLLogHandler(SQLManager aSqlManager, Logger logger) throws SQLException {
		super(aSqlManager);
		createSQLTables();
		registerSQLLogHandler(logger);
		handlers.add(this);
	}


	public static void dispose() throws SQLException {
		for (SQLLogHandler aHandler : handlers) {
			aHandler.commit();
		}
	}


	public static void registerHandler(SQLManager aSqlManager, Logger logger) throws SQLException {
		new SQLLogHandler(aSqlManager, logger);
	}


  public void registerSQLLogHandler(Logger logger) {
    try {
    	Handler handler = new Handler() {

				@Override public void publish(LogRecord record) {
					Date today = new Date();
					saveEntry(record.getMessage(), new Timestamp(today.getTime()),
							record.getLevel().toString());
					logged++;
					if (logged % 1000 == 0) {
						flush();
					}
				}

				@Override public void flush() {
					try {
						sqlManager.commit();
					} catch (SQLException e) {
						e.printStackTrace();
					}
				}

				@Override public void close() throws SecurityException {

					try {
						sqlManager.commit();
					} catch (SQLException e) {
						e.printStackTrace();
					}

				}
    	};
      logger.addHandler(handler);
    } catch (SecurityException e) {
      e.printStackTrace();
    }
  }


  private void saveEntry(String message, Timestamp time, String level) {
    PreparedStatement st;
    String command = "";

    try {
      command += "INSERT INTO " + getTableName() + " " +
        "(message, time, level) VALUES (?, ?, ?)";
      st = sqlManager.createPreparedStatement(command);
      st.setString(1, message);
      st.setTimestamp(2, time);
      st.setString(3, level);
      st.execute();
      st.close();
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }


	@Override public String getTableName() {
		return "Log";
	}


	@Override protected void readResult(ResultSet resultSet) {
	}


	@Override protected String getCreateTableString() {
    String command = "";

    command = getCreateTableStub();
    command +=
      "id BIGINT IDENTITY(0, 1) NOT NULL PRIMARY KEY, \n" +
      "message VARCHAR(5000), \n" +
      "level VARCHAR(100), \n" +
      "time DATETIME2(0) \n" +
      ") ";
    return command;
	}


}
