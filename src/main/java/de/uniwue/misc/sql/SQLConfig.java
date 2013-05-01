package de.uniwue.misc.sql;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;

import de.uniwue.misc.util.EnvironmentUniWue;
import de.uniwue.misc.util.FileUtilsUniWue;
import de.uniwue.misc.util.LogManagerUniWue;

/**
 * The SQLConfig defines the coordinates to access a database. The information
 * can be either given as a JDBC url or via server, database, user, password. In
 * either way the correct dbType has to be set.
 * 
 * @author Georg Fette
 */
@XmlRootElement
public class SQLConfig {

    @XmlAttribute
    public String user = "";
    @XmlAttribute
    public String database = "";
    @XmlAttribute
    public String password = "";
    @XmlAttribute
    public String sqlServer = "";
    @XmlAttribute
    // FIXME public DBType dbType = DBType.MSSQL;
    public DBType dbType = DBType.MySQL;

    @XmlAttribute
    public boolean useJDBUrl = false;
    @XmlAttribute
    public String jdbcURL = "";

    @SuppressWarnings("unused")
    private SQLConfig() {
    }

    public SQLConfig(String aUser, String aDatabase, String aPassword,
            String anSQLServer, DBType aDbType) {
        user = aUser;
        database = aDatabase;
        password = aPassword;
        sqlServer = anSQLServer;
        dbType = aDbType;
    }

    public SQLConfig(SQLConfig anotherConfig) {
        this(anotherConfig.user, anotherConfig.database,
                anotherConfig.password, anotherConfig.sqlServer,
                anotherConfig.dbType);
    }

    public SQLConfig(String aUser, String aDatabase, String aPassword,
            String anSQLServer) {
        this(aUser, aDatabase, aPassword, anSQLServer, decideDBType());
    }

    @Override
    public boolean equals(Object anObj) {
        if (!(anObj instanceof SQLConfig)) {
            return false;
        }
        SQLConfig aConf = (SQLConfig) anObj;
        return (aConf.database.equals(database)
                && aConf.sqlServer.equals(sqlServer) && aConf.user.equals(user) && aConf.password
                    .equals(password));
    }

    @Override
    public int hashCode() {
        return (user + database + password + sqlServer + dbType.toString() + jdbcURL)
                .hashCode();
    }

    public static DBType decideDBType() {
        if (EnvironmentUniWue.isMSSQL()) {
            return DBType.MSSQL;
        } else {
            return DBType.MySQL;
        }
    }

    public static SQLConfig read(File aFile) {
        String text, user = null, db = null, server = null, pass = null;
        try {
            text = FileUtilsUniWue.file2String(aFile);
            String[] lines = text.split("\n");
            for (String aLine : lines) {
                String[] tokens = aLine.split("\t");
                if (tokens[0].equals("user")) {
                    user = tokens[1].trim();
                } else if (tokens[0].equals("database")) {
                    db = tokens[1].trim();
                } else if (tokens[0].equals("password")) {
                    pass = tokens[1].trim();
                } else if (tokens[0].equals("sqlServer")) {
                    server = tokens[1].trim();
                } else {
                    LogManagerUniWue.warning("strange config element '"
                            + tokens[0]);
                }
            }
            SQLConfig result = new SQLConfig(user, db, pass, server);
            return result;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    public String ToXML() {
        JAXBContext context;
        try {
            context = JAXBContext.newInstance(SQLConfig.class);
            Marshaller m = context.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, Boolean.TRUE);
            StringWriter stream = new StringWriter();
            m.marshal(this, stream);
            String text = stream.getBuffer().toString();
            return text;
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static SQLConfig tryUnmarshal(String xml) {
        SQLConfig result = null;
        try {
            JAXBContext context = JAXBContext.newInstance(SQLConfig.class);
            Unmarshaller m = context.createUnmarshaller();
            StringReader reader = new StringReader(xml);
            result = (SQLConfig) m.unmarshal(reader);
            return result;
        } catch (JAXBException e) {
            e.printStackTrace();
            return null;
        }
    }
}
