package io.hops.kafka;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author misdess
 */
public class DbConnection {

    private Connection conn;
    PreparedStatement prepStatements;

    static final Logger CONNECTIONLOGGGER
            = new LoggerProperties(DbConnection.class.getName()).getLogger();

    public DbConnection(String dbType, String dbUrl, String dbUserName, String dbPassword) {

        CONNECTIONLOGGGER.log(Level.INFO, "testing database connection to: {0}", new Object[]{dbUrl});

        try {
            if (dbType.equalsIgnoreCase("mysql")) {
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection("jdbc:mysql://" + dbUrl, dbUserName, dbPassword);
                CONNECTIONLOGGGER.log(Level.INFO, "connection made successfully to: {0}", new Object[]{dbUrl});
            }

        } catch (SQLException | ClassNotFoundException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
    }

    public String getProjectName(String topicName) {

        String projectId = null;
        String projectName = null;
        try {
            prepStatements = conn.prepareStatement("SELECT project_id from topic_acls where topic_name =?");
            prepStatements.setString(1, topicName);
            projectId = prepStatements.executeQuery().getString("project_id");

            if (projectId == null) {
                CONNECTIONLOGGGER.log(Level.SEVERE, null,
                        "The speficied project doesnt exist in database");
                return null;
            }
            prepStatements = conn.prepareStatement("SELECT id, projectname from project where id =?");
            prepStatements.setString(1, projectId);
            projectName = prepStatements.executeQuery().getString("projectname");
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
        return projectName;
    }

    public ResultSet getTopicAcls(String topicName) {

        ResultSet result = null;
        try {
            prepStatements = conn.prepareStatement("SELECT * from topic_acls where topic_name =?");
            prepStatements.setString(1, topicName);
            result = prepStatements.executeQuery();
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
        return result;
    }

    public String getUserRole(String projectName__userName) {

        String projectName = projectName__userName.split("__")[0];
        String userName = projectName__userName.split("__")[1];

        String role = null;
        String projectId = null;
        String email = null;

        try {
            prepStatements = conn.prepareCall("SELECT from project where projectname=?");
            prepStatements.setString(1, projectName);
            projectId = prepStatements.executeQuery().getString("id");

            prepStatements = conn.prepareCall("SELECT from user where username=?");
            prepStatements.setString(1, userName);
            email = prepStatements.executeQuery().getString("useremail");

            prepStatements = conn.prepareCall("SELECT from project_team where"
                    + " project_id=? AND team_member=?");
            prepStatements.setString(1, projectId);
            prepStatements.setString(1, email);
            role = prepStatements.executeQuery().getString("team_role");
        } catch (Exception e) {
        }
        return role;
    }
}
