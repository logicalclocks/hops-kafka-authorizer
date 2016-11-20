package io.hops.kafka;

import java.sql.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author misdess
 */
public class DbConnection {

    private final String TWO_UNDERSCORES = "__";

    private Connection conn;

    private PreparedStatement prepStatements;

    private static final Logger CONNECTIONLOGGGER = Logger.
            getLogger(DbConnection.class.getName());

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

        String projectName = null;
        try {
            prepStatements = conn.prepareStatement("SELECT project_id from topic_acls where topic_name =?");
            prepStatements.setString(1, topicName);
            String projectId = prepStatements.executeQuery().getString("project_id");

            if (projectId == null) {
                CONNECTIONLOGGGER.log(Level.SEVERE, null,
                        "The speficied project doesn't exist in database");
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

        String projectName = projectName__userName.split(TWO_UNDERSCORES)[0];
        String userName = projectName__userName.split(TWO_UNDERSCORES)[1];

        try {
            prepStatements = conn.prepareStatement("SELECT * from project where projectname=?");
            prepStatements.setString(1, projectName);
            String projectId = prepStatements.executeQuery().getString("id");

            return getUserRole(projectId, userName);
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
        return null;
    }

    public String getUserRole(String projectId, String userName) {

        String role = null;
        try {
            prepStatements = conn.prepareStatement("SELECT * from users where username=?");
            prepStatements.setString(1, userName);
            String email = prepStatements.executeQuery().getString("email");

            prepStatements = conn.prepareStatement("SELECT * from project_team where"
                    + " project_id=? AND team_member=?");
            prepStatements.setString(1, projectId);
            prepStatements.setString(2, email);
            role = prepStatements.executeQuery().getString("team_role");
        } catch (SQLException ex) {
            return role;
        }
        return role;
    }

    public boolean isPrincipalMemberOfTopicOwnerProject(String topicName,
            String projectName__userName) {

        String userName = projectName__userName.split(TWO_UNDERSCORES)[1];
        try {
            prepStatements = conn.prepareStatement("SELECT * from project_topics where topicName=?");
            prepStatements.setString(1, topicName);

            String topicOwnerProjectId = prepStatements.executeQuery().getString("project_id");

            //if the user has a role, then it is a member of the topic owner project
            if (getUserRole(topicOwnerProjectId, userName) != null) {
                return true;
            }
        } catch (SQLException ex) {
            return false;
        }

        return false;
    }
}
