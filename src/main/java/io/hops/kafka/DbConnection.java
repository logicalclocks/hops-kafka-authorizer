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
    //Sql preparedStatements
    final String getTopicAclQuery = "SELECT * from topic_acls where topic_name =?";
    final String getPrincipalAclQuery = "SELECT * from topic_acls where project_name =?";
    final String getPrincipalAcls = "SELECT * from topic_acls where project_name =?";
    final String getProjectId = "SELECT * from project_topics where topic_name =?";
    final String getProjectName = "SELECT *  from project where id =?";
    final String getProjectTeams = "SELECT * from project_team";
    final String getProjects = "SELECT id, projectname from project";
    final String getUsers = "SELECT username, email from users";

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

    public ResultSet getTopicAcls(String topicName) {

        ResultSet result = null;
        try {
            prepStatements = conn.prepareStatement(getTopicAclQuery);
            prepStatements.setString(1, topicName);
            result = prepStatements.executeQuery();
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
        return result;
    }

    public Boolean topicExists(String topicName) {

        Boolean topicExists = false;
        try {
            prepStatements = conn.prepareStatement(getTopicAclQuery);
            prepStatements.setString(1, topicName);
            ResultSet result = prepStatements.executeQuery();
            //if next returns true, that means the topic exists
            topicExists = result.next();
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
        return topicExists;
    }

    private String getProjectName(String topicName) {

        String projectId = null;
        String projectName = null;
        ResultSet resultSet = null;
        try {

            prepStatements = conn.prepareStatement(getProjectId);
            prepStatements.setString(1, topicName);
            resultSet = prepStatements.executeQuery();
            while (resultSet.next()) {
                projectId = resultSet.getString("project_id");
            }

            prepStatements = conn.prepareStatement(getProjectName);
            prepStatements.setString(1, projectId);
            resultSet = prepStatements.executeQuery();
            while (resultSet.next()) {
                projectName = resultSet.getString("projectname");
            }
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
        return projectName;
    }

    public boolean isTopicBelongsToProject(String projectName, String topicName) {

        String projectId = null;

        try {

            prepStatements = conn.prepareStatement("SELECT id from project where projectname=?");
            prepStatements.setString(1, projectName);
            ResultSet resultSet = prepStatements.executeQuery();
            while (resultSet.next()) {
                projectId = resultSet.getString("id");
            }

            prepStatements = conn.prepareCall("SELECT * from project_topics where topic_name=? AND project_id=?");
            prepStatements.setString(1, topicName);
            prepStatements.setString(2, projectId);

            resultSet = prepStatements.executeQuery();
            if (resultSet.next() == false) {
                return false;
            }
        } catch (Exception e) {
            return false;
        }

        return true;
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
            ResultSet resutlSet = prepStatements.executeQuery();
            while (resutlSet.next()) {
                projectId = resutlSet.getString("id");
            }

            prepStatements = conn.prepareCall("SELECT from user where username=?");
            prepStatements.setString(1, userName);
            resutlSet = prepStatements.executeQuery();
            while (resutlSet.next()) {
                email = resutlSet.getString("useremail");
            }
            prepStatements = conn.prepareCall("SELECT from project_team where"
                    + " project_id=? AND team_member=?");
            prepStatements.setString(1, projectId);
            prepStatements.setString(1, email);
            resutlSet = prepStatements.executeQuery();
            if (resutlSet.next()) {
                role = resutlSet.getString("team_role");
            }
        } catch (Exception e) {
        }
        return role;
    }
}
