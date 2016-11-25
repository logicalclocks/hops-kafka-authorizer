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
        String projectId = null;
        System.out.println("getProjectName.topicName:"+topicName);
        try {
            prepStatements = conn.prepareStatement("SELECT project_id from topic_acls where topic_name =?");
            prepStatements.setString(1, topicName);
            ResultSet rst = prepStatements.executeQuery();
            while (rst.next()) {
                projectId = rst.getString("project_id");
            }
            System.out.println("getProjectName.projectId:"+projectId);
            if (projectId == null) {
                CONNECTIONLOGGGER.log(Level.SEVERE, null,
                        "The speficied project doesn't exist in database");
                return null;
            }
            prepStatements = conn.prepareStatement("SELECT id, projectname from project where id =?");
            prepStatements.setString(1, projectId);

            
            rst = prepStatements.executeQuery();
            while (rst.next()) {
                projectName = rst.getString("projectname");
            }
            System.out.println("getProjectName.projectName:"+projectName);
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
        
        System.out.println("getUserRole1.projectName__userName:"+projectName__userName);
        
        String projectName = projectName__userName.split(TWO_UNDERSCORES)[0];
        String userName = projectName__userName.split(TWO_UNDERSCORES)[1];
        
        String projectId = null;
        try {
            prepStatements = conn.prepareStatement("SELECT * from project where projectname=?");
            prepStatements.setString(1, projectName);
            ResultSet rst = prepStatements.executeQuery();
            while (rst.next()) {
                projectId = rst.getString("id");
            }
            System.out.println("getUserRole1.projectId"+projectId);
            if (projectId == null) {
                return null;
            }
            
            return getUserRole(projectId, userName);
        } catch (SQLException ex) {
            System.out.println("getUserRole1 error:"+ex.toString());
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
        return null;
    }

    public String getUserRole(String projectId, String userName) {
        System.out.println("getUserRole2.projectId:"+projectId);
        System.out.println("getUserRole2.userName:"+userName);
        String role = null;
        try {
            prepStatements = conn.prepareStatement("SELECT * from users where username=?");
            prepStatements.setString(1, userName);

            ResultSet rst = prepStatements.executeQuery();
            String email = null;
            while (rst.next()) {
                email = rst.getString("email");
            }
            System.out.println("getUserRole2.email:"+email);
            prepStatements = conn.prepareStatement("SELECT * from project_team where"
                    + " project_id=? AND team_member=?");
            prepStatements.setString(1, projectId);
            prepStatements.setString(2, email);
            rst = prepStatements.executeQuery();
            while (rst.next()) {
                role = rst.getString("team_role");
            }
        } catch (SQLException ex) {
            System.out.println("getUserRole2 error:"+ex.toString());
            return role;
        }
        return role;
    }

    public boolean isPrincipalMemberOfTopicOwnerProject(String topicName,
            String projectName__userName) {
        System.out.println("isPrincipalMemberOfTopicOwnerProject.topic:"+topicName);
        System.out.println("isPrincipalMemberOfTopicOwnerProject.projectName__userName:"+projectName__userName);
        String userName = projectName__userName.split(TWO_UNDERSCORES)[1];

        String topicOwnerProjectId = null;
        try {
            prepStatements = conn.prepareStatement("SELECT * from project_topics where topicName=?");
            prepStatements.setString(1, topicName);

            ResultSet rst = prepStatements.executeQuery();
            while (rst.next()) {
                topicOwnerProjectId = rst.getString("project_id");

            }
            System.out.println("isPrincipalMemberOfTopicOwnerProject.topicOwnerProjectId:"+topicOwnerProjectId);
            if (topicOwnerProjectId == null) {
                return false;
            }
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
