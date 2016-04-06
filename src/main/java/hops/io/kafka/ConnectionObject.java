package hops.io.kafka;

import java.sql.*;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.security.auth.Acl;

/**
 *
 * @author misdess
 */
public class ConnectionObject {
      
    private static Connection conn = null;
    PreparedStatement prepStatements;

    static final Logger CONNECTIONLOGGGER = Logger.getLogger(ConnectionObject.class.getName());

    //Sql preparedStatements
    final String getAllAclsQuery = "select * from topic_acls";
    final String getTopicAclQuery = "select * from topic_acls where topic_name =?";
    final String getPrincipalAcls = "select * from topic_acls where project_name =?";
    final String getProjectId = "select * from project_topics where topic_name =?";
    final String getProjectName = "select * projectname from project where id =?";

    final String insertTopicAcl = "insert into topic_acls values()";
    final String insertAcl = "insert into topic_acls (topic_name, project_name, "
            + "role, operation_type, permission_type, host) values(?, ?, ?, ?, ?, ?)";

    final String delecTopicAcls = "delete from topic_acls where topic_name =?"
            + " AND project_name =? AND role=? AND operation_type=? AND"
            + " permission_type=? AND host=?";
    final String deleteTopic = "delete from project_topics where topic_name =?";
  
    public ConnectionObject(String dbType) {
         try {
            if (dbType.equalsIgnoreCase("mysql")) {
                Class.forName("com.mysql.jdbc.Driver");
                conn
                        = DriverManager.getConnection("database url");
            }
        } catch (SQLException | ClassNotFoundException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
       
    }

    public void addTopicAcls(String topicName, Set<Acl> acls) {

        String projectName = getProjectName(topicName); 

        try {

            prepStatements = conn.prepareStatement(insertTopicAcl);
            for (Acl acl : acls) {
                prepStatements.setString(1, topicName);
                prepStatements.setString(2, projectName);
                prepStatements.setString(3, "data owner");
                prepStatements.setString(4, acl.operation().name());
                prepStatements.setString(5, acl.permissionType().name());
                prepStatements.setString(6, acl.host());
                prepStatements.execute();
            }
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
    }

    public ResultSet getAllAcls() {

        ResultSet result = null;

        try {

            prepStatements = conn.prepareStatement(getAllAclsQuery);
            result = prepStatements.executeQuery();

        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
        return result;
    }
 
    public Set<List<String>> getTopicAcls(String topicName) {

        Set<List<String>> topicAcls = new HashSet<>();
        List<String> acl = new LinkedList<>();

        try {

            prepStatements = conn.prepareStatement(getTopicAclQuery);
            prepStatements.setString(1, topicName);
            ResultSet result = prepStatements.executeQuery();

            while (result.next()) {

                acl.add(result.getString("project_name"));
                acl.add(result.getString("permission_type"));
                acl.add(result.getString("operation_type"));
                acl.add(result.getString("host"));
                acl.add(result.getString("role"));

                if (!topicAcls.contains(acl)) {
                    topicAcls.add(acl);
                    acl.clear();
                }
            }
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
        return topicAcls;
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
 
    public Boolean removeTopic(String topicName) {
        
        Boolean aclRemoved = false;
        try {
            prepStatements = conn.prepareStatement(deleteTopic);
            prepStatements.setString(1, topicName);
            aclRemoved = prepStatements.execute();
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
        return aclRemoved;
    }
    
    public Boolean removeAcls(String topicName, Set<Acl> acls) {
        
        Boolean alcsRemoved = false;
        String projectName = getProjectName(topicName);

        try {

            prepStatements = conn.prepareStatement(delecTopicAcls);
            for (Acl acl : acls) {
                prepStatements.setString(1, topicName);
                prepStatements.setString(2, projectName);
                prepStatements.setString(3, "data owner");
                prepStatements.setString(4, acl.operation().name());
                prepStatements.setString(5, acl.permissionType().name());
                prepStatements.setString(6, acl.host());
                alcsRemoved = prepStatements.execute();
            }
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
        return alcsRemoved;
    }
    
    private String getProjectName(String topicName) {

        String projectId = null;
        String projectName = null;

        try {

            prepStatements = conn.prepareStatement(getProjectId);
            prepStatements.setString(1, topicName);
            ResultSet resultSet = prepStatements.executeQuery();
            while (resultSet.next()) {
                projectId = resultSet.getString("id");
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

    public void closeConnection() {
        try {
            conn.close();
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }
    }
}
