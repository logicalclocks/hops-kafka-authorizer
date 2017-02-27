package io.hops.kafka;

import java.sql.*;
import java.util.logging.Level;
import org.apache.log4j.Logger;

/**
 * Class providing database connectivity to HopsWorks Kafka Authorizer.
 * <p>
 */
public class DbConnection {

  private static final Logger LOGGER = Logger.
      getLogger(DbConnection.class.getName());

  private Connection conn;
  private PreparedStatement prepStatements;

  public DbConnection(String dbType, String dbUrl, String dbUserName,
      String dbPassword) {

    LOGGER.info("testing database connection to: {0}");

    try {
      if (dbType.equalsIgnoreCase("mysql")) {
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection("jdbc:mysql://" + dbUrl, dbUserName,
            dbPassword);
        LOGGER.info("connection made successfully to:" + dbUrl);
      }

    } catch (SQLException | ClassNotFoundException ex) {
      LOGGER.error(ex.toString(), ex);
    }
  }

  public String getProjectName(String topic) {

    String projectName = null;
    String projectId = null;
    try {
      prepStatements = conn.prepareStatement(
          "SELECT project_id from topic_acls where topic_name =?");
      prepStatements.setString(1, topic);
      ResultSet rst = prepStatements.executeQuery();
      while (rst.next()) {
        projectId = rst.getString("project_id");
      }
      if (projectId == null) {
        LOGGER.debug("The speficied project doesn't exist in database for topic:" + topic);
        return null;
      }
      prepStatements = conn.prepareStatement(
          "SELECT id, projectname from project where id =?");
      prepStatements.setString(1, projectId);

      rst = prepStatements.executeQuery();
      while (rst.next()) {
        projectName = rst.getString("projectname");
      }
    } catch (SQLException ex) {
      LOGGER.error(ex.toString(), ex);
    }
    return projectName;
  }

  public ResultSet getTopicAcls(String topicName) {

    ResultSet result = null;
    try {
      prepStatements = conn.prepareStatement(
          "SELECT * from topic_acls where topic_name =?");
      prepStatements.setString(1, topicName);
      result = prepStatements.executeQuery();
    } catch (SQLException ex) {
      LOGGER.error(ex.toString(), ex);
    }
    return result;
  }

  public String getUserRole(String projectName__userName) {

    String projectName = projectName__userName.split(Consts.PROJECT_USER_DELIMITER)[0];
    String userName = projectName__userName.split(Consts.PROJECT_USER_DELIMITER)[1];

    String projectId = null;
    try {
      prepStatements = conn.prepareStatement(
          "SELECT * from project where projectname=?");
      prepStatements.setString(1, projectName);
      ResultSet rst = prepStatements.executeQuery();
      while (rst.next()) {
        projectId = rst.getString("id");
      }
      if (projectId == null) {
        return null;
      }

      return getUserRole(projectId, userName);
    } catch (SQLException ex) {
      LOGGER.error(ex.toString(), ex);
    }
    return null;
  }

  public String getUserRole(String projectId, String userName) {
    String role = null;
    try {
      prepStatements = conn.prepareStatement(
          "SELECT * from users where username=?");
      prepStatements.setString(1, userName);

      ResultSet rst = prepStatements.executeQuery();
      String email = null;
      while (rst.next()) {
        email = rst.getString("email");
      }
      prepStatements = conn.prepareStatement("SELECT * from project_team where"
          + " project_id=? AND team_member=?");
      prepStatements.setString(1, projectId);
      prepStatements.setString(2, email);
      rst = prepStatements.executeQuery();
      while (rst.next()) {
        role = rst.getString("team_role");
      }
    } catch (SQLException ex) {
      LOGGER.error(ex.toString(), ex);
      return role;
    }
    return role;
  }

  public boolean isTopicOwner(String topicName,
      String projectName__userName) {
    String userName = projectName__userName.split(Consts.PROJECT_USER_DELIMITER)[1];

    String topicOwnerProjectId = null;
    try {
      prepStatements = conn.prepareStatement(
          "SELECT * from project_topics where topic_name=?");
      prepStatements.setString(1, topicName);

      ResultSet rst = prepStatements.executeQuery();
      while (rst.next()) {
        topicOwnerProjectId = rst.getString("project_id");
      }
      if (topicOwnerProjectId == null) {

        return false;
      }
      //if the user has a role, then it is a member of the topic owner project
      if (getUserRole(topicOwnerProjectId, userName) != null) {
        return true;
      }
    } catch (SQLException ex) {
      LOGGER.error(ex.toString(), ex);
      return false;
    }

    return false;
  }
  
  public void close() {
    try {
      if(conn != null && !conn.isClosed()){
        conn.close();
      }
    } catch (SQLException ex) {
      LOGGER.error("Could not close authorizer connection to database", ex);
    }
  }
}
