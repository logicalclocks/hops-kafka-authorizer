package io.hops.kafka;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.apache.log4j.Logger;

/**
 * Class providing database connectivity to HopsWorks Kafka Authorizer.
 * <p>
 */
public class DbConnection {

  private static final Logger LOG = Logger.getLogger(DbConnection.class.getName());

  HikariDataSource datasource = null;

  public DbConnection(String dbType, String dbUrl, String dbUserName, String dbPassword, int maximumPoolSize,
      String cachePrepStmts, String prepStmtCacheSize, String prepStmtCacheSqlLimit) throws SQLException {
    LOG.info("Initializing database pool to:" + dbUrl);
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:mysql://" + dbUrl);
    config.setUsername(dbUserName);
    config.setPassword(dbPassword);
    config.addDataSourceProperty("cachePrepStmts", cachePrepStmts);
    config.addDataSourceProperty("prepStmtCacheSize", prepStmtCacheSize);
    config.addDataSourceProperty("prepStmtCacheSqlLimit", prepStmtCacheSqlLimit);
    config.addDataSourceProperty("maximumPoolSize", maximumPoolSize);
    datasource = new HikariDataSource(config);
    LOG.info("connection made successfully to:" + dbUrl);
  }

  /**
   *
   * @param topic
   * @return
   * @throws SQLException
   */
  public String getProjectName(String topic) throws SQLException {
    String projectName, projectId = null;
    try (Connection conn = datasource.getConnection()) {
      try (PreparedStatement prepStatement = conn.prepareStatement(
          "SELECT project_id from topic_acls where topic_name =?")) {
        prepStatement.setString(1, topic);
        try (ResultSet rst = prepStatement.executeQuery()) {
          if (rst != null && rst.next()) {
            projectId = rst.getString("project_id");
          }
        }
      }
      if (projectId != null) {
        try (PreparedStatement prepStatement = conn.prepareStatement(
            "SELECT id, projectname from project where id =?")) {
          prepStatement.setString(1, projectId);
          try (ResultSet rst = prepStatement.executeQuery()) {
            while (rst.next()) {
              projectName = rst.getString("projectname");
              return projectName;
            }
          }
        }
      }
    }
    return null;
  }

  /**
   *
   * @param topicName
   * @param principal
   * @return
   * @throws SQLException
   */
  public java.util.Set<HopsAcl> getTopicAcls(String topicName, String principal) throws SQLException {
    try (Connection conn = datasource.getConnection()) {
      try (PreparedStatement prepStatement = conn.prepareStatement(
          "SELECT * from topic_acls where topic_name =? and principal=?")) {
        prepStatement.setString(1, topicName);
        prepStatement.setString(2, principal);
        return fetchTopicAcls(prepStatement);
      }
    }
  }

  public java.util.Set<HopsAcl> getTopicAcls(String topicName) throws SQLException {
    try (Connection conn = datasource.getConnection()) {
      try (PreparedStatement prepStatement = conn.prepareStatement(
          "SELECT * from topic_acls where topic_name =?")) {
        prepStatement.setString(1, topicName);
        return fetchTopicAcls(prepStatement);
      }
    }
  }

  private java.util.Set<HopsAcl> fetchTopicAcls(PreparedStatement prepStatement) throws SQLException {
    try (ResultSet rst = prepStatement.executeQuery()) {
      java.util.Set<HopsAcl> acls = new java.util.HashSet<>();
      while (rst.next()) {
        acls.add(new HopsAcl(rst.getString(Consts.PRINCIPAL), rst.getString(Consts.PERMISSION_TYPE),
            rst.getString(Consts.OPERATION_TYPE), rst.getString(Consts.HOST), rst.getString(Consts.ROLE)));
      }
      return acls;
    }
  }

  /**
   *
   * @param projectName__userName
   * @return
   * @throws java.sql.SQLException
   */
  public String getUserRole(String projectName__userName) throws SQLException {
    String projectName = projectName__userName.split(Consts.PROJECT_USER_DELIMITER)[0];
    String userName = projectName__userName.split(Consts.PROJECT_USER_DELIMITER)[1];
    try (Connection conn = datasource.getConnection()) {
      try (PreparedStatement prepStatement = conn.prepareStatement("SELECT id from project where projectname=?")) {
        prepStatement.setString(1, projectName);
        try (ResultSet rst = prepStatement.executeQuery()) {
          while (rst.next()) {
            String projectId = rst.getString("id");
            if (projectId != null) {
              return getUserRole(projectId, userName);
            }
          }
        }
      }
    }
    return null;
  }

  /**
   *
   * @param projectId
   * @param userName
   * @return
   * @throws java.sql.SQLException
   */
  public String getUserRole(String projectId, String userName) throws SQLException {
    String email = null;
    try (Connection conn = datasource.getConnection()) {
      try (PreparedStatement prepStatement = conn.prepareStatement("SELECT email from users where username=?")) {
        prepStatement.setString(1, userName);
        try (ResultSet rst = prepStatement.executeQuery()) {
          while (rst.next()) {
            email = rst.getString("email");
          }
        }
      }
      if (email != null) {
        try (PreparedStatement prepStatement = conn.prepareStatement(
            "SELECT team_role from project_team where project_id=? AND team_member=?")) {
          prepStatement.setString(1, projectId);
          prepStatement.setString(2, email);
          try (ResultSet rst = prepStatement.executeQuery()) {
            if (rst != null) {
              rst.next();
              return rst.getString("team_role");
            }
          }
        }
      }
    }
    return null;
  }

  /**
   *
   * @param topicName
   * @param projectName__userName
   * @return
   * @throws java.sql.SQLException
   */
  public boolean isTopicOwner(String topicName, String projectName__userName) throws SQLException {
    String userName = projectName__userName.split(Consts.PROJECT_USER_DELIMITER)[1];
    try (Connection conn = datasource.getConnection()) {
      try (PreparedStatement prepStatement = conn.prepareStatement(
          "SELECT project_id from project_topics where topic_name=?")) {
        prepStatement.setString(1, topicName);
        try (ResultSet rst = prepStatement.executeQuery()) {
          while (rst.next()) {
            String topicOwnerProjectId = rst.getString("project_id");
            //if the user has a role, then it is a member of the topic owner project
            if (getUserRole(topicOwnerProjectId, userName) != null) {
              return true;
            }
          }
        }
      }
    }
    return false;
  }

  /**
   * Closes the jdbc datasource pool.
   */
  public void close() {
    if (datasource != null) {
      datasource.close();
    }
  }
}
