package io.hops.kafka;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.*;
import org.apache.log4j.Logger;
//import org.apache.tomcat.jdbc.pool.DataSource;
//import org.apache.tomcat.jdbc.pool.PoolProperties;

/**
 * Class providing database connectivity to HopsWorks Kafka Authorizer.
 * <p>
 */
public class DbConnection {

  private static final Logger LOG = Logger.getLogger(DbConnection.class.getName());

//  DataSource datasource = new DataSource();
  HikariDataSource datasource = null;

  public DbConnection(String dbType, String dbUrl, String dbUserName, String dbPassword) throws SQLException {
    LOG.info("testing database connection to: {0}");

//    PoolProperties p = new PoolProperties();
//    p.setUrl("jdbc:mysql://" + dbUrl);
//    p.setDriverClassName("com.mysql.jdbc.Driver");
//    p.setUsername(dbUserName);
//    p.setPassword(dbPassword);
//    p.setJmxEnabled(true);
//    p.setTestWhileIdle(false);
//    p.setTestOnBorrow(true);
//    p.setValidationQuery("SELECT 1");
//    p.setTestOnReturn(false);
//    p.setValidationInterval(30000);
//    p.setTimeBetweenEvictionRunsMillis(30000);
//    p.setMaxActive(20);
//    p.setInitialSize(2);
//    p.setMaxWait(10000);
//    //Timout connection after 3 hours. NDB invalidates connection after 8 hours.
//    p.setRemoveAbandonedTimeout(3600 * 3);
//    p.setMinEvictableIdleTimeMillis(30000);
//    p.setMinIdle(10);
//    p.setLogAbandoned(true);
//    p.setRemoveAbandoned(true);
//    p.setJdbcInterceptors("org.apache.tomcat.jdbc.pool.interceptor.ConnectionState;"
//        + "org.apache.tomcat.jdbc.pool.interceptor.StatementFinalizer");
//    datasource.setPoolProperties(p);
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:mysql://" + dbUrl);
    config.setUsername(dbUserName);
    config.setPassword(dbPassword);
    config.addDataSourceProperty("cachePrepStmts", "true");
    config.addDataSourceProperty("prepStmtCacheSize", "250");
    config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
    config.addDataSourceProperty("maximumPoolSize", 30);
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
        try (PreparedStatement prepStatement = conn.prepareStatement("SELECT id, projectname from project where id =?")) {
          prepStatement.setString(1, projectId);
          try (ResultSet rst = prepStatement.executeQuery()) {
            rst.next();
            projectName = rst.getString("projectname");
            return projectName;
          }
        }
      }
    }
    return null;
  }

  /**
   *
   * @param topicName
   * @return
   * @throws SQLException
   */
  public HopsAcl getTopicAcls(String topicName) throws SQLException {
    try (Connection conn = datasource.getConnection()) {
      try (PreparedStatement prepStatement = conn.prepareStatement(
          "SELECT * from topic_acls where topic_name =?")) {
        prepStatement.setString(1, topicName);
        try (ResultSet rst = prepStatement.executeQuery()) {
          if (rst != null && rst.next()) {
            HopsAcl acl = new HopsAcl(rst.getString(Consts.PRINCIPAL), rst.getString(Consts.PERMISSION_TYPE),
                rst.getString(Consts.OPERATION_TYPE), rst.getString(Consts.HOST), rst.getString(Consts.ROLE));
            return acl;
          }
        }
      }
    }
    return null;
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
      try (PreparedStatement prepStatement = conn.prepareStatement("SELECT * from project where projectname=?")) {
        prepStatement.setString(1, projectName);
        try (ResultSet rst = prepStatement.executeQuery()) {
          if (rst != null && rst.next()) {
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
      try (PreparedStatement prepStatement = conn.prepareStatement("SELECT * from users where username=?")) {
        prepStatement.setString(1, userName);
        try (ResultSet rst = prepStatement.executeQuery()) {
          if (rst != null && rst.next()) {
            email = rst.getString("email");
          }
        }
      }
      if (email != null) {
        try (PreparedStatement prepStatement = conn.prepareStatement(
            "SELECT * from project_team where project_id=? AND team_member=?")) {
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
          "SELECT * from project_topics where topic_name=?")) {
        prepStatement.setString(1, topicName);
        try (ResultSet rst = prepStatement.executeQuery()) {
          if (rst != null && rst.next()) {
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
