package io.hops.kafka;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

/**
 * Class providing database connectivity to HopsWorks Kafka Authorizer.
 * <p>
 */
public class DbConnection {
  
  private static final Logger LOG = Logger.getLogger(DbConnection.class.getName());

  private static final String SQL_SELECT_TOPIC_PROJECT = "SELECT pt.project_id " +
      "FROM project_topics pt " +
      "WHERE pt.topic_name = ?";

  private static final String SQL_SELECT_PROJECT_ROLE = "SELECT p.id, pt.team_role " +
      "FROM project_team pt " +
      "JOIN project p ON pt.project_id = p.id " +
      "JOIN users u ON pt.team_member = u.email " +
      "WHERE p.projectname = ? AND u.username = ?";

  private static final String SQL_SELECT_SHARED_PROJECT = "SELECT dsw.permission " +
      "FROM dataset_shared_with dsw " +
      "JOIN dataset d ON dsw.dataset = d.id " +
      "WHERE d.feature_store_id IS NOT NULL AND dsw.project = ? AND d.projectId = ?";

  private final HikariDataSource datasource;

  // For testing
  protected DbConnection(HikariDataSource datasource) {
    this.datasource = datasource;
  }
  
  public DbConnection(String dbUrl, String dbUserName, String dbPassword, int maximumPoolSize,
                      String cachePrepStmts, String prepStmtCacheSize, String prepStmtCacheSqlLimit) {
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

  public Integer getTopicProject(String topicName) throws SQLException {
    try (Connection connection = datasource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(SQL_SELECT_TOPIC_PROJECT)) {
      preparedStatement.setString(1, topicName);
      try(ResultSet resultSet = preparedStatement.executeQuery()) {
        if (resultSet.next())
          return resultSet.getInt(1);
        return null;
      }
    }
  }

  public Pair<Integer, String> getProjectRole(String projectName, String username) throws SQLException {
    try (Connection connection = datasource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(SQL_SELECT_PROJECT_ROLE)) {
      preparedStatement.setString(1, projectName);
      preparedStatement.setString(2, username);
      try(ResultSet resultSet = preparedStatement.executeQuery()) {
        if (resultSet.next())
          return new Pair<>(resultSet.getInt(1), resultSet.getString(2));
        return null;
      }
    }
  }

  public String getSharedProject(int userProjectId, int topicProjectId) throws SQLException {
    try (Connection connection = datasource.getConnection();
         PreparedStatement preparedStatement = connection.prepareStatement(SQL_SELECT_SHARED_PROJECT)) {
      preparedStatement.setInt(1, userProjectId);
      preparedStatement.setInt(2, topicProjectId);
      try(ResultSet resultSet = preparedStatement.executeQuery()) {
        if (resultSet.next())
          return resultSet.getString(1);
        return null;
      }
    }
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
