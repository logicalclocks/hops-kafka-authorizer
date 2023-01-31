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

  private static final String SQL_SELECT_TOPIC_PROJECT = "SELECT project_id " +
      "FROM project_topics " +
      "WHERE topic_name = ?";

  private static final String SQL_SELECT_PROJECT_ROLE = "SELECT project.id, project_team.team_role " +
      "FROM project_team " +
      "JOIN project ON project_team.project_id = project.id " +
      "JOIN users ON project_team.team_member = users.email " +
      "WHERE project.projectname = ? and users.username = ?";

  private static final String SQL_SELECT_SHARED_PROJECT = "SELECT dataset_shared_with.permission " +
      "FROM dataset_shared_with " +
      "JOIN dataset ON dataset_shared_with.dataset = dataset.id " +
      "WHERE dataset.feature_store_id IS NOT NULL and dataset_shared_with.project = ? and dataset.projectId = ?";

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
         PreparedStatement preparedStatement = getTopicProjectPreparedStatement(connection, topicName);
         ResultSet resultSet = preparedStatement.executeQuery()) {
      resultSet.next();
      return resultSet.getInt(1);
    }
  }

  public Pair<Integer, String> getProjectRole(String projectName, String username) throws SQLException {
    try (Connection connection = datasource.getConnection();
         PreparedStatement preparedStatement = getProjectRolePreparedStatement(connection, projectName, username);
         ResultSet resultSet = preparedStatement.executeQuery()) {
      resultSet.next();
      return new Pair<>(resultSet.getInt(1), resultSet.getString(2));
    }
  }

  public String getSharedProject(int userProjectId, int topicProjectId) throws SQLException {
    try (Connection connection = datasource.getConnection();
         PreparedStatement preparedStatement = getSharedProjectPreparedStatement(connection, userProjectId,
             topicProjectId);
         ResultSet resultSet = preparedStatement.executeQuery()) {
      resultSet.next();
      return resultSet.getString(1);
    }
  }

  private PreparedStatement getTopicProjectPreparedStatement(Connection connection, String topicName)
      throws SQLException {
    PreparedStatement preparedStatement = connection.prepareStatement(SQL_SELECT_TOPIC_PROJECT);
    preparedStatement.setString(1, topicName);
    return preparedStatement;
  }

  private PreparedStatement getProjectRolePreparedStatement(Connection connection, String projectName, String username)
      throws SQLException {
    PreparedStatement preparedStatement = connection.prepareStatement(SQL_SELECT_PROJECT_ROLE);
    preparedStatement.setString(1, projectName);
    preparedStatement.setString(2, username);
    return preparedStatement;
  }

  private PreparedStatement getSharedProjectPreparedStatement(Connection connection, int userProjectId,
                                                              int topicProjectId)
      throws SQLException {
    PreparedStatement preparedStatement = connection.prepareStatement(SQL_SELECT_SHARED_PROJECT);
    preparedStatement.setInt(1, userProjectId);
    preparedStatement.setInt(2, topicProjectId);
    return preparedStatement;
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
