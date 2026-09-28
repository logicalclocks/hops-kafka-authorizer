package io.hops.kafka;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class providing database connectivity to HopsWorks Kafka Authorizer.
 * <p>
 */
public class DbConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(DbConnection.class.getName());
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
                      String cachePrepStmts, String prepStmtCacheSize, String prepStmtCacheSqlLimit,
                      long connectionTimeoutMs) {
    LOGGER.info("Initializing database pool to: {}", dbUrl);
    HikariConfig config = new HikariConfig();
    config.setJdbcUrl("jdbc:mysql://" + dbUrl);
    config.setUsername(dbUserName);
    config.setPassword(dbPassword);
    config.addDataSourceProperty("cachePrepStmts", cachePrepStmts);
    config.addDataSourceProperty("prepStmtCacheSize", prepStmtCacheSize);
    config.addDataSourceProperty("prepStmtCacheSqlLimit", prepStmtCacheSqlLimit);
    // setMaximumPoolSize, not addDataSourceProperty. The three above are Connector/J
    // properties and belong on the DataSource; this one is HikariCP's own, and handing it to
    // the driver means the driver ignores it and the pool silently keeps its default of 10.
    // database.pool.size has therefore never had any effect - masked until now only because
    // the chart's default happens to be 10 as well.
    config.setMaximumPoolSize(maximumPoolSize);
    // Bounds how long getConnection() blocks when no pooled connection is free - which, while
    // the database is unreachable, is every call. That wait happens on a Kafka
    // request-handler thread, so HikariCP's 30 s default lets a database outage occupy the
    // broker's handler pool: measured at 60 s per authorization (this timeout x the two tries
    // in HopsAclAuthorizer), and four producers were enough to stall an unrelated superuser
    // request from 1.5 s to 22 s.
    config.setConnectionTimeout(connectionTimeoutMs);
    // Do not throw out of the constructor if the database is unreachable. This runs inside
    // Authorizer.configure(), where Kafka treats anything thrown as a fatal fault and
    // terminates the process, so a node whose DNS is not warm yet dies rather than waits:
    // observed on a freshly created KRaft controller, which crash-looped five times resolving
    // mysql.service.consul while the broker beside it was serving happily.
    //
    // This does not make construction non-blocking. HikariCP's checkFailFast() still makes
    // one synchronous connection attempt whatever this is set to; a negative value only stops
    // it throwing. So a blackholed database still delays configure() by about
    // connectionTimeout, which is the other reason to keep that short.
    //
    // Deferring the failure is safe because the lookup path fails closed. A query against an
    // unreachable database surfaces as ExecutionException in authorizeProjectUser and ends in
    // DENIED, so an outage denies requests and logs loudly instead of taking the node down,
    // and recovers on its own once the database answers.
    config.setInitializationFailTimeout(-1);
    datasource = new HikariDataSource(config);
    LOGGER.info("Database pool created for: {} (connections are established on first use)", dbUrl);
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
