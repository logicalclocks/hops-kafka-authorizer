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
  
  /**
   * Resolve {@code database.pool.connection.timeout.ms} from the broker config.
   *
   * Never throws. This is read inside Authorizer.configure(), where Kafka treats anything
   * thrown as a fatal fault and kills the process - so a typo in a broker property would take
   * the node down, which is the failure mode this class exists to avoid. A bad value falls
   * back to the default and says so.
   *
   * Rejects 0 explicitly: HikariCP maps it to Integer.MAX_VALUE, i.e. wait forever, which is
   * the opposite of what this setting is for and would park request-handler threads for
   * ~24.8 days. Values below HikariCP's 250 ms minimum are rejected for the same reason - it
   * would throw on them.
   */
  static long resolveConnectionTimeoutMs(Object raw) {
    if (raw == null) {
      return Consts.DATABASE_CONNECTION_TIMEOUT_MS_DEFAULT;
    }
    long parsed;
    try {
      parsed = Long.parseLong(raw.toString().trim());
    } catch (NumberFormatException e) {
      LOGGER.warn("{} is not a number: '{}'. Using {} ms.", Consts.DATABASE_CONNECTION_TIMEOUT_MS,
          raw, Consts.DATABASE_CONNECTION_TIMEOUT_MS_DEFAULT);
      return Consts.DATABASE_CONNECTION_TIMEOUT_MS_DEFAULT;
    }
    if (parsed < Consts.DATABASE_CONNECTION_TIMEOUT_MS_MIN) {
      LOGGER.warn("{}={} is below the {} ms minimum (0 would mean wait forever). Using {} ms.",
          Consts.DATABASE_CONNECTION_TIMEOUT_MS, parsed, Consts.DATABASE_CONNECTION_TIMEOUT_MS_MIN,
          Consts.DATABASE_CONNECTION_TIMEOUT_MS_DEFAULT);
      return Consts.DATABASE_CONNECTION_TIMEOUT_MS_DEFAULT;
    }
    return parsed;
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
    // Below connectionTimeout on purpose. On borrow, HikariCP validates an idle pooled
    // connection with isValid(validationTimeout) before it rechecks the borrow deadline, so
    // leaving this at its 5000 ms default would let a lookup take ~5 s once the database goes
    // away with connections already in the pool - longer than the timeout we just set. Halved
    // rather than matched so validation cannot consume the entire borrow budget, and floored
    // at HikariCP's own 250 ms minimum.
    config.setValidationTimeout(Math.max(250L, connectionTimeoutMs / 2));
    // Do not throw out of the constructor if the database is unreachable. This runs inside
    // Authorizer.configure(), where Kafka treats anything thrown as a fatal fault and
    // terminates the process, so a node whose DNS is not warm yet dies rather than waits:
    // observed on a freshly created KRaft controller, which crash-looped five times resolving
    // mysql.service.consul while the broker beside it was serving happily.
    //
    // With a negative value HikariCP (3.x and later; this needs at least that) skips its
    // start-up connection attempt entirely, so construction neither throws nor blocks and the
    // first connection is made on first use. 2.6.0 still made one synchronous attempt here and
    // threw PoolInitializationException when a connection opened but its setup failed, which
    // was a second way for configure() to take the node down.
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
