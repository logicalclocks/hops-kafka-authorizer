package io.hops.kafka;

import io.hops.kafka.authorizer.tables.HopsAcl;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * Class providing database connectivity to HopsWorks Kafka Authorizer.
 * <p>
 */
public class DbConnection {
  
  private static final Logger LOG = Logger.getLogger(DbConnection.class.getName());

  private static final String SQL_COMMAND_SPECIFIC = "SELECT acls.*, project_team.team_role " +
      "FROM topic_acls as acls " +
      "JOIN project_team ON acls.project_id = project_team.project_id AND acls.username = project_team.team_member " +
      "WHERE topic_name = '%s'";

  private final HikariDataSource datasource;
  
  public DbConnection(String dbUrl, String dbUserName, String dbPassword, int maximumPoolSize,
                      String cachePrepStmts, String prepStmtCacheSize, String prepStmtCacheSqlLimit)
      throws SQLException {
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

  public Map<String, List<HopsAcl>> getAcls(String topicName) throws SQLException {
    int tries = 1;

    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    Map<String, List<HopsAcl>> topicAcls = new HashMap<>();

    while (tries > 0) {
      try {
        connection = datasource.getConnection();
        statement = connection.createStatement();
        String command = String.format(SQL_COMMAND_SPECIFIC, topicName);
        resultSet = statement.executeQuery(command);

        while (resultSet.next()) {
          HopsAcl acl = new HopsAcl(resultSet.getString(Consts.TOPIC_NAME),
              resultSet.getString(Consts.PRINCIPAL),
              resultSet.getString(Consts.PERMISSION_TYPE),
              resultSet.getString(Consts.OPERATION_TYPE),
              resultSet.getString(Consts.HOST),
              resultSet.getString(Consts.ROLE),
              resultSet.getString(Consts.TEAM_ROLE)
          );

          List<HopsAcl> topicPrincipalAcl = topicAcls.getOrDefault(acl.getPrincipal(), new ArrayList<>());
          topicPrincipalAcl.add(acl);
          topicAcls.put(acl.getPrincipal(), topicPrincipalAcl);
        }
        break;
      } catch (Exception e) {
        LOG.error("Exception during the retrieval of acls for topic: '" + topicName + "'," +
            "retries left: " + tries, e);
        tries--;
        topicAcls = new HashMap<>();
      } finally {
        if (statement != null) {
          statement.close();
        }
        if (resultSet != null) {
          resultSet.close();
        }
        if (connection != null) {
          connection.close();
        }
      }
    }

    return topicAcls;
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
