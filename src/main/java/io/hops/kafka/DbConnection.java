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

  private static final String SQL_COMMAND = "SELECT DISTINCT acls.*,project_team.team_role "
      + "FROM topic_acls as acls, users, project_team "
      + "WHERE project_team.project_id = acls.project_id "
      + "AND project_team.team_member = users.email "
      + "AND users.username = substring_index(principal, '" + Consts.PROJECT_USER_DELIMITER + "', -1) "
      + "ORDER BY topic_name, principal";

  private HikariDataSource datasource = null;
  
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

  public Map<String, Map<String, List<HopsAcl>>> getAcls() throws SQLException {
    Connection connection = null;
    Statement statement = null;
    ResultSet resultSet = null;

    Map<String, Map<String, List<HopsAcl>>> newAcls = new HashMap<>();

    try {
      connection = datasource.getConnection();
      statement = connection.createStatement();
      resultSet = statement.executeQuery(SQL_COMMAND);

      while (resultSet.next()) {
        HopsAcl acl = new HopsAcl(resultSet.getString(Consts.TOPIC_NAME),
            resultSet.getString(Consts.PRINCIPAL),
            resultSet.getString(Consts.PERMISSION_TYPE),
            resultSet.getString(Consts.OPERATION_TYPE),
            resultSet.getString(Consts.HOST),
            resultSet.getString(Consts.ROLE),
            resultSet.getString(Consts.TEAM_ROLE)
        );

        Map<String, List<HopsAcl>> topicAcls = newAcls.getOrDefault(acl.getTopicName(), new HashMap<>());
        List<HopsAcl> topicPrincipalAcl = topicAcls.getOrDefault(acl.getPrincipal(), new ArrayList<>());
        topicPrincipalAcl.add(acl);
        topicAcls.put(acl.getPrincipal(), topicPrincipalAcl);
        newAcls.put(acl.getTopicName(), topicAcls);
      }
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

    return newAcls;
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
