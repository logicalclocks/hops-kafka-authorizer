package io.hops.kafka;

import io.hops.kafka.authorizer.tables.HopsAcl;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
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
   * @param acls <TopicName,<Principal,HopsAcl>>
   * @throws java.sql.SQLException
   * @thro
   * ws java.sql.SQLException
   */
  public void populateACLInfo(ConcurrentMap acls) throws SQLException {
    try (Connection conn = datasource.getConnection()) {
      try (PreparedStatement prepStatement = conn.prepareStatement(
          "SELECT DISTINCT acls.*,project_team.team_role "
          + "FROM topic_acls as acls, project, users, project_team "
          + "WHERE project.id = acls.project_id "
          + "AND project_team.project_id = acls.project_id "
          + "AND project_team.team_member = users.email "
          + "AND users.username = substring_index(principal, '" + Consts.PROJECT_USER_DELIMITER + "', -1) "
          + "ORDER BY topic_name, principal ")) {
        Map<String, Map<String, List<HopsAcl>>> newAcls = new HashMap<>();
        try (ResultSet rst = prepStatement.executeQuery()) {
          acls.clear();

          while (rst.next()) {
            HopsAcl acl = new HopsAcl(rst.getString(Consts.PRINCIPAL),
                rst.getString(Consts.PERMISSION_TYPE),
                rst.getString(Consts.OPERATION_TYPE),
                rst.getString(Consts.HOST),
                rst.getString(Consts.ROLE),
                rst.getString(Consts.TEAM_ROLE));
            if (!newAcls.containsKey(rst.getString(Consts.TOPIC_NAME))) {
              newAcls.put(rst.getString(Consts.TOPIC_NAME), new HashMap<>());
            }
            if (!newAcls.get(rst.getString(Consts.TOPIC_NAME)).containsKey(rst.getString(Consts.PRINCIPAL))) {
              newAcls.get(rst.getString(Consts.TOPIC_NAME)).put(rst.getString(Consts.PRINCIPAL), new ArrayList<>());
            }
            newAcls.get(rst.getString(Consts.TOPIC_NAME)).get(rst.getString(Consts.PRINCIPAL)).add(acl);
          }
        }
        acls.putAll(newAcls);
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
