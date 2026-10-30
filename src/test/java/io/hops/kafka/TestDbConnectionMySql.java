package io.hops.kafka;

import com.zaxxer.hikari.HikariDataSource;
import org.javatuples.Pair;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.mysql.MySQLContainer;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

// The role query against a real MySQL with the columns it reads from the Hopsworks schema.
public class TestDbConnectionMySql {

  private static MySQLContainer mysql;
  private static HikariDataSource datasource;
  private static DbConnection dbConnection;

  @BeforeAll
  public static void setup() throws SQLException {
    mysql = new MySQLContainer("mysql:8.0.33");
    mysql.start();
    datasource = new HikariDataSource();
    datasource.setJdbcUrl(mysql.getJdbcUrl());
    datasource.setUsername(mysql.getUsername());
    datasource.setPassword(mysql.getPassword());
    try (Connection connection = datasource.getConnection(); Statement statement = connection.createStatement()) {
      statement.execute("CREATE TABLE project (id INT PRIMARY KEY, projectname VARCHAR(100), backing_name VARCHAR(100))");
      statement.execute("CREATE TABLE users (email VARCHAR(150) PRIMARY KEY, username VARCHAR(10))");
      statement.execute("CREATE TABLE project_team (project_id INT, team_member VARCHAR(150), team_role VARCHAR(32))");
      // A project whose backing name differs from its display name, and one where both are the same.
      statement.execute("INSERT INTO project VALUES (1, 'shop_sales', '5cad0i9r254v'), (2, 'blah', 'blah')");
      statement.execute("INSERT INTO users VALUES ('u@example.com', 'hopsabl2')");
      statement.execute("INSERT INTO project_team VALUES (1, 'u@example.com', 'Data owner'), (2, 'u@example.com', 'Data scientist')");
    }
    dbConnection = new DbConnection(datasource);
  }

  @AfterAll
  public static void teardown() {
    datasource.close();
    mysql.stop();
  }

  @Test
  public void testGetProjectRoleByBackingName() throws SQLException {
    // The principal carries the backing name: the certificate CN is <backing name>__<username>.
    Assertions.assertEquals(new Pair<>(1, "Data owner"), dbConnection.getProjectRole("5cad0i9r254v", "hopsabl2"));
    Assertions.assertEquals(new Pair<>(2, "Data scientist"), dbConnection.getProjectRole("blah", "hopsabl2"));
  }

  @Test
  public void testGetProjectRoleIgnoresDisplayName() throws SQLException {
    Assertions.assertNull(dbConnection.getProjectRole("shop_sales", "hopsabl2"));
  }
}
