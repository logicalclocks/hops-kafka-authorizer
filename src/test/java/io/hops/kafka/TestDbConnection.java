package io.hops.kafka;

import com.zaxxer.hikari.HikariDataSource;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import static org.mockito.ArgumentMatchers.anyString;

public class TestDbConnection {

  @Test
  public void testGetAcls() throws SQLException {
    // Arrange
    HikariDataSource datasource = Mockito.mock(HikariDataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(datasource.getConnection()).thenReturn(connection);
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.executeQuery(anyString())).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

    DbConnection dbConnection = new DbConnection(datasource);

    // Act
    Map map = dbConnection.getAcls("test_topic");

    // Assert
    Assert.assertEquals(1, map.size());
    Mockito.verify(datasource, Mockito.times(1)).getConnection();
    Mockito.verify(connection, Mockito.times(1)).close();
    Mockito.verify(statement, Mockito.times(1)).close();
    Mockito.verify(resultSet, Mockito.times(1)).close();
  }

  @Test
  public void testGetAclsRetry() throws SQLException {
    // Arrange
    HikariDataSource datasource = Mockito.mock(HikariDataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(datasource.getConnection()).thenReturn(connection);
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.executeQuery(anyString())).thenThrow(new SQLException()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

    DbConnection dbConnection = new DbConnection(datasource);

    // Act
    Map map = dbConnection.getAcls("test_topic");

    // Assert
    Assert.assertEquals(1, map.size());
    Mockito.verify(datasource, Mockito.times(2)).getConnection();
    Mockito.verify(connection, Mockito.times(2)).close();
    Mockito.verify(statement, Mockito.times(2)).close();
    Mockito.verify(resultSet, Mockito.times(1)).close();
  }

  @Test
  public void testGetAclsFail() throws SQLException {
    // Arrange
    HikariDataSource datasource = Mockito.mock(HikariDataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    Statement statement = Mockito.mock(Statement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(datasource.getConnection()).thenReturn(connection);
    Mockito.when(connection.createStatement()).thenReturn(statement);
    Mockito.when(statement.executeQuery(anyString())).thenThrow(new SQLException());
    Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);

    DbConnection dbConnection = new DbConnection(datasource);

    // Act
    Map map = dbConnection.getAcls("test_topic");

    // Assert
    Assert.assertEquals(0, map.size());
    Mockito.verify(datasource, Mockito.times(2)).getConnection();
    Mockito.verify(connection, Mockito.times(2)).close();
    Mockito.verify(statement, Mockito.times(2)).close();
    Mockito.verify(resultSet, Mockito.times(0)).close();
  }
}