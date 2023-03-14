package io.hops.kafka;

import com.zaxxer.hikari.HikariDataSource;
import org.javatuples.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;

public class TestDbConnection {

  @Test
  public void testGetTopicProject() throws SQLException {
    // Arrange
    HikariDataSource datasource = Mockito.mock(HikariDataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(datasource.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);
    Mockito.when(resultSet.getInt(anyInt())).thenReturn(123);

    DbConnection dbConnection = new DbConnection(datasource);

    // Act
    int topicProjectId = dbConnection.getTopicProject("test_topic");

    // Assert
    Assertions.assertEquals(123, topicProjectId);
    Mockito.verify(datasource, Mockito.times(1)).getConnection();
    Mockito.verify(connection, Mockito.times(1)).close();
    Mockito.verify(preparedStatement, Mockito.times(1)).close();
    Mockito.verify(resultSet, Mockito.times(1)).close();
  }

  @Test
  public void testGetTopicProjectNull() throws SQLException {
    // Arrange
    HikariDataSource datasource = Mockito.mock(HikariDataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(datasource.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(false);

    DbConnection dbConnection = new DbConnection(datasource);

    // Act
    Integer topicProjectId = dbConnection.getTopicProject("test_topic");

    // Assert
    Assertions.assertNull(topicProjectId);
    Mockito.verify(datasource, Mockito.times(1)).getConnection();
    Mockito.verify(connection, Mockito.times(1)).close();
    Mockito.verify(preparedStatement, Mockito.times(1)).close();
    Mockito.verify(resultSet, Mockito.times(1)).close();
  }

  @Test
  public void testGetTopicProjectFail() throws SQLException {
    // Arrange
    HikariDataSource datasource = Mockito.mock(HikariDataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(datasource.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    Mockito.when(preparedStatement.executeQuery()).thenThrow(new SQLException());
    Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);
    Mockito.when(resultSet.getInt(anyInt())).thenReturn(123);

    DbConnection dbConnection = new DbConnection(datasource);

    // Act
    int topicProjectId = 0;
    try {
      topicProjectId = dbConnection.getTopicProject("test_topic");
    } catch (SQLException e) {

    }

    // Assert
    Assertions.assertEquals(0, topicProjectId);
    Mockito.verify(datasource, Mockito.times(1)).getConnection();
    Mockito.verify(connection, Mockito.times(1)).close();
    Mockito.verify(preparedStatement, Mockito.times(1)).close();
    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  @Test
  public void testGetProjectRole() throws SQLException {
    // Arrange
    HikariDataSource datasource = Mockito.mock(HikariDataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(datasource.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);
    Mockito.when(resultSet.getInt(1)).thenReturn(123);
    Mockito.when(resultSet.getString(2)).thenReturn("example_role");

    DbConnection dbConnection = new DbConnection(datasource);

    // Act
    Pair<Integer, String> pair = dbConnection.getProjectRole("test_project_name", "test_username");

    // Assert
    Assertions.assertEquals(123, pair.getValue0());
    Assertions.assertEquals("example_role", pair.getValue1());
    Mockito.verify(datasource, Mockito.times(1)).getConnection();
    Mockito.verify(connection, Mockito.times(1)).close();
    Mockito.verify(preparedStatement, Mockito.times(1)).close();
    Mockito.verify(resultSet, Mockito.times(1)).close();
  }

  @Test
  public void testGetProjectRoleNull() throws SQLException {
    // Arrange
    HikariDataSource datasource = Mockito.mock(HikariDataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(datasource.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(false);

    DbConnection dbConnection = new DbConnection(datasource);

    // Act
    Pair<Integer, String> pair = dbConnection.getProjectRole("test_project_name", "test_username");

    // Assert
    Assertions.assertNull(pair);
    Mockito.verify(datasource, Mockito.times(1)).getConnection();
    Mockito.verify(connection, Mockito.times(1)).close();
    Mockito.verify(preparedStatement, Mockito.times(1)).close();
    Mockito.verify(resultSet, Mockito.times(1)).close();
  }

  @Test
  public void testGetProjectRoleFail() throws SQLException {
    // Arrange
    HikariDataSource datasource = Mockito.mock(HikariDataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(datasource.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    Mockito.when(preparedStatement.executeQuery()).thenThrow(new SQLException());
    Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);
    Mockito.when(resultSet.getInt(1)).thenReturn(123);
    Mockito.when(resultSet.getString(2)).thenReturn("example_role");

    DbConnection dbConnection = new DbConnection(datasource);

    // Act
    Pair<Integer, String> pair = new Pair<>(0, "");
    try {
      pair = dbConnection.getProjectRole("test_project_name", "test_username");
    } catch (SQLException e) {

    }

    // Assert
    Assertions.assertEquals(0, pair.getValue0());
    Assertions.assertEquals("", pair.getValue1());
    Mockito.verify(datasource, Mockito.times(1)).getConnection();
    Mockito.verify(connection, Mockito.times(1)).close();
    Mockito.verify(preparedStatement, Mockito.times(1)).close();
    Mockito.verify(resultSet, Mockito.times(0)).close();
  }

  @Test
  public void testGetSharedProject() throws SQLException {
    // Arrange
    HikariDataSource datasource = Mockito.mock(HikariDataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(datasource.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);
    Mockito.when(resultSet.getString(anyInt())).thenReturn("example_permission");

    DbConnection dbConnection = new DbConnection(datasource);

    // Act
    String permission = dbConnection.getSharedProject(119, 120);

    // Assert
    Assertions.assertEquals("example_permission", permission);
    Mockito.verify(datasource, Mockito.times(1)).getConnection();
    Mockito.verify(connection, Mockito.times(1)).close();
    Mockito.verify(preparedStatement, Mockito.times(1)).close();
    Mockito.verify(resultSet, Mockito.times(1)).close();
  }

  @Test
  public void testGetSharedProjectNull() throws SQLException {
    // Arrange
    HikariDataSource datasource = Mockito.mock(HikariDataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(datasource.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    Mockito.when(preparedStatement.executeQuery()).thenReturn(resultSet);
    Mockito.when(resultSet.next()).thenReturn(false);

    DbConnection dbConnection = new DbConnection(datasource);

    // Act
    String permission = dbConnection.getSharedProject(119, 120);

    // Assert
    Assertions.assertNull(permission);
    Mockito.verify(datasource, Mockito.times(1)).getConnection();
    Mockito.verify(connection, Mockito.times(1)).close();
    Mockito.verify(preparedStatement, Mockito.times(1)).close();
    Mockito.verify(resultSet, Mockito.times(1)).close();
  }

  @Test
  public void testGetSharedProjectFail() throws SQLException {
    // Arrange
    HikariDataSource datasource = Mockito.mock(HikariDataSource.class);
    Connection connection = Mockito.mock(Connection.class);
    PreparedStatement preparedStatement = Mockito.mock(PreparedStatement.class);
    ResultSet resultSet = Mockito.mock(ResultSet.class);

    Mockito.when(datasource.getConnection()).thenReturn(connection);
    Mockito.when(connection.prepareStatement(anyString())).thenReturn(preparedStatement);
    Mockito.when(preparedStatement.executeQuery()).thenThrow(new SQLException());
    Mockito.when(resultSet.next()).thenReturn(true).thenReturn(false);
    Mockito.when(resultSet.getString(anyInt())).thenReturn("example_permission");

    DbConnection dbConnection = new DbConnection(datasource);

    // Act
    String permission = "";
    try {
      permission = dbConnection.getSharedProject(119, 120);
    } catch (SQLException e) {

    }

    // Assert
    Assertions.assertEquals("", permission);
    Mockito.verify(datasource, Mockito.times(1)).getConnection();
    Mockito.verify(connection, Mockito.times(1)).close();
    Mockito.verify(preparedStatement, Mockito.times(1)).close();
    Mockito.verify(resultSet, Mockito.times(0)).close();
  }
}