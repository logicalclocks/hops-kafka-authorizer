package io.hops.kafka;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import kafka.network.RequestChannel;
import kafka.security.auth.Operation;
import kafka.security.auth.Operation$;
import kafka.security.auth.Resource;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.javatuples.Pair;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;

public class TestHopsAclAuthorizer {

  private DbConnection dbConnection;

  @BeforeEach
  public void setup() {
    dbConnection = Mockito.mock(DbConnection.class);
  }

  @Test
  public void testAuthorizeRejectAnonymous() throws UnknownHostException {
    // Arrange
    LoadingCache<String, Integer> topicProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<String, Pair<Integer, String>> userProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<Pair<Integer, Integer>, String> projectShareCache = Mockito.mock(LoadingCache.class);

    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(topicProjectCache, userProjectCache, projectShareCache);

    RequestChannel.Session session = new RequestChannel.Session(KafkaPrincipal.ANONYMOUS, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session, buildOperation("describe"), buildResource("test"));

    // Assert
    Assertions.assertFalse(result);
  }

  @Test
  public void testAuthorizeSuperUser() throws UnknownHostException {
    // Arrange
    LoadingCache<String, Integer> topicProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<String, Pair<Integer, String>> userProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<Pair<Integer, Integer>, String> projectShareCache = Mockito.mock(LoadingCache.class);

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    Set<KafkaPrincipal> superUsers = new HashSet<>();
    superUsers.add(kafkaPrincipal);

    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(topicProjectCache, userProjectCache, projectShareCache);
    hopsAclAuthorizer.setSuperUsers(superUsers);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session, buildOperation("describe"), buildResource("test"));

    // Assert
    Assertions.assertTrue(result);
  }

  @Test
  public void testAuthorizeMissingTopic() throws UnknownHostException, ExecutionException {
    // Arrange
    LoadingCache<String, Integer> topicProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<String, Pair<Integer, String>> userProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<Pair<Integer, Integer>, String> projectShareCache = Mockito.mock(LoadingCache.class);

    Mockito.when(topicProjectCache.get(anyString())).thenThrow(new CacheLoader.InvalidCacheLoadException(""));

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(topicProjectCache, userProjectCache, projectShareCache);
    hopsAclAuthorizer.setDbConnection(dbConnection);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session, buildOperation("describe"), buildResource("test"));

    // Assert
    Assertions.assertFalse(result);
    Mockito.verify(topicProjectCache, Mockito.times(1)).get(anyString());
    Mockito.verify(userProjectCache, Mockito.times(0)).get(anyString());
    Mockito.verify(projectShareCache, Mockito.times(0)).get(any());
  }

  @Test
  public void testAuthorizeTopicException() throws UnknownHostException, ExecutionException {
    // Arrange
    LoadingCache<String, Integer> topicProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<String, Pair<Integer, String>> userProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<Pair<Integer, Integer>, String> projectShareCache = Mockito.mock(LoadingCache.class);

    Mockito.when(topicProjectCache.get(anyString())).thenThrow(new ExecutionException(new SQLException()));

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(topicProjectCache, userProjectCache, projectShareCache);
    hopsAclAuthorizer.setDbConnection(dbConnection);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session, buildOperation("describe"), buildResource("test"));

    // Assert
    Assertions.assertFalse(result);
    Mockito.verify(topicProjectCache, Mockito.times(2)).get(anyString());
    Mockito.verify(userProjectCache, Mockito.times(0)).get(anyString());
    Mockito.verify(projectShareCache, Mockito.times(0)).get(any());
  }

  @Test
  public void testAuthorizeMissingPrincipal() throws UnknownHostException, ExecutionException {
    // Arrange
    LoadingCache<String, Integer> topicProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<String, Pair<Integer, String>> userProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<Pair<Integer, Integer>, String> projectShareCache = Mockito.mock(LoadingCache.class);

    Mockito.when(topicProjectCache.get(anyString())).thenReturn(120);
    Mockito.when(userProjectCache.get(anyString())).thenThrow(new CacheLoader.InvalidCacheLoadException(""));

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(topicProjectCache, userProjectCache, projectShareCache);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session, buildOperation("describe"), buildResource("test"));

    // Assert
    Assertions.assertFalse(result);
    Mockito.verify(topicProjectCache, Mockito.times(1)).get(anyString());
    Mockito.verify(userProjectCache, Mockito.times(1)).get(anyString());
    Mockito.verify(projectShareCache, Mockito.times(0)).get(any());
  }

  @Test
  public void testAuthorizePrincipalException() throws UnknownHostException, ExecutionException {
    // Arrange
    LoadingCache<String, Integer> topicProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<String, Pair<Integer, String>> userProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<Pair<Integer, Integer>, String> projectShareCache = Mockito.mock(LoadingCache.class);

    Mockito.when(topicProjectCache.get(anyString())).thenReturn(120);
    Mockito.when(userProjectCache.get(anyString())).thenThrow(new ExecutionException(new SQLException()));

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(topicProjectCache, userProjectCache, projectShareCache);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session, buildOperation("describe"), buildResource("test"));

    // Assert
    Assertions.assertFalse(result);
    Mockito.verify(topicProjectCache, Mockito.times(2)).get(anyString());
    Mockito.verify(userProjectCache, Mockito.times(2)).get(anyString());
    Mockito.verify(projectShareCache, Mockito.times(0)).get(any());
  }

  @Test
  public void testAuthorizeMissingShared() throws UnknownHostException, ExecutionException {
    // Arrange
    LoadingCache<String, Integer> topicProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<String, Pair<Integer, String>> userProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<Pair<Integer, Integer>, String> projectShareCache = Mockito.mock(LoadingCache.class);

    Mockito.when(topicProjectCache.get(anyString())).thenReturn(120);
    Mockito.when(userProjectCache.get(anyString())).thenReturn(new Pair<>(119, Consts.DATA_OWNER));
    Mockito.when(projectShareCache.get(any())).thenThrow(new CacheLoader.InvalidCacheLoadException(""));

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(topicProjectCache, userProjectCache, projectShareCache);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session, buildOperation("describe"), buildResource("test"));

    // Assert
    Assertions.assertFalse(result);
    Mockito.verify(topicProjectCache, Mockito.times(1)).get(anyString());
    Mockito.verify(userProjectCache, Mockito.times(1)).get(anyString());
    Mockito.verify(projectShareCache, Mockito.times(1)).get(any());
  }

  @Test
  public void testAuthorizeSharedException() throws UnknownHostException, ExecutionException {
    // Arrange
    LoadingCache<String, Integer> topicProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<String, Pair<Integer, String>> userProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<Pair<Integer, Integer>, String> projectShareCache = Mockito.mock(LoadingCache.class);

    Mockito.when(topicProjectCache.get(anyString())).thenReturn(120);
    Mockito.when(userProjectCache.get(anyString())).thenReturn(new Pair<>(119, Consts.DATA_OWNER));
    Mockito.when(projectShareCache.get(any())).thenThrow(new ExecutionException(new SQLException()));

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(topicProjectCache, userProjectCache, projectShareCache);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session, buildOperation("describe"), buildResource("test"));

    // Assert
    Assertions.assertFalse(result);
    Mockito.verify(topicProjectCache, Mockito.times(2)).get(anyString());
    Mockito.verify(userProjectCache, Mockito.times(2)).get(anyString());
    Mockito.verify(projectShareCache, Mockito.times(2)).get(any());
  }

  @ParameterizedTest
  @CsvSource({
      // Same project
      "119, 119, Data owner,      Write,    , true",
      "119, 119, Data scientist,  Write,    , false",
      "119, 119, Data owner,      Read,     , true",
      "119, 119, Data scientist,  Read,     , true",
      "119, 119, Data owner,      Describe, , true",
      "119, 119, Data scientist,  Describe, , true",
      // Shared with READ_ONLY permission
      "119, 120, Data owner,      Write,    READ_ONLY,  false",
      "119, 120, Data scientist,  Write,    READ_ONLY,  false",
      "119, 120, Data owner,      Read,     READ_ONLY,  true",
      "119, 120, Data scientist,  Read,     READ_ONLY,  true",
      "119, 120, Data owner,      Describe, READ_ONLY,  true",
      "119, 120, Data scientist,  Describe, READ_ONLY,  true",
      // Shared with not supported permission
      "119, 120, Data owner,      Write,    EDITABLE,   false",
      "119, 120, Data scientist,  Write,    EDITABLE,   false",
      "119, 120, Data owner,      Read,     EDITABLE,   false",
      "119, 120, Data scientist,  Read,     EDITABLE,   false",
      "119, 120, Data owner,      Describe, EDITABLE,   false",
      "119, 120, Data scientist,  Describe, EDITABLE,   false"
  })
  public void testAuthorizeAllow(int topicProjectId, int userProjectId, String projectRole, String operationType,
                                 String sharePermission, boolean expectedResult)
      throws UnknownHostException, ExecutionException {
    // Arrange
    LoadingCache<String, Integer> topicProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<String, Pair<Integer, String>> userProjectCache = Mockito.mock(LoadingCache.class);
    LoadingCache<Pair<Integer, Integer>, String> projectShareCache = Mockito.mock(LoadingCache.class);

    Mockito.when(topicProjectCache.get(anyString())).thenReturn(topicProjectId);
    Mockito.when(userProjectCache.get(anyString())).thenReturn(new Pair<>(userProjectId, projectRole));
    Mockito.when(projectShareCache.get(any())).thenReturn(sharePermission);

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "project__user");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(topicProjectCache, userProjectCache, projectShareCache);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session, buildOperation(operationType), buildResource("test"));

    // Assert
    Assertions.assertEquals(expectedResult, result);
    Mockito.verify(topicProjectCache, Mockito.times(1)).get(anyString());
    Mockito.verify(userProjectCache, Mockito.times(1)).get(anyString());
    if (sharePermission != null) {
      Mockito.verify(projectShareCache, Mockito.times(1)).get(any());
    }
  }

  private Resource buildResource(String topicName) {
   return Resource.fromString("Topic:LITERAL:" + topicName);
  }

  private Operation buildOperation(String OperationName) {
    return Operation$.MODULE$.fromString(OperationName);
  }
}