package io.hops.kafka;

import com.google.common.cache.LoadingCache;
import io.hops.kafka.authorizer.tables.HopsAcl;
import kafka.network.RequestChannel;
import kafka.security.auth.Operation;
import kafka.security.auth.Operation$;
import kafka.security.auth.Resource;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.mockito.ArgumentMatchers.anyString;

public class TestHopsAclAuthorizer {

  private DbConnection dbConnection;

  @Before
  public void setup() {
    dbConnection = Mockito.mock(DbConnection.class);
  }

  @Test
  public void testRejectAnonymous() throws UnknownHostException, ExecutionException {
    // Arrange
    LoadingCache<String, Map<String, List<HopsAcl>>> loadingCache = Mockito.mock(LoadingCache.class);
    Mockito.when(loadingCache.get(anyString())).thenReturn(new HashMap<>());

    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(loadingCache);

    RequestChannel.Session session = new RequestChannel.Session(KafkaPrincipal.ANONYMOUS,
        InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session,
        buildOperation("describe"), buildResource("test"));

    // Assert
    Assert.assertFalse(result);
  }

  @Test
  public void testSuperUser() throws UnknownHostException, ExecutionException {
    // Arrange
    LoadingCache<String, Map<String, List<HopsAcl>>> loadingCache = Mockito.mock(LoadingCache.class);
    Mockito.when(loadingCache.get(anyString())).thenReturn(new HashMap<>());

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    Set<KafkaPrincipal> superUsers = new HashSet<>();
    superUsers.add(kafkaPrincipal);

    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(loadingCache);
    hopsAclAuthorizer.setSuperUsers(superUsers);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session,
        buildOperation("describe"), buildResource("test"));

    // Assert
    Assert.assertTrue(result);
  }

  @Test
  public void testMissingTopic() throws UnknownHostException, ExecutionException {
    // Arrange
    LoadingCache<String, Map<String, List<HopsAcl>>> loadingCache = Mockito.mock(LoadingCache.class);
    Mockito.when(loadingCache.get(anyString())).thenReturn(new HashMap<>());

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(loadingCache);
    hopsAclAuthorizer.setDbConnection(dbConnection);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session,
        buildOperation("describe"), buildResource("test"));

    // Assert
    Assert.assertFalse(result);
    Mockito.verify(loadingCache, Mockito.times(1)).get(anyString());
  }

  @Test
  public void testMissingPrincipal() throws UnknownHostException, ExecutionException {
    // Arrange
    LoadingCache<String, Map<String, List<HopsAcl>>> loadingCache = Mockito.mock(LoadingCache.class);
    Mockito.when(loadingCache.get(anyString())).thenReturn(new HashMap<>());

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(loadingCache);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session,
        buildOperation("describe"), buildResource("test"));

    // Assert
    Assert.assertFalse(result);
  }

  @Test
  public void testAllowWildcard() throws UnknownHostException, ExecutionException {
    // Arrange
    HopsAcl hopsAcl = new HopsAcl("test", "project__user", "allow",
        "*", "*", "*", "data owner");
    Map<String, List<HopsAcl>> map = new HashMap<>();
    map.put(hopsAcl.getPrincipal(), Arrays.asList(hopsAcl));
    LoadingCache<String, Map<String, List<HopsAcl>>> loadingCache = Mockito.mock(LoadingCache.class);
    Mockito.when(loadingCache.get(anyString())).thenReturn(map);

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "project__user");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(loadingCache);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session,
        buildOperation("describe"), buildResource("test"));

    // Assert
    Assert.assertTrue(result);
  }

  @Test
  public void testOperationNotAllowed() throws UnknownHostException, ExecutionException {
    // Arrange
    HopsAcl hopsAcl = new HopsAcl("test", "project__user", "allow",
        "read", "*", "*", "data owner");
    Map<String, List<HopsAcl>> map = new HashMap<>();
    map.put(hopsAcl.getPrincipal(), Arrays.asList(hopsAcl));
    LoadingCache<String, Map<String, List<HopsAcl>>> loadingCache = Mockito.mock(LoadingCache.class);
    Mockito.when(loadingCache.get(anyString())).thenReturn(map);

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "project__user");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(loadingCache);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session,
        buildOperation("describe"), buildResource("test"));

    // Assert
    Assert.assertFalse(result);
  }

  @Test
  public void testHostNotAllowed() throws UnknownHostException, ExecutionException {
    // Arrange
    HopsAcl hopsAcl = new HopsAcl("test", "project__user", "allow",
        "read", "10.0.2.1", "*", "data owner");
    Map<String, List<HopsAcl>> map = new HashMap<>();
    map.put(hopsAcl.getPrincipal(), Arrays.asList(hopsAcl));
    LoadingCache<String, Map<String, List<HopsAcl>>> loadingCache = Mockito.mock(LoadingCache.class);
    Mockito.when(loadingCache.get(anyString())).thenReturn(map);

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "project__user");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(loadingCache);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session,
        buildOperation("describe"), buildResource("test"));

    // Assert
    Assert.assertFalse(result);
  }

  @Test
  public void testDenyWildcard() throws UnknownHostException, ExecutionException {
    // Arrange
    HopsAcl hopsAcl = new HopsAcl("test", "project__user", "deny",
        "*", "*", "*", "data owner");
    Map<String, List<HopsAcl>> map = new HashMap<>();
    map.put(hopsAcl.getPrincipal(), Arrays.asList(hopsAcl));
    LoadingCache<String, Map<String, List<HopsAcl>>> loadingCache = Mockito.mock(LoadingCache.class);
    Mockito.when(loadingCache.get(anyString())).thenReturn(map);

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "project__user");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(loadingCache);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    // Act
    boolean result = hopsAclAuthorizer.authorize(session,
        buildOperation("describe"), buildResource("test"));

    // Assert
    Assert.assertFalse(result);
  }

  private Resource buildResource(String topicName) {
   return Resource.fromString("Topic:LITERAL:" + topicName);
  }

  private Operation buildOperation(String OperationName) {
    return Operation$.MODULE$.fromString("describe");
  }
}