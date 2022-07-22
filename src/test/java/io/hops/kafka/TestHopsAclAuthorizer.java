package io.hops.kafka;

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
import java.sql.SQLException;
import java.util.*;

public class TestHopsAclAuthorizer {

  private DbConnection dbConnection;

  @Before
  public void setup() {
    dbConnection = Mockito.mock(DbConnection.class);
  }

  @Test
  public void testRejectAnonymous() throws UnknownHostException {
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer();

    RequestChannel.Session session = new RequestChannel.Session(KafkaPrincipal.ANONYMOUS,
        InetAddress.getByName("10.0.2.15"));

    Assert.assertFalse(hopsAclAuthorizer.authorize(
        session,
        buildOperation("describe"),
        buildResource("test")));
  }

  @Test
  public void testSuperUser() throws UnknownHostException {
    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    Set<KafkaPrincipal> superUsers = new HashSet<>();
    superUsers.add(kafkaPrincipal);

    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer();
    hopsAclAuthorizer.setSuperUsers(superUsers);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    Assert.assertTrue(hopsAclAuthorizer.authorize(
        session,
        buildOperation("describe"),
        buildResource("test")));
  }

  @Test
  public void testMissingTopic() throws UnknownHostException, SQLException {
    Mockito.when(dbConnection.getAcls()).thenReturn(new HashMap<>());

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer();
    hopsAclAuthorizer.setDbConnection(dbConnection);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    Assert.assertFalse(hopsAclAuthorizer.authorize(
        session,
        buildOperation("describe"),
        buildResource("test")));

    Mockito.verify(dbConnection, Mockito.times(1)).getAcls();
  }

  @Test
  public void testMissingPrincipal() throws UnknownHostException {
    Map<String, Map<String, List<HopsAcl>>> acls = new HashMap<>();
    acls.put("test", new HashMap<>());

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "sudo");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(acls);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    Assert.assertFalse(hopsAclAuthorizer.authorize(
        session,
        buildOperation("describe"),
        buildResource("test")));
  }

  @Test
  public void testAllowWildcard() throws UnknownHostException {
    HopsAcl hopsAcl = new HopsAcl("test", "project__user", "allow",
        "*", "*", "*", "data owner");
    Map<String, Map<String, List<HopsAcl>>> acls = new HashMap<>();
    Map<String, List<HopsAcl>> topicAcl = new HashMap<>();
    topicAcl.put("project__user", Arrays.asList(hopsAcl));
    acls.put("test", topicAcl);

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "project__user");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(acls);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    Assert.assertTrue(hopsAclAuthorizer.authorize(
        session,
        buildOperation("describe"),
        buildResource("test")));
  }

  @Test
  public void testOperationNotAllowed() throws UnknownHostException {
    HopsAcl hopsAcl = new HopsAcl("test", "project__user", "allow",
        "read", "*", "*", "data owner");
    Map<String, Map<String, List<HopsAcl>>> acls = new HashMap<>();
    Map<String, List<HopsAcl>> topicAcl = new HashMap<>();
    topicAcl.put("project__user", Arrays.asList(hopsAcl));
    acls.put("test", topicAcl);

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "project__user");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(acls);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    Assert.assertFalse(hopsAclAuthorizer.authorize(
        session,
        buildOperation("describe"),
        buildResource("test")));
  }

  @Test
  public void testHostNotAllowed() throws UnknownHostException {
    HopsAcl hopsAcl = new HopsAcl("test", "project__user", "allow",
        "read", "10.0.2.1", "*", "data owner");
    Map<String, Map<String, List<HopsAcl>>> acls = new HashMap<>();
    Map<String, List<HopsAcl>> topicAcl = new HashMap<>();
    topicAcl.put("project__user", Arrays.asList(hopsAcl));
    acls.put("test", topicAcl);

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "project__user");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(acls);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    Assert.assertFalse(hopsAclAuthorizer.authorize(
        session,
        buildOperation("describe"),
        buildResource("test")));
  }

  @Test
  public void testDenyWildcard() throws UnknownHostException {
    HopsAcl hopsAcl = new HopsAcl("test", "project__user", "deny",
        "*", "*", "*", "data owner");
    Map<String, Map<String, List<HopsAcl>>> acls = new HashMap<>();
    Map<String, List<HopsAcl>> topicAcl = new HashMap<>();
    topicAcl.put("project__user", Arrays.asList(hopsAcl));
    acls.put("test", topicAcl);

    KafkaPrincipal kafkaPrincipal = new KafkaPrincipal("User", "project__user");
    HopsAclAuthorizer hopsAclAuthorizer = new HopsAclAuthorizer(acls);

    RequestChannel.Session session = new RequestChannel.Session(kafkaPrincipal, InetAddress.getByName("10.0.2.15"));

    Assert.assertFalse(hopsAclAuthorizer.authorize(
        session,
        buildOperation("describe"),
        buildResource("test")));
  }

  private Resource buildResource(String topicName) {
   return Resource.fromString("Topic:LITERAL:" + topicName);
  }

  private Operation buildOperation(String OperationName) {
    return Operation$.MODULE$.fromString("describe");
  }
}