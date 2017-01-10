package io.hops.kafka;

import kafka.network.RequestChannel;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

import java.util.logging.Level;
import java.util.logging.Logger;
import joptsimple.internal.Strings;

/**
 *
 * @author misdess
 */
public class HopsAclAuthorizer implements Authorizer {

  private static final Logger AUTHORIZERLOGGER = Logger.
          getLogger(HopsAclAuthorizer.class.getName());
  //List of users that will be treated as super users and will have access to 
  //all the resources for all actions from all hosts, defaults to no super users.
  private java.util.Set<KafkaPrincipal> superUsers = new java.util.HashSet<>();

  //If set to true when no acls are found for a resource , authorizer allows 
  //access to everyone. Defaults to false.
  private boolean shouldAllowEveryoneIfNoAclIsFound = false;

  DbConnection dbConnection;

  /**
   * Guaranteed to be called before any authorize call is made.
   *
   * @param configs
   */
  @Override
  public void configure(java.util.Map<String, ?> configs) {
    System.out.println("configs:" + configs.toString());
    Object obj = configs.get(Consts.SUPERUSERS_PROP);
    System.out.println("obj:" + obj);
    String databaseType;
    String databaseUrl;
    String databaseUserName;
    String databasePassword;

    if (obj != null) {
      String superUsersStr = (String) obj;
      String[] superUserStrings = superUsersStr.split(Consts.SEMI_COLON);

      for (String user : superUserStrings) {
        superUsers.add(KafkaPrincipal.fromString(user.trim()));
      }
    } else {
      superUsers = new HashSet<>();
    }

    databaseType = configs.get(Consts.DATABASE_TYPE).toString();
    databaseUrl = configs.get(Consts.DATABASE_URL).toString();
    databaseUserName = configs.get(Consts.DATABASE_USERNAME).toString();
    databasePassword = configs.get(Consts.DATABASE_PASSWORD).toString();
    //initialize database connection.
    dbConnection = new DbConnection(databaseType, databaseUrl,
            databaseUserName, databasePassword);

    //grap the default acl property
    shouldAllowEveryoneIfNoAclIsFound = Boolean.valueOf(
            configs.get(Consts.ALLOW_EVERYONE_IF_NO_ACS_FOUND_PROP).toString());
  }

  @Override
  public boolean authorize(RequestChannel.Session session, Operation operation,
          Resource resource) {
   
    KafkaPrincipal principal = session.principal();
    String host = session.clientAddress().getHostAddress();
    System.out.println("authorize :: session:" + session);
    System.out.println("authorize :: principal.name:" + principal.getName());
    System.out.println("authorize :: principal.type:" + principal.
            getPrincipalType());
    System.out.println("authorize :: operation:" + operation);
    System.out.println("authorize :: host:" + host);
    System.out.println("authorize :: resource:" + resource);
    String topicName = resource.name();
    System.out.println("authorize :: topicName:" + topicName);
    String projectName__userName = principal.getName();
    System.out.println("authorize :: projectName__userName:"
            + projectName__userName);

    if (projectName__userName.equalsIgnoreCase(Consts.ANONYMOUS)) {
      AUTHORIZERLOGGER.log(Level.SEVERE,
              "No Acl found for cluster authorization, user: {0}",
              new Object[]{projectName__userName});
      return false;
    }
    
    if (isSuperUser(principal)) {
      return true;
    }
    boolean authorized;

    if (resource.resourceType().equals(
            kafka.security.auth.ResourceType$.MODULE$.fromString(Consts.CLUSTER))) {
      AUTHORIZERLOGGER.log(Level.INFO,
              "This is cluster authorization for broker: {0}",
              new Object[]{projectName__userName});
      return false;
    } 
    if (resource.resourceType().equals(
            kafka.security.auth.ResourceType$.MODULE$.fromString(Consts.GROUP))) {
      //Check if group starts with project name and CN contains it
      String projectCN = projectName__userName.split(Consts.PROJECT_USER_DELIMITER)[0];
      String projectConsumerGroup = resource.name().split(Consts.PROJECT_USER_DELIMITER)[0];
      System.out.println("Consumer group :: projectCN:"+projectCN);
      System.out.println("Consumer group :: projectConsumerGroup:"+projectConsumerGroup);
      if(!projectCN.equals(projectConsumerGroup)){
        AUTHORIZERLOGGER.log(Level.INFO,
              "Principal:{0} is not allowed to access group {1}", new Object[]{projectName__userName,resource.name()});
        return false;
      }
      AUTHORIZERLOGGER.log(Level.INFO,
              "Principal:{0} is allowed to access group {1}", new Object[]{projectName__userName,resource.name()});
      return true;
    } 
    

    java.util.Set<HopsAcl> resourceAcls = getTopicAcls(topicName);
    System.out.println("resourceAcls:" + resourceAcls);

    String role = dbConnection.getUserRole(projectName__userName);

    //check if there is any Deny acl match that would disallow this operation.
    boolean denyMatch = aclMatch(operation.name(), projectName__userName,
            host, Consts.DENY, role, resourceAcls);
    System.out.println("denyMatch:" + denyMatch);

    //if principal is allowed to read or write we allow describe by default,
    //the reverse does not apply to Deny.
    java.util.Set<String> ops = new HashSet<>();
    ops.add(operation.name());
    System.out.println("ops:" + ops);
    if (operation.name().equalsIgnoreCase(Consts.DESCRIBE)) {
      ops.add(Consts.WRITE);
      ops.add(Consts.READ);
    }

    //now check if there is any allow acl that will allow this operation.
    boolean allowMatch = false;
    for (String op : ops) {
      if (aclMatch(op, projectName__userName, host, Consts.ALLOW, role,
              resourceAcls)) {
        allowMatch = true;
      }
    }
    System.out.println("allowMatch:" + allowMatch);
    /*
     * we allow an operation if a user is a super user or if no acls are
     * found and user has configured to allow all users when no acls are found
     * or if no deny acls are found and at least one allow acls matches.
     */
    authorized = isSuperUser(principal)
            || isEmptyAclAndAuthorized(resourceAcls, topicName,
                    projectName__userName)
            || (!denyMatch && allowMatch);

    //logAuditMessage(principal, authorized, operation, resource, host);
    return authorized;
  }

  private Boolean aclMatch(String operations, String principal,
          String host, String permissionType, String role,
          java.util.Set<HopsAcl> acls) {
    System.out.println("aclMatch :: Operation:" + operations);
    System.out.println("aclMatch :: principal:" + principal);
    System.out.println("aclMatch :: host:" + host);
    System.out.println("aclMatch :: permissionType:" + permissionType);
    System.out.println("aclMatch :: role:" + role);
    System.out.println("aclMatch :: acls:" + acls);

    for (HopsAcl acl : acls) {
      System.out.println("aclMatch.acl" + acl);
      if (acl.getPermissionType().equalsIgnoreCase(permissionType)
              && (acl.getPrincipal().equalsIgnoreCase(principal) || acl.
              getPrincipal().equals(Consts.WILDCARD))
              && (acl.getOperationType().equalsIgnoreCase(operations) || acl.
              getOperationType().equalsIgnoreCase(Consts.WILDCARD))
              && (acl.getHost().equalsIgnoreCase(host) || acl.getHost().equals(
              Consts.WILDCARD))
              && (acl.getRole().equalsIgnoreCase(role) || acl.getRole().equals(
              Consts.WILDCARD))) {
        return true;
      }
    }
    return false;
  }

  Boolean isEmptyAclAndAuthorized(java.util.Set<HopsAcl> acls,
          String topicName, String principalName) {
    System.out.println("\n\nisEmptyAclAndAuthorized.acls:" + acls);
    if (acls.isEmpty()) {
      //Authorize if user is member of the topic owner project
      if (dbConnection.isTopicOwner(topicName,
              principalName)) {
        return true;
      }
      return shouldAllowEveryoneIfNoAclIsFound;
    }
    return false;
  }

  boolean isSuperUser(KafkaPrincipal principal) {
    System.out.println("isSuperUser:" + principal);
    if (superUsers.contains(principal)) {
      System.out.println("isSuperUser:" + "principal = " + principal
              + " is a super user, allowing operation without checking acls.");
      AUTHORIZERLOGGER.log(Level.INFO,
              "principal = {0} is a super user, allowing operation without checking acls.",
              new Object[]{principal});
      return true;
    }
    return false;
  }

  @Override
  public void addAcls(Set<Acl> acls, Resource resource) {

  }

  @Override
  public boolean removeAcls(Set<Acl> aclsToBeRemoved, Resource resource) {
    return false;
  }

  @Override
  public boolean removeAcls(Resource resource) {
    return false;
  }

  @Override
  public Set<Acl> getAcls(Resource resource) {
    return new scala.collection.immutable.HashSet<>();
  }

  @Override
  public Map<Resource, Set<Acl>> getAcls(KafkaPrincipal principal) {

    //not used in this authorizer
    scala.collection.immutable.Map<Resource, Set<Acl>> immutablePrincipalAcls
            = new scala.collection.immutable.HashMap<>();
    return immutablePrincipalAcls;
  }

  @Override
  public Map<Resource, Set<Acl>> getAcls() {

    //not used in this authorizer
    scala.collection.immutable.Map<Resource, Set<Acl>> aclCache
            = new scala.collection.immutable.HashMap<>();
    return aclCache;
  }

  @Override
  public void close() {
    dbConnection = null;
  }

  private java.util.Set<HopsAcl> getTopicAcls(String topicName) {

    java.util.Set<HopsAcl> resourceAcls = new java.util.HashSet<>();
    String principal;
    String permission;
    String operation;
    String host;
    String role;

    try {
      String projectName = dbConnection.getProjectName(topicName);

      if (projectName == null) {
        return resourceAcls;
      }
      //get all the acls for the given topic
      ResultSet topicAcls = dbConnection.getTopicAcls(topicName);
      System.out.println("getTopicAcls.topicAcls:" + topicAcls.toString());
      while (topicAcls.next()) {
        // principal = projectName + "__" + topicAcls.getString("username");
        principal = topicAcls.getString(Consts.PRINCIPAL);
        permission = topicAcls.getString(Consts.PERMISSION_TYPE);
        operation = topicAcls.getString(Consts.OPERATION_TYPE);
        host = topicAcls.getString(Consts.HOST);
        role = topicAcls.getString(Consts.ROLE);
        HopsAcl acl = new HopsAcl(principal, permission, operation, host, role);
        System.out.println("acl-1:" + acl);
        resourceAcls.add(acl);
      }
      //get all the acls for wildcard topics
      ResultSet wildcardTopicAcls = dbConnection.getTopicAcls(Consts.WILDCARD);
      while (wildcardTopicAcls.next()) {
        // principal = projectName + "__" + topicAcls.getString("username");
        principal = topicAcls.getString(Consts.PRINCIPAL);
        permission = topicAcls.getString(Consts.PERMISSION_TYPE);
        operation = topicAcls.getString(Consts.OPERATION_TYPE);
        host = topicAcls.getString(Consts.HOST);
        role = topicAcls.getString(Consts.ROLE);
        HopsAcl acl = new HopsAcl(principal, permission, host, operation, role);
        System.out.println("acl-2:" + acl);
        resourceAcls.add(new HopsAcl(principal, permission, host, operation,
                role));
      }
    } catch (SQLException ex) {
      System.out.println("getTopicAcls error:" + ex.toString());
      AUTHORIZERLOGGER.log(Level.SEVERE, null, ex.toString());
    }
    return resourceAcls;
  }
}
