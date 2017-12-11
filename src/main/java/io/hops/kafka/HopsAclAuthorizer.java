package io.hops.kafka;

import io.hops.kafka.authorizer.tables.HopsAcl;
import kafka.network.RequestChannel;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.log4j.Logger;

/**
 *
 * Authorizer class for HopsWorks Kafka. Authorizer project users by extracting their project specific name from
 * the SSL/TLS certificate CN field.
 * <p>
 */
public class HopsAclAuthorizer implements Authorizer {

  private static final Logger LOG = Logger.getLogger("kafka.authorizer.logger");
  //List of users that will be treated as super users and will have access to 
  //all the resources for all actions from all hosts, defaults to no super users.
  private java.util.Set<KafkaPrincipal> superUsers = new java.util.HashSet<>();

  //If set to true when no acls are found for a resource , authorizer allows 
  //access to everyone. Defaults to false.
  private boolean shouldAllowEveryoneIfNoAclIsFound = false;
  DbConnection dbConnection;
  //<TopicName,<Principal,HopsAcl>>
  ConcurrentMap<String, java.util.Map<String, List<HopsAcl>>> acls = new ConcurrentHashMap<>();

  /**
   * Guaranteed to be called before any authorize call is made.
   *
   * @param configs
   */
  @Override
  public void configure(java.util.Map<String, ?> configs) {
    Object obj = configs.get(Consts.SUPERUSERS_PROP);

    if (obj != null) {
      String superUsersStr = (String) obj;
      String[] superUserStrings = superUsersStr.split(Consts.SEMI_COLON);

      for (String user : superUserStrings) {
        superUsers.add(KafkaPrincipal.fromString(user.trim()));
      }
    } else {
      superUsers = new HashSet<>();
    }

    try {
      //initialize database connection.
      dbConnection = new DbConnection(
          configs.get(Consts.DATABASE_TYPE).toString(),
          configs.get(Consts.DATABASE_URL).toString(),
          configs.get(Consts.DATABASE_USERNAME).toString(),
          configs.get(Consts.DATABASE_PASSWORD).toString(),
          Integer.parseInt(configs.get(Consts.DATABASE_MAX_POOL_SIZE).toString()),
          configs.get(Consts.DATABASE_CACHE_PREPSTMTS).toString(),
          configs.get(Consts.DATABASE_PREPSTMT_CACHE_SIZE).toString(),
          configs.get(Consts.DATABASE_PREPSTMT_CACHE_SQL_LIMIT).toString());
    } catch (SQLException ex) {
      LOG.error("HopsAclAuthorizer could not connect to database at:" + configs.get(Consts.DATABASE_URL).toString(),
          ex);
    }

    //grap the default acl property
    shouldAllowEveryoneIfNoAclIsFound = Boolean.valueOf(
        configs.get(Consts.ALLOW_EVERYONE_IF_NO_ACS_FOUND_PROP).toString());

    //Start the ACLs update thread
    ExecutorService executor = Executors.newSingleThreadExecutor();
    executor.submit(new Runnable() {
      @Override
      public void run() {
        while (true) {
          try {
            dbConnection.populateACLInfo(acls);
            LOG.debug("Acls:"+acls);
            Thread.sleep(1000);
          } catch (SQLException ex) {
            LOG.error("HopsAclAuthorizer could not query database at:" + configs.get(Consts.DATABASE_URL).toString(), 
                    ex);
            //Clear the acls to indicate the error getting the acls from the database
            acls.clear();
          } catch (InterruptedException ex) {
            LOG.error("HopsAclAuthorizer db polling exception", ex);
            acls.clear();
          }
        }
      }
    });
  }

  @Override
  public boolean authorize(RequestChannel.Session session, Operation operation,
      Resource resource) {

    KafkaPrincipal principal = session.principal();
    String host = session.clientAddress().getHostAddress();
    LOG.debug("authorize :: session:" + session);
    LOG.debug("authorize :: principal.name:" + principal.getName());
    LOG.debug("authorize :: principal.type:" + principal.
        getPrincipalType());
    LOG.debug("authorize :: operation:" + operation);
    LOG.debug("authorize :: host:" + host);
    LOG.debug("authorize :: resource:" + resource);
    String topicName = resource.name();
    LOG.debug("authorize :: topicName:" + topicName);
    String projectName__userName = principal.getName();
    LOG.debug("authorize :: projectName__userName:"
        + projectName__userName);

    if (projectName__userName.equalsIgnoreCase(Consts.ANONYMOUS)) {
      LOG.info("No Acl found for cluster authorization, user:" + projectName__userName);
      return false;
    }

    if (isSuperUser(principal)) {
      return true;
    }
    boolean authorized;

    if (resource.resourceType().equals(
        kafka.security.auth.ResourceType$.MODULE$.fromString(Consts.CLUSTER))) {
      LOG.info("This is cluster authorization for broker: " + projectName__userName);
      return false;
    }
    if (resource.resourceType().equals(
        kafka.security.auth.ResourceType$.MODULE$.fromString(Consts.GROUP))) {
      //Check if group requested starts with projectname__ and is equal to the current users project
      String projectCN = projectName__userName.split(Consts.PROJECT_USER_DELIMITER)[0];
      if (resource.name().contains(Consts.PROJECT_USER_DELIMITER)) {
        String projectConsumerGroup = resource.name().split(Consts.PROJECT_USER_DELIMITER)[0];
        LOG.debug("Consumer group :: projectCN:" + projectCN);
        LOG.debug("Consumer group :: projectConsumerGroup:" + projectConsumerGroup);
        //Chec
        if (!projectCN.equals(projectConsumerGroup)) {
          LOG.info("Principal:" + projectName__userName + " is not allowed to access group:" + resource.name());
          return false;
        }
      }
      LOG.info("Principal:" + projectName__userName + " is allowed to access group:" + resource.name());
      return true;
    }

    if(acls.get(topicName).get(projectName__userName).isEmpty()){
      LOG.info("For principal: " + projectName__userName + ", operation:" + operation + ", resource:" + resource
        + ", allowMatch: false - no ACL found");
      return false;
    }
    //check if there is any Deny acl match that would disallow this operation.
    boolean denyMatch = aclMatch(operation.name(), projectName__userName,
        host, Consts.DENY, acls.get(topicName).get(projectName__userName).get(0).getProjectRole(), acls.get(topicName).
        get(projectName__userName));

    //if principal is allowed to read or write we allow describe by default,
    //the reverse does not apply to Deny.
    java.util.Set<String> ops = new HashSet<>();
    ops.add(operation.name());
    if (operation.name().equalsIgnoreCase(Consts.DESCRIBE)) {
      ops.add(Consts.WRITE);
      ops.add(Consts.READ);
    }

    //now check if there is any allow acl that will allow this operation.
    boolean allowMatch = false;
    for (String op : ops) {
      if (aclMatch(op,
          projectName__userName,
          host,
          Consts.ALLOW,
          acls.get(topicName).get(projectName__userName).get(0).getProjectRole(),
          acls.get(topicName).
              get(projectName__userName))) {
        allowMatch = true;
      }
    }

    LOG.info("For principal: " + projectName__userName + ", operation:" + operation + ", resource:" + resource
        + ", allowMatch:" + allowMatch);
    /*
     * we allow an operation if a user is a super user or if no acls are
     * found and user has configured to allow all users when no acls are found
     * or if no deny acls are found and at least one allow acls matches.
     */
    authorized = isSuperUser(principal)
        || isEmptyAclAndAuthorized(acls.get(topicName).get(projectName__userName))
        || (!denyMatch && allowMatch);

    //logAuditMessage(principal, authorized, operation, resource, host);
    return authorized;
  }

  private Boolean aclMatch(String operations, String principal,
      String host, String permissionType, String role,
      List<HopsAcl> acls) {
    if (acls != null && !acls.isEmpty()) {
      LOG.debug("aclMatch :: Operation:" + operations);
      LOG.debug("aclMatch :: principal:" + principal);
      LOG.debug("aclMatch :: host:" + host);
      LOG.debug("aclMatch :: permissionType:" + permissionType);
      LOG.debug("aclMatch :: role:" + role);
      LOG.debug("aclMatch :: acls:" + acls);

      for (HopsAcl acl : acls) {
        LOG.debug("aclMatch.acl" + acl);
        if (acl.getPermissionType().equalsIgnoreCase(permissionType)
            && (acl.getPrincipal().equalsIgnoreCase(principal) || acl.getPrincipal().equals(Consts.WILDCARD))
            && (acl.getOperationType().equalsIgnoreCase(operations) || acl.getOperationType().equalsIgnoreCase(
            Consts.WILDCARD))
            && (acl.getHost().equalsIgnoreCase(host) || acl.getHost().equals(Consts.WILDCARD))
            && (acl.getRole().equalsIgnoreCase(role) || acl.getRole().equals(Consts.WILDCARD))) {
          return true;
        }
      }
    }
    return false;
  }

  private Boolean isEmptyAclAndAuthorized(List<HopsAcl> acls) {
    if (acls.isEmpty()) {
      return shouldAllowEveryoneIfNoAclIsFound;
    }
    return false;
  }

  private boolean isSuperUser(KafkaPrincipal principal) {
    if (superUsers.contains(principal)) {
      LOG.debug("principal = " + principal + " is a super user, allowing operation without checking acls.");
      return true;
    }
    LOG.debug("principal = " + principal + " is not a super user.");
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
    dbConnection.close();
  }

}
