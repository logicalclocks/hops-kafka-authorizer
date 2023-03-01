package io.hops.kafka;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import kafka.network.RequestChannel;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import org.apache.kafka.common.security.auth.KafkaPrincipal;

import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.javatuples.Pair;

/**
 *
 * Authorizer class for HopsWorks Kafka. Authorizer project users by extracting their project specific name from
 * the SSL/TLS certificate CN field.
 * <p>
 */
public class HopsAclAuthorizer implements Authorizer {
  
  private static final Logger LOG = Logger.getLogger("kafka.authorizer.logger");
  //List of users that will be treated as superusers and will have access to
  //all the resources for all actions from all posts, defaults to no superusers.
  private Set<KafkaPrincipal> superUsers = new HashSet<>();
  //Identifies if work with __consumer_offsets topic is allowed.
  private boolean consumerOffsetsAccessAllowed = false;

  private DbConnection dbConnection;

  // topicName -> topicProjectId
  private LoadingCache<String, Integer> topicProject;
  // principalName (aka. projectName__username) -> userProjectId, userRole
  private LoadingCache<String, Pair<Integer, String>> userProject;
  // topicProjectId, userProjectId -> sharePermission
  private LoadingCache<Pair<Integer, Integer>, String> projectShare;

  public HopsAclAuthorizer() {}

  // For testing
  protected HopsAclAuthorizer(LoadingCache<String, Integer> topicProjectCache,
                              LoadingCache<String, Pair<Integer, String>> userProjectCache,
                              LoadingCache<Pair<Integer, Integer>, String> projectShareCache) {
    topicProject = topicProjectCache;
    userProject = userProjectCache;
    projectShare = projectShareCache;
  }

  /**
   * Guaranteed to be called before any authorize call is made.
   *
   * @param configs
   */
  @Override
  public void configure(java.util.Map<String, ?> configs) {
    Object superUserObj = configs.get(Consts.SUPERUSERS_PROP);
    if (superUserObj != null) {
      String superUsersStr = (String) superUserObj;
      for (String user : superUsersStr.split(Consts.SEMI_COLON)) {
        superUsers.add(KafkaPrincipal.fromString(user.trim()));
      }
    }

    Object consumerOffsetsAccessAllowedObj = configs.get(Consts.CONSUMER_OFFSETS_ACCESS_ALLOWED);
    if (consumerOffsetsAccessAllowedObj != null) {
      consumerOffsetsAccessAllowed = Boolean.parseBoolean((String) consumerOffsetsAccessAllowedObj);
    }

    //initialize database connection.
    dbConnection = new DbConnection(
        configs.get(Consts.DATABASE_URL).toString(),
        configs.get(Consts.DATABASE_USERNAME).toString(),
        configs.get(Consts.DATABASE_PASSWORD).toString(),
        Integer.parseInt(configs.get(Consts.DATABASE_MAX_POOL_SIZE).toString()),
        configs.get(Consts.DATABASE_CACHE_PREPSTMTS).toString(),
        configs.get(Consts.DATABASE_PREPSTMT_CACHE_SIZE).toString(),
        configs.get(Consts.DATABASE_PREPSTMT_CACHE_SQL_LIMIT).toString());

    long expireDuration = Long.parseLong(String.valueOf(configs.get(Consts.DATABASE_ACL_POLLING_FREQUENCY_MS)));
    long cacheMaxSize = Long.parseLong(String.valueOf(configs.get(Consts.CACHE_MAX_SIZE)));
    topicProject = CacheBuilder.newBuilder()
        .maximumSize(cacheMaxSize)
        .build(new CacheLoader<String, Integer>() {
          @Override
          public Integer load(String topicName) throws SQLException {
            LOG.info(String.format("Getting topics project. topicName: %s", topicName));
            return dbConnection.getTopicProject(topicName);
          }
        });
    userProject = CacheBuilder.newBuilder()
        .expireAfterWrite(expireDuration, TimeUnit.MILLISECONDS)
        .build(new CacheLoader<String, Pair<Integer, String>>() {
          @Override
          public Pair<Integer, String> load(String principalName) throws SQLException {
            String[] principalNameSplit = principalName.split(Consts.PROJECT_USER_DELIMITER);
            String projectName = principalNameSplit[0];
            String username = principalNameSplit[1];
            LOG.info(String.format("Getting users project role. projectName: %s, username: %s", projectName, username));
            return dbConnection.getProjectRole(projectName, username);
          }
        });
    projectShare = CacheBuilder.newBuilder()
        .expireAfterWrite(expireDuration, TimeUnit.MILLISECONDS)
        .build(new CacheLoader<Pair<Integer, Integer>, String>() {
          @Override
          public String load(Pair<Integer, Integer> pair) throws SQLException {
            int topicProjectId = pair.getValue0();
            int userProjectId = pair.getValue1();
            LOG.info(String.format("Getting project share permission. topicProjectId: %s, userProjectId: %s",
                topicProjectId, userProjectId));
            return dbConnection.getSharedProject(userProjectId, topicProjectId);
          }
        });
  }
  
  @Override
  public boolean authorize(RequestChannel.Session session, Operation operation, Resource resource) {
    KafkaPrincipal principal = session.principal();
    String host = session.clientAddress().getHostAddress();
    String topicName = resource.name();
    String principalName = principal.getName();
  
    LOG.debug("authorize :: session:" + session);
    LOG.debug("authorize :: principal.name:" + principalName);
    LOG.debug("authorize :: principal.type:" + principal.getPrincipalType());
    LOG.debug("authorize :: operation:" + operation);
    LOG.debug("authorize :: host:" + host);
    LOG.debug("authorize :: resource:" + resource);
    LOG.debug("authorize :: topicName:" + topicName);
    
    if (principalName.equalsIgnoreCase(Consts.ANONYMOUS)) {
      LOG.info("No Acl found for cluster authorization, user:" + principalName);
      return false;
    }
    
    if (isSuperUser(principal)) {
      return true;
    }

    if ("__consumer_offsets".equals(topicName)) {
      LOG.debug("topic = " + topicName + " access allowed: " + consumerOffsetsAccessAllowed);
      return consumerOffsetsAccessAllowed;
    }
    
    if (resource.resourceType().equals(
        kafka.security.auth.ResourceType$.MODULE$.fromString(Consts.CLUSTER))) {
      LOG.info("This is cluster authorization for broker: " + principalName);
      return false;
    }
    if (resource.resourceType().equals(
        kafka.security.auth.ResourceType$.MODULE$.fromString(Consts.GROUP))) {
      //Check if group requested starts with projectname__ and is equal to the current users project
      String projectCN = principalName.split(Consts.PROJECT_USER_DELIMITER)[0];
      if (resource.name().contains(Consts.PROJECT_USER_DELIMITER)) {
        String projectConsumerGroup = resource.name().split(Consts.PROJECT_USER_DELIMITER)[0];
        LOG.debug("Consumer group :: projectCN:" + projectCN);
        LOG.debug("Consumer group :: projectConsumerGroup:" + projectConsumerGroup);
        //Check principal project name is equal to project consumer group
        if (!projectCN.equals(projectConsumerGroup)) {
          LOG.info("Principal:" + principalName + " is not allowed to access group:" + resource.name());
          return false;
        }
      }
      LOG.info("Principal:" + principalName + " is allowed to access group:" + resource.name());
      return true;
    }

    return authorizeProjectUser(topicName, principalName, operation);
  }

  private boolean authorizeProjectUser(String topicName, String principalName, Operation operation) {
    int tries = 2;
    while (tries > 0) {
      try {
        // get topic related info
        int topicProjectId = topicProject.get(topicName);

        // get user related info
        Pair<Integer, String> pair = userProject.get(principalName);
        int userProjectId = pair.getValue0();
        String userRole = pair.getValue1();

        if (topicProjectId == userProjectId) {
          // Working on the same project
          return authorizeOperation(operation, userRole);
        } else {
          // Working on the shared project
          String sharePermission = projectShare.get(new Pair<>(topicProjectId, userProjectId));
          return authorizePermission(operation, sharePermission);
        }
      } catch (ExecutionException e) {
        tries--;
        LOG.error(String.format("Failed to authorize user '%s' to perform '%s' on topic '%s', retries left: %s",
            principalName, operation.toString(), topicName, tries), e.getCause());
      }
    }
    return false;
  }

  protected boolean authorizePermission(Operation operation, String sharePermission) {
    switch (sharePermission) {
      case Consts.READ_ONLY:
        return authorizeOperation(operation, Consts.DATA_SCIENTIST);
      case Consts.EDITABLE_BY_OWNERS:
      case Consts.EDITABLE:
      default:
        return false;
    }
  }

  protected boolean authorizeOperation(Operation operation, String userRole) {
    switch (operation.toString()) {
      case Consts.WRITE:
        return Consts.DATA_OWNER.equals(userRole);
      case Consts.READ:
      case Consts.DESCRIBE:
        return true;
      default:
        return false;
    }
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
  public void addAcls(scala.collection.immutable.Set<Acl> acls, Resource resource) {
  }

  @Override
  public boolean removeAcls(scala.collection.immutable.Set<Acl> acls, Resource resource) {
    return false;
  }

  @Override
  public boolean removeAcls(Resource resource) {
    return false;
  }

  @Override
  public scala.collection.immutable.Set<Acl> getAcls(Resource resource) {
    return null;
  }

  @Override
  public scala.collection.immutable.Map<Resource,
      scala.collection.immutable.Set<Acl>> getAcls(KafkaPrincipal principal) {
    return null;
  }

  @Override
  public scala.collection.immutable.Map<Resource, scala.collection.immutable.Set<Acl>> getAcls() {
    return null;
  }

  @Override
  public void close() {
    dbConnection.close();
  }

  // For testing
  public void setSuperUsers(Set<KafkaPrincipal> superUsers) {
    this.superUsers = superUsers;
  }

  // For testing
  public void setDbConnection(DbConnection dbConnection) {
    this.dbConnection = dbConnection;
  }
}
