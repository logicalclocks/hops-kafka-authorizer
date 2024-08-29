package io.hops.kafka;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.kafka.common.Endpoint;
import org.apache.kafka.common.acl.AclBinding;
import org.apache.kafka.common.acl.AclBindingFilter;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.resource.ResourceType;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.server.authorizer.AclCreateResult;
import org.apache.kafka.server.authorizer.AclDeleteResult;
import org.apache.kafka.server.authorizer.Action;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.server.authorizer.AuthorizationResult;
import org.apache.kafka.server.authorizer.Authorizer;
import org.apache.kafka.server.authorizer.AuthorizerServerInfo;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.javatuples.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * Authorizer class for HopsWorks Kafka. Authorizer project users by extracting their project specific name from
 * the SSL/TLS certificate CN field.
 * <p>
 */
public class HopsAclAuthorizer implements Authorizer {
  
  private static final Logger LOGGER = LoggerFactory.getLogger("kafka.authorizer.logger");
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
        String[] userSplits = user.split(Consts.COLON_SEPARATOR);
        superUsers.add(new KafkaPrincipal(userSplits[0], userSplits[1]));
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
            LOGGER.info("Getting topics project. topicName: {}", topicName);
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
            LOGGER.info("Getting users project role. projectName: {}, username: {}",
                projectName, username);
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
            LOGGER.info("Getting project share permission. topicProjectId: {}, userProjectId: {}",
                topicProjectId, userProjectId);
            return dbConnection.getSharedProject(userProjectId, topicProjectId);
          }
        });
  }

  @Override
  public Map<Endpoint, ? extends CompletionStage<Void>> start(AuthorizerServerInfo authorizerServerInfo) {
    // Nothing major to do during the startup. We could potentially warm up the caches.
    return new HashMap<>();
  }

  @Override
  public List<AuthorizationResult> authorize(AuthorizableRequestContext authorizableRequestContext, List<Action> list) {
    return list.stream()
        .map(a -> authorize(authorizableRequestContext, a))
        .collect(Collectors.toList());
  }

  public AuthorizationResult authorize(AuthorizableRequestContext requestContext, Action action) {
    KafkaPrincipal principal = requestContext.principal();
    String host = requestContext.clientAddress().getHostAddress();
    ResourceType resourceType = action.resourcePattern().resourceType();
    String topicName = action.resourcePattern().name();
    List<String> subjectNames = Arrays.asList(principal.getName().split(Consts.SEMI_COLON));
    String principalName = getPrincipalName(subjectNames);
    AclOperation operation = action.operation();

    LOGGER.debug("authorize :: session: {}", requestContext);
    LOGGER.debug("authorize :: subjectNames: {}", subjectNames);
    LOGGER.debug("authorize :: principal.name: {}", principalName);
    LOGGER.debug("authorize :: principal.type: {}", principal.getPrincipalType());
    LOGGER.debug("authorize :: operation: {}", operation);
    LOGGER.debug("authorize :: host: {}", host);
    LOGGER.debug("authorize :: resource: {}", resourceType);
    LOGGER.debug("authorize :: topicName: {}", topicName);

    if (principalName.equalsIgnoreCase(Consts.ANONYMOUS)) {
      LOGGER.info("No Acl found for cluster authorization, user: {}", principalName);
      return AuthorizationResult.DENIED;
    }

    if (isSuperUser(subjectNames)) {
      return AuthorizationResult.ALLOWED;
    }

    if ("__consumer_offsets".equals(topicName)) {
      LOGGER.debug("topic = {} access allowed: {}", topicName, consumerOffsetsAccessAllowed);
      return consumerOffsetsAccessAllowed ? AuthorizationResult.ALLOWED : AuthorizationResult.DENIED;
    }

    if (resourceType.equals(ResourceType.CLUSTER)) {
      LOGGER.info("This is cluster authorization for broker: {}", principalName);
      return AuthorizationResult.DENIED;
    }

    if (resourceType.equals(ResourceType.GROUP)) {
      //Check if group requested starts with projectname__ and is equal to the current users project
      String projectCN = principalName.split(Consts.PROJECT_USER_DELIMITER)[0];
      if (topicName.contains(Consts.PROJECT_USER_DELIMITER)) {
        String projectConsumerGroup = topicName.split(Consts.PROJECT_USER_DELIMITER)[0];
        LOGGER.debug("Consumer group :: projectCN: {}", projectCN);
        LOGGER.debug("Consumer group :: projectConsumerGroup: {}", projectConsumerGroup);
        //Check principal project name is equal to project consumer group
        if (!projectCN.equals(projectConsumerGroup)) {
          LOGGER.info("Principal: {} is not allowed to access group: {}", principalName, topicName);
          return AuthorizationResult.DENIED;
        }
      }
      LOGGER.info("Principal: {} is allowed to access group: {}", principalName, topicName);
      return AuthorizationResult.ALLOWED;
    }

    return authorizeProjectUser(topicName, principalName, operation);
  }

  @Override
  public List<? extends CompletionStage<AclCreateResult>> createAcls(
      AuthorizableRequestContext authorizableRequestContext, List<AclBinding> list) {
    return null;
  }

  @Override
  public List<? extends CompletionStage<AclDeleteResult>> deleteAcls(
      AuthorizableRequestContext authorizableRequestContext, List<AclBindingFilter> list) {
    return null;
  }

  @Override
  public Iterable<AclBinding> acls(AclBindingFilter aclBindingFilter) {
    return null;
  }

  @Override
  public AuthorizationResult authorizeByResourceType(AuthorizableRequestContext requestContext,
                                                     AclOperation operation, ResourceType resource) {
    return AuthorizationResult.DENIED;
  }

  private AuthorizationResult authorizeProjectUser(String topicName, String principalName, AclOperation operation) {
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
        LOGGER.error("Failed to authorize user '{}' to perform '{}' on topic '{}', retries left: {}",
            principalName, operation.toString(), topicName, tries, e.getCause());
      } catch (CacheLoader.InvalidCacheLoadException e) {
        // This exception is thrown if cache result is 'null' (nothing in database)
        return AuthorizationResult.DENIED;
      }
    }
    return AuthorizationResult.DENIED;
  }

  protected AuthorizationResult authorizePermission(AclOperation operation, String sharePermission) {
    switch (sharePermission) {
      case Consts.READ_ONLY:
        return authorizeOperation(operation, Consts.DATA_SCIENTIST);
      case Consts.EDITABLE_BY_OWNERS:
      case Consts.EDITABLE:
      default:
        return AuthorizationResult.DENIED;
    }
  }

  protected AuthorizationResult authorizeOperation(AclOperation operation, String userRole) {
    switch (operation) {
      case WRITE:
      case IDEMPOTENT_WRITE:
      case CREATE:
        return Consts.DATA_OWNER.equals(userRole) ? AuthorizationResult.ALLOWED : AuthorizationResult.DENIED;
      case READ:
      case DESCRIBE:
        return AuthorizationResult.ALLOWED;
      default:
        return AuthorizationResult.DENIED;
    }
  }
  
  protected boolean isSuperUser(List<String> subjectNames) {
    // to be considered a super user there has to be an intersection between user subject names and super users
    if (superUsers.stream().anyMatch(su -> subjectNames.contains(su.getName()))) {
      LOGGER.debug(
          "principal = {} is a super user, allowing operation without checking acls.", getPrincipalName(subjectNames));
      return true;
    }
    LOGGER.debug("principal = {} is not a super user.", getPrincipalName(subjectNames));
    return false;
  }

  private String getPrincipalName(List<String> subjectNames) {
    return subjectNames.get(0); // the first cert name is principal name
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
