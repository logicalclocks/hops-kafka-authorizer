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

/**
 *
 * @author misdess
 */
public class HopsAclAuthorizer implements Authorizer {

    private final String SEMI_COLON = ";";

    public final String ANONYMOUS = "ANONYMOUS";

    public final String CLUSTER = "Cluster";
    
    public final String WILDCARD = "*";
    
    public final String ALLOW ="Allow";
    public final String DENY ="Deny";
    public final String READ ="Read";
    public final String WRITE ="Write";
    public final String DESCRIBE ="Describe";
    public final String ALL ="All";
    
    public final String PRINCIPAL ="principal";
    public final String PERMISSION_TYPE ="permission_type";
    public final String OPERATION_TYPE ="operation_type";
    public final String HOST ="host";
     public final String ROLE ="role";
    

    private static final Logger AUTHORIZERLOGGER = Logger.
            getLogger(HopsAclAuthorizer.class.getName());
    //List of users that will be treated as super users and will have access to 
    //all the resources for all actions from all hosts, defaults to no super users.
    private java.util.Set<KafkaPrincipal> superUsers = new java.util.HashSet<>();
    private static final String superUserProp = "super.users";

    //If set to true when no acls are found for a resource , authorizer allows 
    //access to everyone. Defaults to false.
    private boolean shouldAllowEveryoneIfNoAclIsFound = false;
    private static final String AllowEveryoneIfNoAclIsFoundProp
            = "allow.everyone.if.no.acl.found";

    private final String DATABASE_TYPE = "database.type";
    private final String DATABASE_URL = "database.url";
    private final String DATABASE_USERNAME = "database.username";
    private final String DATABASE_PASSWORD = "database.password";

    DbConnection dbConnection;

    /**
     * Guaranteed to be called before any authorize call is made.
     *
     * @param configs
     */
    @Override
    public void configure(java.util.Map<String, ?> configs) {
        System.out.println("configs:"+configs.toString());
        Object obj = configs.get(HopsAclAuthorizer.superUserProp);
        System.out.println("obj:"+obj);
        String databaseType;
        String databaseUrl;
        String databaseUserName;
        String databasePassword;

        if (obj != null) {
            String superUsersStr = (String) obj;
            String[] superUserStrings = superUsersStr.split(SEMI_COLON);

            for (String user : superUserStrings) {
                superUsers.add(KafkaPrincipal.fromString(user.trim()));
            }
        } else {
            superUsers = new HashSet<>();
        }

        databaseType = configs.get(DATABASE_TYPE).toString();
        databaseUrl = configs.get(DATABASE_URL).toString();
        databaseUserName = configs.get(DATABASE_USERNAME).toString();
        databasePassword = configs.get(DATABASE_PASSWORD).toString();
        //initialize database connection.
        dbConnection = new DbConnection(databaseType, databaseUrl,
                databaseUserName, databasePassword);

        //grap the default acl property
        shouldAllowEveryoneIfNoAclIsFound = Boolean.valueOf(
                configs.get(HopsAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp)
                .toString());
    }

    @Override
    public boolean authorize(RequestChannel.Session session, Operation operation,
            Resource resource) {

        KafkaPrincipal principal = session.principal();
        String host = session.clientAddress().getHostAddress();
        System.out.println("authorize :: session:"+session);
        System.out.println("authorize :: principal.name:"+principal.getName()); 
        System.out.println("authorize :: principal.name:"+principal.getPrincipalType());
        System.out.println("authorize :: operation:"+resource);
        System.out.println("authorize :: host:"+host);
        System.out.println("authorize :: resource:"+resource);
        String topicName = resource.name();
        System.out.println("topicName:"+topicName);
        String projectName__userName = principal.getName();
        System.out.println("projectName__userName:"+projectName__userName);
        boolean authorized;

        if (resource.resourceType().equals(
                kafka.security.auth.ResourceType$.MODULE$.fromString(CLUSTER))) {
            AUTHORIZERLOGGER.log(Level.SEVERE, "This is cluster authorization for broker: {0}",
                    new Object[]{projectName__userName});
            return true;
        }

        if (projectName__userName.equalsIgnoreCase(ANONYMOUS)) {
            AUTHORIZERLOGGER.log(Level.SEVERE,
                    "No Acl found for cluster authorization, user: {0}",
                    new Object[]{projectName__userName});
            return true;
        }

        java.util.Set<HopsAcl> resourceAcls = getTopicAcls(topicName);
        System.out.println("resourceAcls:"+resourceAcls);

        String role = dbConnection.getUserRole(projectName__userName);

        //check if there is any Deny acl match that would disallow this operation.
        Boolean denyMatch = aclMatch(operation.name(), projectName__userName,
                host, DENY, role, resourceAcls);
        System.out.println("denyMatch:"+denyMatch);    
        //if principal is allowed to read or write we allow describe by default,
        //the reverse does not apply to Deny.
        java.util.Set<String> ops = new HashSet<>();
        
        ops.add(operation.name());
        System.out.println("ops:"+ops);
        if (operation.name().equalsIgnoreCase(DESCRIBE)) {
            ops.add(WRITE);
            ops.add(READ);
        }

        //now check if there is any allow acl that will allow this operation.
        Boolean allowMatch = false;
        System.out.println("allowMatch:"+allowMatch);    
        for (String op : ops) {
            if (aclMatch(op, projectName__userName, host, ALLOW, role, resourceAcls)) {
                allowMatch = true;
            }
        }
         System.out.println("allowMatch:"+allowMatch);    
        /*we allow an operation if a user is a super user or if no acls are
        found and user has configured to allow all users when no acls are found
        or if no deny acls are found and at least one allow acls matches.
         */
        authorized = isSuperUser(principal)
                || isEmptyAclAndAuthorized(resourceAcls, topicName, projectName__userName)
                || (!denyMatch && allowMatch);

        //logAuditMessage(principal, authorized, operation, resource, host);
        return authorized;
    }

    private Boolean aclMatch(String operations, String principal,
            String host, String permissionType, String role,
            java.util.Set<HopsAcl> acls) {
        System.out.println("Operation:"+operations);
        System.out.println("principal:"+principal);
        System.out.println("host:"+host);
        System.out.println("permissionType:"+permissionType);
        System.out.println("role:"+role);
        System.out.println("acls:"+acls);
        
        for (HopsAcl acl : acls) {
            System.out.println("aclMatch.acl"+acl);
            if (acl.getPermissionType().equalsIgnoreCase(permissionType)
                    && (acl.getPrincipal().equalsIgnoreCase(principal) || acl.getPrincipal().equals(WILDCARD))
                    && (acl.getOperationType().equalsIgnoreCase(operations) || acl.getOperationType().equalsIgnoreCase(WILDCARD))
                    && (acl.getHost().equalsIgnoreCase(host) || acl.getHost().equals(WILDCARD))
                    && (acl.getRole().equalsIgnoreCase(role) || acl.getRole().equals(WILDCARD))) {
                return true;
            }
        }
        return false;
    }

    Boolean isEmptyAclAndAuthorized(java.util.Set<HopsAcl> acls,
            String topicName, String principalName) {
        System.out.println("\n\nisEmptyAclAndAuthorized.acls:"+acls);
        if (acls.isEmpty()) {
            //Authorize if user is member of the toipc owner project
            if (dbConnection.isPrincipalMemberOfTopicOwnerProject(topicName, principalName)) {
                return true;
            }
            return shouldAllowEveryoneIfNoAclIsFound;
        }
        return false;
    }

    Boolean isSuperUser(KafkaPrincipal principal) {
        System.out.println("isSuperUser:"+principal);
        System.out.println("isSuperUser:"+principal);
        if (superUsers.contains(principal)) {
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
            System.out.println("getTopicAcls.topicAcls:"+topicAcls.toString());
            while (topicAcls.next()) {
                // principal = projectName + "__" + topicAcls.getString("username");
                principal = topicAcls.getString(PRINCIPAL);
                permission = topicAcls.getString(PERMISSION_TYPE);
                operation = topicAcls.getString(OPERATION_TYPE);
                host = topicAcls.getString(HOST);
                role = topicAcls.getString(ROLE);
                HopsAcl acl = new HopsAcl(principal, permission, host, operation, role);
                System.out.println("acl-1:"+acl);
                resourceAcls.add(acl);
            }
            //get all the acls for wildcard topics
            ResultSet wildcardTopicAcls = dbConnection.getTopicAcls(WILDCARD);
            while (wildcardTopicAcls.next()) {
                // principal = projectName + "__" + topicAcls.getString("username");
                principal = topicAcls.getString(PRINCIPAL);
                permission = topicAcls.getString(PERMISSION_TYPE);
                operation = topicAcls.getString(OPERATION_TYPE);
                host = topicAcls.getString(HOST);
                role = topicAcls.getString(ROLE);
                HopsAcl acl= new HopsAcl(principal, permission, host, operation, role);
                System.out.println("acl-2:"+acl);
                resourceAcls.add(new HopsAcl(principal, permission, host, operation, role));
            }
        } catch (SQLException ex) {
            System.out.println("getTopicAcls error:"+ex.toString());
            AUTHORIZERLOGGER.log(Level.SEVERE, null, ex.toString());
        }
        return resourceAcls;
    }
}
