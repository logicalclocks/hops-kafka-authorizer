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

    private static final Logger authorizerLogger = Logger.
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

    private final String databaseTypeProp = "database.type";
    private final String databaseUrlProp = "database.url";
    private final String databaseUserNameProp = "database.username";
    private final String databasePasswordProp = "database.password";

    DbConnection dbConnection;

    /**
     * Guaranteed to be called before any authorize call is made.
     *
     * @param configs
     */
    @Override
    public void configure(java.util.Map<String, ?> configs) {

        Object obj = configs.get(HopsAclAuthorizer.superUserProp);
        String databaseType;
        String databaseUrl;
        String databaseUserName;
        String databasePassword;

        if (obj != null) {
            String superUsersStr = (String) obj;
            String[] superUserStrings = superUsersStr.split(";");

            for (String user : superUserStrings) {
                superUsers.add(KafkaPrincipal.fromString(user.trim()));
            }
        } else {
            superUsers = new HashSet<>();
        }

        databaseType = configs.get(databaseTypeProp).toString();
        databaseUrl = configs.get(databaseUrlProp).toString();
        databaseUserName = configs.get(databaseUserNameProp).toString();
        databasePassword = configs.get(databasePasswordProp).toString();
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
        String topicName = resource.name();
        String projectName__userName = principal.getName();

        boolean authorized;

        if (resource.resourceType().equals(
                kafka.security.auth.ResourceType$.MODULE$.fromString("Cluster"))) {
            authorizerLogger.log(Level.SEVERE, "This is cluster authorization for broker: {0}",
                    new Object[]{projectName__userName});
            return true;
        }

        if (projectName__userName.equalsIgnoreCase("ANONYMOUS")) {
            authorizerLogger.log(Level.SEVERE,
                    "No Acl found for cluster authorization, user: {0}",
                    new Object[]{projectName__userName});
            return true;
        }

        java.util.Set<HopsAcl> resourceAcls = getTopicAcls(topicName);

        String role = dbConnection.getUserRole(projectName__userName);

        //check if there is any Deny acl match that would disallow this operation.
        Boolean denyMatch = aclMatch(operation.name(), projectName__userName,
                host, "Deny", role, resourceAcls);

        //if principal is allowed to read or write we allow describe by default,
        //the reverse does not apply to Deny.
        java.util.Set<String> ops = new HashSet<>();
        ops.add(operation.name());

        if (operation.name().equalsIgnoreCase("Describe")) {
            ops.add("Write");
            ops.add("Read");
        }

        //now check if there is any allow acl that will allow this operation.
        Boolean allowMatch = false;
        for (String op : ops) {
            if (aclMatch(op, projectName__userName, host, "Allow", role, resourceAcls)) {
                allowMatch = true;
            }
        }

        /*we allow an operation if a user is a super user or if no acls are
        found and user has configured to allow all users when no acls are found
        or if no deny acls are found and at least one allow acls matches.
         */
        authorized = isSuperUser(principal)
                || isEmptyAclAndAuthorized(resourceAcls)
                || (!denyMatch && allowMatch);

        //logAuditMessage(principal, authorized, operation, resource, host);
        return authorized;
    }

    private Boolean aclMatch(String operations, String principal,
            String host, String permissionType, String role,
            java.util.Set<HopsAcl> acls) {

        HopsAcl acl;
        java.util.Iterator<HopsAcl> iter = acls.iterator();
        for (; iter.hasNext();) {
            acl = iter.next();
            if (acl.getPermissionType().equalsIgnoreCase(permissionType)
                    && (acl.getPrincipal().equalsIgnoreCase(principal) || acl.getPrincipal().equals("*"))
                    && (acl.getOperationType().equalsIgnoreCase(operations) || acl.getOperationType().equalsIgnoreCase("all"))
                    && (acl.getHost().equalsIgnoreCase(host) || acl.getHost().equals("*"))
                    && (acl.getRole().equalsIgnoreCase(role) || acl.getRole().equals("*"))) {
                return true;
            }
        }
        return false;
    }

    Boolean isEmptyAclAndAuthorized(java.util.Set<HopsAcl> acls) {

        if (acls.isEmpty()) {
            return shouldAllowEveryoneIfNoAclIsFound;
        }
        return false;
    }

    Boolean isSuperUser(KafkaPrincipal principal) {

        if (superUsers.contains(principal)) {
            authorizerLogger.log(Level.INFO,
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

        scala.collection.immutable.Map<Resource, Set<Acl>> immutablePrincipalAcls
                = new scala.collection.immutable.HashMap<>();
        return immutablePrincipalAcls;
    }

    @Override
    public Map<Resource, Set<Acl>> getAcls() {

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
            while (topicAcls.next()) {
                principal = projectName + "__" + topicAcls.getString("username");
                permission = topicAcls.getString("permission_type");
                operation = topicAcls.getString("operation_type");
                host = topicAcls.getString("host");
                role = topicAcls.getString("role");
                resourceAcls.add(new HopsAcl(principal, permission, host, operation, role));
            }
            //get all the acls for wildcard topics
            ResultSet wildcardTopicAcls = dbConnection.getTopicAcls("*");
            while (wildcardTopicAcls.next()) {
                principal = projectName + "__" + topicAcls.getString("username");
                permission = topicAcls.getString("permission_type");
                operation = topicAcls.getString("operation_type");
                host = topicAcls.getString("host");
                role = topicAcls.getString("role");
                resourceAcls.add(new HopsAcl(principal, permission, host, operation, role));
            }
        } catch (SQLException ex) {
            Logger.getLogger(HopsAclAuthorizer.class.getName()).log(Level.SEVERE, null, ex);
        }
        return resourceAcls;
    }
}
