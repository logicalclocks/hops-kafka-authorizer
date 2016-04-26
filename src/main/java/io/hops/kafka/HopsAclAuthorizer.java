/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.kafka;

import kafka.network.RequestChannel;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import kafka.security.auth.PermissionType;
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

    private static Logger authorizerLogger
            = new LoggerProperties(KafkaAclAuthorizer.class.getName()).getLogger();

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

        //get role of the principal on this project from database
        String projectName__userName = principal.getName();

        if (resource.resourceType().equals(
                kafka.security.auth.ResourceType$.MODULE$.fromString("Cluster"))) {
            authorizerLogger.log(Level.SEVERE, "This is cluster authorization for broker",
                    new Object[]{projectName__userName});
            return true;
        }

        if (projectName__userName.equalsIgnoreCase("ANONYMOUS")) {
            authorizerLogger.log(Level.SEVERE, "No Acl found for cluster authorization, user: {0}", new Object[]{projectName__userName});
            return true;
        }

        String topicName = resource.name();

        boolean authorized = dbConnection.isTopicBelongsToProject(projectName__userName.split("__")[0], topicName);

        java.util.Set<AclRole> resourceAcls = getTopicAcls(topicName);

        String role = dbConnection.getUserRole(projectName__userName);

        //check if there is any Deny acl match that would disallow this operation.
        Boolean denyMatch = aclMatch(session, operation, resource, principal,
                host, kafka.security.auth.Deny$.MODULE$, resourceAcls, role);

        //if principal is allowed to read or write we allow describe by default,
        //the reverse does not apply to Deny.
        java.util.Set<Operation> ops = new HashSet<>();
        ops.add(operation);

        if (kafka.security.auth.Describe$.MODULE$ == operation) {
            ops.add(kafka.security.auth.Write$.MODULE$);
            ops.add(kafka.security.auth.Read$.MODULE$);
        }

        //now check if there is any allow acl that will allow this operation.
        Boolean allowMatch = false;
        for (Operation op : ops) {
            if (aclMatch(session, op, resource, principal, host,
                    kafka.security.auth.Allow$.MODULE$, resourceAcls, role)) {
                allowMatch = true;
            }
        }

        //we allow an operation if a user is a super user or if no acls are found and user has configured to allow all users
        //when no acls are found or if no deny acls are found and at least one allow acls matches.
        authorized = isSuperUser(principal)
                || isEmptyAclAndAuthorized(operation, resource, principal, host, resourceAcls)
                || (!denyMatch && allowMatch);

        //logAuditMessage(principal, authorized, operation, resource, host);
        authorizerLogger.log(Level.INFO, "No Acl found for cluster authorization: {0}", new Object[]{authorized});
        return authorized;
    }

    private Boolean aclMatch(RequestChannel.Session session, Operation operations,
            Resource resource, KafkaPrincipal principal, String host,
            PermissionType permissionType, java.util.Set<AclRole> acls, String role) {

        AclRole aclRole;
        Acl acl;
        java.util.Iterator<AclRole> iter = acls.iterator();
        for (; iter.hasNext();) {
            aclRole = iter.next();
            acl = aclRole.getAcl();
            if (acl.permissionType() == permissionType
                    && (principal == acl.principal() || acl.principal() == Acl.WildCardPrincipal())
                    && (operations == acl.operation() || acl.operation() == kafka.security.auth.All$.MODULE$)
                    && (host.equalsIgnoreCase(acl.host()) || acl.host().equalsIgnoreCase(Acl.WildCardHost()))
                    && (aclRole.getRole().equalsIgnoreCase(role) || aclRole.equals("*"))) {
                authorizerLogger.log(Level.WARNING, "operation = {0}"
                        + " on resource ={1} from host ={2} is {3} based on acl = {4}",
                        new Object[]{operations, resource, host, permissionType, acl});
                return true;
            }
        }
        return false;
    }

    Boolean isEmptyAclAndAuthorized(Operation operation, Resource resource,
            KafkaPrincipal principal, String host, java.util.Set<AclRole> acls) {

        if (acls.isEmpty()) {
            authorizerLogger.log(Level.INFO, "No acl found for resource {0}, authorized = {1}",
                    new Object[]{resource, shouldAllowEveryoneIfNoAclIsFound});
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

    private void logAuditMessage(KafkaPrincipal principal, Boolean authorized,
            Operation operation, Resource resource, String host) {

        PermissionType permissionType;
        if (authorized) {
            permissionType = kafka.security.auth.Allow$.MODULE$;
        } else {
            permissionType = kafka.security.auth.Deny$.MODULE$;
        }

        authorizerLogger.log(Level.INFO,
                "Perincipal = {0} is {1} Operation = {2} from host =  {3} on resource = {String topicName4}",
                new Object[]{principal, permissionType, operation, host, resource});
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

    private java.util.Set<AclRole> getTopicAcls(String topicName) {

        java.util.Set<AclRole> resourceAcls = new java.util.HashSet<>();
        AclRole acl;
        KafkaPrincipal prin;
        PermissionType perm;
        String ht;
        Operation op;

        try {

            //get all the acls for the given topic
            ResultSet topicAcls = dbConnection.getTopicAcls(topicName);
            while (topicAcls.next()) {
                prin = KafkaPrincipal.fromString("User:" + topicAcls.getString("user_id"));
                perm = kafka.security.auth.PermissionType$.MODULE$.fromString(topicAcls.getString("permission_type"));
                op = kafka.security.auth.Operation$.MODULE$.fromString(topicAcls.getString("operation_type"));
                ht = topicAcls.getString("host");

                acl = new AclRole(topicAcls.getString("role"),
                        new Acl(prin, perm, ht, op));
                resourceAcls.add(acl);
            }
            //get all the acls for wildcard topic
            ResultSet wildcardTopicAcls = dbConnection.getTopicAcls("*");
            while (wildcardTopicAcls.next()) {

                prin = KafkaPrincipal.fromString("User:" + wildcardTopicAcls.getString("user_id"));
                perm = kafka.security.auth.PermissionType$.MODULE$.fromString(wildcardTopicAcls.getString("permission_type"));
                op = kafka.security.auth.Operation$.MODULE$.fromString(wildcardTopicAcls.getString("operation_type"));
                ht = wildcardTopicAcls.getString("host");

                acl = new AclRole(wildcardTopicAcls.getString("role"),
                        new Acl(prin, perm, ht, op));
                resourceAcls.add(acl);
            }
        } catch (SQLException ex) {
            Logger.getLogger(HopsAclAuthorizer.class.getName()).log(Level.SEVERE, null, ex);
        }

        return resourceAcls;
    }
}
