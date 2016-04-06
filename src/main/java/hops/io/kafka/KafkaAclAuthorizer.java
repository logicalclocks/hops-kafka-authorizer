/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package hops.io.kafka;

import static hops.io.kafka.ConnectionObject.CONNECTIONLOGGGER;
import kafka.network.RequestChannel;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;
import scala.collection.JavaConverters$;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;

import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.security.auth.PermissionType;

/**
 *
 * @author misdess
 */
public class KafkaAclAuthorizer implements Authorizer {

    private static Logger authorizerLoger = Logger.getLogger("kafka.authorizer.logger");

    //List of users that will be treated as super users and will have access to all the resources for all actions from all hosts, defaults to no super users.
    private java.util.Set<KafkaPrincipal> superUsers = new java.util.HashSet<>();

    private static String superUserProp = "super.users";

    //If set to true when no acls are found for a resource , authorizer allows access to everyone. Defaults to false.
    private boolean shouldAllowEveryoneIfNoAclIsFound = false;

    private static String AllowEveryoneIfNoAclIsFoundProp = "allow.everyone.if.no.acl.found";
    
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private java.util.Map<Resource, Set<AclRole>> aclCaches = new java.util.HashMap<>();

    ConnectionObject connObject;
    
    /**
     * Guaranteed to be called before any authorize call is made.
     * @param configs 
     */
    @Override
    public void configure(java.util.Map<String, ?> configs) {

        Object obj = configs.get(this.superUserProp);

        if (obj != null) {
            String superUsersStr = (String) obj;
            String[] superUserStrings = superUsersStr.split(";");

            for (String user : superUserStrings) {
                superUsers.add(KafkaPrincipal.fromString(user.trim()));
            }
        } else {
            superUsers = new HashSet<>();
        }

        shouldAllowEveryoneIfNoAclIsFound = Boolean.parseBoolean(configs.get(this.AllowEveryoneIfNoAclIsFoundProp).toString());

        //initialize database connection. Can the type of database be passed in the configuration?
        connObject = new ConnectionObject("mysql");
        
        //load the acls from database into cache
        loadCache();
    }

    @Override
    public boolean authorize(RequestChannel.Session session, Operation operation, Resource resource) {

        KafkaPrincipal principal = session.principal();
        String host = session.clientAddress().getHostAddress();
        
        //get acls for this resource type
        java.util.Set<Acl> resourceAcls = JavaConverters$.MODULE$.setAsJavaSetConverter(getAcls(resource)).asJava();
        
        //get acls for wildcard resources
        java.util.Set<Acl> wildCardResourceAcls = JavaConverters$.MODULE$
                .setAsJavaSetConverter(getAcls(
                        new Resource(resource.resourceType(), Resource.WildCardResource()))).asJava();

        //collect all acls for both this resources and wildcard resources
        resourceAcls.addAll(wildCardResourceAcls);
        
        //check if there is any Deny acl match that would disallow this operation.
        Boolean denyMatch = aclMatch(session, operation, resource, principal, host, kafka.security.auth.Deny$.MODULE$, resourceAcls);

        //if principal is allowed to read or write we allow describe by default, the reverse does not apply to Deny.
        java.util.Set<Operation> ops = new HashSet<>();
        ops.add(operation);

        if (kafka.security.auth.Describe$.MODULE$ == operation) {
            ops.add(kafka.security.auth.Write$.MODULE$);
            ops.add(kafka.security.auth.Read$.MODULE$);
        }

        //now check if there is any allow acl that will allow this operation.
        Boolean allowMatch = false;
        for (Operation op : ops) {
            if (aclMatch(session, op, resource, principal, host, kafka.security.auth.Allow$.MODULE$, resourceAcls)) {
                allowMatch = true;
            }
        }

        //we allow an operation if a user is a super user or if no acls are found and user has configured to allow all users
        //when no acls are found or if no deny acls are found and at least one allow acls matches.
        Boolean authorized = isSuperUser(principal)
                || isEmptyAclAndAuthorized(operation, resource, principal, host, resourceAcls)
                || (!denyMatch && allowMatch);

        logAuditMessage(principal, authorized, operation, resource, host);

        return authorized;
    }

    private Boolean aclMatch(RequestChannel.Session session, Operation operations,
            Resource resource, KafkaPrincipal principal, String host,
            PermissionType permissionType, java.util.Set<Acl> acls) {

        java.util.Iterator<Acl> iter = acls.iterator();
        for (; iter.hasNext();) {
            Acl acl = iter.next();
            if (acl.permissionType() == permissionType
                    && (principal == acl.principal() || acl.principal() == Acl.WildCardPrincipal())
                    && (operations == acl.operation() || acl.operation() == kafka.security.auth.All$.MODULE$)
                    && (host.equalsIgnoreCase(acl.host()) || acl.host().equalsIgnoreCase(Acl.WildCardHost()))) {
                authorizerLoger.log(Level.WARNING, "operation = {0}"
                        + " on resource ={1} from host ={2} is {3} based on acl = {4}",
                        new Object[]{operations, resource, host, permissionType, acl});
                return true;
            }
        }
        return false;
    }

    Boolean isEmptyAclAndAuthorized(Operation operation, Resource resource,
            KafkaPrincipal principal, String host, java.util.Set<Acl> acls) {
        
        if (acls.isEmpty()) {
            authorizerLoger.log(Level.INFO, "No acl found for resource {0}, authorized = {1}",
                    new Object[]{resource, shouldAllowEveryoneIfNoAclIsFound});
            return shouldAllowEveryoneIfNoAclIsFound;
        }
        return false;
    }

    Boolean isSuperUser(KafkaPrincipal principal) {
        
        if (superUsers.contains(principal)) {
            authorizerLoger.log(Level.INFO,
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

        authorizerLoger.log(Level.INFO,
                "Perincipal = {0} is {1} Operation = {2} from host =  {3} on resource = {4}",
                new Object[]{principal, permissionType, operation, host, resource});
    }

    @Override
    public void addAcls(Set<Acl> acls, Resource resource) {

        if (acls != null && !acls.isEmpty()) {
            java.util.Set<Acl> updatedAcls = JavaConverters$.MODULE$
                    .setAsJavaSetConverter(acls).asJava();

            //add the topic acls to the database
            connObject.addTopicAcls(resource.name(), updatedAcls);
        }
    }

    @Override
    public boolean removeAcls(Set<Acl> aclsToBeRemoved, Resource resource) {

        //does topic exist in database
        Boolean topicExists = connObject.topicExists(resource.name());

        if (topicExists) {
            java.util.Set<Acl> existingAcls = JavaConverters$.MODULE$.setAsJavaSetConverter(getAcls(resource)).asJava();
            java.util.Set<Acl> filteredAcls = new HashSet<>();
            java.util.Iterator<Acl> iter = existingAcls.iterator();
            while (iter.hasNext()) {
                Acl acl = iter.next();
                if (!aclsToBeRemoved.contains(acl)) {
                    filteredAcls.add(acl);
                }
            }

            //is topic acl modified
            Boolean aclNeedsRemoval = existingAcls.equals(filteredAcls);
            if (aclNeedsRemoval) {
                if (!filteredAcls.isEmpty()) {
                    connObject.removeAcls(resource.name(), JavaConverters$.MODULE$.setAsJavaSetConverter(aclsToBeRemoved).asJava());
                } else {
                    connObject.removeTopic(resource.name());
                }
            }
            return aclNeedsRemoval;
        }
        return false;
    }

    @Override
    public boolean removeAcls(Resource resource) {
        
        Boolean topicExists = connObject.removeTopic(resource.name());
        return topicExists;
    }

    @Override
    public Set<Acl> getAcls(Resource resource) {

        java.util.Set<Acl> acls = new java.util.HashSet<>();
        Set<AclRole> aclCache = null;

        try {
            lock.readLock().lock();
            aclCache = aclCaches.get(resource);
        } finally {
            lock.readLock().unlock();
        }
        if (aclCache != null) {
            for (scala.collection.Iterator<AclRole> iter = aclCache.iterator(); iter.hasNext();) {
                acls.add(iter.next().getAcl());
            }
        } else {
            return new scala.collection.immutable.HashSet<>();
        }
        return JavaConverters$.MODULE$.asScalaSetConverter(acls).asScala().toSet();
    }

    @Override
    public Map<Resource, Set<Acl>> getAcls(KafkaPrincipal principal) {

        java.util.Set<Acl> resourceAcl = new HashSet<>();
        java.util.Map<Resource, Set<Acl>> principalAcls = new HashMap<>();

        for (java.util.Map.Entry<Resource, Set<AclRole>> aclCache : aclCaches.entrySet()) {
            Resource resource = aclCache.getKey();
            Set<AclRole> aclsRole = aclCache.getValue().toSet();

            for (scala.collection.Iterator<AclRole> iter = aclsRole.iterator(); iter.hasNext();) {
                AclRole next = iter.next();
                Acl acl = next.getAcl();
                if (acl.principal().equals(principal)) {
                    resourceAcl.add(acl);
                }
            }

            Set<Acl> immutableAcl = JavaConverters$.MODULE$
                    .asScalaSetConverter(resourceAcl).asScala().toSet();
            principalAcls.put(resource, immutableAcl);
        }

        scala.collection.immutable.Map<Resource, Set<Acl>> aclCache
                = JavaConverters$.MODULE$.mapAsScalaMapConverter(principalAcls)
                .asScala().toMap(scala.Predef$.MODULE$
                        .<scala.Tuple2<Resource, Set<Acl>>>conforms());

        return aclCache;
    }

    @Override
    public Map<Resource, Set<Acl>> getAcls() {

        java.util.Map<Resource, Set<Acl>> acls = new java.util.HashMap<>();
        java.util.Set<Acl> acl = new java.util.HashSet<>();

        for (java.util.Map.Entry<Resource, Set<AclRole>> entry : aclCaches.entrySet()) {
            Resource key = entry.getKey();
            Set<AclRole> value = entry.getValue();
            for (scala.collection.Iterator<AclRole> iterator = value.iterator(); iterator.hasNext();) {
                acl.add(iterator.next().getAcl());

            }
            Set<Acl> immutableAcl = JavaConverters$.MODULE$.asScalaSetConverter(acl).asScala().toSet();
            acls.put(key, immutableAcl);
        }

        scala.collection.immutable.Map<Resource, Set<Acl>> aclCache
                = JavaConverters$.MODULE$.mapAsScalaMapConverter(acls)
                .asScala().toMap(scala.Predef$.MODULE$
                        .<scala.Tuple2<Resource, Set<Acl>>>conforms());
        return aclCache;
    }

    @Override
    public void close() {
        connObject.closeConnection();
        connObject = null;
    }

    //load the acls from database into aclCache
    private void loadCache() {

        String topicName;
        Set<AclRole> topicAcls;
        java.util.Map<Resource, java.util.Set<AclRole>> allAcls = new java.util.HashMap<>();
        java.util.Set<AclRole> aclSet = new java.util.HashSet<>();

        KafkaPrincipal principal;
        PermissionType permission;
        String host;
        Operation operation;
        String role;

        try {

            ResultSet resultSet = connObject.getAllAcls();
            
            while (resultSet.next()) {
                
                topicName = resultSet.getString("topic_name");
                principal = KafkaPrincipal.fromString("User:" + resultSet.getString("project_name"));
                permission = kafka.security.auth.PermissionType$.MODULE$.fromString(resultSet.getString("permission_type"));
                host = resultSet.getString("host");
                operation = kafka.security.auth.Operation$.MODULE$.fromString(resultSet.getString("operation_type"));
                role = resultSet.getString("role");

                Acl TopicAcl = new Acl(principal, permission, host, operation);
                AclRole aclRole = new AclRole(role, TopicAcl);
                
                Resource resource = Resource.fromString("Topic:" + topicName);
                if (allAcls.keySet().contains(resource)) {
                    allAcls.get(resource).add(aclRole);
                } else {
                    aclSet.add(aclRole);
                    allAcls.put(resource, aclSet);
                }
            }
        } catch (SQLException ex) {
           CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }

        for (java.util.Map.Entry<Resource, java.util.Set<AclRole>> entry : allAcls.entrySet()) {
            Resource resource = entry.getKey();
            //change the topic acls from mutable to immutable scala set collection using JavaConverter$
            topicAcls = JavaConverters$.MODULE$.asScalaSetConverter(entry.getValue()).asScala().toSet();
            updateCache(resource, topicAcls);
        }
    }

    private void updateCache(Resource resource, Set<AclRole> acls) {

        try {
            lock.writeLock().lock();
            if (!acls.isEmpty()) {
                aclCaches.put(resource, acls);
            } else {
                aclCaches.remove(resource);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }
}
