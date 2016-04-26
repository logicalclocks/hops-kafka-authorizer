/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.kafka;

import static io.hops.kafka.HopsDbConnection.CONNECTIONLOGGGER;
import kafka.network.RequestChannel;
import kafka.security.auth.Acl;
import kafka.security.auth.Authorizer;
import kafka.security.auth.Operation;
import kafka.security.auth.Resource;
import kafka.security.auth.PermissionType;
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
import java.util.logging.Handler;
import org.slf4j.LoggerFactory;

/**
 *
 * @author misdess
 */
public class KafkaAclAuthorizer implements Authorizer {

    private static Logger authorizerLogger = new LoggerProperties(KafkaAclAuthorizer.class.getName()).getLogger();
    
    //List of users that will be treated as super users and will have access to 
    //all the resources for all actions from all hosts, defaults to no super users.
    private java.util.Set<KafkaPrincipal> superUsers = new java.util.HashSet<>();

    private static final String superUserProp = "super.users";

    //If set to true when no acls are found for a resource , authorizer allows 
    //access to everyone. Defaults to false.
    private boolean shouldAllowEveryoneIfNoAclIsFound = false;

    private static final String AllowEveryoneIfNoAclIsFoundProp = "allow.everyone.if.no.acl.found";
    
    ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private java.util.Map<Resource, Set<AclRole>> aclCaches = new java.util.HashMap<>();
    
    private final String databaseTypeProp ="database.type";
    private final String databaseUrlProp = "database.url";
    private final String databaseUserNameProp = "database.username";
    private final String databasePasswordProp = "database.password";

    HopsDbConnection connObject;
    
    /**
     * Guaranteed to be called before any authorize call is made.
     * @param configs 
     */
    @Override
    public void configure(java.util.Map<String, ?> configs) {

        Object obj = configs.get(KafkaAclAuthorizer.superUserProp);
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
        
        databaseType= configs.get(databaseTypeProp).toString();
        databaseUrl = configs.get(databaseUrlProp).toString();
        databaseUserName=configs.get(databaseUserNameProp).toString();
        databasePassword= configs.get(databasePasswordProp).toString();       

//        shouldAllowEveryoneIfNoAclIsFound = Boolean.valueOf(
//                configs.get(KafkaAclAuthorizer.AllowEveryoneIfNoAclIsFoundProp).toString());
        //initialize database connection.
       connObject = new HopsDbConnection(databaseType, databaseUrl, databaseUserName, databasePassword);
       
//        try {
//            authorizerLogger.addHandler(new FileHandler("/var/log/kafka/acl_authorizser.log"));
//        } catch (IOException  ex) {
//            Logger.getLogger(KafkaAclAuthorizer.class.getName()).log(Level.SEVERE, null, ex);
//        } catch (SecurityException ex) {
//            Logger.getLogger(KafkaAclAuthorizer.class.getName()).log(Level.SEVERE, null, ex);
//        }
    }

    @Override
    public boolean authorize(RequestChannel.Session session, Operation operation, Resource resource) {

        KafkaPrincipal principal = session.principal();
        String host = session.clientAddress().getHostAddress();

        //get role of the principal on this project from database
        String projectName__userName = principal.getName();
        
        /*How to achieve broker authorization.
            Brokers also have acls, not attached
            to specific topics, for the purpose of inter-broker interaction. 
            
            My two options for broker authorization:
            1: authorize every broker operation
            2: prepare broker_acls table, like topic_acls table: 
                a. check client is broker
                b. create KafkaPrincipal instance accordingly 
              This adds complexities in adding/removing acls, but I prefer if we go
              for this approach.
        
        */
        if(resource.resourceType().equals(kafka.security.auth.ResourceType$.MODULE$.fromString("Cluster"))){
        authorizerLogger.log(Level.SEVERE, "This is cluster authorization for broker", new Object[]{projectName__userName});
            System.out.println("This is the current client: "+projectName__userName);
        return true;
        }

        if (projectName__userName.equalsIgnoreCase("ANONYMOUS")) {
            authorizerLogger.log(Level.SEVERE, "No Acl found for cluster authorization, user: {0}", new Object[]{projectName__userName});
            return true;
        }

        String topicName = resource.name();

        boolean authorized = connObject.isTopicBelongsToProject(projectName__userName.split("__")[0], topicName);

        //        String role = connObject.getUserRole(projectName__userName);
//         
//        if(role==null)
//        return false;
        //load the acls from database into a local cache
//        loadAllAcls();
//        
//        //get acls for this resource type
//        java.util.Set<Acl> resourceAcls = JavaConverters$.MODULE$.setAsJavaSetConverter(getAcls(resource)).asJava();
//        
//        //get acls for wildcard resources
//        java.util.Set<Acl> wildCardResourceAcls = JavaConverters$.MODULE$
//                .setAsJavaSetConverter(getAcls(
//                        new Resource(resource.resourceType(), Resource.WildCardResource()))).asJava();
//
//        //collect all acls for both this resources and wildcard resources
//        resourceAcls.addAll(wildCardResourceAcls);
//        
//        //check if there is any Deny acl match that would disallow this operation.
//        Boolean denyMatch = aclMatch(session, operation, resource, principal, host, kafka.security.auth.Deny$.MODULE$, resourceAcls);
//
//        //if principal is allowed to read or write we allow describe by default, the reverse does not apply to Deny.
//        java.util.Set<Operation> ops = new HashSet<>();
//        ops.add(operation);
//
//        if (kafka.security.auth.Describe$.MODULE$ == operation) {
//            ops.add(kafka.security.auth.Write$.MODULE$);
//            ops.add(kafka.security.auth.Read$.MODULE$);
//        }
//
//        //now check if there is any allow acl that will allow this operation.
//        Boolean allowMatch = false;
//        for (Operation op : ops) {
//            if (aclMatch(session, op, resource, principal, host, kafka.security.auth.Allow$.MODULE$, resourceAcls)) {
//                allowMatch = true;
//            }
//        }
//
//        //we allow an operation if a user is a super user or if no acls are found and user has configured to allow all users
//        //when no acls are found or if no deny acls are found and at least one allow acls matches.
//        Boolean authorized = isSuperUser(principal)
//                || isEmptyAclAndAuthorized(operation, resource, principal, host, resourceAcls)
//                || (!denyMatch && allowMatch);
//
//        logAuditMessage(principal, authorized, operation, resource, host);

        authorizerLogger.log(Level.INFO, "No Acl found for cluster authorization: {0}", new Object[]{authorized});
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
                authorizerLogger.log(Level.WARNING, "operation = {0}"
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

//        if (acls != null && !acls.isEmpty()) {
//            java.util.Set<Acl> newAcls = JavaConverters$.MODULE$
//                    .setAsJavaSetConverter(acls).asJava();
//
//            //add topic acls to the database
//            connObject.addTopicAcls(resource.name(), newAcls);
//        }
        return;
    }

    @Override
    public boolean removeAcls(Set<Acl> aclsToBeRemoved, Resource resource) {
//        
//        java.util.Set<Acl> removeAcls = new java.util.HashSet<>();
//        if (aclsToBeRemoved != null && !aclsToBeRemoved.isEmpty()) {
//            removeAcls = JavaConverters$.MODULE$
//                    .setAsJavaSetConverter(aclsToBeRemoved).asJava();
//        }
//        if (removeAcls.isEmpty()) {
//            return false;
//        }
//        return connObject.removeTopicAcls(resource.name(), JavaConverters$.MODULE$
//                .setAsJavaSetConverter(aclsToBeRemoved).asJava());
return false;
    }

    @Override
    public boolean removeAcls(Resource resource) {

        return false ; // connObject.removeAllTopicAcls(resource.name());
    }

    @Override
    public Set<Acl> getAcls(Resource resource) {

        Acl acl;
        KafkaPrincipal principal;
        PermissionType permission;
        String host;
        Operation operation;

        java.util.Set<Acl> aclSet = new java.util.HashSet<>();
        ResultSet resultSet = connObject.getTopicAcls(resource.name());

        try {
            while (resultSet.next()) {
                //change the resource again, and create the acls
                if (resource.name().equals(resultSet.getString("topic_name"))) {
                    principal = KafkaPrincipal.fromString("User:" + resultSet.getString("user_id"));
                    permission = kafka.security.auth.PermissionType$.MODULE$.fromString(resultSet.getString("permission_type"));
                    operation = kafka.security.auth.Operation$.MODULE$.fromString(resultSet.getString("operation_type"));
                    host = resultSet.getString("host");

                    acl = new Acl(principal, permission, host, operation);
                    aclSet.add(acl);
                }
            }
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }

        if (aclSet.isEmpty()) {
            return new scala.collection.immutable.HashSet<>();
        }

        return JavaConverters$.MODULE$.asScalaSetConverter(aclSet).asScala().toSet();
    }

    @Override
    public Map<Resource, Set<Acl>> getAcls(KafkaPrincipal principal) {

        String topicName;
        java.util.Map<Resource, java.util.Set<Acl>> allAcls = new java.util.HashMap<>();
        java.util.Set<Acl> aclSet = new java.util.HashSet<>();

        PermissionType permission;
        String host;
        Operation operation;

        try {
            ResultSet resultSet = connObject.getPrinipalAcls(principal.getName());

            while (resultSet.next()) {
                //check the principal again and create the acls
                if (principal.equals(KafkaPrincipal.fromString("User:" + resultSet.getString("user_id"))));
                {
                    topicName = resultSet.getString("topic_name");
                    permission = kafka.security.auth.PermissionType$.MODULE$.fromString(resultSet.getString("permission_type"));
                    host = resultSet.getString("host");
                    operation = kafka.security.auth.Operation$.MODULE$.fromString(resultSet.getString("operation_type"));

                    Acl TopicAcl = new Acl(principal, permission, host, operation);
                    Resource resource = Resource.fromString("Topic:" + topicName);

                    if (allAcls.keySet().contains(resource)) {
                        allAcls.get(resource).add(TopicAcl);
                    } else {
                        aclSet.add(TopicAcl);
                        allAcls.put(resource, aclSet);
                    }
                }
            }
        } catch (SQLException ex) {
            CONNECTIONLOGGGER.log(Level.SEVERE, null, ex.toString());
        }

        //change the acl from java.util.Set to scala.collection.immutable.Set
        java.util.Map<Resource, Set<Acl>> principalAcls = new HashMap<>();
        for (java.util.Map.Entry<Resource, java.util.Set<Acl>> entry : allAcls.entrySet()) {
            Resource resource = entry.getKey();
            java.util.Set<Acl> value = entry.getValue();

            Set<Acl> immutableAcl = JavaConverters$.MODULE$.asScalaSetConverter(value).asScala().toSet();
            principalAcls.put(resource, immutableAcl);
        }

        //change the principalAcls from java.util.Map to scala.collection.immutable.Map
        scala.collection.immutable.Map<Resource, Set<Acl>> immutablePrincipalAcls
                = JavaConverters$.MODULE$.mapAsScalaMapConverter(principalAcls)
                .asScala().toMap(scala.Predef$.MODULE$
                        .<scala.Tuple2<Resource, Set<Acl>>>conforms());

        return immutablePrincipalAcls;
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
      
        connObject = null;
    }

    //load the acls from database into aclCache
    private void loadAllAcls() {

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
                principal = KafkaPrincipal.fromString("User:" + resultSet.getString("user_id"));
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
