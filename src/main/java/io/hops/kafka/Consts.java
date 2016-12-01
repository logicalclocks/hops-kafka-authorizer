package io.hops.kafka;

/**
 * Constants used by HopsWorks Kafka Authorizer.
 * <p>
 */
public interface Consts {

  public static final String COLON_SEPARATOR = ":";
  public static final String COMMA_SEPARATOR = ",";
  public static final String ASSIGN_SEPARATOR = "=";
  public static final String SEMI_COLON = ";";
  public final String TWO_UNDERSCORES = "__";

  public static final String ANONYMOUS = "ANONYMOUS";

  //Operations
  public static final String ALLOW = "Allow";
  public static final String DENY = "Deny";
  public static final String READ = "Read";
  public static final String WRITE = "Write";
  public static final String DESCRIBE = "Describe";
  public static final String ALL = "All";

  public static final String PRINCIPAL = "principal";
  public static final String PERMISSION_TYPE = "permission_type";
  public static final String OPERATION_TYPE = "operation_type";
  public static final String HOST = "host";
  public static final String ROLE = "role";

  public static final String CLUSTER = "Cluster";
  public static final String WILDCARD = "*";

  //Properties attributes
  public static final String SUPERUSERS_PROP = "super.users";
  public static final String ALLOW_EVERYONE_IF_NO_ACS_FOUND_PROP
          = "allow.everyone.if.no.acl.found";

  //Database property names
  public final String DATABASE_TYPE = "database.type";
  public final String DATABASE_URL = "database.url";
  public final String DATABASE_USERNAME = "database.username";
  public final String DATABASE_PASSWORD = "database.password";

}
