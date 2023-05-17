package io.hops.kafka;

/**
 * Constants used by HopsWorks Kafka Authorizer.
 * <p>
 */
public final class Consts {

  public static final String COLON_SEPARATOR = ":";
  public static final String SEMI_COLON = ";";
  public static final String PROJECT_USER_DELIMITER = "__";

  public static final String ANONYMOUS = "ANONYMOUS";

  //User roles
  public static final String DATA_OWNER = "Data owner";
  public static final String DATA_SCIENTIST = "Data scientist";

  //Project permissions
  public static final String EDITABLE = "EDITABLE";
  public static final String READ_ONLY = "READ_ONLY";
  public static final String EDITABLE_BY_OWNERS = "EDITABLE_BY_OWNERS";

  //Properties attributes
  public static final String SUPERUSERS_PROP = "super.users";

  //Database property names
  public static final String DATABASE_URL = "database.url";
  public static final String DATABASE_USERNAME = "database.username";
  public static final String DATABASE_PASSWORD = "database.password";
  public static final String DATABASE_CACHE_PREPSTMTS = "database.pool.prepstmt.cache.enabled";
  public static final String DATABASE_PREPSTMT_CACHE_SIZE = "database.pool.prepstmt.cache.size";
  public static final String DATABASE_PREPSTMT_CACHE_SQL_LIMIT = "database.pool.prepstmt.cache.sql.limit";
  public static final String DATABASE_MAX_POOL_SIZE = "database.pool.size";
  public static final String DATABASE_ACL_POLLING_FREQUENCY_MS = "acl.polling.frequency.ms";
  public static final String CONSUMER_OFFSETS_ACCESS_ALLOWED = "consumer_offsets.access_allowed";
  public static final String CACHE_MAX_SIZE = "cache.max_size";
  
}
