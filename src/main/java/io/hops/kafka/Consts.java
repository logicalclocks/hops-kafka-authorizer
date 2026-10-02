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
  // How long a caller waits for a pooled connection before giving up. Kept short on purpose:
  // this wait happens on a Kafka request-handler thread, so while the database is unreachable
  // every authorization that misses the cache occupies one of the broker's (default 8)
  // handlers for the whole timeout. HikariCP's own default is 30 s, which is a sensible
  // number for a web request and far too long here - four producers were enough to occupy
  // every handler and take an unrelated superuser `kafka-topics --list` from 1.5 s to 22 s.
  public static final String DATABASE_CONNECTION_TIMEOUT_MS = "database.pool.connection.timeout.ms";
  public static final long DATABASE_CONNECTION_TIMEOUT_MS_DEFAULT = 3000L;
  // HikariCP's own floor. Anything lower makes it throw, and 0 is worse than low: it maps to
  // Integer.MAX_VALUE, so getConnection() would block a request-handler thread for ~24.8 days.
  // DbConnection.resolveConnectionTimeoutMs falls back to the default rather than either.
  public static final long DATABASE_CONNECTION_TIMEOUT_MS_MIN = 250L;
  public static final String DATABASE_ACL_POLLING_FREQUENCY_MS = "acl.polling.frequency.ms";
  public static final String CONSUMER_OFFSETS_ACCESS_ALLOWED = "consumer_offsets.access_allowed";
  public static final String CACHE_MAX_SIZE = "cache.max_size";
  
}
