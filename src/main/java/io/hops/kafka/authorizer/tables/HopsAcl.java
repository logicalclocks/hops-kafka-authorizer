package io.hops.kafka.authorizer.tables;

/**
 *
 * Topic ACL View in Database.
 */
public class HopsAcl {

  int Id;
  String topicName;
  int projectId;
  String principal;
  String permissionType;
  String operationType;
  String host;
  String role;
  String projectRole;

  public HopsAcl() {
  }

  public HopsAcl(String principal, String permissionType, String operationType,
      String host, String role, String projectRole) {
    this.principal = principal;
    this.permissionType = permissionType;
    this.operationType = operationType;
    this.host = host;
    this.role = role;
    this.projectRole = projectRole;
  }

  public HopsAcl(int Id, String topicName, int projectId, String principal, String permissionType, String operationType,
      String host, String role) {
    this.Id = Id;
    this.topicName = topicName;
    this.projectId = projectId;
    this.principal = principal;
    this.permissionType = permissionType;
    this.operationType = operationType;
    this.host = host;
    this.role = role;
  }

  public String getPrincipal() {
    return principal;
  }

  public String getPermissionType() {
    return permissionType;
  }

  public String getOperationType() {
    return operationType;
  }

  public String getHost() {
    return host;
  }

  public String getRole() {
    return role;
  }

  public void setPrincipal(String userName) {
    this.principal = userName;
  }

  public void setPermissionType(String permissionType) {
    this.permissionType = permissionType;
  }

  public void setOperationType(String operationType) {
    this.operationType = operationType;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public void setRole(String role) {
    this.role = role;
  }

  public int getId() {
    return Id;
  }

  public void setId(int Id) {
    this.Id = Id;
  }

  public String getTopicName() {
    return topicName;
  }

  public void setTopicName(String topicName) {
    this.topicName = topicName;
  }

  public int getProjectId() {
    return projectId;
  }

  public void setProjectId(int projectId) {
    this.projectId = projectId;
  }

  public String getProjectRole() {
    return projectRole;
  }

  public void setProjectRole(String projectRole) {
    this.projectRole = projectRole;
  }

  @Override
  public String toString() {
    return "HopsAcl{" + "permissionType=" + permissionType + ", operationType=" + operationType + ", host=" 
        + host + ", role=" + role + '}';
  }

}
