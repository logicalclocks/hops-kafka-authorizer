package io.hops.kafka;

/**
 *
 * @author misdess
 */
public class HopsAcl {

  String principal;
  String permissionType;
  String operationType;
  String host;
  String role;

  public HopsAcl() {
  }

  public HopsAcl(String principal, String permissionType, String operationType,
      String host, String role) {
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

  @Override
  public String toString() {
    return "HopsAcl{" + "principal=" + principal + ", permissionType=" + permissionType + ", operationType="
        + operationType + ", host=" + host + ", role=" + role + '}';
  }

}
