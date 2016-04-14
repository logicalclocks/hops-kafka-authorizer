/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.hops.kafka;

import kafka.security.auth.Acl; 

/**
 *
 * @author misdess
 */
public class AclRole {
    String role;
    Acl acl;

    public AclRole(String role, Acl acl) {
        this.role = role;
        this.acl = acl;
    }

    public Acl getAcl() {
        return acl;
    }

    public void setAcl(Acl acl) {
        this.acl = acl;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getRole() {
        return role;
    }
    
}
