package io.hops.kafka;

import io.hops.kafka.authorizer.tables.HopsAcl;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Periodically updated map of ACLs and user project roles.
 * <p>
 */
public class Acls implements Runnable {
  //<TopicName,<Principal,HopsAcl>>
  Map<String, Map<String, HopsAcl>> acls = new HashMap<>();
  //<Principal, project_role>
  Map<String, String> principalRole = new HashMap<>();
  
  

  @Override
  public void run() {
    while(true){
      try {
        
        
        
        Thread.sleep(1000);
      } catch (InterruptedException ex) {
        Logger.getLogger(Acls.class.getName()).log(Level.SEVERE, null, ex);
      }
    }
  }
  
  
  
}
