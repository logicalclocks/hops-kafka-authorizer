/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package hops.io.kafka;

import com.sun.security.auth.UserPrincipal;
import java.util.Map;
import java.security.Principal;

import org.apache.kafka.common.network.TransportLayer;
import org.apache.kafka.common.network.Authenticator;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.PrincipalBuilder;

/**
 *
 * @author misdess
 */
public class KafkaPrincipalBuilder implements PrincipalBuilder {

    @Override
    public void configure(Map<String, ?> configs) {

    }

    /*
    *By default, the TLS user name will be of the form 
    * "CN=host1.example.com,OU=,O=Confluent,L=London,ST=London,C=GB".
    *This builder class extracts host1.example.com as a userName, 
    *gets the principalType and returns a KafkaPrincipal
    */
    @Override
    public Principal buildPrincipal(TransportLayer transportLayer, Authenticator authenticator) throws KafkaException {
        try {

            Principal principal = transportLayer.peerPrincipal();
            String[] TLSUserName = principal.getName().split(",", 6);
            String projetcName__userName = TLSUserName[0].split("=", 2)[1];
            String userName = projetcName__userName.split("__", 2)[1];
            String userType = principal.toString().split(":", 2)[0];
            
            KafkaPrincipal userPrincipal = new KafkaPrincipal(userType, userName);
            return userPrincipal;
        } catch (Exception e) {
            throw new KafkaException("Failed to build principal due to: ", e);
        }
    }

    @Override
    public void close() throws KafkaException {
    }
}
