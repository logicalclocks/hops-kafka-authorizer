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
package io.hops.kafka;

import java.io.IOException;
import java.security.Principal;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.security.auth.AuthenticationContext;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SslAuthenticationContext;
import org.apache.kafka.common.security.authenticator.DefaultKafkaPrincipalBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.security.x509.X500Name;

/**
 * Customized Principal Builder for Hopsworks.
 * <p>
 */
public class HopsPrincipalBuilder extends DefaultKafkaPrincipalBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger("kafka.authorizer.logger");

  public HopsPrincipalBuilder() {
    super(null, null);
  }

  /*
   * By default, the TLS username will be of the form
   * "CN=host1.example.com,OU=,O=Confluent,L=London,ST=London,C=GB".
   * The order of the fields of the X500 name IS NOT GUARANTEED
   * This builder class extracts host1.example.com.
   * In our case it will be, principalType:projectName__userName.
   */
  @Override
  public KafkaPrincipal build(AuthenticationContext authenticationContext) {
    try {
      SslAuthenticationContext sslAuthenticationContext = (SslAuthenticationContext) authenticationContext;

      Principal principal = getPrincipal(sslAuthenticationContext);

      String tlsUserName = principal.getName();
      if (tlsUserName.equalsIgnoreCase(Consts.ANONYMOUS)) {
        return KafkaPrincipal.ANONYMOUS;
      }

      String userType = principal.toString().split(Consts.COLON_SEPARATOR)[0];
      X500Name name = new X500Name(tlsUserName);
      String principleName = name.getCommonName();

      // add alternative names to principal name
      List<String> alternativeNameList = getAlternativeNames(sslAuthenticationContext);
      for (String alternativeName: alternativeNameList) {
        if (!principleName.equals(alternativeName)) {
          principleName = principleName.concat(Consts.SEMI_COLON + alternativeName);
        }
      }

      return new KafkaPrincipal(userType, principleName);
    } catch (IOException e) {
      throw new KafkaException("Failed to build Kafka principal due to: ", e);
    }
  }

  protected Principal getPrincipal(SslAuthenticationContext sslAuthenticationContext) throws IOException {
    return sslAuthenticationContext.session().getPeerPrincipal();
  }

  private List<String> getAlternativeNames(SslAuthenticationContext sslAuthenticationContext) {
    List<String> alternativeNameList = new ArrayList<>();
    try {
      for (Certificate certificate: sslAuthenticationContext.session().getPeerCertificates()) {
        if (certificate instanceof X509Certificate) {
          X509Certificate x509Certificate = (X509Certificate) certificate;
          List<String> altNames = getSubjectAlternativeNames(x509Certificate);
          alternativeNameList.addAll(altNames);
        }
      }
    } catch (Exception e) {
      LOGGER.error("Failed to get subject alternative names", e);
    }
    return alternativeNameList;
  }

  private List<String> getSubjectAlternativeNames(X509Certificate cert) throws CertificateParsingException {
    Collection<List<?>> altNames = cert.getSubjectAlternativeNames();
    if (altNames == null) {
      return Collections.emptyList();
    }
    return altNames.stream()
        .map(entry -> entry.get(1).toString())
        .collect(Collectors.toList());
  }
}
