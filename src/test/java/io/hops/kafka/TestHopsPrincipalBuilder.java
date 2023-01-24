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

import com.sun.net.httpserver.HttpPrincipal;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.security.auth.x500.X500Principal;
import java.security.Principal;

public class TestHopsPrincipalBuilder {

  @Test
  public void testNonX500Principal() throws Exception {
    // Arrange
    HopsPrincipalBuilder realPB = new HopsPrincipalBuilder();
    HopsPrincipalBuilder pb = Mockito.spy(realPB);
    Principal originPrincipal = new HttpPrincipal("kafka", "name");

    Mockito.doReturn(originPrincipal).when(pb).getPrincipal(Mockito.any());

    // Act
    Principal p = pb.buildPrincipal(null, null);

    // Assert
    Assertions.assertEquals(originPrincipal, p);
  }

  @Test
  public void testAnonymousX500Principal() throws Exception {
    // Arrange
    HopsPrincipalBuilder realPB = new HopsPrincipalBuilder();
    HopsPrincipalBuilder pb = Mockito.spy(realPB);
    Principal originPrincipal = new KafkaPrincipal("kafka", Consts.ANONYMOUS);

    Mockito.doReturn(originPrincipal).when(pb).getPrincipal(Mockito.any());

    // Act
    Principal p = pb.buildPrincipal(null, null);

    // Assert
    Assertions.assertEquals(originPrincipal, p);
  }

  @Test
  public void testX500Principle() throws Exception {
    // Arrange
    HopsPrincipalBuilder realPB = new HopsPrincipalBuilder();
    HopsPrincipalBuilder pb = Mockito.spy(realPB);
    Principal originPrincipal = new X500Principal("OU=0,C=SE,O=organization,CN=my_common_name");
    Mockito.doReturn(originPrincipal).when(pb).getPrincipal(Mockito.any());

    // Arrange
    Principal p = pb.buildPrincipal(null, null);

    // Assert
    Assertions.assertNotEquals(originPrincipal, p);
    Assertions.assertTrue(p instanceof KafkaPrincipal);
    Assertions.assertEquals("my_common_name", p.getName());

    originPrincipal = new X500Principal("CN=another_common_name, OU=0,C=SE,O=organization");
    Mockito.doReturn(originPrincipal).when(pb).getPrincipal(Mockito.any());
    p = pb.buildPrincipal(null, null);
    Assertions.assertEquals("another_common_name", p.getName());
  }
}
