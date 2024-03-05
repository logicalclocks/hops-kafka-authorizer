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

import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import javax.security.auth.x500.X500Principal;
import java.security.Principal;
import java.util.Arrays;
import java.util.List;

public class TestHopsPrincipalBuilder {

  @Test
  public void testAnonymousX500Principal() throws Exception {
    // Arrange
    HopsPrincipalBuilder realPB = new HopsPrincipalBuilder();
    HopsPrincipalBuilder pb = Mockito.spy(realPB);
    Principal originPrincipal = new KafkaPrincipal("User", Consts.ANONYMOUS);

    Mockito.doReturn(originPrincipal).when(pb).getPrincipal(Mockito.any());

    // Act
    Principal p = pb.build(null);

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
    Principal p = pb.build(null);

    // Assert
    Assertions.assertNotEquals(originPrincipal, p);
    Assertions.assertTrue(p instanceof KafkaPrincipal);
    Assertions.assertEquals("my_common_name", p.getName());

    originPrincipal = new X500Principal("CN=another_common_name, OU=0,C=SE,O=organization");
    Mockito.doReturn(originPrincipal).when(pb).getPrincipal(Mockito.any());
    p = pb.build(null);
    Assertions.assertEquals("another_common_name", p.getName());
  }

  @Test
  public void testX500PrincipleWithAlternativeNames() throws Exception {
    // Arrange
    HopsPrincipalBuilder realPB = new HopsPrincipalBuilder();
    HopsPrincipalBuilder pb = Mockito.spy(realPB);
    Principal originPrincipal = new X500Principal("OU=0,C=SE,O=organization,CN=my_common_name");
    Mockito.doReturn(originPrincipal).when(pb).getPrincipal(Mockito.any());

    List<String> alternativeNameList = Arrays.asList("another_common_name", "random_common_name");
    Mockito.doReturn(alternativeNameList).when(pb).getAlternativeNames(Mockito.any());

    // Arrange
    Principal p = pb.build(null);

    // Assert
    Assertions.assertNotEquals(originPrincipal, p);
    Assertions.assertTrue(p instanceof KafkaPrincipal);
    Assertions.assertEquals("my_common_name;another_common_name;random_common_name", p.getName());
  }
}
