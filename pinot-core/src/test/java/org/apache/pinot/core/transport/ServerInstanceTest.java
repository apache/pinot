/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.transport;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


public class ServerInstanceTest {
  @Test
  public void equalsVerifier() {
    EqualsVerifier.configure().forClass(ServerInstance.class).withOnlyTheseFields("_instanceId")
        .withNonnullFields("_instanceId").verify();
  }

  @Test
  public void testExtractHostnameFromConfigWithServerPrefix() {
    InstanceConfig config = new InstanceConfig(new ZNRecord("Server_myhost_1234"));
    config.setHostName("Server_myhost");
    assertEquals(ServerInstance.extractHostnameFromConfig(config), "myhost");
  }

  @Test
  public void testExtractHostnameFromConfigWithoutPrefix() {
    InstanceConfig config = new InstanceConfig(new ZNRecord("Server_myhost_1234"));
    config.setHostName("myhost");
    assertEquals(ServerInstance.extractHostnameFromConfig(config), "myhost");
  }

  @Test
  public void testExtractHostnameFromConfigFallbackToInstanceName() {
    // When hostname is null, falls back to parsing instance name
    InstanceConfig config = new InstanceConfig(new ZNRecord("Server_myhost_1234"));
    assertEquals(ServerInstance.extractHostnameFromConfig(config), "myhost");
  }

  @Test
  public void testExtractHostnameFromConfigEmptyInstanceNameThrows() {
    InstanceConfig config = new InstanceConfig(new ZNRecord(""));
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> ServerInstance.extractHostnameFromConfig(config));
  }

  @Test
  public void testExtractPortFromConfigWithPort() {
    InstanceConfig config = new InstanceConfig(new ZNRecord("Server_myhost_1234"));
    config.setHostName("myhost");
    config.setPort("1234");
    assertEquals(ServerInstance.extractPortFromConfig(config), 1234);
  }

  @Test
  public void testExtractPortFromConfigFallbackToInstanceName() {
    // When port is null, falls back to parsing instance name
    InstanceConfig config = new InstanceConfig(new ZNRecord("Server_myhost_1234"));
    assertEquals(ServerInstance.extractPortFromConfig(config), 1234);
  }

  @Test
  public void testExtractPortFromConfigNoPortThrows() {
    InstanceConfig config = new InstanceConfig(new ZNRecord("Server_myhost"));
    assertThrows(ArrayIndexOutOfBoundsException.class, () -> ServerInstance.extractPortFromConfig(config));
  }
}
