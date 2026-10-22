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
package org.apache.pinot.common.coordination;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;


/// Bootstrap validation must reject incomplete native settings rather than attempt legacy coordination.
public class NativeMetadataConfigTest {
  @Test
  public void testBackendSelectionAndNativeConnectionValidation() throws Exception {
    PinotConfiguration config = new PinotConfiguration();
    assertEquals(CoordinationBackend.from(config), CoordinationBackend.HELIX);
    config.setProperty(CoordinationBackend.CONFIG_KEY, " RATIS ");
    assertEquals(CoordinationBackend.from(config), CoordinationBackend.RATIS);
    assertThrows(IllegalArgumentException.class, () -> new NativeMetadataConfig(config));
    config.setProperty(CoordinationBackend.CONFIG_KEY, "RATSI");
    assertThrows(IllegalArgumentException.class, () -> CoordinationBackend.from(config));

    Path pem = Files.createTempFile("native-config", ".pem");
    try {
      // Config parsing checks readable paths; the connection validates PEM contents before opening channels.
      PinotConfiguration nativeConfig = new PinotConfiguration(Map.of(
          NativeMetadataConfig.PREFIX + "historyId", UUID.randomUUID().toString(),
          NativeMetadataConfig.PREFIX + "participantId", "Broker_test",
          NativeMetadataConfig.PREFIX + "metadata.endpoints", "localhost:8000,[::1]:8001",
          NativeMetadataConfig.PREFIX + "tls.certChain", pem.toString(),
          NativeMetadataConfig.PREFIX + "tls.privateKey", pem.toString(),
          NativeMetadataConfig.PREFIX + "tls.trustCerts", pem.toString()));
      NativeMetadataConfig parsed = new NativeMetadataConfig(nativeConfig);
      assertEquals(parsed.endpoints().size(), 2);
      assertEquals(parsed.endpoints().get(1).host(), "::1");
      assertEquals(parsed.endpoints().get(1).port(), 8001);
      assertThrows(UnsupportedOperationException.class, () -> parsed.endpoints().clear());
      PinotConfiguration invalid = nativeConfig;
      invalid.setProperty(NativeMetadataConfig.PREFIX + "metadata.endpoints", "localhost:8000,localhost:8000");
      assertThrows(IllegalArgumentException.class, () -> new NativeMetadataConfig(invalid));
      invalid.setProperty(NativeMetadataConfig.PREFIX + "metadata.endpoints", "localhost:8000");
      invalid.setProperty(NativeMetadataConfig.PREFIX + "rpc.timeoutMs", 30001);
      assertThrows(IllegalArgumentException.class, () -> new NativeMetadataConfig(invalid));
    } finally {
      Files.deleteIfExists(pem);
    }
  }
}
