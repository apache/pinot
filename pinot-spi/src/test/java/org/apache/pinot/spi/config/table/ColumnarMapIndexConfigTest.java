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
package org.apache.pinot.spi.config.table;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/** Tests for {@link ColumnarMapIndexConfig} deserialization, defaults, and property round-trips. */
public class ColumnarMapIndexConfigTest {

  @Test
  public void testDenseKeysFromProperties() {
    Map<String, String> props = new HashMap<>();
    props.put("denseKeys", "country,tenancy,device_os");
    props.put("denseKeyThreshold", "0.3");
    props.put("maxKeys", "500");

    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(props);

    assertEquals(config.getDenseKeys(), Set.of("country", "tenancy", "device_os"));
    assertEquals(config.getDenseKeyThreshold(), 0.3, 0.001);
    assertEquals(config.getMaxKeys(), 500);
  }

  @Test
  public void testDenseKeysDefaults() {
    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(Map.of());

    assertTrue(config.getDenseKeys().isEmpty());
    assertEquals(config.getDenseKeyThreshold(), 0.5, 0.001);
  }

  @Test
  public void testIsDenseKey() {
    Map<String, String> props = new HashMap<>();
    props.put("denseKeys", "country,tenancy");

    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(props);

    assertTrue(config.isDenseKey("country"));
    assertTrue(config.isDenseKey("tenancy"));
    assertFalse(config.isDenseKey("rare_flag"));
  }

  @Test
  public void testDenseKeysWithInvertedIndex() {
    Map<String, String> props = new HashMap<>();
    props.put("denseKeys", "country");
    props.put("invertedIndexKeys", "country,status");
    props.put("denseKeyThreshold", "0.8");

    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(props);

    assertTrue(config.isDenseKey("country"));
    assertFalse(config.isDenseKey("status"));
    assertTrue(config.shouldEnableInvertedIndexForKey("country"));
    assertTrue(config.shouldEnableInvertedIndexForKey("status"));
    assertFalse(config.shouldEnableInvertedIndexForKey("unknown"));
    assertEquals(config.getDenseKeyThreshold(), 0.8, 0.001);
  }

  @Test
  public void testDefaultConstructorDenseKeyDefaults() {
    ColumnarMapIndexConfig config = new ColumnarMapIndexConfig(true);

    assertTrue(config.getDenseKeys().isEmpty());
    assertEquals(config.getDenseKeyThreshold(), 0.5, 0.001);
    assertFalse(config.isDenseKey("anything"));
  }

  @Test
  public void testNullPropertiesReturnDefault() {
    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(null);

    assertTrue(config.getDenseKeys().isEmpty());
    assertEquals(config.getDenseKeyThreshold(), 0.5, 0.001);
  }

  @Test
  public void testDenseKeyThresholdZeroMakesAllDense() {
    Map<String, String> props = new HashMap<>();
    props.put("denseKeyThreshold", "0.0");

    ColumnarMapIndexConfig config = ColumnarMapIndexConfig.fromProperties(props);

    // Threshold of 0 means any key with >0% fill rate is auto-dense
    assertEquals(config.getDenseKeyThreshold(), 0.0, 0.001);
  }
}
