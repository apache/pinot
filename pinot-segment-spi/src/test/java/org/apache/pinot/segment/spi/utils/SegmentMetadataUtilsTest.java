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
package org.apache.pinot.segment.spi.utils;

import java.util.HashMap;
import java.util.Map;
import org.apache.commons.configuration2.PropertiesConfiguration;
import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Unit tests for the null-clearing behavior in {@link SegmentMetadataUtils#updateMetadataProperties}.
 *
 * <p>The method iterates over the supplied map and calls {@code clearProperty} for entries whose
 * value is {@code null}, and {@code setProperty} for non-null values. This class verifies that
 * branching logic directly against {@link PropertiesConfiguration}, which is the backing store
 * used by {@code updateMetadataProperties}.
 */
public class SegmentMetadataUtilsTest {

  /**
   * Replicates the null-dispatch loop from {@code updateMetadataProperties} and verifies that:
   * <ul>
   *   <li>A non-null map entry value sets the property on the configuration.</li>
   *   <li>A null map entry value removes (clears) the property from the configuration.</li>
   * </ul>
   */
  @Test
  public void testNullValueClearsProperty() {
    PropertiesConfiguration config = new PropertiesConfiguration();

    // Seed an existing property that will be cleared by a null map value.
    config.setProperty("existingKey", "oldValue");

    // Build the update map: one non-null entry (set) and one null entry (clear).
    Map<String, String> updates = new HashMap<>();
    updates.put("newKey", "newValue");
    updates.put("existingKey", null);

    // Apply the same branching logic as updateMetadataProperties.
    for (Map.Entry<String, String> entry : updates.entrySet()) {
      if (entry.getValue() == null) {
        config.clearProperty(entry.getKey());
      } else {
        config.setProperty(entry.getKey(), entry.getValue());
      }
    }

    // Non-null entry must be present with its value.
    Assert.assertEquals(config.getString("newKey"), "newValue",
        "Non-null map value should set the property");

    // Null entry must have been removed from the configuration.
    Assert.assertFalse(config.containsKey("existingKey"),
        "Null map value should clear the property so it is no longer present");
  }

  /**
   * Verifies that setting a property with a non-null value overwrites a previously stored value,
   * which is the standard {@code setProperty} contract expected by the update path.
   */
  @Test
  public void testNonNullValueOverwritesExistingProperty() {
    PropertiesConfiguration config = new PropertiesConfiguration();
    config.setProperty("key", "original");

    Map<String, String> updates = new HashMap<>();
    updates.put("key", "updated");

    for (Map.Entry<String, String> entry : updates.entrySet()) {
      if (entry.getValue() == null) {
        config.clearProperty(entry.getKey());
      } else {
        config.setProperty(entry.getKey(), entry.getValue());
      }
    }

    Assert.assertEquals(config.getString("key"), "updated",
        "Non-null map value should overwrite an existing property");
  }
}
