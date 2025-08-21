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
package org.apache.pinot.common.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.testng.annotations.Test;

import static org.assertj.core.api.Assertions.assertThat;


/**
 * Unit tests for DefaultClusterConfigChangeHandler focusing on the getChangedProperties method.
 */
public class DefaultClusterConfigChangeHandlerTest {

  @Test
  public void testGetChangedPropertiesWithBothMapsEmpty() {
    // Given
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    Map<String, String> oldProperties = new HashMap<>();
    Map<String, String> newProperties = new HashMap<>();

    // When
    Set<String> changedProperties = handler.getChangedProperties(oldProperties, newProperties);

    // Then
    assertThat(changedProperties).isEmpty();
  }

  @Test
  public void testGetChangedPropertiesWithOldMapEmpty() {
    // Given
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    Map<String, String> oldProperties = new HashMap<>();
    Map<String, String> newProperties = new HashMap<>();
    newProperties.put("key1", "value1");
    newProperties.put("key2", "value2");

    // When
    Set<String> changedProperties = handler.getChangedProperties(oldProperties, newProperties);

    // Then
    assertThat(changedProperties).containsExactlyInAnyOrder("key1", "key2");
  }

  @Test
  public void testGetChangedPropertiesWithNewMapEmpty() {
    // Given
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    Map<String, String> oldProperties = new HashMap<>();
    oldProperties.put("key1", "value1");
    oldProperties.put("key2", "value2");
    Map<String, String> newProperties = new HashMap<>();

    // When
    Set<String> changedProperties = handler.getChangedProperties(oldProperties, newProperties);

    // Then
    assertThat(changedProperties).containsExactlyInAnyOrder("key1", "key2");
  }

  @Test
  public void testGetChangedPropertiesWithAddedKeys() {
    // Given
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    Map<String, String> oldProperties = new HashMap<>();
    oldProperties.put("key1", "value1");
    Map<String, String> newProperties = new HashMap<>();
    newProperties.put("key1", "value1");
    newProperties.put("key2", "value2");
    newProperties.put("key3", "value3");

    // When
    Set<String> changedProperties = handler.getChangedProperties(oldProperties, newProperties);

    // Then
    assertThat(changedProperties).containsExactlyInAnyOrder("key2", "key3");
  }

  @Test
  public void testGetChangedPropertiesWithDeletedKeys() {
    // Given
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    Map<String, String> oldProperties = new HashMap<>();
    oldProperties.put("key1", "value1");
    oldProperties.put("key2", "value2");
    oldProperties.put("key3", "value3");
    Map<String, String> newProperties = new HashMap<>();
    newProperties.put("key1", "value1");

    // When
    Set<String> changedProperties = handler.getChangedProperties(oldProperties, newProperties);

    // Then
    assertThat(changedProperties).containsExactlyInAnyOrder("key2", "key3");
  }

  @Test
  public void testGetChangedPropertiesWithUpdatedValues() {
    // Given
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    Map<String, String> oldProperties = new HashMap<>();
    oldProperties.put("key1", "value1");
    oldProperties.put("key2", "value2");
    Map<String, String> newProperties = new HashMap<>();
    newProperties.put("key1", "newValue1");
    newProperties.put("key2", "value2");

    // When
    Set<String> changedProperties = handler.getChangedProperties(oldProperties, newProperties);

    // Then
    assertThat(changedProperties).containsExactlyInAnyOrder("key1");
  }

  @Test
  public void testGetChangedPropertiesWithMixedChanges() {
    // Given
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    Map<String, String> oldProperties = new HashMap<>();
    oldProperties.put("key1", "value1");
    oldProperties.put("key2", "value2");
    oldProperties.put("key3", "value3");
    Map<String, String> newProperties = new HashMap<>();
    newProperties.put("key1", "value1");
    newProperties.put("key2", "newValue2");
    newProperties.put("key4", "value4");

    // When
    Set<String> changedProperties = handler.getChangedProperties(oldProperties, newProperties);

    // Then
    assertThat(changedProperties).containsExactlyInAnyOrder("key2", "key3", "key4");
  }

  @Test
  public void testGetChangedPropertiesWithNullOldMap() {
    // Given
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    Map<String, String> newProperties = new HashMap<>();
    newProperties.put("key1", "value1");

    // When
    Set<String> changedProperties = handler.getChangedProperties(null, newProperties);

    // Then
    assertThat(changedProperties).containsExactlyInAnyOrder("key1");
  }

  @Test
  public void testGetChangedPropertiesWithNullNewMap() {
    // Given
    DefaultClusterConfigChangeHandler handler = new DefaultClusterConfigChangeHandler();
    Map<String, String> oldProperties = new HashMap<>();
    oldProperties.put("key1", "value1");

    // When
    Set<String> changedProperties = handler.getChangedProperties(oldProperties, null);

    // Then
    assertThat(changedProperties).containsExactlyInAnyOrder("key1");
  }
}
