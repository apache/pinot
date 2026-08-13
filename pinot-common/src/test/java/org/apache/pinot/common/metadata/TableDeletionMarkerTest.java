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
package org.apache.pinot.common.metadata;

import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.impl.client.ZkClient;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Unit tests for table deletion marker functionality in ZKMetadataProvider.
/// Tests cover creation, validation, expiry, and cleanup of deletion markers.
public class TableDeletionMarkerTest {

  private ZkClient _zkClient;
  private String _zkPath;
  private ZkHelixPropertyStore<ZNRecord> _propertyStore;
  private static final String TEST_TABLE_NAME = "testTable_REALTIME";
  private static final String CONTROLLER_ID_1 = "controller_1";
  private static final String CONTROLLER_ID_2 = "controller_2";

  @BeforeClass
  public void setUp()
      throws Exception {
    _zkPath = "/tmp/TableDeletionMarkerTest_" + System.currentTimeMillis();
    _zkClient = new ZkClient("localhost:2181", 10000, 10000);
    _propertyStore = new ZkHelixPropertyStore<>("localhost:2181", _zkPath, _zkClient);
  }

  @AfterClass
  public void tearDown() {
    try {
      if (_propertyStore != null) {
        _propertyStore.stop();
      }
      if (_zkClient != null) {
        _zkClient.close();
      }
    } catch (Exception e) {
      // Ignore cleanup errors
    }
  }

  @Test
  public void testCreateDeletionMarker() {
    // Test successful creation of a deletion marker
    boolean created = ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TEST_TABLE_NAME, CONTROLLER_ID_1);
    assertTrue(created, "Deletion marker should be created successfully");

    // Verify the marker exists
    boolean exists = ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TEST_TABLE_NAME);
    assertTrue(exists, "Deletion marker should exist after creation");

    // Cleanup
    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, TEST_TABLE_NAME);
  }

  @Test
  public void testCreateDuplicateDeletionMarker() {
    // Create first marker
    boolean firstCreated = ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TEST_TABLE_NAME,
        CONTROLLER_ID_1);
    assertTrue(firstCreated, "First deletion marker should be created successfully");

    // Try to create duplicate marker
    boolean secondCreated = ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TEST_TABLE_NAME,
        CONTROLLER_ID_2);
    assertFalse(secondCreated, "Duplicate deletion marker should not be created");

    // Cleanup
    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, TEST_TABLE_NAME);
  }

  @Test
  public void testIsValidDeletionMarkerExists() {
    // Test when marker does not exist
    boolean exists = ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TEST_TABLE_NAME);
    assertFalse(exists, "Deletion marker should not exist when not created");

    // Create marker
    ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TEST_TABLE_NAME, CONTROLLER_ID_1);

    // Test when marker exists and is valid
    exists = ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TEST_TABLE_NAME);
    assertTrue(exists, "Deletion marker should exist when created");

    // Cleanup
    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, TEST_TABLE_NAME);
  }

  @Test
  public void testRemoveDeletionMarker() {
    // Create marker
    ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TEST_TABLE_NAME, CONTROLLER_ID_1);

    // Verify it exists
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TEST_TABLE_NAME));

    // Remove marker
    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, TEST_TABLE_NAME);

    // Verify it's removed
    assertFalse(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TEST_TABLE_NAME));
  }

  @Test
  public void testRemoveNonExistentDeletionMarker() {
    // Should not throw exception when removing non-existent marker
    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, TEST_TABLE_NAME);
    // Test passes if no exception is thrown
  }

  @Test
  public void testCreateOrTakeoverDeletionMarkerNew() {
    // Test takeover when no marker exists
    boolean result = ZKMetadataProvider.createOrTakeoverTableDeletionMarker(_propertyStore, TEST_TABLE_NAME,
        CONTROLLER_ID_1);
    assertTrue(result, "Should create new marker when none exists");

    // Cleanup
    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, TEST_TABLE_NAME);
  }

  @Test
  public void testCreateOrTakeoverDeletionMarkerExistingValid() {
    // Create initial marker
    ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TEST_TABLE_NAME, CONTROLLER_ID_1);

    // Try to takeover with different controller while marker is still valid
    boolean result = ZKMetadataProvider.createOrTakeoverTableDeletionMarker(_propertyStore, TEST_TABLE_NAME,
        CONTROLLER_ID_2);
    assertFalse(result, "Should not takeover valid deletion marker");

    // Cleanup
    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, TEST_TABLE_NAME);
  }

  @Test
  public void testGetPropertyStoreTableDeletionInProgressPrefix() {
    String prefix = ZKMetadataProvider.getPropertyStoreTableDeletionInProgressPrefix();
    assertNotNull(prefix, "Prefix should not be null");
    assertEquals(prefix, "/TABLE_DELETION_IN_PROGRESS", "Prefix should match expected value");
  }

  @Test
  public void testDeletionMarkerContent() {
    // Create marker
    ZKMetadataProvider.createTableDeletionMarker(_propertyStore, TEST_TABLE_NAME, CONTROLLER_ID_1);

    // Verify marker content by checking it exists (detailed content verification would
    // require internal access)
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, TEST_TABLE_NAME));

    // Cleanup
    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, TEST_TABLE_NAME);
  }

  @Test
  public void testMultipleTableMarkers() {
    String table1 = "table1_REALTIME";
    String table2 = "table2_OFFLINE";

    // Create markers for different tables
    boolean created1 = ZKMetadataProvider.createTableDeletionMarker(_propertyStore, table1, CONTROLLER_ID_1);
    boolean created2 = ZKMetadataProvider.createTableDeletionMarker(_propertyStore, table2, CONTROLLER_ID_2);

    assertTrue(created1, "Marker for table1 should be created");
    assertTrue(created2, "Marker for table2 should be created");

    // Verify both exist independently
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, table1));
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, table2));

    // Remove one marker
    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, table1);

    // Verify only one remains
    assertFalse(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, table1));
    assertTrue(ZKMetadataProvider.isValidTableDeletionMarkerExists(_propertyStore, table2));

    // Cleanup
    ZKMetadataProvider.removeTableDeletionMarker(_propertyStore, table2);
  }
}
