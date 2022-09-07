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
package org.apache.pinot.common.minion;

import org.apache.helix.AccessOption;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.helix.zookeeper.zkclient.exception.ZkException;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.helix.FakePropertyStore;
import org.apache.zookeeper.data.Stat;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.*;


/**
 * Tests for {@link MinionTaskMetadataUtils}
 */
public class MinionTaskMetadataUtilsTest {
  private static final String TABLE_NAME_WITH_TYPE = "TestTable_OFFLINE";
  private static final String TASK_TYPE = "TestTaskType";
  private static final String NEW_MINION_METADATA_PATH =
      ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadata(TABLE_NAME_WITH_TYPE, TASK_TYPE);
  private static final String OLD_MINION_METADATA_PATH =
      ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadataDeprecated(TASK_TYPE, TABLE_NAME_WITH_TYPE);
  private static final DummyTaskMetadata NEW_TASK_METADATA = new DummyTaskMetadata(TABLE_NAME_WITH_TYPE, 1000);
  private static final DummyTaskMetadata OLD_TASK_METADATA = new DummyTaskMetadata(TABLE_NAME_WITH_TYPE, 100);
  private static final int EXPECTED_VERSION = -1;
  private static final int ACCESS_OPTION = AccessOption.PERSISTENT;

  @Test
  public void testFetchTaskMetadata() {
    // no metadata path exists
    HelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();
    assertNull(MinionTaskMetadataUtils.fetchTaskMetadata(propertyStore, TASK_TYPE, TABLE_NAME_WITH_TYPE));

    // only the old metadata path exists
    propertyStore = new FakePropertyStore();
    propertyStore.set(OLD_MINION_METADATA_PATH, OLD_TASK_METADATA.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
    assertEquals(MinionTaskMetadataUtils.fetchTaskMetadata(propertyStore, TASK_TYPE, TABLE_NAME_WITH_TYPE),
        OLD_TASK_METADATA.toZNRecord());

    // only the new metadata path exists
    propertyStore = new FakePropertyStore();
    propertyStore.set(NEW_MINION_METADATA_PATH, NEW_TASK_METADATA.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
    assertEquals(MinionTaskMetadataUtils.fetchTaskMetadata(propertyStore, TASK_TYPE, TABLE_NAME_WITH_TYPE),
        NEW_TASK_METADATA.toZNRecord());

    // if two metadata paths exist at the same time, the new one will be used.
    propertyStore = new FakePropertyStore();
    propertyStore.set(OLD_MINION_METADATA_PATH, OLD_TASK_METADATA.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
    propertyStore.set(NEW_MINION_METADATA_PATH, NEW_TASK_METADATA.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
    assertEquals(MinionTaskMetadataUtils.fetchTaskMetadata(propertyStore, TASK_TYPE, TABLE_NAME_WITH_TYPE),
        NEW_TASK_METADATA.toZNRecord());
  }

  @Test
  public void testDeleteTaskMetadata() {
    // no error
    HelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();
    MinionTaskMetadataUtils.deleteTaskMetadata(propertyStore, TASK_TYPE, TABLE_NAME_WITH_TYPE);

    // both metadata paths will be removed
    propertyStore = new FakePropertyStore();
    propertyStore.set(OLD_MINION_METADATA_PATH, OLD_TASK_METADATA.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
    propertyStore.set(NEW_MINION_METADATA_PATH, NEW_TASK_METADATA.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
    assertTrue(propertyStore.exists(OLD_MINION_METADATA_PATH, ACCESS_OPTION));
    assertTrue(propertyStore.exists(NEW_MINION_METADATA_PATH, ACCESS_OPTION));
    MinionTaskMetadataUtils.deleteTaskMetadata(propertyStore, TASK_TYPE, TABLE_NAME_WITH_TYPE);
    assertFalse(propertyStore.exists(OLD_MINION_METADATA_PATH, ACCESS_OPTION));
    assertFalse(propertyStore.exists(NEW_MINION_METADATA_PATH, ACCESS_OPTION));

    // 1. ZNode MINION_TASK_METADATA/TestTable_OFFLINE and its descendants will be removed
    // 2. ZNode MINION_TASK_METADATA/<any task type>/TestTable_OFFLINE will also be removed
    String anotherTable = "anotherTable_OFFLINE";
    String anotherOldMinionMetadataPath =
        ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadataDeprecated(TASK_TYPE, anotherTable);
    DummyTaskMetadata anotherOldTaskMetadata = new DummyTaskMetadata(anotherTable, 20);
    String anotherNewMinionMetadataPath =
        ZKMetadataProvider.constructPropertyStorePathForMinionTaskMetadata(anotherTable, TASK_TYPE);
    DummyTaskMetadata anotherNewTaskMetadata = new DummyTaskMetadata(anotherTable, 200);
    propertyStore = new FakePropertyStore();
    propertyStore.set(OLD_MINION_METADATA_PATH, OLD_TASK_METADATA.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
    propertyStore.set(NEW_MINION_METADATA_PATH, NEW_TASK_METADATA.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
    propertyStore.set(anotherOldMinionMetadataPath, anotherOldTaskMetadata.toZNRecord(),
        EXPECTED_VERSION, ACCESS_OPTION);
    propertyStore.set(anotherNewMinionMetadataPath, anotherNewTaskMetadata.toZNRecord(),
        EXPECTED_VERSION, ACCESS_OPTION);
    assertTrue(propertyStore.exists(OLD_MINION_METADATA_PATH, ACCESS_OPTION));
    assertTrue(propertyStore.exists(NEW_MINION_METADATA_PATH, ACCESS_OPTION));
    assertTrue(propertyStore.exists(anotherOldMinionMetadataPath, ACCESS_OPTION));
    assertTrue(propertyStore.exists(anotherNewMinionMetadataPath, ACCESS_OPTION));
    MinionTaskMetadataUtils.deleteTaskMetadata(propertyStore, TABLE_NAME_WITH_TYPE);
    assertFalse(propertyStore.exists(OLD_MINION_METADATA_PATH, ACCESS_OPTION));
    assertFalse(propertyStore.exists(NEW_MINION_METADATA_PATH, ACCESS_OPTION));
    assertTrue(propertyStore.exists(anotherOldMinionMetadataPath, ACCESS_OPTION));
    assertTrue(propertyStore.exists(anotherNewMinionMetadataPath, ACCESS_OPTION));
  }

  @Test
  public void testDeleteTaskMetadataWithException() {
    // Test happy path. No exceptions thrown.
    HelixPropertyStore<ZNRecord> mockPropertyStore = Mockito.mock(HelixPropertyStore.class);
    when(mockPropertyStore.remove(ArgumentMatchers.anyString(), ArgumentMatchers.anyInt())).thenReturn(true);
    MinionTaskMetadataUtils.deleteTaskMetadata(mockPropertyStore, TASK_TYPE, TABLE_NAME_WITH_TYPE);

    // Test exception thrown
    when(mockPropertyStore.remove(ArgumentMatchers.anyString(), ArgumentMatchers.anyInt())).thenReturn(false);
    try {
      MinionTaskMetadataUtils.deleteTaskMetadata(mockPropertyStore, TASK_TYPE, TABLE_NAME_WITH_TYPE);
      fail("ZkException should have been thrown");
    } catch (ZkException e) {
      assertEquals(e.getMessage(), "Failed to delete task metadata: TestTaskType, TestTable_OFFLINE");
    }
  }

  @Test
  public void testPersistTaskMetadata() {
    DummyTaskMetadata taskMetadata = new DummyTaskMetadata(TABLE_NAME_WITH_TYPE, 2000);

    // the metadata will be written to the new path
    HelixPropertyStore<ZNRecord> propertyStore = new FakePropertyStore();
    MinionTaskMetadataUtils.persistTaskMetadata(propertyStore, TASK_TYPE, taskMetadata, EXPECTED_VERSION);
    assertTrue(propertyStore.exists(NEW_MINION_METADATA_PATH, ACCESS_OPTION));
    assertFalse(propertyStore.exists(OLD_MINION_METADATA_PATH, ACCESS_OPTION));
    assertEquals(MinionTaskMetadataUtils.fetchTaskMetadata(propertyStore, TASK_TYPE, TABLE_NAME_WITH_TYPE),
        taskMetadata.toZNRecord());

    // the metadata will be written to the old path if only the old path exists
    propertyStore = new FakePropertyStore();
    propertyStore.set(OLD_MINION_METADATA_PATH, OLD_TASK_METADATA.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
    MinionTaskMetadataUtils.persistTaskMetadata(propertyStore, TASK_TYPE, taskMetadata, EXPECTED_VERSION);
    assertFalse(propertyStore.exists(NEW_MINION_METADATA_PATH, ACCESS_OPTION));
    assertTrue(propertyStore.exists(OLD_MINION_METADATA_PATH, ACCESS_OPTION));
    assertEquals(MinionTaskMetadataUtils.fetchTaskMetadata(propertyStore, TASK_TYPE, TABLE_NAME_WITH_TYPE),
        taskMetadata.toZNRecord());

    // the metadata will be written to the new path if only the new path exists
    propertyStore = new FakePropertyStore();
    propertyStore.set(NEW_MINION_METADATA_PATH, NEW_TASK_METADATA.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
    MinionTaskMetadataUtils.persistTaskMetadata(propertyStore, TASK_TYPE, taskMetadata, EXPECTED_VERSION);
    assertTrue(propertyStore.exists(NEW_MINION_METADATA_PATH, ACCESS_OPTION));
    assertFalse(propertyStore.exists(OLD_MINION_METADATA_PATH, ACCESS_OPTION));
    assertEquals(MinionTaskMetadataUtils.fetchTaskMetadata(propertyStore, TASK_TYPE, TABLE_NAME_WITH_TYPE),
        taskMetadata.toZNRecord());

    // the metadata will be written to the new path if both paths exist
    propertyStore = new FakePropertyStore();
    propertyStore.set(OLD_MINION_METADATA_PATH, OLD_TASK_METADATA.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
    propertyStore.set(NEW_MINION_METADATA_PATH, NEW_TASK_METADATA.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
    MinionTaskMetadataUtils.persistTaskMetadata(propertyStore, TASK_TYPE, taskMetadata, EXPECTED_VERSION);
    assertTrue(propertyStore.exists(NEW_MINION_METADATA_PATH, ACCESS_OPTION));
    assertTrue(propertyStore.exists(OLD_MINION_METADATA_PATH, ACCESS_OPTION));
    assertEquals(propertyStore.get(NEW_MINION_METADATA_PATH, new Stat(), ACCESS_OPTION), taskMetadata.toZNRecord());
    assertEquals(MinionTaskMetadataUtils.fetchTaskMetadata(propertyStore, TASK_TYPE, TABLE_NAME_WITH_TYPE),
        taskMetadata.toZNRecord());
  }

  @Test
  public void testPersistTaskMetadataWithException() {
    DummyTaskMetadata taskMetadata = new DummyTaskMetadata(TABLE_NAME_WITH_TYPE, 1000);
    HelixPropertyStore<ZNRecord> mockPropertyStore = Mockito.mock(HelixPropertyStore.class);
    String expectedPath = NEW_MINION_METADATA_PATH;

    // Test happy path. No exceptions thrown.
    when(mockPropertyStore.set(expectedPath, taskMetadata.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION)).thenReturn(
        true);
    MinionTaskMetadataUtils.persistTaskMetadata(mockPropertyStore, TASK_TYPE, taskMetadata, EXPECTED_VERSION);
    verify(mockPropertyStore, times(1)).set(expectedPath, taskMetadata.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);

    // Test exception thrown
    when(mockPropertyStore.set(expectedPath, taskMetadata.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION)).thenReturn(
        false);
    try {
      MinionTaskMetadataUtils.persistTaskMetadata(mockPropertyStore, TASK_TYPE, taskMetadata, EXPECTED_VERSION);
      fail("ZkException should have been thrown");
    } catch (ZkException e) {
      verify(mockPropertyStore, times(2)).set(expectedPath, taskMetadata.toZNRecord(), EXPECTED_VERSION, ACCESS_OPTION);
      assertEquals(e.getMessage(), "Failed to persist minion metadata for task: TestTaskType and metadata:"
          + " {\"tableNameWithType\":\"TestTable_OFFLINE\"}");
    }
  }

  public static class DummyTaskMetadata extends BaseTaskMetadata {

    private final String _tableNameWithType;
    private final long _metadataVal;

    public DummyTaskMetadata(String tableNameWithType, long metadataVal) {
      _tableNameWithType = tableNameWithType;
      _metadataVal = metadataVal;
    }

    public String getTableNameWithType() {
      return _tableNameWithType;
    }

    public ZNRecord toZNRecord() {
      ZNRecord znRecord = new ZNRecord(_tableNameWithType);
      znRecord.setLongField("metadataVal", _metadataVal);
      return znRecord;
    }
  }
}
