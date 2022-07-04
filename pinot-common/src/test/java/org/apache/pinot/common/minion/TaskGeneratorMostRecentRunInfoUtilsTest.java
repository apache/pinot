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
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.helix.FakePropertyStore;
import org.apache.zookeeper.data.Stat;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link TaskGeneratorMostRecentRunInfoUtils}
 */
public class TaskGeneratorMostRecentRunInfoUtilsTest {
  private static final String TABLE_NAME_WITH_TYPE = "table_OFFLINE";
  private static final String TASK_TYPE = "taskType";
  private static final String ZNODE_PATH =
      ZKMetadataProvider.constructPropertyStorePathForTaskGeneratorInfo(TABLE_NAME_WITH_TYPE, TASK_TYPE);
  private HelixPropertyStore<ZNRecord> _propertyStore;

  @BeforeMethod
  public void setUp() {
    _propertyStore = new FakePropertyStore();
  }

  @Test
  public void testSaveSuccessRunTsToZkWithoutInitialZNode() {
    saveAndCheckSuccessRunTs(-1);
  }

  @Test
  public void testSaveSuccessRunTsToZkWithInitialZNode() {
    initializeZNRecord();
    saveAndCheckSuccessRunTs(10);
  }

  private void saveAndCheckSuccessRunTs(int expectedVersion) {
    TaskGeneratorMostRecentRunInfoUtils.saveSuccessRunTsToZk(_propertyStore, TABLE_NAME_WITH_TYPE, TASK_TYPE, 10);
    ZNRecord znRecord = _propertyStore.get(ZNODE_PATH, new Stat(), AccessOption.PERSISTENT);
    assertEquals(znRecord.getId(),
        TABLE_NAME_WITH_TYPE + TaskGeneratorMostRecentRunInfo.ZNRECORD_DELIMITER + TASK_TYPE);
    assertEquals(znRecord.getVersion(), expectedVersion);
    assertEquals(znRecord.getListFields().size(), 1);
    assertEquals(znRecord.getListField(TaskGeneratorMostRecentRunInfo.MOST_RECENT_SUCCESS_RUN_TS).size(), 1);
    assertTrue(znRecord.getListField(TaskGeneratorMostRecentRunInfo.MOST_RECENT_SUCCESS_RUN_TS).contains("10"));
    assertEquals(znRecord.getMapFields().size(), 1);
    assertTrue(znRecord.getMapFields().containsKey(TaskGeneratorMostRecentRunInfo.MOST_RECENT_ERROR_RUN_MESSAGE));
    assertTrue(znRecord.getMapField(TaskGeneratorMostRecentRunInfo.MOST_RECENT_ERROR_RUN_MESSAGE).isEmpty());
  }

  @Test
  public void testSaveErrorRunMessageToZkWithoutInitialZNode() {
    saveAndCheckErrorRunMessage(-1);
  }

  @Test
  public void testSaveErrorRunMessageToZkWithInitialZNode() {
    initializeZNRecord();
    saveAndCheckErrorRunMessage(10);
  }

  private void saveAndCheckErrorRunMessage(int expectedVersion) {
    TaskGeneratorMostRecentRunInfoUtils.saveErrorRunMessageToZk(_propertyStore, TABLE_NAME_WITH_TYPE, TASK_TYPE, 10,
        "error");
    ZNRecord znRecordUpdated = _propertyStore.get(ZNODE_PATH, new Stat(), AccessOption.PERSISTENT);
    assertEquals(znRecordUpdated.getId(),
        TABLE_NAME_WITH_TYPE + TaskGeneratorMostRecentRunInfo.ZNRECORD_DELIMITER + TASK_TYPE);
    assertEquals(znRecordUpdated.getVersion(), expectedVersion);
    assertEquals(znRecordUpdated.getMapFields().size(), 1);
    assertEquals(znRecordUpdated.getMapField(TaskGeneratorMostRecentRunInfo.MOST_RECENT_ERROR_RUN_MESSAGE).size(), 1);
    assertTrue(
        znRecordUpdated.getMapField(TaskGeneratorMostRecentRunInfo.MOST_RECENT_ERROR_RUN_MESSAGE).containsKey("10"));
    assertEquals(znRecordUpdated.getListFields().size(), 1);
    assertTrue(znRecordUpdated.getListFields().containsKey(TaskGeneratorMostRecentRunInfo.MOST_RECENT_SUCCESS_RUN_TS));
    assertTrue(znRecordUpdated.getListField(TaskGeneratorMostRecentRunInfo.MOST_RECENT_SUCCESS_RUN_TS).isEmpty());
  }

  private void initializeZNRecord() {
    ZNRecord znRecord =
        new ZNRecord(TABLE_NAME_WITH_TYPE + TaskGeneratorMostRecentRunInfo.ZNRECORD_DELIMITER + TASK_TYPE);
    znRecord.setVersion(10);
    _propertyStore.set(ZNODE_PATH, znRecord, 10, AccessOption.PERSISTENT);
  }
}
