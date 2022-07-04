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

import org.I0Itec.zkclient.exception.ZkException;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.HelixPropertyStore;
import org.apache.pinot.common.metadata.ZKMetadataProvider;
import org.apache.pinot.common.utils.helix.FakePropertyStore;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;


/**
 * Tests for {@link TaskGeneratorInfoUtils}
 */
public class TaskGeneratorInfoUtilsTest {
  private static final String TABLE_NAME_WITH_TYPE = "table_OFFLINE";
  private static final String TASK_TYPE = "taskType";

  @Test
  public void testFetchTaskGeneratorInfo() {
    // ZNRecord does not exist
    HelixPropertyStore<ZNRecord> fakePropertyStore = new FakePropertyStore();
    assertNull(TaskGeneratorInfoUtils.fetchTaskGeneratorInfo(fakePropertyStore, TABLE_NAME_WITH_TYPE, TASK_TYPE));
    // ZNRecord exists
    ZNRecord znRecord =
        new ZNRecord(TABLE_NAME_WITH_TYPE + TaskGeneratorMostRecentRunInfo.ZNRECORD_DELIMITER + TASK_TYPE);
    fakePropertyStore.set(
        ZKMetadataProvider.constructPropertyStorePathForTaskGeneratorInfo(TABLE_NAME_WITH_TYPE, TASK_TYPE), znRecord,
        AccessOption.PERSISTENT);
    assertEquals(TaskGeneratorInfoUtils.fetchTaskGeneratorInfo(fakePropertyStore, TABLE_NAME_WITH_TYPE, TASK_TYPE),
        znRecord);
  }

  @Test
  public void testPersistTaskGeneratorInfo() {
    HelixPropertyStore<ZNRecord> fakePropertyStore = new FakePropertyStore();
    TaskGeneratorInfoUtils.persistTaskGeneratorInfo(fakePropertyStore, TABLE_NAME_WITH_TYPE,
        TaskGeneratorMostRecentRunInfo.newInstance(TABLE_NAME_WITH_TYPE, TASK_TYPE), -1);
  }

  @Test(expectedExceptions = ZkException.class)
  public void testPersistTaskGeneratorInfoThrows() {
    HelixPropertyStore<ZNRecord> mockedPropertyStore = mock(HelixPropertyStore.class);
    TaskGeneratorInfoUtils.persistTaskGeneratorInfo(mockedPropertyStore, TABLE_NAME_WITH_TYPE,
        TaskGeneratorMostRecentRunInfo.newInstance(TABLE_NAME_WITH_TYPE, TASK_TYPE), -1);
  }

  @Test
  public void testDeleteTaskGeneratorInfo() {
    HelixPropertyStore<ZNRecord> fakePropertyStore = new FakePropertyStore();
    TaskGeneratorInfoUtils.deleteTaskGeneratorInfo(fakePropertyStore, TABLE_NAME_WITH_TYPE, TASK_TYPE);
  }

  @Test(expectedExceptions = ZkException.class)
  public void testDeleteTaskGeneratorInfoThrows() {
    HelixPropertyStore<ZNRecord> mockedPropertyStore = mock(HelixPropertyStore.class);
    TaskGeneratorInfoUtils.deleteTaskGeneratorInfo(mockedPropertyStore, TABLE_NAME_WITH_TYPE, TASK_TYPE);
  }
}
