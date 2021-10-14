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
import org.mockito.Mockito;
import org.testng.annotations.Test;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;


/**
 * Tests for {@link MinionTaskMetadataUtils}
 */
public class MinionTaskMetadataUtilsTest {

  @Test
  public void testPersistTaskMetadata() {
    DummyTaskMetadata taskMetadata = new DummyTaskMetadata("TestTable_OFFLINE", 1000);
    HelixPropertyStore<ZNRecord> mockPropertyStore = Mockito.mock(HelixPropertyStore.class);
    String expectedPath = "/MINION_TASK_METADATA/TestTaskType/TestTable_OFFLINE";
    int expectedVersion = -1;

    // Test happy path. No exceptions thrown.
    when(mockPropertyStore.set(expectedPath, taskMetadata.toZNRecord(), expectedVersion,
        AccessOption.PERSISTENT)).thenReturn(true);
    MinionTaskMetadataUtils.persistTaskMetadata(mockPropertyStore, "TestTaskType", taskMetadata, expectedVersion);
    verify(mockPropertyStore, times(1)).set(expectedPath, taskMetadata.toZNRecord(), expectedVersion,
        AccessOption.PERSISTENT);

    // Test exception thrown
    when(mockPropertyStore.set(expectedPath, taskMetadata.toZNRecord(), -1, AccessOption.PERSISTENT))
        .thenReturn(false);
    try {
      MinionTaskMetadataUtils.persistTaskMetadata(mockPropertyStore, "TestTaskType", taskMetadata, expectedVersion);
      fail("ZkException should have been thrown");
    } catch (ZkException e) {
      verify(mockPropertyStore, times(2)).set(expectedPath, taskMetadata.toZNRecord(), expectedVersion,
          AccessOption.PERSISTENT);
      assertEquals(e.getMessage(), "Failed to persist minion metadata for task: TestTaskType and metadata:"
          + " {\"tableNameWithType\":\"TestTable_OFFLINE\"}");
    }
  }

  public class DummyTaskMetadata extends BaseTaskMetadata {

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
