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

import org.apache.helix.zookeeper.datamodel.ZNRecord;
import org.apache.pinot.common.minion.RealtimeToOfflineSegmentsTaskMetadata;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


/**
 * Tests for converting to and from ZNRecord to {@link RealtimeToOfflineSegmentsTaskMetadata}
 */
public class RealtimeToOfflineSegmentsTaskMetadataTest {

  @Test
  public void testToFromZNRecord() {
    RealtimeToOfflineSegmentsTaskMetadata metadata =
        new RealtimeToOfflineSegmentsTaskMetadata("testTable_REALTIME", 1000);
    ZNRecord znRecord = metadata.toZNRecord();
    assertEquals(znRecord.getId(), "testTable_REALTIME");
    assertEquals(znRecord.getSimpleField("watermarkMs"), "1000");

    RealtimeToOfflineSegmentsTaskMetadata realtimeToOfflineSegmentsTaskMetadata =
        RealtimeToOfflineSegmentsTaskMetadata.fromZNRecord(znRecord);
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getTableNameWithType(), "testTable_REALTIME");
    assertEquals(realtimeToOfflineSegmentsTaskMetadata.getWatermarkMs(), 1000);
  }
}
