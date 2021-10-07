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
package org.apache.pinot.controller.api.upload;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;
import org.apache.commons.httpclient.HttpConnectionManager;
import org.apache.helix.AccessOption;
import org.apache.helix.ZNRecord;
import org.apache.helix.store.zk.ZkHelixPropertyStore;
import org.apache.pinot.common.metrics.ControllerMetrics;
import org.apache.pinot.controller.ControllerConf;
import org.apache.pinot.controller.helix.core.PinotHelixResourceManager;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.joda.time.Interval;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class SegmentValidatorTest {

  private PinotHelixResourceManager _pinotHelixResourceManager = mock(PinotHelixResourceManager.class);
  private ControllerConf _controllerConf = mock(ControllerConf.class);
  private Executor _executor = mock(Executor.class);
  private HttpConnectionManager _connectionManager = mock(HttpConnectionManager.class);
  private ControllerMetrics _controllerMetrics = mock(ControllerMetrics.class);

  private SegmentValidator _underTest;

  @BeforeMethod
  public void init() {
    _underTest =
        new SegmentValidator(_pinotHelixResourceManager, _controllerConf, _executor, _connectionManager,
            _controllerMetrics, true);
  }

  @Test
  public void testValidateOfflineSegment() {
    SegmentMetadata segmentMetadata = mock(SegmentMetadata.class);
    File tempSegmentDir = mock(File.class);
    ZkHelixPropertyStore<ZNRecord> propertyStore = mock(ZkHelixPropertyStore.class);
    when(_pinotHelixResourceManager.getPropertyStore()).thenReturn(propertyStore);
    ZNRecord znRecord = mock(ZNRecord.class);
    when(propertyStore.get("/CONFIGS/TABLE/offlineTableName_OFFLINE", null, AccessOption.PERSISTENT)).thenReturn(
        znRecord);
    when(znRecord.getId()).thenReturn("/CONFIGS/TABLE/offlineTableName_OFFLINE");
    when(znRecord.getSimpleFields()).thenReturn(simpleFieldsMap());
    when(segmentMetadata.getName()).thenReturn("segmentName");
    when(segmentMetadata.getTimeInterval()).thenReturn(
        Interval.parse("2021-11-01T08:00:00.000/2021-11-01T10:00:00.000"));
    when(_controllerConf.getEnableStorageQuotaCheck()).thenReturn(false);

    _underTest.validateOfflineSegment("offlineTableName", segmentMetadata, tempSegmentDir);

    // At this point validation has been successful
  }

  private Map<String, String> simpleFieldsMap() {
    Map<String, String> simpleFields = new HashMap<>();
    simpleFields.put("tableType", "OFFLINE");
    simpleFields.put("isDim", "false");
    simpleFields.put("segmentsConfig", "{\n"
        + "      \"schemaName\": \"someSchema\",\n"
        + "      \"timeColumnName\": \"daysSinceEpoch\",\n"
        + "      \"timeType\": \"DAYS\",\n"
        + "      \"allowNullTimeValue\": false,\n"
        + "      \"replication\": \"3\",\n"
        + "      \"retentionTimeUnit\": \"DAYS\",\n"
        + "      \"retentionTimeValue\": \"365\",\n"
        + "      \"segmentPushFrequency\": \"DAILY\",\n"
        + "      \"segmentPushType\": \"APPEND\"\n"
        + "    }");
    simpleFields.put("tenants", "{\n"
        + "      \"broker\": \"myBrokerTenant\",\n"
        + "      \"server\": \"myServerTenant\"\n"
        + "    }");
    simpleFields.put("tableIndexConfig", "{\n"
        + "      \"invertedIndexColumns\": [\"foo\", \"bar\", \"moo\"],\n"
        + "      \"createInvertedIndexDuringSegmentGeneration\": false,\n"
        + "      \"sortedColumn\": [\"pk\"],\n"
        + "      \"bloomFilterColumns\": [],\n"
        + "      \"starTreeIndexConfigs\": [],\n"
        + "      \"noDictionaryColumns\": [],\n"
        + "      \"rangeIndexColumns\": [],\n"
        + "      \"onHeapDictionaryColumns\": [],\n"
        + "      \"varLengthDictionaryColumns\": [],\n"
        + "      \"segmentPartitionConfig\": {\n"
        + "        \"columnPartitionMap\": {\n"
        + "          \"pk\": {\n"
        + "            \"functionName\": \"Murmur\",\n"
        + "            \"numPartitions\": 32\n"
        + "          }\n"
        + "        }\n"
        + "      },\n"
        + "      \"loadMode\": \"MMAP\",\n"
        + "      \"columnMinMaxValueGeneratorMode\": null,\n"
        + "      \"nullHandlingEnabled\": false\n"
        + "    }");
    simpleFields.put("metadata", "{}");
    return simpleFields;
  }
}
