/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.common.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.helix.ZNRecord;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.metadata.resource.OfflineDataResourceZKMetadata;
import com.linkedin.pinot.common.utils.CommonConstants.Helix;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.ResourceType;
import com.linkedin.pinot.common.utils.CommonConstants.Helix.DataSource.SegmentAssignmentStrategy;


public class OfflineDataResourceMetadataTest {

  @Test
  public void OfflineDataResourceMetadataCovertionTest() {
    ZNRecord znRecordRaw = getTestZNRecord();
    OfflineDataResourceZKMetadata offlineDataResourceZKMetadataFromZNRecord = new OfflineDataResourceZKMetadata(znRecordRaw);
    OfflineDataResourceZKMetadata offlineDataResourceZKMetadata = getTestOfflineDataResourceZKMetadata();
    ZNRecord znRecordFromMetadata = offlineDataResourceZKMetadata.toZNRecord();

    Assert.assertTrue(MetadataUtils.comparisonZNRecords(znRecordRaw, znRecordFromMetadata));
    Assert.assertTrue(offlineDataResourceZKMetadataFromZNRecord.equals(offlineDataResourceZKMetadata));
    Assert.assertTrue(MetadataUtils.comparisonZNRecords(znRecordRaw, new OfflineDataResourceZKMetadata(znRecordRaw).toZNRecord()));
    Assert.assertTrue(offlineDataResourceZKMetadata.equals(new OfflineDataResourceZKMetadata(offlineDataResourceZKMetadata.toZNRecord())));

  }

  private ZNRecord getTestZNRecord() {
    ZNRecord record = new ZNRecord("testResource_O");
    record.setSimpleField(Helix.DataSource.RESOURCE_NAME, "testResource");
    record.setEnumField(Helix.DataSource.RESOURCE_TYPE, ResourceType.OFFLINE);
    List<String> sortedColumnList = new ArrayList<String>();
    sortedColumnList.add("sortColumn0");
    sortedColumnList.add("sortColumn1");
    sortedColumnList.add("sortColumn2");
    record.setListField(Helix.DataSource.SORTED_COLUMN_LIST, sortedColumnList);
    record.setSimpleField(Helix.DataSource.TIME_COLUMN_NAME, "daysSinceEpoch");
    record.setSimpleField(Helix.DataSource.TIME_TYPE, "daysSinceEpoch");
    record.setIntField(Helix.DataSource.NUMBER_OF_DATA_INSTANCES, 6);
    record.setIntField(Helix.DataSource.NUMBER_OF_COPIES, 3);
    record.setEnumField(Helix.DataSource.RETENTION_TIME_UNIT, TimeUnit.DAYS);
    record.setIntField(Helix.DataSource.RETENTION_TIME_VALUE, 7);
    record.setSimpleField(Helix.DataSource.BROKER_TAG_NAME, "testBroker");
    record.setIntField(Helix.DataSource.NUMBER_OF_BROKER_INSTANCES, 2);
    record.setMapField(Helix.DataSource.METADATA, new HashMap<String, String>());
    record.setSimpleField(Helix.DataSource.PUSH_FREQUENCY, "daily");
    record.setEnumField(Helix.DataSource.SEGMENT_ASSIGNMENT_STRATEGY, SegmentAssignmentStrategy.RandomAssignmentStrategy);
    return record;
  }

  private OfflineDataResourceZKMetadata getTestOfflineDataResourceZKMetadata() {
    OfflineDataResourceZKMetadata offlineDataResourceZKMetadata = new OfflineDataResourceZKMetadata();
    offlineDataResourceZKMetadata.setResourceName("testResource");
    List<String> sortedColumnList = new ArrayList<String>();
    sortedColumnList.add("sortColumn0");
    sortedColumnList.add("sortColumn1");
    sortedColumnList.add("sortColumn2");
    offlineDataResourceZKMetadata.setSortedColumns(sortedColumnList);
    offlineDataResourceZKMetadata.setTimeColumnName("daysSinceEpoch");
    offlineDataResourceZKMetadata.setTimeType("daysSinceEpoch");
    offlineDataResourceZKMetadata.setNumDataInstances(6);
    offlineDataResourceZKMetadata.setNumDataReplicas(3);
    offlineDataResourceZKMetadata.setRetentionTimeUnit(TimeUnit.DAYS);
    offlineDataResourceZKMetadata.setRetentionTimeValue(7);
    offlineDataResourceZKMetadata.setBrokerTag("testBroker");
    offlineDataResourceZKMetadata.setNumBrokerInstance(2);
    offlineDataResourceZKMetadata.setMetadata(new HashMap<String, String>());
    offlineDataResourceZKMetadata.setPushFrequency("daily");
    offlineDataResourceZKMetadata.setSegmentAssignmentStrategy("RandomAssignmentStrategy");
    return offlineDataResourceZKMetadata;
  }
}
