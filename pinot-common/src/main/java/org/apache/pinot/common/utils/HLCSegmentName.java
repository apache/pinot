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
package org.apache.pinot.common.utils;

// old style name (that needs to be recognized, but not built anymore)
//  return StringUtils.join(
//      Lists.newArrayList(tableName, instanceName, groupId, partitionName, sequenceNumber, creationTime), "__");
// Example tableName_REALTIME__Server_host_name.domain.com_8001__tableName_REALTIME_1449605490946_1__0__1450910568438
// Splitting this:
//    table name: tableName_REALTIME,
//    instanceName: Server_host_name.domain.com_8001
//    groupId: tableName_REALTIME_1449605490946_1 (has tablename, group creation time, and a group sequence nuumber)
//    sequenceNumber: 0
//    creationTime: 1450910568438
//
// shorter name: {groupId}__{partitionRange}__{sequenceNumber}
// Example: tableName_REALTIME_1433316466991_0__0__1465314044238
// Splitting this:
//    groupId: tableName_REALTIME_1433316466991_0
//    partitionRange: 0
//    sequenceNumber: 1465314044238
// see org.apache.pinot.controller.helix.core.PinotTableIdealStateBuilder#getGroupIdFromRealtimeDataTable for details on the groupId

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;


public class HLCSegmentName extends SegmentName {
  private final boolean _isOldStyleNaming;
  private final String _groupId;
  private final String _sequenceNumber;
  private final String _partitionRange;
  private final String _segmentName;
  private final RealtimeSegmentType _segmentType;
  private final String _tableName;

  // Can be called with old or new style naming.
  public HLCSegmentName(String segmentName) {
    // Decide if it is old style or new style v1 naming here.
    if (!isHighLevelConsumerSegmentName(segmentName)) {
      throw new RuntimeException(segmentName + " is not a HighLevelConsumer segment name");
    }
    String parts[] = segmentName.split(SEPARATOR);
    if (parts.length == 5) {
      _isOldStyleNaming = true;
    } else if (parts.length == 3) {
      _isOldStyleNaming = false;
    } else {
      throw new RuntimeException(segmentName + " is not HighLevelConsumer segment name ");
    }

    if (isOldStyleNaming()) {
      _tableName = parts[0];
      _groupId = parts[2];
      _partitionRange = parts[3];
      _sequenceNumber = parts[4];
      _segmentType = RealtimeSegmentType.HLC_LONG;
    } else {
      _groupId = parts[0];
      _partitionRange = parts[1];
      _sequenceNumber = parts[2];
      _tableName = _groupId.substring(0, _groupId.indexOf(REALTIME_SUFFIX) + REALTIME_SUFFIX_LENGTH);
      _segmentType = RealtimeSegmentType.HLC_SHORT;
    }
    _segmentName = segmentName;
  }

  /**
   * Builds only the newer style.
   * @param groupId is like myTable_REALTIME_1442428556382_0 (where 1442428556382 is the time when the group was created
   *                or it is line myTable
   * @param partitionRange
   * @param sequenceNumber
   */
  public HLCSegmentName(String groupId, String partitionRange, String sequenceNumber) {
    if (isValidComponentName(groupId) && isValidComponentName(partitionRange) && isValidComponentName(sequenceNumber)) {
      _isOldStyleNaming = false;
      _groupId = groupId;
      _partitionRange = partitionRange;
      _sequenceNumber = sequenceNumber;
      _segmentName = StringUtils.join(Lists.newArrayList(groupId, partitionRange, sequenceNumber), SEPARATOR);
      _tableName = _groupId.substring(0, _groupId.indexOf(REALTIME_SUFFIX) + REALTIME_SUFFIX_LENGTH);
      _segmentType = RealtimeSegmentType.HLC_SHORT;
    } else {
      throw new IllegalArgumentException(
          "Invalid group id (" + groupId + "), partition range (" + partitionRange + ") or sequence number ("
              + sequenceNumber + ")");
    }
  }

  public boolean isOldStyleNaming() {
    return _isOldStyleNaming;
  }

  @Override
  public String getGroupId() {
    return _groupId;
  }

  @Override
  public String getSequenceNumberStr() {
    return _sequenceNumber;
  }

  @Override
  public int getSequenceNumber() {
    return Integer.valueOf(_sequenceNumber);
  }

  @Override
  public String getPartitionRange() {
    return _partitionRange;
  }

  @Override
  public String getSegmentName() {
    return _segmentName;
  }

  @Override
  public String getTableName() {
    return _tableName;
  }

  @Override
  public RealtimeSegmentType getSegmentType() {
    return _segmentType;
  }

  @Override
  public String toString() {
    return getSegmentName();
  }
}
