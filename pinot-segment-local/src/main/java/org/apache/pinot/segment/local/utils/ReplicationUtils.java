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
package org.apache.pinot.core.util;

import org.apache.pinot.spi.config.table.SegmentsValidationAndRetentionConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.stream.StreamConfig;
import org.apache.pinot.spi.utils.IngestionConfigUtils;


/**
 * Methods related to replication
 */
public class ReplicationUtils {

  /**
   * Decides if {@link SegmentsValidationAndRetentionConfig ::getReplicationNumber} should be used
   */
  public static boolean useReplication(TableConfig tableConfig) {

    TableType tableType = tableConfig.getTableType();
    if (tableType.equals(TableType.REALTIME)) {
      StreamConfig streamConfig = new StreamConfig(tableConfig.getTableName(),
          IngestionConfigUtils.getStreamConfigMap(tableConfig));
      return streamConfig.hasHighLevelConsumerType();
    }
    return true;
  }

  /**
   * Decides if {@link SegmentsValidationAndRetentionConfig ::getReplicasPerPartitionNumber} should be used
   */
  public static boolean useReplicasPerPartition(TableConfig tableConfig) {

    TableType tableType = tableConfig.getTableType();
    if (tableType.equals(TableType.REALTIME)) {
      StreamConfig streamConfig = new StreamConfig(tableConfig.getTableName(),
          IngestionConfigUtils.getStreamConfigMap(tableConfig));
      return streamConfig.hasLowLevelConsumerType();
    }
    return false;
  }
}
