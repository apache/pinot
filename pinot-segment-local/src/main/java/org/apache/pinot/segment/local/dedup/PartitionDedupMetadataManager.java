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
package org.apache.pinot.segment.local.dedup;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.segment.local.utils.HashUtils;
import org.apache.pinot.segment.local.utils.RecordInfo;
import org.apache.pinot.spi.config.table.HashFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class PartitionDedupMetadataManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionUpsertMetadataManager.class);

  private final String _tableNameWithType;
  private final int _partitionId;
  private final ServerMetrics _serverMetrics;
  private final HashFunction _hashFunction;

  // TODO(saurabh) : We can replace this with a ocncurrent Set
  @VisibleForTesting
  final ConcurrentHashMap<Object, Boolean> _primaryKeySet = new ConcurrentHashMap<>();

  public PartitionDedupMetadataManager(String tableNameWithType, int partitionId, ServerMetrics serverMetrics,
      HashFunction hashFunction) {
    _tableNameWithType = tableNameWithType;
    _partitionId = partitionId;
    _serverMetrics = serverMetrics;
    _hashFunction = hashFunction;
  }

  public boolean checkRecordPresentOrUpdate(RecordInfo recordInfo) {
    Boolean isPresent =
        _primaryKeySet.putIfAbsent(HashUtils.hashPrimaryKey(recordInfo.getPrimaryKey(), _hashFunction), true);
    return (isPresent != null);
  }
}
