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
package org.apache.pinot.integration.tests.realtime.utils;

import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.common.utils.PauselessConsumptionUtils;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.core.data.manager.realtime.ConsumerCoordinator;
import org.apache.pinot.segment.local.dedup.PartitionDedupMetadataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.upsert.PartitionUpsertMetadataManager;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;


public class FailureInjectingRealtimeTableDataManager extends RealtimeTableDataManager {
  public static final int MAX_NUMBER_OF_FAILURES = 10;
  private final AtomicInteger _numberOfFailures = new AtomicInteger(0);

  public FailureInjectingRealtimeTableDataManager(Semaphore segmentBuildSemaphore) {
    this(segmentBuildSemaphore, () -> true);
  }

  public FailureInjectingRealtimeTableDataManager(Semaphore segmentBuildSemaphore,
      Supplier<Boolean> isServerReadyToServeQueries) {
    super(segmentBuildSemaphore, isServerReadyToServeQueries);
  }

  @Override
  protected RealtimeSegmentDataManager createRealtimeSegmentDataManager(SegmentZKMetadata zkMetadata,
      TableConfig tableConfig, IndexLoadingConfig indexLoadingConfig, Schema schema, LLCSegmentName llcSegmentName,
      ConsumerCoordinator semaphoreAccessCoordinator,
      PartitionUpsertMetadataManager partitionUpsertMetadataManager,
      PartitionDedupMetadataManager partitionDedupMetadataManager, BooleanSupplier isTableReadyToConsumeData)
      throws AttemptsExceededException, RetriableOperationException {

    boolean addFailureToCommits = PauselessConsumptionUtils.isPauselessEnabled(tableConfig);
    if (addFailureToCommits && _numberOfFailures.getAndIncrement() >= MAX_NUMBER_OF_FAILURES) {
      addFailureToCommits = false;
    }
    return new FailureInjectingRealtimeSegmentDataManager(zkMetadata, tableConfig, this, _indexDir.getAbsolutePath(),
        indexLoadingConfig, schema, llcSegmentName, semaphoreAccessCoordinator, _serverMetrics, addFailureToCommits);
  }
}
