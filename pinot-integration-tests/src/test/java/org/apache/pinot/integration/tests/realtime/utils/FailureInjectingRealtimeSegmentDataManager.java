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

import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.common.metrics.ServerMetrics;
import org.apache.pinot.common.utils.LLCSegmentName;
import org.apache.pinot.core.data.manager.realtime.ConsumerCoordinator;
import org.apache.pinot.core.data.manager.realtime.RealtimeSegmentDataManager;
import org.apache.pinot.core.data.manager.realtime.RealtimeTableDataManager;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.retry.AttemptsExceededException;
import org.apache.pinot.spi.utils.retry.RetriableOperationException;


/**
 * A specialized RealtimeSegmentDataManager that lets us inject a forced failure
 * in the commit step, which occurs strictly after the segmentConsumed message.
 */
public class FailureInjectingRealtimeSegmentDataManager extends RealtimeSegmentDataManager {

  // This flag controls whether commit should forcibly fail.
  private final boolean _failCommit;

  /**
   * Creates a manager that will forcibly fail the commit segment step.
   */
  public FailureInjectingRealtimeSegmentDataManager(SegmentZKMetadata segmentZKMetadata, TableConfig tableConfig,
      RealtimeTableDataManager realtimeTableDataManager, String resourceDataDir, IndexLoadingConfig indexLoadingConfig,
      Schema schema, LLCSegmentName llcSegmentName, ConsumerCoordinator consumerCoordinator,
      ServerMetrics serverMetrics, boolean failCommit)
      throws AttemptsExceededException, RetriableOperationException {
    // Pass through to the real parent constructor
    super(segmentZKMetadata, tableConfig, realtimeTableDataManager, resourceDataDir, indexLoadingConfig, schema,
        llcSegmentName, consumerCoordinator, serverMetrics, null /* no PartitionUpsertMetadataManager */,
        null /* no PartitionDedupMetadataManager */, () -> true /* isReadyToConsumeData always true for tests */);

    _failCommit = failCommit;
  }

  protected SegmentBuildDescriptor buildSegmentInternal(boolean forCommit) {
     if (_failCommit) {
       throw new RuntimeException("Forced failure in buildSegmentInternal");
     }
     return super.buildSegmentInternal(forCommit);
  }
}
