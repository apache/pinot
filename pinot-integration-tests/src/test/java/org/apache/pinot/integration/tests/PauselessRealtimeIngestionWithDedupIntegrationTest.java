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
package org.apache.pinot.integration.tests;

import org.apache.pinot.spi.config.table.DedupConfig;
import org.apache.pinot.spi.config.table.HashFunction;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.ingestion.IngestionConfig;
import org.apache.pinot.spi.config.table.ingestion.ParallelSegmentConsumptionPolicy;
import org.testng.annotations.Test;


public class PauselessRealtimeIngestionWithDedupIntegrationTest extends BasePauselessRealtimeIngestionTest {

  @Override
  protected String getFailurePoint() {
    return null;  // No failure point for basic test
  }

  @Override
  protected int getExpectedSegmentsWithFailure() {
    return NUM_REALTIME_SEGMENTS;  // Always expect full segments
  }

  @Override
  protected int getExpectedZKMetadataWithFailure() {
    return NUM_REALTIME_SEGMENTS;  // Always expect full metadata
  }

  @Override
  protected long getCountStarResultWithFailure() {
    return DEFAULT_COUNT_STAR_RESULT;  // Always expect full count
  }

  @Override
  protected void injectFailure() {
    // Do nothing - no failure to inject
  }

  @Override
  protected void disableFailure() {
    // Do nothing - no failure to disable
  }

  @Override
  protected int getNumKafkaPartitions() {
    return 1;
  }

  @Override
  protected TableConfig getPauselessTableConfig() {
    TableConfig tableConfig = super.getPauselessTableConfig();
    DedupConfig dedupConfig = new DedupConfig(true, HashFunction.NONE);
    tableConfig.setDedupConfig(dedupConfig);

    IngestionConfig ingestionConfig = tableConfig.getIngestionConfig();
    ingestionConfig.getStreamIngestionConfig().setEnforceConsumptionInOrder(true);
    ingestionConfig.getStreamIngestionConfig()
        .setParallelSegmentConsumptionPolicy(ParallelSegmentConsumptionPolicy.ALLOW_DURING_BUILD_ONLY);
    return tableConfig;
  }

  @Test(description = "Ensure that all the segments are ingested, built and uploaded when pauseless consumption is "
      + "enabled")
  public void testSegmentAssignment() {
    testBasicSegmentAssignment();
  }
}
