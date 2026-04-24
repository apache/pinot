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
package org.apache.pinot.integration.tests.custom;

import java.io.File;
import org.apache.pinot.spi.config.table.TableConfig;
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;


/**
 * Realtime integration coverage for IVF_PQ configuration, which should fall back to exact scan.
 */
@Test(suiteName = "CustomClusterIntegrationTest")
public class IvfPqVectorRealtimeTest extends IvfPqVectorTest {
  private static final String REALTIME_TABLE_NAME = "IvfPqVectorRealtimeTest";

  @Override
  public String getTableName() {
    return REALTIME_TABLE_NAME;
  }

  @Override
  public boolean isRealtimeTable() {
    return true;
  }

  @Override
  protected TableConfig createRealtimeTableConfig(File sampleAvroFile) {
    return super.createRealtimeTableConfig(sampleAvroFile);
  }

  @Override
  @Test(dataProvider = "useBothQueryEngines")
  public void testExplainShowsDefaultIvfPqRuntimeParams(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String explain = getExplainString(useMultiStageQueryEngine,
        String.format("set vectorNprobe=%d; set vectorMaxCandidates=%d;", NLIST, getCountStarResult()),
        5);

    assertTrue(explain.contains("VECTOR_SIMILARITY_EXACT_SCAN") || explain.contains("VectorSimilarityExactScan"),
        "Realtime IVF_PQ should fall back to exact scan: " + explain);
    assertExplainContains(explain, "backend", "IVF_PQ");
    assertExplainContains(explain, "distanceFunction", "EUCLIDEAN");
    assertExplainContains(explain, "fallbackReason", "ivf_pq_mutable_segment_unavailable");
  }

  @Override
  @Test(dataProvider = "useBothQueryEngines")
  public void testExplainShowsRerankOverride(boolean useMultiStageQueryEngine)
      throws Exception {
    setUseMultiStageQueryEngine(useMultiStageQueryEngine);
    String explain = getExplainString(useMultiStageQueryEngine,
        "set vectorNprobe=2; set vectorExactRerank=false; set vectorMaxCandidates=17;", 5);

    assertTrue(explain.contains("VECTOR_SIMILARITY_EXACT_SCAN") || explain.contains("VectorSimilarityExactScan"),
        "Realtime IVF_PQ should still use exact scan when query options are present: " + explain);
    assertExplainContains(explain, "backend", "IVF_PQ");
    assertExplainContains(explain, "fallbackReason", "ivf_pq_mutable_segment_unavailable");
  }
}
