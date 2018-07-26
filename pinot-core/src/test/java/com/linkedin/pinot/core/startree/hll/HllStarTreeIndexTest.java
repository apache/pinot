/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree.hll;

import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.startree.BaseStarTreeIndexTest;
import com.linkedin.pinot.core.startree.StarTreeIndexTestSegmentHelper;
import com.linkedin.pinot.startree.hll.HllConfig;
import com.linkedin.pinot.startree.hll.HllConstants;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * This test generates a Star-Tree segment with random data and HLL derived columns, and ensures that FASTHLL results
 * computed using star-tree index operator are the same as the results computed by scanning raw docs.
 */
public class HllStarTreeIndexTest extends BaseStarTreeIndexTest {
  private static final String DATA_DIR = System.getProperty("java.io.tmpdir") + File.separator + "HllStarTreeIndexTest";
  private static final String SEGMENT_NAME = "starTreeSegment";

  // Test on column 'd3' and 'd4' because they have higher cardinality than other dimensions
  private static final Set<String> COLUMNS_TO_DERIVE_HLL_FIELDS = new HashSet<>(Arrays.asList("d3", "d4"));
  private static final List<String> HLL_METRIC_COLUMNS =
      Arrays.asList("d3" + HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX,
          "d4" + HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX);
  protected static final HllConfig HLL_CONFIG = new HllConfig(HllConstants.DEFAULT_LOG2M, COLUMNS_TO_DERIVE_HLL_FIELDS,
      HllConstants.DEFAULT_HLL_DERIVE_COLUMN_SUFFIX);

  private static final String[] HARD_CODED_QUERIES = new String[]{
      "SELECT FASTHLL(d3_hll) FROM T",
      "SELECT FASTHLL(d3_hll) FROM T WHERE d1 = 'd1-v1'",
      "SELECT FASTHLL(d3_hll) FROM T WHERE d1 <> 'd1-v1' AND d1 >= 'd1-v2'",
      "SELECT FASTHLL(d3_hll) FROM T WHERE d1 BETWEEN 'd1-v1' AND 'd1-v3' AND d1 <> 'd1-v2'",
      "SELECT FASTHLL(d3_hll) FROM T WHERE d1 IN ('d1-v1', 'd1-v2')",
      "SELECT FASTHLL(d3_hll) FROM T WHERE d1 IN ('d1-v1', 'd1-v2') AND d2 NOT IN ('d2-v1')",
      "SELECT FASTHLL(d3_hll) FROM T GROUP BY d1",
      "SELECT FASTHLL(d3_hll) FROM T GROUP BY d1, d2",
      "SELECT FASTHLL(d3_hll) FROM T WHERE d1 = 'd1-v2' GROUP BY d1",
      "SELECT FASTHLL(d3_hll) FROM T WHERE d1 BETWEEN 'd1-v1' AND 'd1-v3' GROUP BY d2",
      "SELECT FASTHLL(d3_hll) FROM T WHERE d1 = 'd1-v2' GROUP BY d2, d3",
      "SELECT FASTHLL(d3_hll) FROM T WHERE d1 <> 'd1-v1' GROUP BY d2",
      "SELECT FASTHLL(d3_hll) FROM T WHERE d1 IN ('d1-v1', 'd1-v2') GROUP BY d2",
      "SELECT FASTHLL(d3_hll) FROM T WHERE d1 IN ('d1-v1', 'd1-v2') AND d2 NOT IN ('d2-v1') GROUP BY d3",
      "SELECT FASTHLL(d3_hll) FROM T WHERE d1 NOT IN ('d1-v1', 'd1-v2') AND d2 NOT IN ('d2-v1') AND d2 > 'd2-v2' GROUP BY d3, d4"
  };

  @Override
  protected String[] getHardCodedQueries() {
    return HARD_CODED_QUERIES;
  }

  @Override
  protected List<String> getMetricColumns() {
    return HLL_METRIC_COLUMNS;
  }

  @Override
  protected Map<List<Integer>, List<Double>> compute(Operator filterOperator) throws Exception {
    BlockDocIdIterator docIdIterator = filterOperator.nextBlock().getBlockDocIdSet().iterator();

    Map<List<Integer>, List<HyperLogLog>> intermediateResults = new HashMap<>();
    int docId;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      // Array of dictionary Ids (zero-length array for non-group-by query)
      List<Integer> groupKeys = new ArrayList<>(_numGroupByColumns);
      for (int i = 0; i < _numGroupByColumns; i++) {
        _groupByValIterators[i].skipTo(docId);
        groupKeys.add(_groupByValIterators[i].nextIntVal());
      }

      List<HyperLogLog> hyperLogLogs = intermediateResults.get(groupKeys);
      if (hyperLogLogs == null) {
        hyperLogLogs = new ArrayList<>(_numMetricColumns);
        for (int i = 0; i < _numMetricColumns; i++) {
          hyperLogLogs.add(new HyperLogLog(HLL_CONFIG.getHllLog2m()));
        }
        intermediateResults.put(groupKeys, hyperLogLogs);
      }
      for (int i = 0; i < _numMetricColumns; i++) {
        _metricValIterators[i].skipTo(docId);
        int dictId = _metricValIterators[i].nextIntVal();
        HyperLogLog hyperLogLog = hyperLogLogs.get(i);
        hyperLogLog.addAll(HllUtil.convertStringToHll(_metricDictionaries[i].getStringValue(dictId)));
        hyperLogLogs.set(i, hyperLogLog);
      }
    }

    // Compute the final result
    Map<List<Integer>, List<Double>> finalResults = new HashMap<>();
    for (Map.Entry<List<Integer>, List<HyperLogLog>> entry : intermediateResults.entrySet()) {
      List<HyperLogLog> hyperLogLogs = entry.getValue();
      List<Double> finalResult = new ArrayList<>();
      for (HyperLogLog hyperLogLog : hyperLogLogs) {
        finalResult.add((double) hyperLogLog.cardinality());
      }
      finalResults.put(entry.getKey(), finalResult);
    }

    return finalResults;
  }

  @BeforeClass
  void setUp() throws Exception {
    StarTreeIndexTestSegmentHelper.buildSegmentWithHll(DATA_DIR, SEGMENT_NAME, HLL_CONFIG);
  }

  @Test
  public void testQueries() throws Exception {
    _segment = ImmutableSegmentLoader.load(new File(DATA_DIR, SEGMENT_NAME), ReadMode.mmap);
    testHardCodedQueries();
    _segment.destroy();
  }

  @AfterClass
  void tearDown() {
    FileUtils.deleteQuietly(new File(DATA_DIR));
  }
}
