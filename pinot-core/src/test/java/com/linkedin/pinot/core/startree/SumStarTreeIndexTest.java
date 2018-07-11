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
package com.linkedin.pinot.core.startree;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * This test generates a Star-Tree segment with random data, and ensures that SUM results computed using star-tree index
 * operator are the same as the results computed by scanning raw docs.
 */
public class SumStarTreeIndexTest extends BaseStarTreeIndexTest {
  private static final String DATA_DIR = System.getProperty("java.io.tmpdir") + File.separator + "SumStarTreeIndexTest";
  private static final String SEGMENT_NAME = "starTreeSegment";
  private static final String[] HARD_CODED_QUERIES = new String[]{
      "SELECT SUM(m1) FROM T",
      "SELECT SUM(m1) FROM T WHERE d1 = 'd1-v1'",
      "SELECT SUM(m1) FROM T WHERE d1 <> 'd1-v1' AND d1 >= 'd1-v2'",
      "SELECT SUM(m1) FROM T WHERE d1 BETWEEN 'd1-v1' AND 'd1-v3' AND d1 <> 'd1-v2'",
      "SELECT SUM(m1) FROM T WHERE d1 IN ('d1-v1', 'd1-v2')",
      "SELECT SUM(m1) FROM T WHERE d1 IN ('d1-v1', 'd1-v2') AND d2 NOT IN ('d2-v1')",
      "SELECT SUM(m1) FROM T GROUP BY d1",
      "SELECT SUM(m1) FROM T GROUP BY d1, d2",
      "SELECT SUM(m1) FROM T WHERE d1 = 'd1-v2' GROUP BY d1",
      "SELECT SUM(m1) FROM T WHERE d1 BETWEEN 'd1-v1' AND 'd1-v3' GROUP BY d2",
      "SELECT SUM(m1) FROM T WHERE d1 = 'd1-v2' GROUP BY d2, d3",
      "SELECT SUM(m1) FROM T WHERE d1 <> 'd1-v1' GROUP BY d2",
      "SELECT SUM(m1) FROM T WHERE d1 IN ('d1-v1', 'd1-v2') GROUP BY d2",
      "SELECT SUM(m1) FROM T WHERE d1 IN ('d1-v1', 'd1-v2') AND d2 NOT IN ('d2-v1') GROUP BY d3",
      "SELECT SUM(m1) FROM T WHERE d1 IN ('d1-v1', 'd1-v2') AND d2 NOT IN ('d2-v1') AND d2 > 'd2-v2' GROUP BY d3, d4"
  };

  @Override
  protected String[] getHardCodedQueries() {
    return HARD_CODED_QUERIES;
  }

  @Override
  protected List<String> getMetricColumns() {
    // Test against all metric columns
    return _segment.getSegmentMetadata().getSchema().getMetricNames();
  }

  @Override
  protected Map<List<Integer>, List<Double>> compute(Operator filterOperator) {
    BlockDocIdIterator docIdIterator = filterOperator.nextBlock().getBlockDocIdSet().iterator();

    Map<List<Integer>, List<Double>> results = new HashMap<>();
    int docId;
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      // Array of dictionary Ids (zero-length array for non-group-by query)
      List<Integer> groupKeys = new ArrayList<>(_numGroupByColumns);
      for (int i = 0; i < _numGroupByColumns; i++) {
        _groupByValIterators[i].skipTo(docId);
        groupKeys.add(_groupByValIterators[i].nextIntVal());
      }

      List<Double> sums = results.get(groupKeys);
      if (sums == null) {
        sums = new ArrayList<>(_numMetricColumns);
        for (int i = 0; i < _numMetricColumns; i++) {
          sums.add(0.0);
        }
        results.put(groupKeys, sums);
      }
      for (int i = 0; i < _numMetricColumns; i++) {
        _metricValIterators[i].skipTo(docId);
        int dictId = _metricValIterators[i].nextIntVal();
        sums.set(i, sums.get(i) + _metricDictionaries[i].getDoubleValue(dictId));
      }
    }

    return results;
  }

  @BeforeClass
  void setUp() throws Exception {
    StarTreeIndexTestSegmentHelper.buildSegment(DATA_DIR, SEGMENT_NAME);
  }

  @Test
  public void testQueries() throws Exception {
    File indexDir = new File(DATA_DIR, SEGMENT_NAME);

    _segment = ImmutableSegmentLoader.load(indexDir, ReadMode.heap);
    testHardCodedQueries();
    _segment.destroy();

    _segment = ImmutableSegmentLoader.load(indexDir, ReadMode.mmap);
    testHardCodedQueries();
    _segment.destroy();
  }

  @AfterClass
  void tearDown() {
    FileUtils.deleteQuietly(new File(DATA_DIR));
  }
}
