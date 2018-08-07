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
package com.linkedin.pinot.core.startree.v2;

import java.io.File;
import java.util.List;
import org.testng.Assert;
import java.util.ArrayList;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import com.google.common.io.Files;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;


public class SumStarTreeV2Test extends BaseStarTreeV2Test<Double, Double> {

  private final String[] STAR_TREE1_HARD_CODED_QUERIES = new String[] {
      "SELECT SUM(salary) FROM T",
      "SELECT SUM(salary) FROM T GROUP BY Name",
      "SELECT SUM(salary) FROM T WHERE Name = 'Rahul'"
  };

  @BeforeClass
  void setUp() throws Exception {

    String _segmentName = "starTreeV2BuilderTest";
    String _segmentOutputDir = Files.createTempDir().toString();

    Schema schema = StarTreeV2SegmentHelper.createSegmentSchema();
    List<GenericRow> _rows = StarTreeV2SegmentHelper.createSegmentSmallData(schema);

    //List<GenericRow> _rows = StarTreeV2SegmentHelper.createSegmentLargeData(schema);

    RecordReader _recordReader = new GenericRowRecordReader(_rows, schema);
    File _indexDir = StarTreeV2SegmentHelper.createSegment(schema, _segmentName, _segmentOutputDir, _recordReader);
    File _filepath = new File(_indexDir, "v3");


    List<AggregationFunctionColumnPair> metric2aggFuncPairs1 = new ArrayList<>();

    AggregationFunctionColumnPair pair1 = new AggregationFunctionColumnPair(AggregationFunctionType.SUM, "salary");
    metric2aggFuncPairs1.add(pair1);

    StarTreeV2Config starTreeV2Config = new StarTreeV2Config();
    starTreeV2Config.setOutDir(_filepath);
    starTreeV2Config.setMaxNumLeafRecords(1);
    starTreeV2Config.setDimensions(schema.getDimensionNames());
    starTreeV2Config.setMetric2aggFuncPairs(metric2aggFuncPairs1);


    OnHeapStarTreeV2Builder buildTest = new OnHeapStarTreeV2Builder();
    buildTest.init(_indexDir, starTreeV2Config);
    buildTest.build();
    buildTest.serialize();

    _indexSegment = ImmutableSegmentLoader.load(_indexDir, ReadMode.heap);
    _starTreeV2 = _indexSegment.getStarTrees().get(0);
  }

  @Test
  public void testQueries() {
    for (String s: STAR_TREE1_HARD_CODED_QUERIES) {
      testQuery(s);
      System.out.println("Passed Query : " + s);
    }
  }

  @Override
  protected Double getNextValue(@Nonnull BlockSingleValIterator valueIterator, @Nullable Dictionary dictionary) {
    if (dictionary == null) {
      return valueIterator.nextDoubleVal();
    } else {
      return dictionary.getDoubleValue(valueIterator.nextIntVal());
    }
  }

  @Override
  protected Double aggregate(@Nonnull List<Double> values) {
    double sumVal = 0;
    for (Double value : values) {
      sumVal += value;
    }
    return sumVal;
  }

  @Override
  protected void assertAggregatedValue(Double starTreeResult, Double nonStarTreeResult) {
    Assert.assertEquals(starTreeResult, nonStarTreeResult, 1e-5);
  }
}
