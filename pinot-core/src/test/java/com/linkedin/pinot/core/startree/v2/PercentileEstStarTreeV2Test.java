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

import com.google.common.io.Files;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.datatable.ObjectCustomSerDe;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
import com.linkedin.pinot.core.query.aggregation.function.customobject.QuantileDigest;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


public class PercentileEstStarTreeV2Test extends BaseStarTreeV2Test<byte[], QuantileDigest> {

  private File _indexDir;
  private int ROWS_COUNT = 60;
  private final int _percentile = 80;

  private StarTreeV2Config _starTreeV2Config;
  private final String[] STAR_TREE_HARD_CODED_QUERIES = new String[] {
      "SELECT PERCENTILEEST80(salary) FROM T"
  };

  @BeforeClass
  private void setUp() throws Exception {

    String segmentName = "starTreeV2BuilderTest";
    String segmentOutputDir = Files.createTempDir().toString();

    Schema schema = StarTreeV2SegmentHelper.createSegmentSchema();
    List<GenericRow> rows = StarTreeV2SegmentHelper.createSegmentData(schema, ROWS_COUNT);

    RecordReader recordReader = new GenericRowRecordReader(rows, schema);
    _indexDir = StarTreeV2SegmentHelper.createSegment(schema, segmentName, segmentOutputDir, recordReader);
    File filepath = new File(_indexDir, "v3");

    List<AggregationFunctionColumnPair> metric2aggFuncPairs1 = new ArrayList<>();

    AggregationFunctionColumnPair pair1 =
        new AggregationFunctionColumnPair(AggregationFunctionType.PERCENTILEEST, "salary");
    metric2aggFuncPairs1.add(pair1);

    _starTreeV2Config = new StarTreeV2Config();
    _starTreeV2Config.setOutDir(filepath);
    _starTreeV2Config.setMaxNumLeafRecords(1);
    _starTreeV2Config.setDimensions(schema.getDimensionNames());
    _starTreeV2Config.setMetric2aggFuncPairs(metric2aggFuncPairs1);
  }

  private void onHeapSetUp() throws Exception {
    setUp();
    OnHeapStarTreeV2Builder buildTest = new OnHeapStarTreeV2Builder();
    buildTest.init(_indexDir, _starTreeV2Config);
    buildTest.build();

    _indexSegment = ImmutableSegmentLoader.load(_indexDir, ReadMode.heap);
    _starTreeV2 = _indexSegment.getStarTrees().get(0);
  }

  private void offHeapSetUp() throws Exception {
    setUp();
    OffHeapStarTreeV2Builder buildTest = new OffHeapStarTreeV2Builder();
    buildTest.init(_indexDir, _starTreeV2Config);
    buildTest.build();

    _indexSegment = ImmutableSegmentLoader.load(_indexDir, ReadMode.heap);
    _starTreeV2 = _indexSegment.getStarTrees().get(0);
  }

  @Test
  public void testQueries() throws Exception {
    onHeapSetUp();
    System.out.println("Testing On-Heap Version");
    for (String s : STAR_TREE_HARD_CODED_QUERIES) {
      testQuery(s);
      System.out.println("Passed Query : " + s);
    }
    System.out.println();

    offHeapSetUp();
    System.out.println("Testing Off-Heap Version");
    for (String s : STAR_TREE_HARD_CODED_QUERIES) {
      testQuery(s);
      System.out.println("Passed Query : " + s);
    }
  }

  @Override
  protected byte[] getNextValue(@Nonnull BlockSingleValIterator valueIterator, @Nullable Dictionary dictionary) {
    if (dictionary == null) {
      return valueIterator.nextBytesVal();
    } else {
      Object val = dictionary.get(valueIterator.nextIntVal());
      long d = ((Number) val).longValue();
      QuantileDigest qDigest = new QuantileDigest(0.05);
      qDigest.add(d);

      try {
        return ObjectCustomSerDe.serialize(qDigest);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  @Override
  protected QuantileDigest aggregate(@Nonnull List<byte[]> values) {
    QuantileDigest qDigest = new QuantileDigest(0.05);

    for (byte[] obj : values) {
      try {
        QuantileDigest value = QuantileDigest.Builder.build(obj);
        qDigest.merge(value);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return qDigest;
  }

  @Override
  protected void assertAggregatedValue(QuantileDigest starTreeResult, QuantileDigest nonStarTreeResult) {

    System.out.println("Star-Tree Result Object Quantile: " + Long.toString(starTreeResult.getQuantile(_percentile / 100.0)));
    System.out.println("Non Star-Tree Result Object Quantile: " + Long.toString(nonStarTreeResult.getQuantile(_percentile / 100.0)));

    Assert.assertEquals(starTreeResult.getQuantile(_percentile / 100.0), nonStarTreeResult.getQuantile(_percentile / 100.0));
  }
}