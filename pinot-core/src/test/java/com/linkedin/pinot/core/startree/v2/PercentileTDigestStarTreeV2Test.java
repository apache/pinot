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

import com.clearspring.analytics.stream.quantile.TDigest;
import com.google.common.io.Files;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.datatable.ObjectCustomSerDe;
import com.linkedin.pinot.core.common.datatable.ObjectType;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.query.aggregation.function.AggregationFunctionType;
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


public class PercentileTDigestStarTreeV2Test extends BaseStarTreeV2Test<byte[], TDigest> {

  private File _indexDir;
  private final int _percentile = 90;
  private final double VALUE_RANGE = Integer.MAX_VALUE;
  private final double DELTA = 0.15 * VALUE_RANGE; // Allow 15% quantile error

  private StarTreeV2Config _starTreeV2Config;
  private final String[] STAR_TREE_HARD_CODED_QUERIES =
      new String[]{"SELECT PERCENTILETDIGEST90(salary) FROM T WHERE Name = 'Rahul'"};

  @BeforeClass
  private void setUp() throws Exception {

    String segmentName = "starTreeV2BuilderTest";
    String segmentOutputDir = Files.createTempDir().toString();

    Schema schema = StarTreeV2SegmentHelper.createSegmentSchema();
    List<GenericRow> rows = StarTreeV2SegmentHelper.createSegmentSmallData(schema);

    // List<GenericRow> rows = StarTreeV2SegmentHelper.createSegmentLargeData(schema);

    RecordReader recordReader = new GenericRowRecordReader(rows, schema);
    _indexDir = StarTreeV2SegmentHelper.createSegment(schema, segmentName, segmentOutputDir, recordReader);
    File filepath = new File(_indexDir, "v3");

    List<AggregationFunctionColumnPair> metric2aggFuncPairs1 = new ArrayList<>();

    AggregationFunctionColumnPair pair1 =
        new AggregationFunctionColumnPair(AggregationFunctionType.PERCENTILETDIGEST, "salary");
    metric2aggFuncPairs1.add(pair1);

    _starTreeV2Config = new StarTreeV2Config();
    _starTreeV2Config.setOutDir(filepath);
    _starTreeV2Config.setMaxNumLeafRecords(10);
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

      double d = ((Number) val).doubleValue();
      TDigest tDigest = new TDigest(100);
      tDigest.add(d);

      try {
        return ObjectCustomSerDe.serialize(tDigest);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return null;
  }

  @Override
  protected TDigest aggregate(@Nonnull List<byte[]> values) {
    TDigest tDigest = new TDigest(100);
    for (byte[] obj : values) {
      try {
        tDigest.add(ObjectCustomSerDe.deserialize(obj, ObjectType.TDigest));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return tDigest;
  }

  @Override
  protected void assertAggregatedValue(TDigest starTreeResult, TDigest nonStarTreeResult) {
    System.out.println("Star Tree Result Object Size: " + Integer.toString(starTreeResult.size()));
    System.out.println("Star Tree Result Object Size: " + Integer.toString(nonStarTreeResult.size()));

    if ((nonStarTreeResult.size() != starTreeResult.size()) && (starTreeResult.size() != 1)) {
      Assert.assertEquals(starTreeResult.quantile(_percentile / 100.0), nonStarTreeResult.quantile(_percentile / 100.0),
          DELTA, "failed badly");
    }
  }
}
