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


public class PercentileTDigestStarTreeV2Test extends BaseStarTreeV2Test<Object, TDigest> {

  private final String[] STAR_TREE1_HARD_CODED_QUERIES =
      new String[]{"SELECT PERCENTILETDIGEST90(salary) FROM T WHERE Name = 'Rahul'"};

  @BeforeClass
  void setUp() throws Exception {

    String _segmentName = "starTreeV2BuilderTest";
    String _segmentOutputDir = Files.createTempDir().toString();

    Schema schema = StarTreeV2SegmentHelper.createSegmentSchema();
    List<GenericRow> _rows = StarTreeV2SegmentHelper.createSegmentSmallData(schema);

    //List<GenericRow> _rows = StarTreeV2SegmentHelper.createSegmentLargeData(schema);

    RecordReader _recordReader = new GenericRowRecordReader(_rows, schema);
    File indexDir = StarTreeV2SegmentHelper.createSegment(schema, _segmentName, _segmentOutputDir, _recordReader);
    File filepath = new File(indexDir, "v3");

    List<AggregationFunctionColumnPair> metric2aggFuncPairs1 = new ArrayList<>();

    AggregationFunctionColumnPair pair1 =
        new AggregationFunctionColumnPair(AggregationFunctionType.PERCENTILETDIGEST, "salary");
    metric2aggFuncPairs1.add(pair1);

    StarTreeV2Config starTreeV2Config = new StarTreeV2Config();
    starTreeV2Config.setOutDir(filepath);
    starTreeV2Config.setMaxNumLeafRecords(1);
    starTreeV2Config.setDimensions(schema.getDimensionNames());
    starTreeV2Config.setMetric2aggFuncPairs(metric2aggFuncPairs1);

    OnHeapStarTreeV2Builder buildTest = new OnHeapStarTreeV2Builder();
    buildTest.init(indexDir, starTreeV2Config);
    buildTest.build();

    _indexSegment = ImmutableSegmentLoader.load(indexDir, ReadMode.heap);
    _starTreeV2 = _indexSegment.getStarTrees().get(0);
  }

  @Test
  public void testQueries() {
    for (String s : STAR_TREE1_HARD_CODED_QUERIES) {
      testQuery(s);
      System.out.println("Passed Query : " + s);
    }
  }

  @Override
  protected Object getNextValue(@Nonnull BlockSingleValIterator valueIterator, @Nullable Dictionary dictionary) {
    if (dictionary == null) {
      return valueIterator.nextBytesVal();
    } else {
      return dictionary.getBytesValue(valueIterator.nextIntVal());
    }
  }

  @Override
  protected TDigest aggregate(@Nonnull List<Object> values) {
    TDigest tDigest = new TDigest(100);
    for (Object obj : values) {
      try {
        tDigest.add(ObjectCustomSerDe.deserialize((byte[]) obj, ObjectType.TDigest));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    return tDigest;
  }

  @Override
  protected void assertAggregatedValue(TDigest starTreeResult, TDigest nonStarTreeResult) {
    Assert.assertEquals(starTreeResult instanceof TDigest, nonStarTreeResult instanceof TDigest);
  }
}
