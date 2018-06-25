/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.startreeV2;

import java.io.File;
import java.util.List;
import java.util.ArrayList;
import com.google.common.io.Files;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.readers.PinotSegmentUtil;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;


public class OnHeapStarTreeV2BuilderTest {


  private static File _indexDir;
  private List<GenericRow> _rows;
  private String _segmentOutputDir;
  private RecordReader _recordReader;
  private static StarTreeV2Config _starTreeV2Config;

  @BeforeClass
  void setUp() throws Exception {

    Schema schema = StarTreeV2SegmentHelper.createSegmentchema();
    String segmentName = "starTreeV2BuilderTest";
    _segmentOutputDir = Files.createTempDir().toString();
    //_rows = PinotSegmentUtil.createTestData(schema, NUM_ROWS);
    _rows = StarTreeV2SegmentHelper.buildSegment(schema);
    _recordReader = new GenericRowRecordReader(_rows, schema);

    _indexDir = PinotSegmentUtil.createSegment(schema, segmentName, _segmentOutputDir, _recordReader);

    _starTreeV2Config = new StarTreeV2Config();

    List<Met2AggfuncPair> metric2aggFuncPairs = new ArrayList<>();
    Met2AggfuncPair pair1 = new Met2AggfuncPair("salary", "sum");
    metric2aggFuncPairs.add(pair1);
    Met2AggfuncPair pair2 = new Met2AggfuncPair("salary", "max");
    metric2aggFuncPairs.add(pair2);
    Met2AggfuncPair pair3 = new Met2AggfuncPair("salary", "min");
    metric2aggFuncPairs.add(pair3);
    _starTreeV2Config.setDimensions(schema.getDimensionNames());
    _starTreeV2Config.setMaxNumLeafRecords(1);
    _starTreeV2Config.setMetric2aggFuncPairs(metric2aggFuncPairs);

    return;
  }

  @Test
  public void testBuild() throws Exception {
    OnHeapStarTreeV2Builder onTest = new OnHeapStarTreeV2Builder();
    try {
      onTest.init(_indexDir, _starTreeV2Config);
    } catch (Exception e) {
      e.printStackTrace();
    }

    onTest.build();
  }
}