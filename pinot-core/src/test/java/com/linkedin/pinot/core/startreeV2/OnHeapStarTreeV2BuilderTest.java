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
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.startree.StarTreeIndexTestSegmentHelper;


public class OnHeapStarTreeV2BuilderTest {

  private static File _indexDir;
  private static StarTreeV2Config _starTreeV2Config;
  private static final String SEGMENT_NAME = "starTreeV2Segment";
  private static final String DATA_DIR =
      System.getProperty("java.io.tmpdir") + File.separator + "StarTreeV2BuilderTest";

  @BeforeClass
  void setUp() throws Exception {
    Schema schema = StarTreeV2SegmentHelper.buildSegment(DATA_DIR, SEGMENT_NAME);
    //Schema schema = StarTreeIndexTestSegmentHelper.buildSegment(DATA_DIR, SEGMENT_NAME);
    _indexDir = new File(DATA_DIR, SEGMENT_NAME);
    _starTreeV2Config = new StarTreeV2Config();

    List<Met2AggfuncPair> metric2aggFuncPairs = new ArrayList<>();
    Met2AggfuncPair pair1 = new Met2AggfuncPair("salary", "SUM");
    metric2aggFuncPairs.add(pair1);
    Met2AggfuncPair pair2 = new Met2AggfuncPair("salary", "MAX");
    metric2aggFuncPairs.add(pair2);
    Met2AggfuncPair pair3 = new Met2AggfuncPair("salary", "MIN");
    metric2aggFuncPairs.add(pair3);
    _starTreeV2Config.setDimensions(schema.getDimensionNames());
    _starTreeV2Config.setMaxNumLeafRecords(10);
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