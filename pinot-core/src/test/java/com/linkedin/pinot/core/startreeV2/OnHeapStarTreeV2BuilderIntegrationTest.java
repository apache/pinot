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
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import com.google.common.io.Files;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import org.apache.commons.configuration.PropertiesConfiguration;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;


public class OnHeapStarTreeV2BuilderIntegrationTest {

  private static File _indexDir;
  private List<GenericRow> _rows;
  private String _segmentOutputDir;
  private RecordReader _recordReader;
  private static List<StarTreeV2Config> _starTreeV2ConfigList = new ArrayList<>();

  @BeforeClass
  void setUp() throws Exception {

    Schema schema = StarTreeV2SegmentHelper.createSegmentSchema();
    String segmentName = "starTreeV2BuilderTest";
    _segmentOutputDir = Files.createTempDir().toString();
    _rows = StarTreeV2SegmentHelper.createSegmentData(schema);
    _recordReader = new GenericRowRecordReader(_rows, schema);
    _indexDir = StarTreeV2SegmentHelper.createSegment(schema, segmentName, _segmentOutputDir, _recordReader);
    File filepath = new File(_indexDir, "v3");

    List<Met2AggfuncPair> metric2aggFuncPairs1 = new ArrayList<>();
    List<Met2AggfuncPair> metric2aggFuncPairs2 = new ArrayList<>();

    Met2AggfuncPair pair1 = new Met2AggfuncPair("salary", "sum");
    Met2AggfuncPair pair2 = new Met2AggfuncPair("salary", "max");
    Met2AggfuncPair pair3 = new Met2AggfuncPair("salary", "min");

    metric2aggFuncPairs1.add(pair1);
    metric2aggFuncPairs1.add(pair2);
    metric2aggFuncPairs2.add(pair3);
    metric2aggFuncPairs2.add(pair1);

    StarTreeV2Config _starTreeV2Config1 = new StarTreeV2Config();
    _starTreeV2Config1.setOutDir(filepath);
    _starTreeV2Config1.setMaxNumLeafRecords(1);
    _starTreeV2Config1.setDimensions(schema.getDimensionNames());
    _starTreeV2Config1.setMetric2aggFuncPairs(metric2aggFuncPairs1);
    _starTreeV2ConfigList.add(_starTreeV2Config1);

    StarTreeV2Config _starTreeV2Config2 = new StarTreeV2Config();
    _starTreeV2Config2.setOutDir(filepath);
    _starTreeV2Config2.setMaxNumLeafRecords(1);
    _starTreeV2Config2.setDimensions(schema.getDimensionNames());
    _starTreeV2Config2.setMetric2aggFuncPairs(metric2aggFuncPairs2);
    _starTreeV2ConfigList.add(_starTreeV2Config2);

    return;
  }

  @Test
  public void testBuild() throws Exception {
    OnHeapStarTreeV2Builder onTest = new OnHeapStarTreeV2Builder();
    try {

      File metadataFile = new File(new File(_indexDir, "v3"), V1Constants.MetadataKeys.METADATA_FILE_NAME);
      PropertiesConfiguration properties = new PropertiesConfiguration(metadataFile);
      properties.setProperty(V1Constants.MetadataKeys.StarTree.STAR_TREE_ENABLED, true);
      properties.save();

      for (int i = 0; i < _starTreeV2ConfigList.size(); i++) {
        onTest.init(_indexDir, _starTreeV2ConfigList.get(i));
        onTest.build();
        onTest.serialize();
        Map<String, String> metadata = onTest.getMetaData();
        for (String key : metadata.keySet()) {
          String value = metadata.get(key);
          properties.setProperty(key, value);
        }
        properties.setProperty(StarTreeV2Constant.STAR_TREE_V2_COUNT, i+1);
        properties.setProperty(StarTreeV2Constant.STAR_TREE_V2_ENABLED, true);

        properties.save();

        onTest.convertFromV1toV3(i);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}