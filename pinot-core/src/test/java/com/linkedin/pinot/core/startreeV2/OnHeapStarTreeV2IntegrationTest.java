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
import java.util.HashMap;
import java.util.ArrayList;
import com.google.common.io.Files;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeTest;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.startree.StarTree;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;


public class OnHeapStarTreeV2IntegrationTest {

  private File _filepath;
  private static File _indexDir;
  private List<GenericRow> _rows;
  private String _segmentOutputDir;
  private RecordReader _recordReader;
  private static List<StarTreeV2Config> _starTreeV2ConfigList = new ArrayList<>();
  private List<Map<String, DataSource>> _starTreeDataSourcesList = new ArrayList<>();

  @BeforeTest
  void setUp() throws Exception {

    Schema schema = StarTreeV2SegmentHelper.createSegmentSchema();
    String segmentName = "starTreeV2BuilderTest";
    _segmentOutputDir = Files.createTempDir().toString();
    _rows = StarTreeV2SegmentHelper.createSegmentData(schema);
    _recordReader = new GenericRowRecordReader(_rows, schema);
    _indexDir = StarTreeV2SegmentHelper.createSegment(schema, segmentName, _segmentOutputDir, _recordReader);
    _filepath = new File(_indexDir, "v3");

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
    _starTreeV2Config1.setOutDir(_filepath);
    _starTreeV2Config1.setMaxNumLeafRecords(1);
    _starTreeV2Config1.setDimensions(schema.getDimensionNames());
    _starTreeV2Config1.setMetric2aggFuncPairs(metric2aggFuncPairs1);
    _starTreeV2ConfigList.add(_starTreeV2Config1);

    StarTreeV2Config _starTreeV2Config2 = new StarTreeV2Config();
    _starTreeV2Config2.setOutDir(_filepath);
    _starTreeV2Config2.setMaxNumLeafRecords(1);
    _starTreeV2Config2.setDimensions(schema.getDimensionNames());
    _starTreeV2Config2.setMetric2aggFuncPairs(metric2aggFuncPairs2);
    _starTreeV2ConfigList.add(_starTreeV2Config2);

    return;
  }

  @Test
  public void testBuilder() throws Exception {
    OnHeapStarTreeV2Builder buildTest = new OnHeapStarTreeV2Builder();
    try {

      for (int i = 0; i < _starTreeV2ConfigList.size(); i++) {
        buildTest.init(_indexDir, _starTreeV2ConfigList.get(i));
        buildTest.build();
        buildTest.serialize();
      }


    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testLoader() throws Exception {
    OnHeapStarTreeV2Loader loadTest = new OnHeapStarTreeV2Loader();
    try {
      loadTest.init(_filepath);
      loadTest.load();
      for (int i = 0; i < _starTreeV2ConfigList.size(); i++) {

        StarTree s = loadTest.getStarTree(i);
        StarTreeV2LoaderHelper.printStarTree(s);
        Map<String, DataSource> startTreeDataSources = new HashMap<>();

        for (String dimension: _starTreeV2ConfigList.get(i).getDimensions()) {
          DataSource source = loadTest.getDimensionDataSource(i, dimension);
          startTreeDataSources.put(dimension, source);
        }

        for (Met2AggfuncPair pair: _starTreeV2ConfigList.get(i).getMetric2aggFuncPairs()) {
          String metpair = pair.getMetricName() + "_" + pair.getAggregatefunction();
          DataSource source = loadTest.getMetricAggPairDataSource(i, metpair);
          startTreeDataSources.put(metpair, source);
        }

        DataSource source = loadTest.getMetricAggPairDataSource(i, "count");
        startTreeDataSources.put("count", source);
        _starTreeDataSourcesList.add(startTreeDataSources);

      }

      DataSource source = loadTest.getDimensionDataSource(0, "Name");
      StarTreeV2LoaderHelper.printDimensionDataFromDataSource(source);
      source = loadTest.getMetricAggPairDataSource(0, "salary_sum");
      StarTreeV2LoaderHelper.printMetricAggfuncDataFromDataSource(source);

      StarTreeV2ExecutorHelper.loadSegment(_filepath);
      for (int i = 0; i < _starTreeV2ConfigList.size(); i++) {
        StarTreeV2ExecutorHelper.execute(_starTreeV2ConfigList, _starTreeDataSourcesList, i);
      }

    } catch (Exception e) {
        e.printStackTrace();
    }
  }

  @Test
  public void testExecutor() throws Exception {
    try {
     // to be filled.
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}