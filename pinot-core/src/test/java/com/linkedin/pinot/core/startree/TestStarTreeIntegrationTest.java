/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.mutable.MutableLong;
import org.apache.commons.math.util.MathUtils;
import org.testng.annotations.Test;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.common.data.TimeFieldSpec;
import com.linkedin.pinot.common.request.BrokerRequest;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.data.FieldSpec.DataType;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.Constants;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.FileFormat;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.operator.filter.StarTreeIndexOperator;
import com.linkedin.pinot.core.plan.FilterPlanNode;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.pql.parsers.Pql2Compiler;


public class TestStarTreeIntegrationTest {
  @Test
  public void testSimple() throws Exception {
    int numDimensions = 4;
    int numMetrics = 2;
    int ROWS = (int) MathUtils.factorial(numDimensions);
    final Schema schema = new Schema();
    for (int i = 0; i < numDimensions; i++) {
      String dimName = "d" + (i + 1);
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(dimName, DataType.STRING, true);
      schema.addField(dimName, dimensionFieldSpec);
    }
    schema.setTimeFieldSpec(new TimeFieldSpec("daysSinceEpoch", DataType.INT, TimeUnit.DAYS));
    for (int i = 0; i < numMetrics; i++) {
      String metricName = "m" + (i + 1);
      MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, DataType.INT);
      schema.addField(metricName, metricFieldSpec);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setEnableStarTreeIndex(true);
    String tempOutputDir = "/tmp/star-tree-index";
    config.setOutDir(tempOutputDir);
    config.setFormat(FileFormat.AVRO);
    config.setSegmentName("testSimple");
    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    final List<GenericRow> data = new ArrayList<>();
    for (int row = 0; row < ROWS; row++) {
      HashMap<String, Object> map = new HashMap<>();
      for (int i = 0; i < numDimensions; i++) {
        String dimName = schema.getDimensionFieldSpecs().get(i).getName();
        map.put(dimName, dimName + "-v" + row % (numDimensions - i));
      }
      //time
      map.put("daysSinceEpoch", 1);
      for (int i = 0; i < numMetrics; i++) {
        String metName = schema.getMetricFieldSpecs().get(i).getName();
        map.put(metName, 1);
      }
      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      data.add(genericRow);
    }
    RecordReader reader = createReader(schema, data);
    driver.init(config, reader);
    driver.build();

    ReadMode mode = ReadMode.heap;
    //query to test
    String[] metricNames = new String[] { "m1" };
    String query = "select sum(m1) from T";
    Pql2Compiler compiler = new Pql2Compiler();
    BrokerRequest brokerRequest = compiler.compileToBrokerRequest(query);

    IndexSegment segment = Loaders.IndexSegment.load(new File(tempOutputDir, driver.getSegmentName()), mode);

    FilterPlanNode planNode = new FilterPlanNode(segment, brokerRequest);
    Operator rawOperator = planNode.run();
    BlockDocIdIterator rawDocIdIterator = rawOperator.nextBlock().getBlockDocIdSet().iterator();
    double[] expectedSums = computeSum(segment, rawDocIdIterator, metricNames);
    System.out.println("expectedSums=" + Arrays.toString(expectedSums));
    //dump contents
    Iterator<GenericRow> rowIterator =
        ((IndexSegmentImpl) segment).iterator(0, segment.getSegmentMetadata().getTotalDocs());
    int counter = 0;
    while (rowIterator.hasNext()) {

      GenericRow genericRow = rowIterator.next();
      StringBuilder sb = new StringBuilder().append(counter++).append(": \t");
      for (String dimName : schema.getDimensionNames()) {
        sb.append(dimName).append(":").append(genericRow.getValue(dimName)).append(", ");
      }
      if (schema.getTimeColumnName() != null) {
        sb.append(schema.getTimeColumnName()).append(":").append(genericRow.getValue(schema.getTimeColumnName()))
            .append(", ");
      }
      for (String metName : schema.getMetricNames()) {
        sb.append(metName).append(":").append(genericRow.getValue(metName)).append(", ");
      }
      System.out.println(sb);
    }

    StarTreeIndexOperator starTreeOperator = new StarTreeIndexOperator(segment, brokerRequest);
    starTreeOperator.open();
    BlockDocIdIterator starTreeDocIdIterator = starTreeOperator.nextBlock().getBlockDocIdSet().iterator();

    double[] actualSums = computeSum(segment, starTreeDocIdIterator, metricNames);
    System.out.println("actualSums=" + Arrays.toString(actualSums));
  }

  private double[] computeSum(IndexSegment segment, BlockDocIdIterator docIdIterator, String... metricNames) {
    int docId;
    int numMetrics = metricNames.length;
    Dictionary[] dictionaries = new Dictionary[numMetrics];
    BlockSingleValIterator[] valIterators = new BlockSingleValIterator[numMetrics];
    for (int i = 0; i < numMetrics; i++) {
      String metricName = metricNames[i];
      DataSource dataSource = segment.getDataSource(metricName);
      dictionaries[i] = dataSource.getDictionary();
      valIterators[i] = (BlockSingleValIterator) dataSource.getNextBlock().getBlockValueSet().iterator();
    }
    double[] sums = new double[metricNames.length];
    while ((docId = docIdIterator.next()) != Constants.EOF) {
      System.out.println("docId:" + docId);
      for (int i = 0; i < numMetrics; i++) {
        valIterators[i].skipTo(docId);
        int dictId = valIterators[i].nextIntVal();
        sums[i] += dictionaries[i].getDoubleValue(dictId);
      }
    }
    return sums;
  }

  private RecordReader createReader(final Schema schema, final List<GenericRow> data) {
    return new RecordReader() {

      int counter = 0;

      @Override
      public void rewind() throws Exception {
        counter = 0;
      }

      @Override
      public GenericRow next() {
        return data.get(counter++);
      }

      @Override
      public void init() throws Exception {

      }

      @Override
      public boolean hasNext() {
        return counter < data.size();
      }

      @Override
      public Schema getSchema() {
        return schema;
      }

      @Override
      public Map<String, MutableLong> getNullCountMap() {
        return null;
      }

      @Override
      public void close() throws Exception {

      }
    };
  }

}
