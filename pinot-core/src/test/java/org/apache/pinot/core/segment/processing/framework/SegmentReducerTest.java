/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.segment.processing.framework;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import it.unimi.dsi.fastutil.booleans.BooleanComparator;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.collector.CollectorConfig;
import org.apache.pinot.core.segment.processing.collector.CollectorFactory;
import org.apache.pinot.core.segment.processing.collector.ValueAggregatorFactory;
import org.apache.pinot.core.segment.processing.utils.SegmentProcessorUtils;
import org.apache.pinot.plugin.inputformat.avro.AvroRecordReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Tests for {@link SegmentReducer}
 */
public class SegmentReducerTest {

  private File _baseDir;
  private File _partDir;
  private Schema _pinotSchema;
  private org.apache.avro.Schema _avroSchema;
  private final List<Object[]> _rawData1597795200000L = Lists
      .newArrayList(new Object[]{"abc", 4000, 1597795200000L}, new Object[]{"abc", 3000, 1597795200000L},
          new Object[]{"pqr", 1000, 1597795200000L}, new Object[]{"xyz", 4000, 1597795200000L},
          new Object[]{"pqr", 1000, 1597795200000L});
  private Comparator<Object[]> _comparator;

  @BeforeClass
  public void before()
      throws IOException {
    _baseDir = new File(FileUtils.getTempDirectory(), "segment_reducer_test_" + System.currentTimeMillis());
    FileUtils.deleteQuietly(_baseDir);
    assertTrue(_baseDir.mkdirs());

    // mapper output directory
    _partDir = new File(_baseDir, "mapper_output/1597795200000");
    assertTrue(_partDir.mkdirs());

    _pinotSchema = new Schema.SchemaBuilder().setSchemaName("mySchema")
        .addSingleValueDimension("campaign", FieldSpec.DataType.STRING).addMetric("clicks", FieldSpec.DataType.INT)
        .addDateTime("timeValue", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    _avroSchema = SegmentProcessorUtils.convertPinotSchemaToAvroSchema(_pinotSchema);

    // create 2 avro files
    DataFileWriter<GenericData.Record> recordWriter1 = new DataFileWriter<>(new GenericDatumWriter<>(_avroSchema));
    recordWriter1.create(_avroSchema, new File(_partDir, "map1.avro"));
    DataFileWriter<GenericData.Record> recordWriter2 = new DataFileWriter<>(new GenericDatumWriter<>(_avroSchema));
    recordWriter2.create(_avroSchema, new File(_partDir, "map2.avro"));
    for (int i = 0; i < 5; i++) {
      GenericData.Record record = new GenericData.Record(_avroSchema);
      record.put("campaign", _rawData1597795200000L.get(i)[0]);
      record.put("clicks", _rawData1597795200000L.get(i)[1]);
      record.put("timeValue", _rawData1597795200000L.get(i)[2]);
      if (i < 2) {
        recordWriter1.append(record);
      } else {
        recordWriter2.append(record);
      }
    }
    recordWriter1.close();
    recordWriter2.close();

    _comparator = (o1, o2) -> {
      int o = ((String) o1[0]).compareTo((String) o2[0]);
      return o == 0 ? (Integer.compare((int) o1[1], (int) o2[1])) : o;
    };
  }

  @Test(dataProvider = "segmentReducerDataProvider")
  public void segmentReducerTest(String reducerId, SegmentReducerConfig reducerConfig, Set<String> expectedFileNames,
      List<Object[]> expectedRecords)
      throws Exception {

    File reducerOutputDir = new File(_baseDir, "reducer_output");
    FileUtils.deleteQuietly(reducerOutputDir);
    assertTrue(reducerOutputDir.mkdirs());
    SegmentReducer segmentReducer = new SegmentReducer(reducerId, _partDir, reducerConfig, reducerOutputDir);
    segmentReducer.reduce();
    segmentReducer.cleanup();

    // check num expected files
    File[] avroFileNames = reducerOutputDir.listFiles();
    assertEquals(avroFileNames.length, expectedFileNames.size());

    GenericRow next = new GenericRow();
    int numRecords = 0;
    List<Object[]> actualRecords = new ArrayList<>();
    for (File avroFile : reducerOutputDir.listFiles()) {
      String fileName = avroFile.getName();
      assertTrue(expectedFileNames.contains(fileName));
      RecordReader avroRecordReader = new AvroRecordReader();
      avroRecordReader.init(avroFile, _pinotSchema.getColumnNames(), null);

      while (avroRecordReader.hasNext()) {
        avroRecordReader.next(next);
        actualRecords.add(new Object[]{next.getValue("campaign"), next.getValue("clicks"), next.getValue("timeValue")});
        numRecords++;
      }
    }
    assertEquals(numRecords, expectedRecords.size());
    actualRecords.sort(_comparator);
    for (int i = 0; i < numRecords; i++) {
      assertEquals(actualRecords.get(i)[0], expectedRecords.get(i)[0]);
      assertEquals(actualRecords.get(i)[1], expectedRecords.get(i)[1]);
      assertEquals(actualRecords.get(i)[2], expectedRecords.get(i)[2]);
    }

    FileUtils.deleteQuietly(reducerOutputDir);
  }

  @DataProvider(name = "segmentReducerDataProvider")
  public Object[][] segmentReducerDataProvider() {
    String reducerId = "aReducerId";
    List<Object[]> outputData = new ArrayList<>();
    _rawData1597795200000L.forEach(r -> outputData.add(new Object[]{r[0], r[1], r[2]}));
    outputData.sort(_comparator);

    List<Object[]> inputs = new ArrayList<>();

    // default - CONCAT
    SegmentReducerConfig config1 = new SegmentReducerConfig(_pinotSchema, new CollectorConfig.Builder().build(), 100);
    HashSet<String> expectedFileNames1 = Sets.newHashSet(SegmentReducer.createReducerOutputFileName(reducerId, 0));
    inputs.add(new Object[]{reducerId, config1, expectedFileNames1, outputData});

    // CONCAT, numRecordsPerPart = 2
    SegmentReducerConfig config2 = new SegmentReducerConfig(_pinotSchema, new CollectorConfig.Builder().build(), 2);
    HashSet<String> expectedFileNames2 = Sets.newHashSet(SegmentReducer.createReducerOutputFileName(reducerId, 0),
        SegmentReducer.createReducerOutputFileName(reducerId, 1),
        SegmentReducer.createReducerOutputFileName(reducerId, 2));
    inputs.add(new Object[]{reducerId, config2, expectedFileNames2, outputData});

    // ROLLUP - default aggregation
    SegmentReducerConfig config3 = new SegmentReducerConfig(_pinotSchema,
        new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP).build(), 100);
    HashSet<String> expectedFileNames3 = Sets.newHashSet(SegmentReducer.createReducerOutputFileName(reducerId, 0));
    List<Object> rollupRows = Lists
        .newArrayList(new Object[]{"abc", 7000, 1597795200000L}, new Object[]{"pqr", 2000, 1597795200000L},
            new Object[]{"xyz", 4000, 1597795200000L});
    inputs.add(new Object[]{reducerId, config3, expectedFileNames3, rollupRows});

    // ROLLUP MAX
    Map<String, ValueAggregatorFactory.ValueAggregatorType> valueAggregators = new HashMap<>();
    valueAggregators.put("clicks", ValueAggregatorFactory.ValueAggregatorType.MAX);
    SegmentReducerConfig config4 = new SegmentReducerConfig(_pinotSchema,
        new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP)
            .setAggregatorTypeMap(valueAggregators).build(), 100);
    HashSet<String> expectedFileNames4 = Sets.newHashSet(SegmentReducer.createReducerOutputFileName(reducerId, 0));
    List<Object> rollupRow4 = Lists
        .newArrayList(new Object[]{"abc", 4000, 1597795200000L}, new Object[]{"pqr", 1000, 1597795200000L},
            new Object[]{"xyz", 4000, 1597795200000L});
    inputs.add(new Object[]{reducerId, config4, expectedFileNames4, rollupRow4});

    // ROLLUP MAX, numRecordsPerPart = 2
    SegmentReducerConfig config5 = new SegmentReducerConfig(_pinotSchema,
        new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP)
            .setAggregatorTypeMap(valueAggregators).build(), 2);
    HashSet<String> expectedFileNames5 = Sets.newHashSet(SegmentReducer.createReducerOutputFileName(reducerId, 0),
        SegmentReducer.createReducerOutputFileName(reducerId, 1));
    List<Object> rollupRow5 = Lists
        .newArrayList(new Object[]{"abc", 4000, 1597795200000L}, new Object[]{"pqr", 1000, 1597795200000L},
            new Object[]{"xyz", 4000, 1597795200000L});
    inputs.add(new Object[]{reducerId, config5, expectedFileNames5, rollupRow5});

    return inputs.toArray(new Object[0][]);
  }

  @AfterClass
  public void after() {

  }
}
