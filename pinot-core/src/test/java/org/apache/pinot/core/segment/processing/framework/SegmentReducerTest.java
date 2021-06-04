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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.segment.processing.collector.CollectorConfig;
import org.apache.pinot.core.segment.processing.collector.CollectorFactory;
import org.apache.pinot.core.segment.processing.collector.ValueAggregatorFactory;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileManager;
import org.apache.pinot.core.segment.processing.genericrow.GenericRowFileWriter;
import org.apache.pinot.core.segment.processing.utils.SegmentProcessingUtils;
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
  private static final File TEMP_DIR = new File(FileUtils.getTempDirectory(), "SegmentReducerTest");

  private final File _mapperOutputDir = new File(TEMP_DIR, "mapper_output/1597795200000");
  private final Schema _pinotSchema = new Schema.SchemaBuilder().setSchemaName("mySchema")
      .addSingleValueDimension("campaign", FieldSpec.DataType.STRING).addMetric("clicks", FieldSpec.DataType.INT)
      .addDateTime("timeValue", FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
  private final List<Object[]> _rawData1597795200000L = Arrays
      .asList(new Object[]{"abc", 4000, 1597795200000L}, new Object[]{"abc", 3000, 1597795200000L},
          new Object[]{"pqr", 1000, 1597795200000L}, new Object[]{"xyz", 4000, 1597795200000L},
          new Object[]{"pqr", 1000, 1597795200000L});

  private GenericRowFileManager _fileManager;

  @BeforeClass
  public void setUp()
      throws IOException {
    FileUtils.deleteQuietly(TEMP_DIR);
    assertTrue(_mapperOutputDir.mkdirs());

    List<FieldSpec> fieldSpecs = SegmentProcessingUtils.getFieldSpecs(_pinotSchema);
    _fileManager = new GenericRowFileManager(_mapperOutputDir, fieldSpecs, false);
    GenericRowFileWriter fileWriter = _fileManager.getFileWriter();
    GenericRow reuse = new GenericRow();
    for (int i = 0; i < 5; i++) {
      reuse.putValue("campaign", _rawData1597795200000L.get(i)[0]);
      reuse.putValue("clicks", _rawData1597795200000L.get(i)[1]);
      reuse.putValue("timeValue", _rawData1597795200000L.get(i)[2]);
      fileWriter.write(reuse);
      reuse.clear();
    }
    _fileManager.closeFileWriter();
  }

  @Test(dataProvider = "segmentReducerDataProvider")
  public void segmentReducerTest(String reducerId, SegmentReducerConfig reducerConfig, Set<String> expectedFileNames,
      List<Object[]> expectedRecords, Comparator comparator)
      throws Exception {
    File reducerOutputDir = new File(TEMP_DIR, "reducer_output");
    FileUtils.deleteQuietly(reducerOutputDir);
    assertTrue(reducerOutputDir.mkdirs());
    SegmentReducer segmentReducer = new SegmentReducer(reducerId, _fileManager, reducerConfig, reducerOutputDir);
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
    if (comparator != null) {
      // for runs with no sort order, apply same comparator across expected and actual to help with comparison
      actualRecords.sort(comparator);
    }
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

    Comparator<Object[]> comparator =
        Comparator.comparing((Object[] o) -> (String) o[0]).thenComparingInt(o -> (int) o[1]);
    outputData.sort(comparator);

    List<Object[]> inputs = new ArrayList<>();

    // default - CONCAT
    SegmentReducerConfig config1 = new SegmentReducerConfig(_pinotSchema, new CollectorConfig.Builder().build(), 100);
    HashSet<String> expectedFileNames1 = Sets.newHashSet(SegmentReducer.createReducerOutputFileName(reducerId, 0));
    inputs.add(new Object[]{reducerId, config1, expectedFileNames1, outputData, comparator});

    // CONCAT, numRecordsPerPart = 2
    SegmentReducerConfig config2 = new SegmentReducerConfig(_pinotSchema, new CollectorConfig.Builder().build(), 2);
    HashSet<String> expectedFileNames2 = Sets.newHashSet(SegmentReducer.createReducerOutputFileName(reducerId, 0),
        SegmentReducer.createReducerOutputFileName(reducerId, 1),
        SegmentReducer.createReducerOutputFileName(reducerId, 2));
    inputs.add(new Object[]{reducerId, config2, expectedFileNames2, outputData, comparator});

    // ROLLUP - default aggregation
    SegmentReducerConfig config3 = new SegmentReducerConfig(_pinotSchema,
        new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP).build(), 100);
    HashSet<String> expectedFileNames3 = Sets.newHashSet(SegmentReducer.createReducerOutputFileName(reducerId, 0));
    List<Object> rollupRows3 = Lists
        .newArrayList(new Object[]{"abc", 7000, 1597795200000L}, new Object[]{"pqr", 2000, 1597795200000L},
            new Object[]{"xyz", 4000, 1597795200000L});
    inputs.add(new Object[]{reducerId, config3, expectedFileNames3, rollupRows3, comparator});

    // ROLLUP MAX
    Map<String, ValueAggregatorFactory.ValueAggregatorType> valueAggregators = new HashMap<>();
    valueAggregators.put("clicks", ValueAggregatorFactory.ValueAggregatorType.MAX);
    SegmentReducerConfig config4 = new SegmentReducerConfig(_pinotSchema,
        new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP)
            .setAggregatorTypeMap(valueAggregators).build(), 100);
    HashSet<String> expectedFileNames4 = Sets.newHashSet(SegmentReducer.createReducerOutputFileName(reducerId, 0));
    List<Object> rollupRows4 = Lists
        .newArrayList(new Object[]{"abc", 4000, 1597795200000L}, new Object[]{"pqr", 1000, 1597795200000L},
            new Object[]{"xyz", 4000, 1597795200000L});
    inputs.add(new Object[]{reducerId, config4, expectedFileNames4, rollupRows4, comparator});

    // CONCAT and sort
    SegmentReducerConfig config6 = new SegmentReducerConfig(_pinotSchema,
        new CollectorConfig.Builder().setSortOrder(Lists.newArrayList("campaign", "clicks")).build(), 100);
    HashSet<String> expectedFileNames6 = Sets.newHashSet(SegmentReducer.createReducerOutputFileName(reducerId, 0));
    inputs.add(new Object[]{reducerId, config6, expectedFileNames6, outputData, null});

    // ROLLUP and sort
    SegmentReducerConfig config7 = new SegmentReducerConfig(_pinotSchema,
        new CollectorConfig.Builder().setCollectorType(CollectorFactory.CollectorType.ROLLUP)
            .setSortOrder(Lists.newArrayList("campaign", "clicks")).build(), 100);
    HashSet<String> expectedFileNames7 = Sets.newHashSet(SegmentReducer.createReducerOutputFileName(reducerId, 0));
    List<Object> rollupRows7 = Lists
        .newArrayList(new Object[]{"abc", 7000, 1597795200000L}, new Object[]{"pqr", 2000, 1597795200000L},
            new Object[]{"xyz", 4000, 1597795200000L});
    inputs.add(new Object[]{reducerId, config7, expectedFileNames7, rollupRows7, null});

    return inputs.toArray(new Object[0][]);
  }

  @AfterClass
  public void tearDown()
      throws IOException {
    _fileManager.cleanUp();
    FileUtils.deleteQuietly(TEMP_DIR);
  }
}
