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

import com.google.common.base.Preconditions;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;
import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructType;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;


public class SegmentCreationSparkTest extends SharedJavaSparkContext implements Serializable {
  private static final File SAMPLE_DATA_FILE = new File(Preconditions
      .checkNotNull(SegmentCreationSparkTest.class.getClassLoader().getResource("test_sample_data.csv"))
      .getFile());

  private File _tempDir;

  @BeforeClass
  public void setup() {
    _tempDir = new File(FileUtils.getTempDirectory(), getClass().getSimpleName());
    _tempDir.mkdir();
    System.out.println("Temp dir: " + _tempDir.getAbsolutePath());
  }

  @Test
  public void testManualCreation() {
    String[] jars = new String[]{};
    java.util.Map<String, String> environment = new java.util.HashMap<>();
    new JavaSparkContext(new SparkConf().setMaster("local").setAppName("name")).stop();
    new JavaSparkContext("local", "name", new SparkConf()).stop();
    new JavaSparkContext("local", "name").stop();
    new JavaSparkContext("local", "name", "sparkHome", jars).stop();
    new JavaSparkContext("local", "name", "sparkHome", jars, environment).stop();
  }

  @Test
  public void testSegmentCreationInSpark() {
    String tableName = "testTableName";
    TableConfig tableConfig = new TableConfigBuilder(TableType.OFFLINE).setTableName(tableName).build();
    Schema tableSchema = new Schema.SchemaBuilder().setSchemaName(tableName).build();

    SparkConf conf = new SparkConf().setMaster("local").setAppName("test").set("spark.ui.enabled", "false");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    Dataset<Row> df = sqlContext.read().format("csv").load("file://" + SAMPLE_DATA_FILE.getAbsolutePath());
    StructType dfSchema = df.schema();
    JavaRDD<GenericRow> transformedRDD = df.javaRDD().map((Function<Row, GenericRow>) row -> {
      GenericRow genericRow = new GenericRow();
      String[] fieldNames = dfSchema.fieldNames();
      int size = row.size();
      for (int i = 0; i < size; i++) {
        genericRow.putValue(fieldNames[i], row.get(i));
      }
      return genericRow;
    });

    final List<String> expectedSegmentNames = new ArrayList<>();
    int numPartitions = transformedRDD.getNumPartitions();
    for (int i = 0; i < numPartitions; i++) {
      expectedSegmentNames.add("testSegment_" + i);
    }

    AtomicReference<Integer> count = new AtomicReference<>(0);
    transformedRDD.foreachPartition((VoidFunction<Iterator<GenericRow>>) genericRowIterator -> {
      List<GenericRow> genericRowList = new ArrayList<>();
      while (genericRowIterator.hasNext()) {
        genericRowList.add(genericRowIterator.next());
      }

      SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, tableSchema);
      segmentGeneratorConfig.setOutDir(_tempDir.getPath());
      segmentGeneratorConfig.setOnHeap(true);

      // Set segment name
      String segmentName = "testSegment_" + count.getAndUpdate(v -> v + 1);
      segmentGeneratorConfig.setSegmentName(segmentName);

      RecordReader recordReader = new GenericRowRecordReader(genericRowList);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(segmentGeneratorConfig, recordReader);
      driver.build();
    });

    String[] segments = _tempDir.list();
    assertNotNull(segments);
    assertEquals(segments.length, expectedSegmentNames.size());
    assertTrue(expectedSegmentNames.containsAll(Arrays.asList(segments)));
  }

  @AfterClass
  public void teardown() {
    _tempDir.delete();
  }
}
