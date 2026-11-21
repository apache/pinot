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

package org.apache.pinot.controller.recommender.realtime.provisioning;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.controller.recommender.io.metadata.SchemaWithMetaData;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.utils.DataSizeUtils;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


public class MemoryEstimatorTest {

  @Test
  public void testSegmentGenerator()
      throws Exception {
    runTest("memory_estimation/schema-with-metadata.json", metadata -> {
      assertEquals(extract(metadata, "segment.total.docs = (\\d+)"), "100000");
      assertEquals(extract(metadata, "column.colInt.cardinality = (\\d+)"), "100");
      assertEquals(extract(metadata, "column.colIntMV.cardinality = (\\d+)"), "150");
      assertEquals(extract(metadata, "column.colFloat.cardinality = (\\d+)"), "200");
      assertEquals(extract(metadata, "column.colFloatMV.cardinality = (\\d+)"), "250");
      assertEquals(extract(metadata, "column.colString.cardinality = (\\d+)"), "300");
      assertEquals(extract(metadata, "column.colStringMV.cardinality = (\\d+)"), "350");
      assertEquals(extract(metadata, "column.colBytes.cardinality = (\\d+)"), "400");
      assertEquals(extract(metadata, "column.colLong.cardinality = (\\d+)"), "500");
      assertEquals(extract(metadata, "column.colLongMV.cardinality = (\\d+)"), "550");
      assertEquals(extract(metadata, "column.colDouble.cardinality = (\\d+)"), "600");
      assertEquals(extract(metadata, "column.colDoubleMV.cardinality = (\\d+)"), "650");
      assertEquals(extract(metadata, "column.colDoubleMetric.cardinality = (\\d+)"), "700");
      assertEquals(extract(metadata, "column.colFloatMetric.cardinality = (\\d+)"), "800");
      assertEquals(extract(metadata, "column.colTime.cardinality = (\\d+)"), "900");
      assertEquals(extract(metadata, "column.colInt.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colIntMV.maxNumberOfMultiValues = (\\d+)"), "3");
      assertEquals(extract(metadata, "column.colFloat.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colFloatMV.maxNumberOfMultiValues = (\\d+)"), "2");
      assertEquals(extract(metadata, "column.colString.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colStringMV.maxNumberOfMultiValues = (\\d+)"), "2");
      assertEquals(extract(metadata, "column.colBytes.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colLong.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colLongMV.maxNumberOfMultiValues = (\\d+)"), "3");
      assertEquals(extract(metadata, "column.colDouble.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colDoubleMV.maxNumberOfMultiValues = (\\d+)"), "4");
      assertEquals(extract(metadata, "column.colDoubleMetric.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colFloatMetric.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colTime.maxNumberOfMultiValues = (\\d+)"), "0");
    });
  }

  @Test
  public void testSegmentGeneratorWithDateTimeFieldSpec()
      throws Exception {
    runTest("memory_estimation/schema-with-metadata__dateTimeFieldSpec.json", metadata -> {
      assertEquals(extract(metadata, "segment.total.docs = (\\d+)"), "100000");
      assertEquals(extract(metadata, "column.colInt.cardinality = (\\d+)"), "500");
      assertEquals(extract(metadata, "column.colFloat.cardinality = (\\d+)"), "600");
      assertEquals(extract(metadata, "column.colString.cardinality = (\\d+)"), "700");
      assertEquals(extract(metadata, "column.colBytes.cardinality = (\\d+)"), "800");
      assertEquals(extract(metadata, "column.colMetric.cardinality = (\\d+)"), "900");
      assertEquals(extract(metadata, "column.colTime.cardinality = (\\d+)"), "250");
      assertEquals(extract(metadata, "column.colTime2.cardinality = (\\d+)"), "750");
      assertEquals(extract(metadata, "column.colTime3.cardinality = (\\d+)"), "850");
      assertEquals(extract(metadata, "column.colInt.maxNumberOfMultiValues = (\\d+)"), "3");
      assertEquals(extract(metadata, "column.colFloat.maxNumberOfMultiValues = (\\d+)"), "2");
      assertEquals(extract(metadata, "column.colString.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colBytes.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colMetric.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colTime.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colTime2.maxNumberOfMultiValues = (\\d+)"), "0");
      assertEquals(extract(metadata, "column.colTime3.maxNumberOfMultiValues = (\\d+)"), "0");
    });
  }

  @Test
  public void testMemoryLimitsLeaveEstimateValuesVisible()
      throws Exception {
    File fixtureDir = Files.createTempDirectory("memory-estimator-limit").toFile();
    try {
      File schemaFile = readFile("memory_estimation/schema-with-metadata.json");
      Schema schema = JsonUtils.fileToObject(schemaFile, Schema.class);
      SchemaWithMetaData schemaWithMetadata = JsonUtils.fileToObject(schemaFile, SchemaWithMetaData.class);
      TableConfig tableConfig = JsonUtils.fileToObject(readFile("memory_estimation/table-config.json"),
          TableConfig.class);
      File sample = new MemoryEstimator.SegmentGenerator(schemaWithMetadata, schema, tableConfig, 1000, true,
          fixtureDir).generate();

      MemoryEstimator baseline = estimateMemory(fixtureDir, sample, schema, tableConfig, Long.MAX_VALUE);
      String[][] memory = baseline.getActiveMemoryPerHost();
      String[] activeAndMapped = memory[0][0].split("/");
      long active = DataSizeUtils.toBytes(activeAndMapped[0]);
      long mapped = DataSizeUtils.toBytes(activeAndMapped[1]);
      assertTrue(mapped > 2 * active, "Expected ample separation between active and mapped memory");
      assertEquals(memory[1], new String[]{MemoryEstimator.NOT_APPLICABLE, MemoryEstimator.NOT_APPLICABLE});
      assertEquals(baseline.getWarnings().size(), 1);
      assertTrue(baseline.getWarnings().get(0).contains("retention hours"));

      long limitBetweenActiveAndMapped = active + (mapped - active) / 2;
      MemoryEstimator mappedOnly = estimateMemory(fixtureDir, sample, schema, tableConfig, limitBetweenActiveAndMapped);
      assertEquals(mappedOnly.getWarnings(), baseline.getWarnings(), "Mapped memory alone should not warn");

      MemoryEstimator overLimit = estimateMemory(fixtureDir, sample, schema, tableConfig, 1);
      assertEquals(overLimit.getActiveMemoryPerHost()[0], memory[0]);
      assertEquals(overLimit.getOptimalSegmentSize()[0], baseline.getOptimalSegmentSize()[0]);
      assertEquals(overLimit.getConsumingMemoryPerHost()[0], baseline.getConsumingMemoryPerHost()[0]);
      assertEquals(overLimit.getNumRowsInSegment()[0], baseline.getNumRowsInSegment()[0]);
      assertEquals(overLimit.getNumSegmentsQueriedPerHost()[0], baseline.getNumSegmentsQueriedPerHost()[0]);
      assertEquals(overLimit.getActiveMemoryPerHost()[1], memory[1]);
      assertEquals(overLimit.getWarnings().size(), 2);
      assertTrue(overLimit.getWarnings().get(0).contains("at numHours=1,numHosts=1) exceeds max usable host memory"));
      assertEquals(overLimit.getWarnings().get(1), baseline.getWarnings().get(0));
    } finally {
      FileUtils.deleteDirectory(fixtureDir);
    }
  }

  private MemoryEstimator estimateMemory(File fixtureDir, File sample, Schema schema, TableConfig tableConfig,
      long hostLimit)
      throws Exception {
    File workingDir = Files.createTempDirectory(fixtureDir.toPath(), "estimate-").toFile();
    MemoryEstimator estimator = new MemoryEstimator(tableConfig, schema, sample, 1, hostLimit, 120, workingDir);
    estimator.estimateMemoryUsed(estimator.initializeStatsHistory(), new int[]{1, 2}, new int[]{1, 2}, 2, 1);
    return estimator;
  }

  private void runTest(String schemaFileName, Consumer<String> assertFunc)
      throws Exception {

    // arrange inputs
    File workingDir = Files.createTempDirectory("working-dir").toFile();
    File schemaFile = readFile(schemaFileName);
    File tableConfigFile = readFile("memory_estimation/table-config.json");
    Schema schema = JsonUtils.fileToObject(schemaFile, Schema.class);
    SchemaWithMetaData schemaWithMetadata = JsonUtils.fileToObject(schemaFile, SchemaWithMetaData.class);
    TableConfig tableConfig = JsonUtils.fileToObject(tableConfigFile, TableConfig.class);
    int numberOfRows = 100_000;

    // act
    MemoryEstimator.SegmentGenerator segmentGenerator =
        new MemoryEstimator.SegmentGenerator(schemaWithMetadata, schema, tableConfig, numberOfRows, true, workingDir);
    File generatedSegment = segmentGenerator.generate();

    // assert
    Path metadataFile = Paths.get(generatedSegment.getPath(), "v3", "metadata.properties");
    String metadata = new String(Files.readAllBytes(metadataFile));
    assertFunc.accept(metadata);

    // cleanup
    FileUtils.deleteDirectory(workingDir);
  }

  private String extract(String metadataContent, String patternStr) {
    Pattern pattern = Pattern.compile(patternStr);
    Matcher matcher = pattern.matcher(metadataContent);
    matcher.find();
    return matcher.group(1);
  }

  private File readFile(String fileName)
      throws Exception {
    URL resource = getClass().getClassLoader().getResource(fileName);
    return new File(resource.toURI());
  }
}
