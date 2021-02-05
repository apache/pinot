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

package org.apache.pinot.tools.realtime.provisioning;

import java.io.File;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.utils.JsonUtils;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;


public class MemoryEstimatorTest {

  @Test
  public void testSegmentGenerator() throws Exception {

    // arrange inputs
    File schemaFile = readFile("memory_estimation/schema.json");
    File dataCharacteristicsFile = readFile("memory_estimation/data-characteristics.json");
    File tableConfigFile = readFile("memory_estimation/table-config.json");
    TableConfig tableConfig = JsonUtils.fileToObject(tableConfigFile, TableConfig.class);

    // act
    MemoryEstimator.SegmentGenerator
        segmentGenerator = new MemoryEstimator.SegmentGenerator(dataCharacteristicsFile, schemaFile, tableConfig, true);
    File generatedSegment = segmentGenerator.generate();

    // assert
    Path metadataFile = Paths.get(generatedSegment.getPath(), "v3", "metadata.properties");
    String metadata = new String(Files.readAllBytes(metadataFile));
    assertEquals(extract(metadata, "segment.total.docs = (\\d+)"), "10000");
    assertEquals(extract(metadata, "column.colInt.cardinality = (\\d+)"), "500");
    assertEquals(extract(metadata, "column.colFloat.cardinality = (\\d+)"), "600");
    assertEquals(extract(metadata, "column.colString.cardinality = (\\d+)"), "700");
    assertEquals(extract(metadata, "column.colBytes.cardinality = (\\d+)"), "800");
    assertEquals(extract(metadata, "column.colMetric.cardinality = (\\d+)"), "900");
    assertEquals(extract(metadata, "column.colTime.cardinality = (\\d+)"), "250");
    assertEquals(extract(metadata, "column.colInt.maxNumberOfMultiValues = (\\d+)"), "3");
    assertEquals(extract(metadata, "column.colFloat.maxNumberOfMultiValues = (\\d+)"), "2");
    assertEquals(extract(metadata, "column.colString.maxNumberOfMultiValues = (\\d+)"), "0");
    assertEquals(extract(metadata, "column.colBytes.maxNumberOfMultiValues = (\\d+)"), "0");
    assertEquals(extract(metadata, "column.colMetric.maxNumberOfMultiValues = (\\d+)"), "0");
    assertEquals(extract(metadata, "column.colTime.maxNumberOfMultiValues = (\\d+)"), "0");

    // cleanup
    FileUtils.deleteDirectory(generatedSegment);
  }

  private String extract(String metadataContent, String patternStr) {
    Pattern pattern = Pattern.compile(patternStr);
    Matcher matcher = pattern.matcher(metadataContent);
    matcher.find();
    return matcher.group(1);
  }

  private File readFile(String fileName) throws Exception {
    URL resource = getClass().getClassLoader().getResource(fileName);
    return new File(resource.toURI());
  }
}
