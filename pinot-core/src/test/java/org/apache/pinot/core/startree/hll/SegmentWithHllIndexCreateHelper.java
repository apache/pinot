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
package org.apache.pinot.core.startree.hll;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.MetricFieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.common.data.StarTreeIndexSpec;
import org.apache.pinot.core.data.readers.FileFormat;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.segment.creator.SegmentIndexCreationDriver;
import org.apache.pinot.core.segment.creator.impl.SegmentCreationDriverFactory;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segments.v1.creator.SegmentTestUtils;
import org.apache.pinot.startree.hll.HllConfig;
import org.apache.pinot.util.TestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class SegmentWithHllIndexCreateHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(SegmentWithHllIndexCreateHelper.class);

  private final String tableName;
  private final File INDEX_DIR;
  private final File inputAvro;
  private final String timeColumnName;
  private final TimeUnit timeUnit;
  private String segmentName;
  private Schema schema;

  public SegmentWithHllIndexCreateHelper(String tableName, URL avroUrl, String timeColumnName, TimeUnit timeUnit,
      String segmentName)
      throws IOException {
    this(tableName, TestUtils.getFileFromResourceUrl(avroUrl), timeColumnName, timeUnit, segmentName);
  }

  public SegmentWithHllIndexCreateHelper(String tableName, String avroDataPath, String timeColumnName,
      TimeUnit timeUnit, String segmentName)
      throws IOException {
    INDEX_DIR = Files.createTempDirectory(SegmentWithHllIndexCreateHelper.class.getName() + "_" + tableName).toFile();
    LOGGER.info("INDEX_DIR: {}", INDEX_DIR.getAbsolutePath());
    inputAvro = new File(avroDataPath);
    LOGGER.info("Input Avro: {}", inputAvro.getAbsolutePath());
    this.timeColumnName = timeColumnName;
    this.timeUnit = timeUnit;
    this.tableName = tableName;
    this.segmentName = segmentName;
  }

  /**
   * must call this to clean up
   */
  public void cleanTempDir() {
    if (INDEX_DIR != null) {
      FileUtils.deleteQuietly(INDEX_DIR);
    }
  }

  private static void printSchema(Schema schema) {
    LOGGER.info("schemaName: {}", schema.getSchemaName());
    LOGGER.info("Dimension columnNames: ");
    int i = 0;
    for (DimensionFieldSpec spec : schema.getDimensionFieldSpecs()) {
      String columnInfo = i + " " + spec.getName();
      if (!spec.isSingleValueField()) {
        LOGGER.info(columnInfo + " Multi-Value.");
      } else {
        LOGGER.info(columnInfo);
      }
      i += 1;
    }
    LOGGER.info("Metric columnNames: ");
    i = 0;
    for (MetricFieldSpec spec : schema.getMetricFieldSpecs()) {
      String columnInfo = i + " " + spec.getName();
      if (!spec.isSingleValueField()) {
        LOGGER.info(columnInfo + " Multi-Value.");
      } else {
        LOGGER.info(columnInfo);
      }
      i += 1;
    }
    LOGGER.info("Time column: {}", schema.getTimeColumnName());
  }

  private void setupStarTreeConfig(SegmentGeneratorConfig segmentGenConfig) {
    // StarTree related
    StarTreeIndexSpec starTreeIndexSpec = new StarTreeIndexSpec();
    starTreeIndexSpec.setMaxLeafRecords(StarTreeIndexSpec.DEFAULT_MAX_LEAF_RECORDS);
    segmentGenConfig.enableStarTreeIndex(starTreeIndexSpec);
    LOGGER.info("segmentGenConfig Schema (w/o derived fields): ");
    printSchema(segmentGenConfig.getSchema());
  }

  public SegmentIndexCreationDriver build(boolean enableStarTree, HllConfig hllConfig)
      throws Exception {
    final SegmentGeneratorConfig segmentGenConfig =
        new SegmentGeneratorConfig(SegmentTestUtils.extractSchemaFromAvroWithoutTime(inputAvro));

    // set other fields in segmentGenConfig
    segmentGenConfig.setInputFilePath(inputAvro.getAbsolutePath());
    segmentGenConfig.setTimeColumnName(timeColumnName);
    segmentGenConfig.setSegmentTimeUnit(timeUnit);
    segmentGenConfig.setFormat(FileFormat.AVRO);
    segmentGenConfig.setSegmentVersion(SegmentVersion.v1);
    segmentGenConfig.setTableName(tableName);
    segmentGenConfig.setOutDir(INDEX_DIR.getAbsolutePath());
    segmentGenConfig.createInvertedIndexForAllColumns();
    segmentGenConfig.setSegmentName(segmentName);
    segmentGenConfig.setSegmentNamePostfix("1");
    // The segment generation code in SegmentColumnarIndexCreator will throw
    // exception if start and end time in time column are not in acceptable
    // range. For this test, we first need to fix the input avro data
    // to have the time column values in allowed range. Until then, the check
    // is explicitly disabled
    segmentGenConfig.setSkipTimeValueCheck(true);

    if (enableStarTree) {
      setupStarTreeConfig(segmentGenConfig);
      segmentGenConfig.setHllConfig(hllConfig);
    }

    if (hllConfig != null) {
      segmentGenConfig.setHllConfig(hllConfig);
    }

    final SegmentIndexCreationDriver driver = SegmentCreationDriverFactory.get(null);
    driver.init(segmentGenConfig);
    /**
     * derived field (hll) is added during the segment build process
     *
     * {@link SegmentIndexCreationDriverImpl#buildStarTree}
     * {@link SegmentIndexCreationDriverImpl#augmentSchemaWithDerivedColumns}
     * {@link SegmentIndexCreationDriverImpl#populateDefaultDerivedColumnValues}
     */
    driver.build();

    LOGGER.info("segmentGenConfig Schema (w/ derived fields): ");
    schema = segmentGenConfig.getSchema();
    printSchema(schema);

    return driver;
  }

  public Schema getSchema() {
    if (schema == null) {
      throw new RuntimeException("Call build first to get schema.");
    }
    return schema;
  }

  public String getSegmentName() {
    return segmentName;
  }

  public File getSegmentDirectory() {
    return new File(INDEX_DIR, segmentName);
  }

  public File getIndexDir() {
    return INDEX_DIR;
  }
}
