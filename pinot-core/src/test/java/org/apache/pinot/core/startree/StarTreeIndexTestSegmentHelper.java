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
package org.apache.pinot.core.startree;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.apache.commons.math.util.MathUtils;
import org.apache.pinot.common.data.DimensionFieldSpec;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.MetricFieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.common.data.StarTreeIndexSpec;
import org.apache.pinot.common.data.TimeFieldSpec;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.readers.FileFormat;
import org.apache.pinot.core.data.readers.GenericRowRecordReader;
import org.apache.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.startree.hll.HllConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StarTreeIndexTestSegmentHelper {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeIndexTestSegmentHelper.class);
  private static final Random RANDOM = new Random();

  private static final String TIME_COLUMN_NAME = "daysSinceEpoch";
  private static final int NUM_DIMENSIONS = 4;
  private static final int NUM_METRICS = 2;
  private static final int METRIC_MAX_VALUE = 10000;

  public static Schema buildSegment(String segmentDirName, String segmentName)
      throws Exception {
    return buildSegment(segmentDirName, segmentName, null);
  }

  public static Schema buildSegmentWithHll(String segmentDirName, String segmentName, HllConfig hllConfig)
      throws Exception {
    return buildSegment(segmentDirName, segmentName, hllConfig);
  }

  private static Schema buildSegment(String segmentDirName, String segmentName, HllConfig hllConfig)
      throws Exception {
    int numRows = (int) MathUtils.factorial(NUM_DIMENSIONS) * 100;
    Schema schema = new Schema();

    for (int i = 0; i < NUM_DIMENSIONS; i++) {
      String dimName = "d" + (i + 1);
      DimensionFieldSpec dimensionFieldSpec = new DimensionFieldSpec(dimName, FieldSpec.DataType.STRING, true);
      schema.addField(dimensionFieldSpec);
    }

    schema.addField(new TimeFieldSpec(TIME_COLUMN_NAME, FieldSpec.DataType.INT, TimeUnit.DAYS));
    for (int i = 0; i < NUM_METRICS; i++) {
      String metricName = "m" + (i + 1);
      MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, FieldSpec.DataType.INT);
      schema.addField(metricFieldSpec);
    }

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    StarTreeIndexSpec starTreeIndexSpec = new StarTreeIndexSpec();
    starTreeIndexSpec.setMaxLeafRecords(10);
    config.enableStarTreeIndex(starTreeIndexSpec);
    config.setOutDir(segmentDirName);
    config.setFormat(FileFormat.AVRO);
    config.setSegmentName(segmentName);
    config.setHllConfig(hllConfig);
    config.setCheckTimeColumnValidityDuringGeneration(false);

    List<GenericRow> rows = new ArrayList<>(numRows);
    for (int rowId = 0; rowId < numRows; rowId++) {
      HashMap<String, Object> map = new HashMap<>();
      // Dim columns.
      for (int i = 0; i < NUM_DIMENSIONS / 2; i++) {
        String dimName = schema.getDimensionFieldSpecs().get(i).getName();
        map.put(dimName, dimName + "-v" + rowId % (NUM_DIMENSIONS - i));
      }
      // Random values make cardinality of d3, d4 column values larger to better test hll
      for (int i = NUM_DIMENSIONS / 2; i < NUM_DIMENSIONS; i++) {
        String dimName = schema.getDimensionFieldSpecs().get(i).getName();
        map.put(dimName, dimName + "-v" + RANDOM.nextInt(i * 100));
      }

      // Metric columns.
      for (int i = 0; i < NUM_METRICS; i++) {
        String metName = schema.getMetricFieldSpecs().get(i).getName();
        map.put(metName, RANDOM.nextInt(METRIC_MAX_VALUE));
      }

      // Time column.
      map.put(TIME_COLUMN_NAME, rowId % 7);

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows, schema));
    driver.build();

    LOGGER.info("Built segment {} at {}", segmentName, segmentDirName);
    return schema;
  }
}
