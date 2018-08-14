/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree.v2;

import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Random;


public class StarTreeV2SegmentHelper {

  private static final String[] names = {"Rahul", "Jennifer", "Neha", "Subbu", "Sunita", "Zackie"};
  private static final String[] country = {"IN", "MX", "CA", "CH", "CH", "US"};
  private static final String[] language = {"Hin", "Hin", "Eng", "Eng", "Kor", "Eng"};
  private static final int[] metricValues = {3, 5, 2, 8, 9, 1};

  private static final Random RANDOM = new Random();

  static Schema createSegmentSchema() {

    Schema schema = new Schema();

    // dimension
    String dimName = "Name";
    DimensionFieldSpec dimensionFieldSpec1 = new DimensionFieldSpec(dimName, FieldSpec.DataType.STRING, true);
    schema.addField(dimensionFieldSpec1);

    dimName = "Country";
    DimensionFieldSpec dimensionFieldSpec2 = new DimensionFieldSpec(dimName, FieldSpec.DataType.STRING, true);
    schema.addField(dimensionFieldSpec2);

    dimName = "Language";
    DimensionFieldSpec dimensionFieldSpec3 = new DimensionFieldSpec(dimName, FieldSpec.DataType.STRING, true);
    schema.addField(dimensionFieldSpec3);

    // metric
    String metricName = "salary";
    MetricFieldSpec metricFieldSpec = new MetricFieldSpec(metricName, FieldSpec.DataType.INT);
    schema.addField(metricFieldSpec);

    return schema;
  }

  public static List<GenericRow> createSegmentSmallData(Schema schema, int rowsCount) throws Exception {

    int index;
    List<GenericRow> rows = new ArrayList<>(rowsCount);
    for (int rowId = 0; rowId < rowsCount; rowId++) {
      HashMap<String, Object> map = new HashMap<>();

      // Dim columns.
      for (int i = 0; i < 3; i++) {
        String dimName = schema.getDimensionFieldSpecs().get(i).getName();
        switch (i) {
          case 0:
            index = RANDOM.nextInt(6);
            map.put(dimName, names[index]);
            break;
          case 1:
            index = RANDOM.nextInt(6);
            map.put(dimName, country[index]);
            break;
          case 2:
            index = RANDOM.nextInt(6);
            map.put(dimName, language[index]);
            break;
        }
      }

      // Metric columns.
      for (int i = 0; i < 1; i++) {
        String metName = schema.getMetricFieldSpecs().get(i).getName();
        map.put(metName, metricValues[RANDOM.nextInt(6)]);
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    return rows;
  }

  public static List<GenericRow> createSegmentData(Schema schema, int rowsCount) throws Exception {
    List<GenericRow> rows = new ArrayList<>(rowsCount);
    for (int rowId = 0; rowId < rowsCount; rowId++) {
      HashMap<String, Object> map = new HashMap<>();

      // Dim columns.
      for (int i = 0; i < 3; i++) {
        String dimName = schema.getDimensionFieldSpecs().get(i).getName();
        map.put(dimName, dimName + "-v" + RANDOM.nextInt(100));
      }

      // Metric columns.
      for (int i = 0; i < 1; i++) {
        String metName = schema.getMetricFieldSpecs().get(i).getName();
        map.put(metName, RANDOM.nextInt(10000));
      }

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    return rows;
  }

  public static File createSegment(Schema schema, String segmentName, String segmentOutputDir,
      RecordReader recordReader) throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setTableName(segmentName);
    segmentGeneratorConfig.setOutDir(segmentOutputDir);
    segmentGeneratorConfig.setSegmentName(segmentName);
    segmentGeneratorConfig.setEnableStarTreeIndex(false);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, recordReader);
    driver.build();
    File segmentIndexDir = new File(segmentOutputDir, segmentName);

    return segmentIndexDir;
  }
}
