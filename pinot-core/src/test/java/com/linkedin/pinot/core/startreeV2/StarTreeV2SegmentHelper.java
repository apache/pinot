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

package com.linkedin.pinot.core.startreeV2;

import java.io.File;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.core.data.readers.RecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;


public class StarTreeV2SegmentHelper {

  private static final int numRows = 6;
  private static final String[] names = {"Rahul", "Rahul", "Rahul", "Zackie", "Zackie", "Zackie"};
  private static final String[] country = {"IN", "IN", "CH", "CH", "CH", "US"};
  private static final String[] language = {"Hin", "Hin", "Eng", "Eng", "Eng", "Eng"};

  private static final int[] metricValues = {3, 5, 2, 8, 9, 1};

  public static Schema createSegmentSchema() {

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

  public static List<GenericRow> createSegmentData(Schema schema) throws Exception {

    List<GenericRow> rows = new ArrayList<>(numRows);
    for (int rowId = 0; rowId < numRows; rowId++) {
      HashMap<String, Object> map = new HashMap<>();

      // dimension
      String dimName = schema.getDimensionFieldSpecs().get(0).getName();
      map.put(dimName, names[rowId]);
      dimName = schema.getDimensionFieldSpecs().get(1).getName();
      map.put(dimName, country[rowId]);
      dimName = schema.getDimensionFieldSpecs().get(2).getName();
      map.put(dimName, language[rowId]);

      // metric
      String metName = schema.getMetricFieldSpecs().get(0).getName();
      map.put(metName, metricValues[rowId]);

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
