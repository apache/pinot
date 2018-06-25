/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import java.util.List;
import java.util.Random;
import java.util.HashMap;
import java.util.ArrayList;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.MetricFieldSpec;
import com.linkedin.pinot.common.data.DimensionFieldSpec;
import com.linkedin.pinot.core.data.readers.GenericRowRecordReader;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;


public class StarTreeV2SegmentHelper {

  private static final int numRows = 6;
  private static final int METRIC_MAX_VALUE = 10000;
  private static final Random RANDOM = new Random();

  private static final String [] names = {"Rahul", "Rahul", "Rahul", "Zackie", "Zackie", "Zackie"};
  private static final String [] country = {"IN", "IN", "CH", "CH", "CH", "US"};
  private static final String [] language = {"Hin", "Hin", "Eng", "Eng", "Eng", "Eng"};


  public static Schema buildSegment(String segmentDirName, String segmentName) throws Exception {
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


    SegmentGeneratorConfig config = new SegmentGeneratorConfig(schema);
    config.setOutDir(segmentDirName);
    config.setSegmentName(segmentName);

    List<GenericRow> rows = new ArrayList<>(numRows);
    for (int rowId = 0; rowId < numRows; rowId++) {
      HashMap<String, Object> map = new HashMap<>();

      // dimension
      dimName = schema.getDimensionFieldSpecs().get(0).getName();
      map.put(dimName, names[rowId]);
      dimName = schema.getDimensionFieldSpecs().get(1).getName();
      map.put(dimName, country[rowId]);
      dimName = schema.getDimensionFieldSpecs().get(2).getName();
      map.put(dimName, language[rowId]);

      // metric
      String metName = schema.getMetricFieldSpecs().get(0).getName();
      map.put(metName, RANDOM.nextInt(METRIC_MAX_VALUE));

      GenericRow genericRow = new GenericRow();
      genericRow.init(map);
      rows.add(genericRow);
    }

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(config, new GenericRowRecordReader(rows, schema));

    return schema;
  }
}
