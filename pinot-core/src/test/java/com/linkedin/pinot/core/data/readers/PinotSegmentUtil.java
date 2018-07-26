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
package com.linkedin.pinot.core.data.readers;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.generator.SegmentGeneratorConfig;
import com.linkedin.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.apache.commons.lang3.RandomStringUtils;


/**
 * Util class for pinot segment
 */
public class PinotSegmentUtil {
  private static int DEFAULT_NUM_MULTIVALUE = 5;
  private static int DEFAULT_STRING_VALUE_LENGTH = 2;

  private PinotSegmentUtil() {
  }

  public static List<GenericRow> createTestData(Schema schema, int numRows) {
    List<GenericRow> rows = new ArrayList<>();
    final Random random = new Random();
    Map<String, Object> fields;
    for (int i = 0; i < numRows; i++) {
      fields = new HashMap<>();
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        Object value;
        if (fieldSpec.isSingleValueField()) {
          value = generateSingleValue(random, fieldSpec.getDataType());
        } else {
          value = generateMultiValue(random, fieldSpec.getDataType());
        }
        fields.put(fieldSpec.getName(), value);
      }
      GenericRow row = new GenericRow();
      row.init(fields);
      rows.add(row);
    }
    return rows;
  }

  static boolean compareMultiValueColumn(Object value1, Object value2) {
    Object[] value1Array = (Object[]) value1;
    Object[] value2Array = (Object[]) value2;
    Set<Object> value1Set = new HashSet<>(Arrays.asList(value1Array));
    Set<Object> value2Set = new HashSet<>(Arrays.asList(value2Array));
    return value1Set.containsAll(value2Set);
  }

  static File createSegment(Schema schema, String segmentName, String segmentOutputDir, RecordReader recordReader)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(schema);
    segmentGeneratorConfig.setTableName(segmentName);
    segmentGeneratorConfig.setOutDir(segmentOutputDir);
    segmentGeneratorConfig.setSegmentName(segmentName);

    SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
    driver.init(segmentGeneratorConfig, recordReader);
    driver.build();
    File segmentIndexDir = new File(segmentOutputDir, segmentName);

    if (!segmentIndexDir.exists()) {
      throw new IllegalStateException("Segment generation failed");
    }

    return segmentIndexDir;
  }

  private static Object generateSingleValue(Random random, FieldSpec.DataType dataType) {
    switch (dataType) {
      case INT:
        return Math.abs(random.nextInt());
      case LONG:
        return Math.abs(random.nextLong());
      case FLOAT:
        return Math.abs(random.nextFloat());
      case DOUBLE:
        return Math.abs(random.nextDouble());
      case STRING:
        return RandomStringUtils.randomAlphabetic(DEFAULT_STRING_VALUE_LENGTH);
      default:
        throw new IllegalStateException("Illegal data type");
    }
  }

  private static Object[] generateMultiValue(Random random, FieldSpec.DataType dataType) {
    Object[] value = new Object[DEFAULT_NUM_MULTIVALUE];
    for (int i = 0; i < DEFAULT_NUM_MULTIVALUE; i++) {
      value[i] = generateSingleValue(random, dataType);
    }
    return value;
  }
}
