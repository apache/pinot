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
package org.apache.pinot.core.data.readers;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.pinot.core.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.readers.RecordReader;
import org.apache.pinot.spi.utils.TimeUtils;


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
    final ThreadLocalRandom random = ThreadLocalRandom.current();
    Map<String, Object> fields;
    for (int i = 0; i < numRows; i++) {
      fields = new HashMap<>();
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        Object value;
        if (fieldSpec.isSingleValueField()) {
          value = generateSingleValue(random, fieldSpec);
        } else {
          value = generateMultiValue(random, fieldSpec);
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

  public static File createSegment(TableConfig tableConfig, Schema schema, String segmentName, String segmentOutputDir,
      RecordReader recordReader)
      throws Exception {
    SegmentGeneratorConfig segmentGeneratorConfig = new SegmentGeneratorConfig(tableConfig, schema);
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

  private static Object generateSingleValue(ThreadLocalRandom random, FieldSpec fieldSpec) {
    if (fieldSpec instanceof TimeFieldSpec) {
      // explicitly generate the time column values within allowed range so that
      // segment generation code doesn't throw exception
      TimeFieldSpec timeFieldSpec = (TimeFieldSpec)fieldSpec;
      TimeUnit unit = timeFieldSpec.getIncomingGranularitySpec().getTimeType();
      return generateTimeValue(random, unit);
    } else if (fieldSpec instanceof DateTimeFieldSpec) {
      DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
      TimeUnit unit = new DateTimeFormatSpec(dateTimeFieldSpec.getFormat()).getColumnUnit();
      return generateTimeValue(random, unit);
    } else {
      switch (fieldSpec.getDataType()) {
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
      }
    }

    throw new IllegalStateException("Illegal data type");
  }

  private static Object generateTimeValue(ThreadLocalRandom random, TimeUnit unit) {
    final long milliMin = TimeUtils.getValidMinTimeMillis();
    final long milliMax = TimeUtils.getValidMaxTimeMillis();
    final long daysMin = TimeUnit.DAYS.convert(milliMin, TimeUnit.MILLISECONDS);
    final long daysMax = TimeUnit.DAYS.convert(milliMax, TimeUnit.MILLISECONDS);
    final long hoursMin = TimeUnit.HOURS.convert(milliMin, TimeUnit.MILLISECONDS);
    final long hoursMax = TimeUnit.HOURS.convert(milliMax, TimeUnit.MILLISECONDS);
    final long minutesMin = TimeUnit.MINUTES.convert(milliMin, TimeUnit.MILLISECONDS);
    final long minutesMax = TimeUnit.MINUTES.convert(milliMax, TimeUnit.MILLISECONDS);
    switch (unit) {
      case MILLISECONDS:
        return random.nextLong(milliMin, milliMax);
      case SECONDS:
        return random.nextLong(milliMin/1000, milliMax/1000);
      case MICROSECONDS:
        return random.nextLong(milliMin*1000, milliMax*1000);
      case NANOSECONDS:
        return random.nextLong(milliMin*1000*1000, milliMax*1000*1000);
      case DAYS:
        return random.nextLong(daysMin, daysMax);
      case HOURS:
        return random.nextLong(hoursMin, hoursMax);
      case MINUTES:
        return random.nextLong(minutesMin, minutesMax);
    }
    throw new IllegalStateException("Illegal data type");
  }

  private static Object[] generateMultiValue(ThreadLocalRandom random, FieldSpec fieldSpec) {
    Object[] value = new Object[DEFAULT_NUM_MULTIVALUE];
    for (int i = 0; i < DEFAULT_NUM_MULTIVALUE; i++) {
      value[i] = generateSingleValue(random, fieldSpec);
    }
    return value;
  }
}
