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
package org.apache.pinot.core.operator.transform.function;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.common.function.DateTimeUtils;
import org.apache.pinot.common.function.TimeZoneKey;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.RequestContextConvertUtils;
import org.apache.pinot.core.operator.DocIdSetOperator;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.creator.impl.SegmentIndexCreationDriverImpl;
import org.apache.pinot.segment.local.segment.readers.GenericRowRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.creator.SegmentGeneratorConfig;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.config.table.TableType;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import org.apache.pinot.spi.utils.builder.TableConfigBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.testng.Assert;
import org.testng.annotations.Test;


public class DateTruncTransformFunctionTest {
  private static final String TIME_COLUMN = "time";
  private static final ZoneOffset WEIRD_ZONE = ZoneOffset.ofHoursMinutes(7, 9);
  private static final DateTimeZone WEIRD_DATE_TIME_ZONE = DateTimeZone.forID(WEIRD_ZONE.getId());
  private static final DateTime WEIRD_TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, WEIRD_DATE_TIME_ZONE);
  private static final String WEIRD_TIMESTAMP_ISO8601_STRING = "2001-08-22T03:04:05.321+07:09";
  private static final DateTimeZone UTC_TIME_ZONE =
      DateTimeUtils.DateTimeZoneIndex.getDateTimeZone(TimeZoneKey.UTC_KEY);
  private static final String TIMESTAMP_ISO8601_STRING = "2001-08-22T03:04:05.321+00:00";

  private static final DateTime TIMESTAMP = new DateTime(2001, 8, 22, 3, 4, 5, 321, UTC_TIME_ZONE);
  // This is TIMESTAMP w/o TZ

  private static long iso8601ToUtcEpochMillis(String iso8601) {
    DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser().withOffsetParsed();
    return formatter.parseDateTime(iso8601).getMillis();
  }

  private static void testDateTruncHelper(Schema schema, String literalInput, String unit, String tz, long expected)
      throws Exception {
    long zmillisInput = iso8601ToUtcEpochMillis(literalInput);
    GenericRow row = new GenericRow();
    row.init(ImmutableMap.of(TIME_COLUMN, zmillisInput));
    List<GenericRow> rows = ImmutableList.of(row);
    TableConfig tableConfig =
        new TableConfigBuilder(TableType.OFFLINE).setTableName("test").setTimeColumnName(TIME_COLUMN).build();

    SegmentGeneratorConfig config = new SegmentGeneratorConfig(tableConfig, schema);
    String segmentName = "testSegment";
    String indexDirPath =
        Paths.get(Files.createTempDirectory("pinot_date_trunc_test").toAbsolutePath().toString(), segmentName)
            .toAbsolutePath().toString();
    try {
      FileUtils.deleteQuietly(new File(indexDirPath));
      config.setOutDir(indexDirPath);
      config.setSegmentName(segmentName);
      SegmentIndexCreationDriverImpl driver = new SegmentIndexCreationDriverImpl();
      driver.init(config, new GenericRowRecordReader(rows));
      driver.build();

      IndexSegment indexSegment = ImmutableSegmentLoader.load(new File(indexDirPath, segmentName), ReadMode.heap);
      Set<String> columnNames = indexSegment.getPhysicalColumnNames();
      HashMap<String, DataSource> dataSourceMap = new HashMap<>(columnNames.size());
      for (String columnName : columnNames) {
        dataSourceMap.put(columnName, indexSegment.getDataSource(columnName));
      }

      ProjectionBlock projectionBlock = new ProjectionOperator(dataSourceMap,
          new DocIdSetOperator(new MatchAllFilterOperator(rows.size()), DocIdSetPlanNode.MAX_DOC_PER_CALL)).nextBlock();

      ExpressionContext expression = RequestContextConvertUtils.getExpression(
          String.format("dateTrunc('%s', %s, '%s', '%s')", unit, TIME_COLUMN, TimeUnit.MILLISECONDS, tz));
      TransformFunction transformFunction = TransformFunctionFactory.get(expression, dataSourceMap);
      Assert.assertTrue(transformFunction instanceof DateTruncTransformFunction);
      Assert.assertEquals(transformFunction.getName(), DateTruncTransformFunction.FUNCTION_NAME);
      long[] longValues = transformFunction.transformToLongValuesSV(projectionBlock);
      Assert.assertEquals(longValues[0], expected);
    } finally {
      FileUtils.deleteDirectory(new File(indexDirPath));
    }
  }

  @Test
  public void testPrestoCompatibleDateTimeConversionTransformFunction()
      throws Exception {
    Schema schemaTimeFieldSpec = new Schema.SchemaBuilder()
        .addTime(new TimeGranularitySpec(FieldSpec.DataType.LONG, TimeUnit.MILLISECONDS, TIME_COLUMN), null).build();
    testDateTrunc(schemaTimeFieldSpec);

    Schema schemaDateTimeFieldSpec = new Schema.SchemaBuilder()
        .addDateTime(TIME_COLUMN, FieldSpec.DataType.LONG, "1:MILLISECONDS:EPOCH", "1:MILLISECONDS").build();
    testDateTrunc(schemaDateTimeFieldSpec);
  }

  private void testDateTrunc(Schema schema)
      throws Exception {

    DateTime result = TIMESTAMP;
    result = result.withMillisOfSecond(0);
    testDateTruncHelper(schema, TIMESTAMP_ISO8601_STRING, "second", UTC_TIME_ZONE.getID(), result.getMillis());

    result = result.withSecondOfMinute(0);
    testDateTruncHelper(schema, TIMESTAMP_ISO8601_STRING, "minute", UTC_TIME_ZONE.getID(), result.getMillis());

    result = result.withMinuteOfHour(0);
    testDateTruncHelper(schema, TIMESTAMP_ISO8601_STRING, "hour", UTC_TIME_ZONE.getID(), result.getMillis());

    result = result.withHourOfDay(0);
    testDateTruncHelper(schema, TIMESTAMP_ISO8601_STRING, "day", UTC_TIME_ZONE.getID(), result.getMillis());

    // ISO8601 week begins on Monday. For this timestamp (2001-08-22), 20th is the Monday of that week
    result = result.withDayOfMonth(20);
    testDateTruncHelper(schema, TIMESTAMP_ISO8601_STRING, "week", UTC_TIME_ZONE.getID(), result.getMillis());

    result = result.withDayOfMonth(1);
    testDateTruncHelper(schema, TIMESTAMP_ISO8601_STRING, "month", UTC_TIME_ZONE.getID(), result.getMillis());

    result = result.withMonthOfYear(7);
    testDateTruncHelper(schema, TIMESTAMP_ISO8601_STRING, "quarter", UTC_TIME_ZONE.getID(), result.getMillis());

    result = result.withMonthOfYear(1);
    testDateTruncHelper(schema, TIMESTAMP_ISO8601_STRING, "year", UTC_TIME_ZONE.getID(), result.getMillis());

    result = WEIRD_TIMESTAMP;
    result = result.withMillisOfSecond(0);
    testDateTruncHelper(schema, WEIRD_TIMESTAMP_ISO8601_STRING, "second", WEIRD_DATE_TIME_ZONE.getID(),
        result.getMillis());

    result = result.withSecondOfMinute(0);
    testDateTruncHelper(schema, WEIRD_TIMESTAMP_ISO8601_STRING, "minute", WEIRD_DATE_TIME_ZONE.getID(),
        result.getMillis());

    result = result.withMinuteOfHour(0);
    testDateTruncHelper(schema, WEIRD_TIMESTAMP_ISO8601_STRING, "hour", WEIRD_DATE_TIME_ZONE.getID(),
        result.getMillis());

    result = result.withHourOfDay(0);
    testDateTruncHelper(schema, WEIRD_TIMESTAMP_ISO8601_STRING, "day", WEIRD_DATE_TIME_ZONE.getID(),
        result.getMillis());

    result = result.withDayOfMonth(20);
    testDateTruncHelper(schema, WEIRD_TIMESTAMP_ISO8601_STRING, "week", WEIRD_DATE_TIME_ZONE.getID(),
        result.getMillis());

    result = result.withDayOfMonth(1);
    testDateTruncHelper(schema, WEIRD_TIMESTAMP_ISO8601_STRING, "month", WEIRD_DATE_TIME_ZONE.getID(),
        result.getMillis());

    result = result.withMonthOfYear(7);
    testDateTruncHelper(schema, WEIRD_TIMESTAMP_ISO8601_STRING, "quarter", WEIRD_DATE_TIME_ZONE.getID(),
        result.getMillis());

    result = result.withMonthOfYear(1);
    testDateTruncHelper(schema, WEIRD_TIMESTAMP_ISO8601_STRING, "year", WEIRD_DATE_TIME_ZONE.getID(),
        result.getMillis());
  }
}
