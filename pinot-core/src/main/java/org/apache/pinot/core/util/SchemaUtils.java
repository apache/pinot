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
package org.apache.pinot.core.util;

import java.util.List;
import java.util.Locale;
import javax.annotation.Nullable;
import org.apache.calcite.sql.parser.SqlAbstractParserImpl;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.pinot.core.data.function.FunctionEvaluator;
import org.apache.pinot.core.data.function.FunctionEvaluatorFactory;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Schema utils
 * FIXME: Merge this SchemaUtils with the SchemaUtils from pinot-common when merging of modules happens
 */
public class SchemaUtils {
  public static final String MAP_KEY_COLUMN_SUFFIX = "__KEYS";
  public static final String MAP_VALUE_COLUMN_SUFFIX = "__VALUES";

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaUtils.class);

  private static final SqlAbstractParserImpl.Metadata SQL_PARSER_METADATA = SqlParser.create("",
      SqlParser.configBuilder().setConformance(SqlConformanceEnum.BABEL).setParserFactory(SqlBabelParserImpl.FACTORY)
          .build()).getMetadata();

  /**
   * Validates the following:
   * 1) Checks if sql reserved keywords are being used as field names. This check can be disabled, for not breaking existing schemas with invalid names
   * 2) Checks valid transform function -
   *   for a field spec with transform function, the source column name and destination column name are exclusive i.e. do not allow using source column name for destination column
   *   ensure transform function string can be used to create a {@link FunctionEvaluator}
   * 3) Checks valid timeFieldSpec - if incoming and outgoing granularity spec are different a) the names cannot be same b) cannot use SIMPLE_DATE_FORMAT for conversion
   * 4) Checks valid dateTimeFieldSpecs - checks format and granularity string
   * 5) Schema validations from {@link Schema#validate(Logger)}
   */
  public static boolean validate(Schema schema) {
    return validate(schema, true, LOGGER);
  }

  /**
   * Validates the following:
   * 1) Checks if sql reserved keywords are being used as field names. This check can be disabled, for not breaking existing schemas with invalid names
   * 2) Checks valid transform function -
   *   for a field spec with transform function, the source column name and destination column name are exclusive i.e. do not allow using source column name for destination column
   *   ensure transform function string can be used to create a {@link FunctionEvaluator}
   * 3) Checks valid timeFieldSpec - if incoming and outgoing granularity spec are different a) the names cannot be same b) cannot use SIMPLE_DATE_FORMAT for conversion
   * 4) Checks valid dateTimeFieldSpecs - checks format and granularity string
   * 5) Schema validations from {@link Schema#validate(Logger)}
   *
   * @param validateFieldNames if false, does not validate field names. This is to prevent validation failing on existing schemas with invalid field names during a schema update
   */
  public static boolean validate(Schema schema, boolean validateFieldNames, @Nullable Logger logger) {
    try {
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        if (!fieldSpec.isVirtualColumn()) {
          if (validateFieldNames && !isValidFieldName(fieldSpec)) {
            return false;
          }
          if (!isValidTransformFunction(fieldSpec)) {
            return false;
          }
          FieldSpec.FieldType fieldType = fieldSpec.getFieldType();
          if (fieldType.equals(FieldSpec.FieldType.DATE_TIME)) {
            if (!isValidDateTimeFieldSpec(fieldSpec)) {
              return false;
            }
          } else if (fieldType.equals(FieldSpec.FieldType.TIME)) {
            if (!isValidTimeFieldSpec(fieldSpec)) {
              return false;
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Exception in validating schema {}", schema.getSchemaName(), e);
      return false;
    }
    return schema.validate(logger);
  }

  /**
   * Checks if any of the keywords which are reserved under the sql parser are used
   */
  private static boolean isValidFieldName(FieldSpec fieldSpec) {
    String columnName = fieldSpec.getName();
    if (SQL_PARSER_METADATA.isReservedWord(columnName.toUpperCase(Locale.ROOT))) {
      LOGGER.error("Cannot use SQL reserved word {} as field name in the schema", columnName);
      return false;
    }
    return true;
  }

  /**
   * Checks for valid transform function string
   */
  private static boolean isValidTransformFunction(FieldSpec fieldSpec) {
    String column = fieldSpec.getName();
    String transformFunction = fieldSpec.getTransformFunction();
    if (transformFunction != null) {
      FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
      if (functionEvaluator != null) {
        List<String> arguments = functionEvaluator.getArguments();
        // output column used as input
        if (arguments.contains(column)) {
          LOGGER.error("The arguments of transform function: {}, should not contain the destination column: {}",
              transformFunction, column);
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Checks for valid incoming and outgoing granularity spec in the time field spec
   */
  private static boolean isValidTimeFieldSpec(FieldSpec fieldSpec) {
    TimeFieldSpec timeFieldSpec = (TimeFieldSpec) fieldSpec;
    TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
    TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();

    if (!incomingGranularitySpec.equals(outgoingGranularitySpec)) {
      // different incoming and outgoing spec, but same name
      if (incomingGranularitySpec.getName().equals(outgoingGranularitySpec.getName())) {
        LOGGER.error("Cannot convert from incoming field spec:{} to outgoing field spec:{} if name is the same",
            incomingGranularitySpec, outgoingGranularitySpec);
        return false;
      } else {
        if (!incomingGranularitySpec.getTimeFormat().equals(TimeGranularitySpec.TimeFormat.EPOCH.toString())
            || !outgoingGranularitySpec.getTimeFormat().equals(TimeGranularitySpec.TimeFormat.EPOCH.toString())) {
          LOGGER.error(
              "When incoming and outgoing specs are different, cannot perform time conversion for time format other than EPOCH");
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Checks for valid format and granularity string in dateTimeFieldSpec
   */
  private static boolean isValidDateTimeFieldSpec(FieldSpec fieldSpec) {
    DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
    return DateTimeFormatSpec.isValidFormat(dateTimeFieldSpec.getFormat()) && DateTimeGranularitySpec
        .isValidGranularity(dateTimeFieldSpec.getGranularity());
  }
}
