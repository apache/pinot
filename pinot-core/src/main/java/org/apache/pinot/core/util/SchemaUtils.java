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
   * Validates that for a field spec with transform function, the source column name and destination column name are exclusive
   * i.e. do not allow using source column name for destination column
   */
  public static boolean validate(Schema schema) {
    return validate(schema, true, LOGGER);
  }

  /**
   * Validates the following:
   * 1) for a field spec with transform function, the source column name and destination column name are exclusive
   * i.e. do not allow using source column name for destination column
   * 2) Basic schema validations
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
          if (fieldSpec.getFieldType().equals(FieldSpec.FieldType.TIME) && !isValidTimeFieldSpec(fieldSpec)) {
            return false;
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
}
