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
package org.apache.pinot.segment.local.utils;

import com.google.common.base.Preconditions;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.DateTimeFieldSpec;
import org.apache.pinot.spi.data.DateTimeFormatSpec;
import org.apache.pinot.spi.data.DateTimeGranularitySpec;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;


/**
 * Schema utils
 * FIXME: Merge this SchemaUtils with the SchemaUtils from pinot-common when merging of modules happens
 */
public class SchemaUtils {
  public static final String MAP_KEY_COLUMN_SUFFIX = "__KEYS";
  public static final String MAP_VALUE_COLUMN_SUFFIX = "__VALUES";

  /**
   * Validates the schema.
   * First checks that the schema is compatible with any provided table configs associated with it.
   * This check is useful to ensure schema and table are compatible, in the event that schema is updated or added after the table config
   * Then validates the schema using {@link SchemaUtils#validate(Schema schema)}
   *
   * @param schema schema to validate
   * @param tableConfigs table configs associated with this schema (table configs with raw name = schema name)
   */
  public static void validate(Schema schema, List<TableConfig> tableConfigs) {
    for (TableConfig tableConfig : tableConfigs) {
      validateCompatibilityWithTableConfig(schema, tableConfig);
    }
    validate(schema);
  }

  /**
   * Validates the following:
   * 1) Column name should not contain blank space.
   * 2) Checks valid transform function -
   *   for a field spec with transform function, the source column name and destination column name are exclusive i.e. do not allow using source column name for destination column
   *   ensure transform function string can be used to create a {@link FunctionEvaluator}
   * 3) Checks for chained transforms/derived transform - not supported yet
   * TODO: Transform functions have moved to table config. Once we stop supporting them in schema, remove the validations 2 and 3
   * 4) Checks valid timeFieldSpec - if incoming and outgoing granularity spec are different a) the names cannot be same b) cannot use SIMPLE_DATE_FORMAT for conversion
   * 5) Checks valid dateTimeFieldSpecs - checks format and granularity string
   * 6) Schema validations from {@link Schema#validate}
   */
  public static void validate(Schema schema) {
    schema.validate();

    Set<String> transformedColumns = new HashSet<>();
    Set<String> argumentColumns = new HashSet<>();
    Set<String> primaryKeyColumnCandidates = new HashSet<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        String column = fieldSpec.getName();
        Preconditions.checkState(!StringUtils.containsWhitespace(column),
            "The column name \"%s\" should not contain blank space.", column);
        primaryKeyColumnCandidates.add(column);
        String transformFunction = fieldSpec.getTransformFunction();
        if (transformFunction != null) {
          try {
            List<String> arguments = FunctionEvaluatorFactory.getExpressionEvaluator(fieldSpec).getArguments();
            Preconditions.checkState(!arguments.contains(column),
                "The arguments of transform function %s should not contain the destination column %s",
                transformFunction, column);
            transformedColumns.add(column);
            argumentColumns.addAll(arguments);
          } catch (Exception e) {
            throw new IllegalStateException(
                "Exception in getting arguments for transform function '" + transformFunction + "' for column '"
                    + column + "'", e);
          }
        }
        if (fieldSpec.getFieldType().equals(FieldSpec.FieldType.TIME)) {
          validateTimeFieldSpec(fieldSpec);
        }
        if (fieldSpec.getFieldType().equals(FieldSpec.FieldType.DATE_TIME)) {
          validateDateTimeFieldSpec(fieldSpec);
        }
      }
    }
    Preconditions.checkState(Collections.disjoint(transformedColumns, argumentColumns),
        "Columns: %s are a result of transformations, and cannot be used as arguments to other transform functions",
        transformedColumns.retainAll(argumentColumns));
    if (schema.getPrimaryKeyColumns() != null) {
      for (String primaryKeyColumn : schema.getPrimaryKeyColumns()) {
        Preconditions
            .checkState(primaryKeyColumnCandidates.contains(primaryKeyColumn), "The primary key column must exist");
      }
    }
  }

  /**
   * Validates that the schema is compatible with the given table config
   */
  private static void validateCompatibilityWithTableConfig(Schema schema, TableConfig tableConfig) {
    try {
      TableConfigUtils.validate(tableConfig, schema);
    } catch (Exception e) {
      throw new IllegalStateException(
          "Schema is incompatible with tableConfig with name: " + tableConfig.getTableName() + " and type: "
              + tableConfig.getTableType(), e);
    }
  }

  /**
   * Checks for valid incoming and outgoing granularity spec in the time field spec
   */
  private static void validateTimeFieldSpec(FieldSpec fieldSpec) {
    TimeFieldSpec timeFieldSpec = (TimeFieldSpec) fieldSpec;
    TimeGranularitySpec incomingGranularitySpec = timeFieldSpec.getIncomingGranularitySpec();
    TimeGranularitySpec outgoingGranularitySpec = timeFieldSpec.getOutgoingGranularitySpec();

    if (!incomingGranularitySpec.equals(outgoingGranularitySpec)) {
      Preconditions.checkState(!incomingGranularitySpec.getName().equals(outgoingGranularitySpec.getName()),
          "Cannot convert from incoming field spec %s to outgoing field spec %s if name is the same",
          incomingGranularitySpec, outgoingGranularitySpec);

      Preconditions.checkState(
          incomingGranularitySpec.getTimeFormat().equals(TimeGranularitySpec.TimeFormat.EPOCH.toString())
              && outgoingGranularitySpec.getTimeFormat().equals(TimeGranularitySpec.TimeFormat.EPOCH.toString()),
          "Cannot perform time conversion for time format other than EPOCH. TimeFieldSpec: %s", fieldSpec);
    }
  }

  /**
   * Checks for valid format and granularity string in dateTimeFieldSpec
   */
  private static void validateDateTimeFieldSpec(FieldSpec fieldSpec) {
    DateTimeFieldSpec dateTimeFieldSpec = (DateTimeFieldSpec) fieldSpec;
    DateTimeFormatSpec.validateFormat(dateTimeFieldSpec.getFormat());
    DateTimeGranularitySpec.validateGranularity(dateTimeFieldSpec.getGranularity());
  }
}
