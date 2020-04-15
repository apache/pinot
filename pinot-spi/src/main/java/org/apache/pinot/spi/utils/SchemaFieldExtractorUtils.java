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
package org.apache.pinot.spi.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.TimeFieldSpec;
import org.apache.pinot.spi.data.TimeGranularitySpec;
import org.apache.pinot.spi.data.function.evaluators.ExpressionEvaluator;
import org.apache.pinot.spi.data.function.evaluators.ExpressionEvaluatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Extracts names of the source fields from the schema
 */
public class SchemaFieldExtractorUtils {
  public static final String MAP_KEY_COLUMN_SUFFIX = "__KEYS";
  public static final String MAP_VALUE_COLUMN_SUFFIX = "__VALUES";

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaFieldExtractorUtils.class);

  /**
   * Extracts the source fields and destination fields from the schema
   * For field specs with a transform expression defined, use the arguments provided to the function
   * By default, add the field spec name
   *
   * TODO: for now, we assume that arguments to transform function are in the source i.e. there's no columns which are derived from transformed columns
   */
  public static Set<String> extract(Schema schema) {
    Set<String> sourceFieldNames = new HashSet<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        ExpressionEvaluator expressionEvaluator = ExpressionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
        if (expressionEvaluator != null) {
          sourceFieldNames.addAll(expressionEvaluator.getArguments());
        }
        sourceFieldNames.add(fieldSpec.getName());
      }
    }
    return sourceFieldNames;
  }

  @VisibleForTesting
  public static Set<String> extractSource(Schema schema) {
    Set<String> sourceFieldNames = new HashSet<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        ExpressionEvaluator expressionEvaluator = ExpressionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
        if (expressionEvaluator != null) {
          sourceFieldNames.addAll(expressionEvaluator.getArguments());
        } else {
          sourceFieldNames.add(fieldSpec.getName());
        }
      }
    }
    return sourceFieldNames;
  }

  /**
   * Validates that for a field spec with transform function, the source column name and destination column name are exclusive
   * i.e. do not allow using source column name for destination column
   */
  public static boolean validate(Schema schema) {
    try {
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        if (!fieldSpec.isVirtualColumn()) {
          String column = fieldSpec.getName();
          String transformFunction = fieldSpec.getTransformFunction();
          if (transformFunction != null) {
            ExpressionEvaluator expressionEvaluator = ExpressionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
            if (expressionEvaluator != null) {
              List<String> arguments = expressionEvaluator.getArguments();
              // output column used as input
              if (arguments.contains(column)) {
                LOGGER.error("The arguments of transform function: {}, should not contain the destination column: {}",
                    transformFunction, column);
                return false;
              }
            }
          } else if (fieldSpec.getFieldType().equals(FieldSpec.FieldType.TIME)) {
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
                if (!incomingGranularitySpec.getTimeFormat().equals(TimeGranularitySpec.TimeFormat.EPOCH.toString()) || !outgoingGranularitySpec.getTimeFormat()
                    .equals(TimeGranularitySpec.TimeFormat.EPOCH.toString())) {
                  LOGGER.error(
                      "When incoming and outgoing specs are different, cannot perform time conversion for time format other than EPOCH");
                  return false;
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {
      LOGGER.error("Exception in validating schema {}", schema.getSchemaName(), e);
      return false;
    }
    return true;
  }
}
