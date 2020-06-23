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

  /**
   * Validates that for a field spec with transform function, the source column name and destination column name are exclusive
   * i.e. do not allow using source column name for destination column
   */
  public static boolean validate(Schema schema) {
    return validate(schema, LOGGER);
  }

  /**
   * Validates the following:
   * 1) for a field spec with transform function, the source column name and destination column name are exclusive
   * i.e. do not allow using source column name for destination column
   * 2) Basic schema validations
   */
  public static boolean validate(Schema schema, Logger logger) {
    try {
      for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
        if (!fieldSpec.isVirtualColumn()) {
          String column = fieldSpec.getName();
          String transformFunction = fieldSpec.getTransformFunction();
          if (transformFunction != null) {
            FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
            if (functionEvaluator != null) {
              List<String> arguments = functionEvaluator.getArguments();
              // output column used as input
              if (arguments.contains(column)) {
                logger.error("The arguments of transform function: {}, should not contain the destination column: {}",
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
                logger.error("Cannot convert from incoming field spec:{} to outgoing field spec:{} if name is the same",
                    incomingGranularitySpec, outgoingGranularitySpec);
                return false;
              } else {
                if (!incomingGranularitySpec.getTimeFormat().equals(TimeGranularitySpec.TimeFormat.EPOCH.toString())
                    || !outgoingGranularitySpec.getTimeFormat()
                    .equals(TimeGranularitySpec.TimeFormat.EPOCH.toString())) {
                  logger.error(
                      "When incoming and outgoing specs are different, cannot perform time conversion for time format other than EPOCH");
                  return false;
                }
              }
            }
          }
        }
      }
    } catch (Exception e) {
      logger.error("Exception in validating schema {}", schema.getSchemaName(), e);
      return false;
    }
    return schema.validate(logger);
  }
}
