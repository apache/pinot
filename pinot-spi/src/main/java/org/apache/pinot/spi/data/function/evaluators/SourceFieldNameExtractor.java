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
package org.apache.pinot.spi.data.function.evaluators;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


/**
 * Extracts names of the source fields from the schema
 */
public class SourceFieldNameExtractor {
  public static final String MAP_KEY_COLUMN_SUFFIX = "__KEYS";
  public static final String MAP_VALUE_COLUMN_SUFFIX = "__VALUES";

  /**
   * Extracts the source fields from the schema
   * For field specs with a transform expression defined, use the arguments provided to the function
   * Otherwise, use the column name as is
   * TODO: for now, we assume that arguments to transform function are in the source i.e. there's no columns which are derived from transformed columns
   */
  public static List<String> extract(Schema schema) {
    Set<String> sourceFieldNames = new HashSet<>();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        String columnName = fieldSpec.getName();
        ExpressionEvaluator expressionEvaluator = ExpressionEvaluatorFactory.getExpressionEvaluator(fieldSpec);

        if (expressionEvaluator != null) {
          sourceFieldNames.addAll(expressionEvaluator.getArguments());
        } else {
          sourceFieldNames.add(columnName);
        }
      }
    }
    return new ArrayList<>(sourceFieldNames);
  }
}
