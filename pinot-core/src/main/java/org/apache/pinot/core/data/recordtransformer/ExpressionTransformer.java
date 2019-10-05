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
package org.apache.pinot.core.data.recordtransformer;

import java.util.HashMap;
import java.util.Map;
import org.apache.pinot.common.data.FieldSpec;
import org.apache.pinot.common.data.Schema;
import org.apache.pinot.core.data.GenericRow;
import org.apache.pinot.core.data.function.FunctionExpressionEvaluator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * The {@code ExpressionTransformer} class will evaluate the function expressions.
 * <p>NOTE: should put this before the {@link DataTypeTransformer}. After this, transformed column can be treated as
 * regular column for other record transformers.
 */
public class ExpressionTransformer implements RecordTransformer {
  private static final Logger LOGGER = LoggerFactory.getLogger(ExpressionTransformer.class);

  private final Map<String, FunctionExpressionEvaluator> _expressionEvaluators = new HashMap<>();

  public ExpressionTransformer(Schema schema) {
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      if (!fieldSpec.isVirtualColumn()) {
        String expression = fieldSpec.getTransformFunction();
        if (expression != null) {
          try {
            _expressionEvaluators.put(fieldSpec.getName(), new FunctionExpressionEvaluator(expression));
          } catch (Exception e) {
            LOGGER.error("Caught exception while constructing expression evaluator for: {}, skipping", expression, e);
          }
        }
      }
    }
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (Map.Entry<String, FunctionExpressionEvaluator> entry : _expressionEvaluators.entrySet()) {
      String column = entry.getKey();
      // Skip transformation if column value already exist
      // NOTE: column value might already exist for OFFLINE data
      if (record.getValue(column) == null) {
        record.putValue(column, entry.getValue().evaluate(record));
      }
    }
    return record;
  }
}
