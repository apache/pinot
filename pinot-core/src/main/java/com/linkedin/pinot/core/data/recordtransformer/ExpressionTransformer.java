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
package com.linkedin.pinot.core.data.recordtransformer;

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.function.FunctionExpressionEvaluator;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;
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
    for (Map.Entry<String, FieldSpec> entry : schema.getFieldSpecMap().entrySet()) {
      FieldSpec fieldSpec = entry.getValue();
      String expression = fieldSpec.getTransformFunction();
      if (expression != null) {
        try {
          _expressionEvaluators.put(entry.getKey(), new FunctionExpressionEvaluator(expression));
        } catch (Exception e) {
          LOGGER.error("Caught exception while constructing expression evaluator for: {}, skipping", expression, e);
        }
      }
    }
  }

  @Nullable
  public GenericRow transform(GenericRow record) {
    for (Map.Entry<String, FunctionExpressionEvaluator> entry : _expressionEvaluators.entrySet()) {
      String column = entry.getKey();
      // Skip transformation if column value already exist
      // NOTE: column value might already exist for OFFLINE data
      if (record.getValue(column) == null) {
        record.putField(column, entry.getValue().evaluate(record));
      }
    }
    return record;
  }
}
