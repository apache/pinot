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
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.data.function.evaluators.ExpressionEvaluator;
import org.apache.pinot.spi.data.function.evaluators.ExpressionEvaluatorFactory;


/**
 * The {@code ExpressionTransformer} class will evaluate the function expressions.
 * <p>NOTE: should put this before the {@link DataTypeTransformer}. After this, transformed column can be treated as
 * regular column for other record transformers.
 */
public class ExpressionTransformer implements RecordTransformer {

  private final Map<String, ExpressionEvaluator> _expressionEvaluators = new HashMap<>();

  private final String _timeColumn;

  public ExpressionTransformer(Schema schema) {
    _timeColumn = schema.getTimeColumnName();
    for (FieldSpec fieldSpec : schema.getAllFieldSpecs()) {
      ExpressionEvaluator expressionEvaluator = ExpressionEvaluatorFactory.getExpressionEvaluator(fieldSpec);
      if (expressionEvaluator != null) {
        _expressionEvaluators.put(fieldSpec.getName(), expressionEvaluator);
      }
    }
  }

  @Override
  public GenericRow transform(GenericRow record) {
    for (Map.Entry<String, ExpressionEvaluator> entry : _expressionEvaluators.entrySet()) {
      String column = entry.getKey();
      ExpressionEvaluator transformExpressionEvaluator = entry.getValue();
      // Skip transformation if column value already exist. Value can exist for time transformation (incoming name = outgoing name)
      // NOTE: column value might already exist for OFFLINE data
      if (record.getValue(column) == null || column.equals(_timeColumn)) {
        Object result = transformExpressionEvaluator.evaluate(record);
        record.putValue(column, result);
      }
    }
    return record;
  }
}
