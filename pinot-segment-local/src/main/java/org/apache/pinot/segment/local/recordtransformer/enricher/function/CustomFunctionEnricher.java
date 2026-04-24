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

package org.apache.pinot.segment.local.recordtransformer.enricher.function;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.evaluator.FunctionEvaluatorFactory;
import org.apache.pinot.segment.local.recordtransformer.IngestionFunctionEvaluation;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.function.FunctionEvaluator;
import org.apache.pinot.spi.recordtransformer.enricher.RecordEnricher;
import org.apache.pinot.spi.utils.JsonUtils;


/**
 * {@link RecordEnricher} that applies a JSON {@code fieldToFunctionMap} of transform expressions. Evaluation is
 * delegated to {@link IngestionFunctionEvaluation#applyEnricherEvaluations}; for table/schema-driven transforms and
 * dependency ordering, see {@link org.apache.pinot.segment.local.recordtransformer.ExpressionTransformer}.
 */
public class CustomFunctionEnricher implements RecordEnricher {
  private final LinkedHashMap<String, FunctionEvaluator> _fieldToFunctionEvaluator;
  private final List<String> _fieldsToExtract;

  public CustomFunctionEnricher(JsonNode enricherProps)
      throws IOException {
    CustomFunctionEnricherConfig config = JsonUtils.jsonNodeToObject(enricherProps, CustomFunctionEnricherConfig.class);
    _fieldToFunctionEvaluator = new LinkedHashMap<>();
    _fieldsToExtract = new ArrayList<>();
    for (Map.Entry<String, String> entry : config.getFieldToFunctionMap().entrySet()) {
      String column = entry.getKey();
      String function = entry.getValue();
      FunctionEvaluator functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(function);
      _fieldToFunctionEvaluator.put(column, functionEvaluator);
      _fieldsToExtract.addAll(functionEvaluator.getArguments());
    }
  }

  @Override
  public List<String> getInputColumns() {
    return _fieldsToExtract;
  }

  @Override
  public void enrich(GenericRow record) {
    IngestionFunctionEvaluation.applyEnricherEvaluations(record, _fieldToFunctionEvaluator);
  }
}
