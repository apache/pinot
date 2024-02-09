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

package org.apache.pinot.segment.local.recordenricher.groovy;

import java.util.List;
import java.util.Map;
import org.apache.pinot.segment.local.function.FunctionEvaluator;
import org.apache.pinot.segment.local.function.FunctionEvaluatorFactory;
import org.apache.pinot.segment.local.recordenricher.RecordEnricher;
import org.apache.pinot.spi.data.readers.GenericRow;


public class CustomFunctionEnricher extends RecordEnricher {
  private static final String FUNCTION = "function";
  private static final String FIELD = "field";
  private FunctionEvaluator _functionEvaluator;
  private String _field;
  @Override
  public void init(Map<String, String> enricherProps) {
    _functionEvaluator = FunctionEvaluatorFactory.getExpressionEvaluator(enricherProps.get(FUNCTION));
    _field = enricherProps.get(FIELD);
  }

  @Override
  public List<String> getInputColumns() {
    return _functionEvaluator.getArguments();
  }

  @Override
  public void enrich(GenericRow record) {
    record.putValue(_field, _functionEvaluator.evaluate(record));
  }
}
