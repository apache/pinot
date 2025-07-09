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
package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.scalar.JsonFunctions;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


/**
 * Transform function for jsonKeys(jsonField, depth)
 * Returns all key paths in a JSON object up to the specified depth as a multi-value string column.
 */
public class JsonKeysTransformFunction extends BaseTransformFunction {
  public static final String FUNCTION_NAME = "jsonKeys";

  private TransformFunction _jsonFieldTransformFunction;
  private int _depth;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    super.init(arguments, columnContextMap);
    if (arguments.size() != 2) {
      throw new IllegalArgumentException("jsonKeys expects 2 arguments: jsonField, depth");
    }
    _jsonFieldTransformFunction = arguments.get(0);
    TransformFunction depthArg = arguments.get(1);
    if (!(depthArg instanceof LiteralTransformFunction)) {
      throw new IllegalArgumentException("Depth argument must be a literal integer");
    }
    _depth = Integer.parseInt(((LiteralTransformFunction) depthArg).getStringLiteral());
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return STRING_MV_NO_DICTIONARY_METADATA;
  }

  @Override
  public String[][] transformToStringValuesMV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initStringValuesMV(length);
    String[] jsonStrings = _jsonFieldTransformFunction.transformToStringValuesSV(valueBlock);
    for (int i = 0; i < length; i++) {
      List<String> keys = JsonFunctions.jsonKeys(jsonStrings[i], _depth);
      _stringValuesMV[i] = keys.toArray(new String[0]);
    }
    return _stringValuesMV;
  }
}
