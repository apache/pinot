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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.common.function.FunctionInvoker;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.spi.data.FieldSpec;


public class GenericTransformFunction extends BaseTransformFunction {

  private FunctionInfo _info;
  FunctionInvoker _functionInvoker;
  String _name;
  Object[] _args;
  List<Integer> _nonLiteralArgIndices;
  List<FieldSpec.DataType> _nonLiteralArgType;
  List<TransformFunction> _nonLiteralTransformFunction;
  String[] _stringResult;

  public GenericTransformFunction() {
    _nonLiteralArgIndices = new ArrayList();
    _nonLiteralArgType = new ArrayList();
  }

  @Override
  public String getName() {
    return _name;
  }

  public void setFunction(String functionName, FunctionInfo info)
      throws Exception {
    _name = functionName;
    _info = info;
    _functionInvoker = new FunctionInvoker(info);
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    //assert method.args.length == arguments.size
    _args = new Object[arguments.size()];

    for (int i = 0; i < arguments.size(); i++) {
      TransformFunction function = arguments.get(i);
      if (function instanceof LiteralTransformFunction) {
        String literal = ((LiteralTransformFunction) function).getLiteral();
        //convert String to the right dataType based on method param

        Class paramType = _functionInvoker.getParameterTypes()[i];
        switch (paramType.getTypeName()) {
          case "Integer":
            _args[i] = Integer.parseInt(literal);
            break;
          case "String":
            _args[i] = literal;
            break;
            //add other types and throw exception for non primitive/string classes
        }
      } else {
        _nonLiteralArgIndices.add(i);
        Class paramType = _functionInvoker.getParameterTypes()[i];
        //find the right pinot data Type
        switch (paramType.getTypeName()) {
          case "Integer":
            _nonLiteralArgType.add(FieldSpec.DataType.INT);
            break;
          case "String":
            _nonLiteralArgType.add(FieldSpec.DataType.STRING);
            break;
            //todo add other types
        }
      }
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return STRING_SV_NO_DICTIONARY_METADATA;
  }

  @SuppressWarnings("Duplicates")
  @Override
  public String[] transformToStringValuesSV(ProjectionBlock projectionBlock) {
    if (_stringResult == null) {
      _stringResult = new String[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }

    int length = projectionBlock.getNumDocs();
    int numNonLiteralArgs = _nonLiteralArgIndices.size();
    Object[][] nonLiteralBlockValues = new Object[numNonLiteralArgs][];

    for (int i = 0; i < numNonLiteralArgs; i++) {
      TransformFunction transformFunc = _nonLiteralTransformFunction.get(i);
      FieldSpec.DataType returnType = _nonLiteralArgType.get(i);
      switch (returnType) {
        case STRING:
          nonLiteralBlockValues[i] = transformFunc.transformToStringValuesSV(projectionBlock);
          //todo handle other types
      }
    }

    //now invoke the actual function
    for (int i = 0; i < length; i++) {
      for (int k = 0; k < numNonLiteralArgs; k++) {
        _args[_nonLiteralArgIndices.get(k)] = nonLiteralBlockValues[k][i];
      }
      _stringResult[i] = (String) _functionInvoker.process(_args);
    }
    return _stringResult;
  }
}
