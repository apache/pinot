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

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.query.utils.idset.IdSet;
import org.apache.pinot.core.query.utils.idset.IdSets;
import org.apache.pinot.spi.data.FieldSpec.DataType;


/**
 * The IN_ID_SET transform function takes 2 arguments:
 * <ul>
 *   <li>Expression: a single-value expression</li>
 *   <li>Base64 encoded IdSet: a literal string</li>
 * </ul>
 * <p>For each docId, the function returns {@code 1} if the IdSet contains the value of the expression, {code 0} if not.
 * <p>E.g. {@code SELECT COUNT(*) FROM myTable WHERE IN_ID_SET(col, '<base64 encoded IdSet>') = 1)}
 */
public class InIdSetTransformFunction extends BaseTransformFunction {
  private TransformFunction _transformFunction;
  private IdSet _idSet;

  @Override
  public String getName() {
    return TransformFunctionType.INIDSET.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    Preconditions.checkArgument(arguments.size() == 2,
        "2 arguments are required for IN_ID_SET transform function: expression, base64 encoded IdSet");
    Preconditions.checkArgument(arguments.get(0).getResultMetadata().isSingleValue(),
        "First argument for IN_ID_SET transform function must be a single-value expression");
    Preconditions.checkArgument(arguments.get(1) instanceof LiteralTransformFunction,
        "Second argument for IN_ID_SET transform function must be a literal string of the base64 encoded IdSet");

    _transformFunction = arguments.get(0);
    try {
      _idSet = IdSets.fromBase64String(((LiteralTransformFunction) arguments.get(1)).getStringLiteral());
    } catch (IOException e) {
      throw new IllegalArgumentException("Caught exception while deserializing IdSet", e);
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initIntValuesSV(length);
    DataType storedType = _transformFunction.getResultMetadata().getDataType().getStoredType();
    switch (storedType) {
      case INT:
        int[] intValues = _transformFunction.transformToIntValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = _idSet.contains(intValues[i]) ? 1 : 0;
        }
        break;
      case LONG:
        long[] longValues = _transformFunction.transformToLongValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = _idSet.contains(longValues[i]) ? 1 : 0;
        }
        break;
      case FLOAT:
        float[] floatValues = _transformFunction.transformToFloatValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = _idSet.contains(floatValues[i]) ? 1 : 0;
        }
        break;
      case DOUBLE:
        double[] doubleValues = _transformFunction.transformToDoubleValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = _idSet.contains(doubleValues[i]) ? 1 : 0;
        }
        break;
      case STRING:
        String[] stringValues = _transformFunction.transformToStringValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = _idSet.contains(stringValues[i]) ? 1 : 0;
        }
        break;
      case BYTES:
        byte[][] bytesValues = _transformFunction.transformToBytesValuesSV(valueBlock);
        for (int i = 0; i < length; i++) {
          _intValuesSV[i] = _idSet.contains(bytesValues[i]) ? 1 : 0;
        }
        break;
      default:
        throw new IllegalStateException();
    }
    return _intValuesSV;
  }
}
