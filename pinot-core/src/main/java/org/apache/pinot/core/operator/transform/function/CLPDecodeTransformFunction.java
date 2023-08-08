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
import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.MessageDecoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.spi.data.FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;


/**
 * Decodes a CLP-encoded column group into the original value. In Pinot, a CLP-encoded field is encoded into three
 * Pinot columns, what we collectively refer to as a column group. E.g., A CLP-encoded "message" field would be
 * stored in three columns: "message_logtype", "message_dictionaryVars", and "message_encodedVars".
 * <p>
 * Usage:
 * <pre>
 *   clpDecode("columnGroupName_logtype", "columnGroupName_dictionaryVars",
 *             "columnGroupName_encodedVars"[, defaultValue])
 * </pre>
 * The "defaultValue" is optional and is used when a column group can't be decoded for some reason.
 * <p>
 * Sample queries:
 * <pre>
 *   SELECT clpDecode("message_logtype", "message_dictionaryVars", "message_encodedVars") FROM table
 *   SELECT clpDecode("message_logtype", "message_dictionaryVars", "message_encodedVars", 'null') FROM table
 * </pre>
 * For instance, consider a record that contains a "message" field with this value:
 * <pre>INFO Task task_12 assigned to container: [ContainerID:container_15], operation took 0.335 seconds. 8 tasks
 * remaining.</pre>
 * {@link org.apache.pinot.plugin.inputformat.clplog.CLPLogMessageDecoder} will encode it into 3 columns:
 * <ul>
 *   <li>message_logtype: "INFO Task \x12 assigned to container: [ContainerID:\x12], operation took \x13 seconds.
 *   \x11 tasks remaining."</li>
 *   <li>message_dictionaryVars: [“task_12”, “container_15”]</li>
 *   <li>message_encodedVars: [[0x190000000000014f, 8]]</li>
 * </ul>
 * Then we can use the sample queries above to decode the columns back into the original value of the "message" field.
 */
public class CLPDecodeTransformFunction extends BaseTransformFunction {
  private static final Logger _logger = LoggerFactory.getLogger(CLPDecodeTransformFunction.class);
  private final List<TransformFunction> _transformFunctions = new ArrayList<>();
  private String _defaultValue = DEFAULT_DIMENSION_NULL_VALUE_OF_STRING;

  @Override
  public String getName() {
    return TransformFunctionType.CLPDECODE.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    int numArgs = arguments.size();
    Preconditions.checkArgument(3 == numArgs || 4 == numArgs,
        "Syntax error: clpDecode takes 3 or 4 arguments - "
            + "clpDecode(ColumnGroupName_logtype, ColumnGroupName_dictionaryVars, ColumnGroupName_encodedVars, "
            + "defaultValue)");

    int i;
    for (i = 0; i < 3; i++) {
      TransformFunction f = arguments.get(i);
      Preconditions.checkArgument(f instanceof IdentifierTransformFunction,
          "Argument " + i + " must be a column name (identifier)");
      _transformFunctions.add(f);
    }
    if (i < numArgs) {
      TransformFunction f = arguments.get(i++);
      Preconditions.checkArgument(f instanceof LiteralTransformFunction,
          "Argument " + i + " must be a default value (literal)");
      _defaultValue = ((LiteralTransformFunction) f).getStringLiteral();
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(FieldSpec.DataType.STRING, true, false);
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    initStringValuesSV(length);

    int functionIdx = 0;
    TransformFunction logtypeTransformFunction = _transformFunctions.get(functionIdx++);
    TransformFunction dictionaryVarsTransformFunction = _transformFunctions.get(functionIdx++);
    TransformFunction encodedVarsTransformFunction = _transformFunctions.get(functionIdx);
    String[] logtypes = logtypeTransformFunction.transformToStringValuesSV(valueBlock);
    String[][] dictionaryVars = dictionaryVarsTransformFunction.transformToStringValuesMV(valueBlock);
    long[][] encodedVars = encodedVarsTransformFunction.transformToLongValuesMV(valueBlock);

    MessageDecoder clpMessageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    for (int i = 0; i < length; i++) {
      try {
        _stringValuesSV[i] = clpMessageDecoder.decodeMessage(logtypes[i], dictionaryVars[i], encodedVars[i]);
      } catch (Exception ex) {
        _logger.error("Failed to decode CLP-encoded field.", ex);
        _stringValuesSV[i] = _defaultValue;
      }
    }

    return _stringValuesSV;
  }
}
