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
import com.yscope.clp.compressorfrontend.AbstractClpEncodedSubquery;
import com.yscope.clp.compressorfrontend.BuiltInVariableHandlingRuleVersions;
import com.yscope.clp.compressorfrontend.EightByteClpEncodedSubquery;
import com.yscope.clp.compressorfrontend.EightByteClpWildcardQueryEncoder;
import com.yscope.clp.compressorfrontend.MessageDecoder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Performs a wildcard match on the encoded variables of a CLP-encoded column group. This is used by the clpMatch
 * function (implemented using {@link org.apache.pinot.sql.parsers.rewriter.ClpRewriter}) and likely wouldn't be called
 * manually by a user.
 * <p>
 * Syntax:
 * <pre>
 *   clpEncodedVarsMatch(columnGroupName_logtype, columnGroupName_encodedVars, wildcardQuery, subQueryIndex)
 * </pre>
 */
public class ClpEncodedVarsMatchTransformFunction extends BaseTransformFunction {
  private static final Logger _logger = LoggerFactory.getLogger(ClpEncodedVarsMatchTransformFunction.class);

  private final List<TransformFunction> _transformFunctions = new ArrayList<>();
  private byte[] _serializedVarTypes;
  private byte[] _serializedVarWildcardQueries;
  private int[] _varWildcardQueryEndIndexes;

  @Override
  public String getName() {
    return TransformFunctionType.CLP_ENCODED_VARS_MATCH.getName();
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap) {
    Preconditions.checkArgument(arguments.size() == 4, "Syntax error: clpEncodedVarsMatch takes 4 arguments - "
        + "clpEncodedVarsMatch(columnGroupName_logtype, columnGroupName_encodedVars, wildcardQuery, subQueryIndex");

    Iterator<TransformFunction> argsIter = arguments.iterator();

    TransformFunction f = argsIter.next();
    Preconditions.checkArgument(f instanceof IdentifierTransformFunction, "1st argument must be an identifier");
    _transformFunctions.add(f);

    f = argsIter.next();
    Preconditions.checkArgument(f instanceof IdentifierTransformFunction, "2nd argument must be an identifier");
    _transformFunctions.add(f);

    f = argsIter.next();
    Preconditions.checkArgument(f instanceof LiteralTransformFunction, "3rd argument must be a literal");
    String wildcardQuery = ((LiteralTransformFunction) f).getStringLiteral();

    f = argsIter.next();
    Preconditions.checkArgument(f instanceof LiteralTransformFunction, "4th argument must be a literal");
    long subqueryIndex = ((LiteralTransformFunction) f).getLongLiteral();

    EightByteClpWildcardQueryEncoder queryEncoder =
        new EightByteClpWildcardQueryEncoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
            BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    EightByteClpEncodedSubquery[] subqueries = queryEncoder.encode(wildcardQuery);
    if (subqueryIndex < 0 || subqueryIndex > subqueries.length) {
      throw new IllegalArgumentException("Invalid subquery index.");
    }
    EightByteClpEncodedSubquery subquery = subqueries[(int) subqueryIndex];
    int numEncodedVarWildcardQueries = subquery.getNumEncodedVarWildcardQueries();
    if (0 == numEncodedVarWildcardQueries) {
      throw new IllegalArgumentException("Subquery doesn't contain any wildcard queries for encoded variables.");
    }

    try {
      ByteArrayOutputStream serializedVarTypes = new ByteArrayOutputStream();
      ByteArrayOutputStream serializedWildcardQueries = new ByteArrayOutputStream();
      List<Integer> serializedWildcardQueryEndIndices = new ArrayList<>();
      for (AbstractClpEncodedSubquery.VariableWildcardQuery q : subquery.getEncodedVarWildcardQueries()) {
        serializedVarTypes.write(q.getType());
        serializedWildcardQueries.write(q.getQuery().toByteArray());
        serializedWildcardQueryEndIndices.add(serializedWildcardQueries.size());
      }
      _serializedVarTypes = serializedVarTypes.toByteArray();
      _serializedVarWildcardQueries = serializedWildcardQueries.toByteArray();
      _varWildcardQueryEndIndexes = ArrayUtils.toPrimitive(serializedWildcardQueryEndIndices.toArray(new Integer[0]));
    } catch (IOException e) {
      throw new IllegalArgumentException("Wildcard query could not be serialized", e);
    }
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return new TransformResultMetadata(FieldSpec.DataType.BOOLEAN, true, false);
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    int length = valueBlock.getNumDocs();
    if (null == _intValuesSV) {
      _intValuesSV = new int[length];
    }

    int functionIdx = 0;
    TransformFunction logtypeTransformFunction = _transformFunctions.get(functionIdx++);
    TransformFunction encodedVarsTransformFunction = _transformFunctions.get(functionIdx++);
    byte[][] logtypes = logtypeTransformFunction.transformToBytesValuesSV(valueBlock);
    long[][] encodedVars = encodedVarsTransformFunction.transformToLongValuesMV(valueBlock);

    MessageDecoder clpMessageDecoder = new MessageDecoder(BuiltInVariableHandlingRuleVersions.VariablesSchemaV2,
        BuiltInVariableHandlingRuleVersions.VariableEncodingMethodsV1);
    try {
      clpMessageDecoder.batchEncodedVarsWildcardMatch(logtypes, encodedVars, _serializedVarTypes,
          _serializedVarWildcardQueries, _varWildcardQueryEndIndexes, _intValuesSV);
    } catch (IOException ex) {
      _logger.error("Wildcard match on encoded variables failed.", ex);
      Arrays.fill(_intValuesSV, 0);
    }

    return _intValuesSV;
  }
}
