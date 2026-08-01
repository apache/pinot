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
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.core.operator.ColumnContext;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;


/// Vectorized single-stage implementation of {@code variantExists}.
///
/// <p>The path must be a string literal and is compiled once during initialization. A present encoded Variant null
/// counts as present, a missing path returns {@code false}, and SQL null remains SQL null. Instances are query-local
/// and not thread-safe.
public class VariantExistsTransformFunction extends BaseVariantTransformFunction {
  public static final String FUNCTION_NAME = "variantExists";

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      boolean nullHandlingEnabled) {
    super.init(arguments, columnContextMap, nullHandlingEnabled);
    initVariantArguments(arguments, 2, 2, true);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return BOOLEAN_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public int[] transformToIntValuesSV(ValueBlock valueBlock) {
    ensureEvaluated(valueBlock);
    return _intValuesSV;
  }

  @Override
  protected void initResultValues(int numDocs) {
    initIntValuesSV(numDocs);
  }

  @Override
  protected boolean evaluateVariant(@Nullable byte[] variant, int index) {
    Boolean exists = VariantUtils.variantExists(variant, getVariantPath(), getReusableResult());
    if (exists != null) {
      _intValuesSV[index] = exists ? 1 : 0;
      return true;
    }
    return false;
  }

  @Override
  protected void setNullValue(int index) {
    _intValuesSV[index] = 0;
  }
}
