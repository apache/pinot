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
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.VariantUtils;
import org.apache.pinot.common.utils.VariantUtils.ReusableResult;
import org.apache.pinot.common.utils.VariantUtils.VariantPath;
import org.apache.pinot.core.operator.blocks.ValueBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;


/// Shared single-stage lifecycle for functions that inspect or extract a VARIANT value.
///
/// <p>The first argument is consistently validated as a single-value VARIANT-compatible operand, the optional path is
/// compiled once, and each input block is evaluated at most once even when the engine asks for both values and a null
/// bitmap. Instances are query-local and not thread-safe.
abstract class BaseVariantTransformFunction extends BaseTransformFunction {
  private static final VariantPath ROOT_PATH = VariantUtils.compilePath("$");

  private TransformFunction _variantTransformFunction;
  private VariantPath _path;
  private final ReusableResult _reusableResult = new ReusableResult();
  @Nullable
  private ValueBlock _cachedValueBlock;
  @Nullable
  private RoaringBitmap _cachedNullBitmap;

  /// Initializes the common VARIANT operand and path contract.
  ///
  /// @param arguments function arguments
  /// @param minArguments minimum accepted argument count
  /// @param maxArguments maximum accepted argument count
  /// @param pathRequired whether argument 1 is mandatory
  protected final void initVariantArguments(List<TransformFunction> arguments, int minArguments, int maxArguments,
      boolean pathRequired) {
    int numArguments = arguments.size();
    if (numArguments < minArguments || numArguments > maxArguments) {
      throw new IllegalArgumentException(argumentCountMessage(minArguments, maxArguments));
    }

    _variantTransformFunction = arguments.get(0);
    TransformResultMetadata inputMetadata = _variantTransformFunction.getResultMetadata();
    DataType inputType = inputMetadata.getDataType();
    if (!inputMetadata.isSingleValue()
        || (inputType != DataType.VARIANT && inputType != DataType.BYTES && inputType != DataType.UNKNOWN)) {
      throw new IllegalArgumentException(getName() + " first argument must be a single-value VARIANT");
    }

    if (pathRequired || numArguments >= 2) {
      TransformFunction pathTransformFunction = arguments.get(1);
      if (!(pathTransformFunction instanceof LiteralTransformFunction)
          || pathTransformFunction.getResultMetadata().getDataType() != DataType.STRING) {
        throw new IllegalArgumentException(getName() + " path must be a string literal");
      }
      _path = VariantUtils.compilePath(((LiteralTransformFunction) pathTransformFunction).getStringLiteral());
    } else {
      _path = ROOT_PATH;
    }
    _cachedValueBlock = null;
    _cachedNullBitmap = null;
  }

  private String argumentCountMessage(int minArguments, int maxArguments) {
    if (minArguments == maxArguments) {
      return getName() + " expects exactly " + minArguments + " arguments";
    }
    return getName() + " expects " + minArguments + " to " + maxArguments + " arguments";
  }

  protected final VariantPath getVariantPath() {
    return _path;
  }

  protected final ReusableResult getReusableResult() {
    return _reusableResult;
  }

  @Nullable
  @Override
  public final RoaringBitmap getNullBitmap(ValueBlock valueBlock) {
    ensureEvaluated(valueBlock);
    return _cachedNullBitmap;
  }

  protected final void ensureEvaluated(ValueBlock valueBlock) {
    if (_cachedValueBlock == valueBlock) {
      return;
    }

    // Invalidate before mutating reusable output arrays. A strict extraction failure must never leave a partially
    // overwritten block marked as cached.
    _cachedValueBlock = null;
    _cachedNullBitmap = null;
    int numDocs = valueBlock.getNumDocs();
    initResultValues(numDocs);

    RoaringBitmap inputNulls = _variantTransformFunction.getNullBitmap(valueBlock);
    RoaringBitmap resultNulls =
        inputNullIsResultNull() && inputNulls != null ? inputNulls.clone() : new RoaringBitmap();
    byte[][] variants = _variantTransformFunction.transformToBytesValuesSV(valueBlock);
    for (int i = 0; i < numDocs; i++) {
      if (inputNulls != null && inputNulls.contains(i)) {
        setNullValue(i);
        continue;
      }
      if (!evaluateVariant(variants[i], i)) {
        resultNulls.add(i);
        setNullValue(i);
      }
    }

    _cachedNullBitmap = resultNulls.isEmpty() ? null : resultNulls;
    _cachedValueBlock = valueBlock;
  }

  /// Returns whether an input SQL null should remain SQL null in the result.
  protected boolean inputNullIsResultNull() {
    return true;
  }

  /// Initializes the output array for a block.
  protected abstract void initResultValues(int numDocs);

  /// Evaluates one non-SQL-null VARIANT. Returns {@code false} when the result should be SQL null.
  protected abstract boolean evaluateVariant(@Nullable byte[] variant, int index);

  /// Stores the type-specific placeholder for a SQL-null result.
  protected abstract void setNullValue(int index);
}
