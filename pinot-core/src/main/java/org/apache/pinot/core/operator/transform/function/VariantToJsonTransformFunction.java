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
import org.apache.pinot.spi.utils.CommonConstants.NullValuePlaceHolder;


/// Renders a Variant value as canonical JSON text.
///
/// <p>SQL null produces SQL null, an encoded Variant null produces the JSON text {@code null}, and the Variant
/// string {@code "null"} produces quoted JSON text. Instances are query-local and not thread-safe.
public class VariantToJsonTransformFunction extends BaseVariantTransformFunction {
  public static final String FUNCTION_NAME = "variantToJson";

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, ColumnContext> columnContextMap,
      boolean nullHandlingEnabled) {
    super.init(arguments, columnContextMap, nullHandlingEnabled);
    initVariantArguments(arguments, 1, 1, false);
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return STRING_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public String[] transformToStringValuesSV(ValueBlock valueBlock) {
    ensureEvaluated(valueBlock);
    return _stringValuesSV;
  }

  @Override
  protected void initResultValues(int numDocs) {
    initStringValuesSV(numDocs);
  }

  @Override
  protected boolean evaluateVariant(@Nullable byte[] variant, int index) {
    String json = VariantUtils.variantToJson(variant, getReusableResult());
    if (json != null) {
      _stringValuesSV[index] = json;
      return true;
    }
    return false;
  }

  @Override
  protected void setNullValue(int index) {
    _stringValuesSV[index] = NullValuePlaceHolder.STRING;
  }
}
