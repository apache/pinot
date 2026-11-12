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
package org.apache.pinot.common.function.scalar.uuid;

import java.util.Set;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.common.function.FunctionInfo;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.UuidUtils;


/// Polymorphic scalar function that validates STRING, BYTES, or UUID values as UUID inputs.
///
/// This implementation is stateless and thread-safe.
@ScalarFunction(names = {"IS_UUID"})
public class IsUuidScalarFunction extends AbstractUuidInputFunction {
  public IsUuidScalarFunction() {
    super(IsUuidScalarFunction.class, "IS_UUID", "isUuid", SqlTypeName.BOOLEAN);
  }

  @Override
  public Set<String> getNames() {
    return Set.of("IS_UUID", "ISUUID");
  }

  @Override
  protected FunctionInfo getDefaultFunctionInfo() {
    // The argument-count-only ingestion evaluator must preserve IS_UUID's false-on-invalid contract.
    return getStringFunctionInfo();
  }

  public static boolean isUuid(@Nullable String value) {
    return UuidUtils.isUuid(value);
  }

  public static boolean isUuid(@Nullable byte[] value) {
    return UuidUtils.isUuid(value);
  }

  public static boolean isUuid(@Nullable UUID value) {
    return value != null;
  }
}
