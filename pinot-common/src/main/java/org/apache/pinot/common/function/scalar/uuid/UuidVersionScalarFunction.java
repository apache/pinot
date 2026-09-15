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

import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.UuidUtils;


/// Returns the UUID version from STRING, BYTES, or UUID inputs.
///
/// This implementation is stateless and thread-safe.
@ScalarFunction(names = {"UUID_VERSION"})
public class UuidVersionScalarFunction extends AbstractUuidInputFunction {
  public UuidVersionScalarFunction() {
    super(UuidVersionScalarFunction.class, "UUID_VERSION", "uuidVersion", SqlTypeName.INTEGER);
  }

  @Nullable
  public static Integer uuidVersion(@Nullable String value) {
    return value != null ? UuidUtils.getVersion(UuidUtils.toBytes(value)) : null;
  }

  @Nullable
  public static Integer uuidVersion(@Nullable byte[] value) {
    return value != null ? UuidUtils.getVersion(value) : null;
  }

  @Nullable
  public static Integer uuidVersion(@Nullable UUID value) {
    return value != null ? UuidUtils.getVersion(value) : null;
  }
}
