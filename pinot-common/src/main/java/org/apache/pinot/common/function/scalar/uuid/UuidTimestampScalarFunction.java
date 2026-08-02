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


/// Returns the embedded Unix-millisecond timestamp from a time-based STRING, BYTES, or UUID input.
///
/// This implementation is stateless and thread-safe.
@ScalarFunction(names = {"UUID_TIMESTAMP"})
public class UuidTimestampScalarFunction extends AbstractUuidInputFunction {
  public UuidTimestampScalarFunction() {
    super(UuidTimestampScalarFunction.class, "UUID_TIMESTAMP", "uuidTimestamp", SqlTypeName.BIGINT);
  }

  @Nullable
  public static Long uuidTimestamp(@Nullable String value) {
    return value != null ? UuidUtils.getTimestampMillis(UuidUtils.toBytes(value)) : null;
  }

  @Nullable
  public static Long uuidTimestamp(@Nullable byte[] value) {
    return value != null ? UuidUtils.getTimestampMillis(value) : null;
  }

  @Nullable
  public static Long uuidTimestamp(@Nullable UUID value) {
    return value != null ? UuidUtils.getTimestampMillis(value) : null;
  }
}
