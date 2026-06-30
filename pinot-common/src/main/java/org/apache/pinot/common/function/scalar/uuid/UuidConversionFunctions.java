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
import org.apache.pinot.spi.annotations.ScalarFunction;
import org.apache.pinot.spi.utils.UuidUtils;


/**
 * UUID conversion scalar functions.
 *
 * <p>These functions are stateless and thread-safe.
 */
public final class UuidConversionFunctions {
  private UuidConversionFunctions() {
  }

  @ScalarFunction(names = {"UUID_TO_BYTES"}, nullableParameters = true)
  public static byte[] uuidToBytes(UUID uuid) {
    return uuid != null ? UuidUtils.toBytes(uuid) : null;
  }

  @ScalarFunction(names = {"BYTES_TO_UUID"}, nullableParameters = true)
  public static UUID bytesToUuid(byte[] bytes) {
    return bytes != null ? UuidUtils.toUUID(bytes) : null;
  }

  @ScalarFunction(names = {"UUID_TO_STRING"}, nullableParameters = true)
  public static String uuidToString(UUID uuid) {
    return uuid != null ? UuidUtils.toString(uuid) : null;
  }

  /**
   * Generates a fresh random RFC 4122 version-4 UUID. Each invocation produces a new value, so this function
   * is non-deterministic and is annotated with {@code isDeterministic = false} to prevent the broker's
   * {@code CompileTimeFunctionsInvoker} from folding the call into a single literal that is reused for every row.
   */
  @ScalarFunction(names = {"UUID_V4"}, isDeterministic = false)
  public static UUID uuidV4() {
    return UuidUtils.randomV4();
  }

  /**
   * Generates a fresh RFC 9562 version-7 (Unix-time-based) UUID. The leading 48 bits encode the current Unix
   * time in milliseconds, making v7 UUIDs k-sortable and well-suited to time-ordered primary keys. Each
   * invocation produces a new value, so this function is non-deterministic and is annotated with
   * {@code isDeterministic = false} to prevent compile-time folding.
   */
  @ScalarFunction(names = {"UUID_V7"}, isDeterministic = false)
  public static UUID uuidV7() {
    return UuidUtils.randomV7();
  }

  /**
   * Returns the 4-bit version field (0-15) of the given UUID. Common values: 1, 3, 4, 5, 6, 7, 8.
   */
  @ScalarFunction(names = {"UUID_VERSION"}, nullableParameters = true)
  public static Integer uuidVersion(UUID uuid) {
    return uuid != null ? UuidUtils.getVersion(uuid) : null;
  }

  /**
   * Returns the embedded Unix-millisecond timestamp from a time-based UUID (version 1, 6, or 7). Throws
   * for non-time-based versions.
   */
  @ScalarFunction(names = {"UUID_TIMESTAMP"}, nullableParameters = true)
  public static Long uuidTimestamp(UUID uuid) {
    return uuid != null ? UuidUtils.getTimestampMillis(uuid) : null;
  }
}
