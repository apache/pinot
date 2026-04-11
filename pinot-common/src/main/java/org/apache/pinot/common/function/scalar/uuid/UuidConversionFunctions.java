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
}
