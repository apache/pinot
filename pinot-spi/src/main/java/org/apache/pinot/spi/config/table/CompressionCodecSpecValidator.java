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
package org.apache.pinot.spi.config.table;

import javax.annotation.Nullable;
import org.apache.pinot.spi.config.table.FieldConfig.CompressionCodec;


/**
 * Validates configured compression codec specs and provides a narrow compatibility shim for legacy enum accessors.
 *
 * <p>This class is thread-safe because it is stateless.
 */
public final class CompressionCodecSpecValidator {

  private CompressionCodecSpecValidator() {
  }

  public static void validate(@Nullable CompressionCodecSpec compressionCodecSpec) {
    if (compressionCodecSpec == null) {
      return;
    }
    CompressionCodec codec = compressionCodecSpec.getCodec();
    CompressionCodecCapabilities.validateLevelSupport(codec.name(), compressionCodecSpec.getLevel(),
        compressionCodecSpec.toString());
  }

  @Nullable
  public static CompressionCodec getCompressionCodec(@Nullable CompressionCodecSpec compressionCodecSpec) {
    return compressionCodecSpec != null ? compressionCodecSpec.getCodec() : null;
  }
}
