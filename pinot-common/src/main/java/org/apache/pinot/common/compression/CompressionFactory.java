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
package org.apache.pinot.common.compression;

import org.apache.pinot.spi.utils.CommonConstants;


public class CompressionFactory {
  private CompressionFactory() {
  }

  public static String getDefaultCompressionType() {
    return CommonConstants.Broker.Grpc.DEFAULT_COMPRESSION;
  }

  public static String[] getCompressionTypes() {
    return new String[]{
        "LZ4", "LZ4_FAST", "LZ4_HIGH",
        "ZSTD", "ZSTANDARD",
        "DEFLATE",
        "GZIP",
        "SNAPPY",
        "PASS_THROUGH", "NONE"
    };
  }

  public static Compressor getCompressor(String type) {
    switch (type.toUpperCase()) {
      case "LZ4":
      case "LZ4_FAST":
        return Lz4Compressor.FAST_INSTANCE;
      case "LZ4_HIGH":
        return Lz4Compressor.HIGH_INSTANCE;
      case "ZSTD":
      case "ZSTANDARD":
        return ZstdCompressor.DEFAULT_INSTANCE;
      case "DEFLATE":
        return DeflateCompressor.INSTANCE;
      case "GZIP":
        return GzipCompressor.INSTANCE;
      case "SNAPPY":
        return SnappyCompressor.INSTANCE;
      case "PASS_THROUGH":
      case "NONE":
        return NoCompressor.INSTANCE;
      default:
        throw new IllegalArgumentException("Unknown compression type: " + type);
    }
  }
}
