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
package org.apache.pinot.core.segment.index.readers.bloom;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.apache.pinot.spi.utils.StringUtils;


@SuppressWarnings("UnstableApiUsage")
public class GuavaBloomFilterReaderUtils {
  private GuavaBloomFilterReaderUtils() {
  }

  // DO NOT change the hash function. It has to be aligned with the bloom filter creator.
  private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

  /**
   * Returns the hash of the given value as a byte array.
   */
  public static byte[] hash(String value) {
    return HASH_FUNCTION.hashBytes(StringUtils.encodeUtf8(value)).asBytes();
  }
}
