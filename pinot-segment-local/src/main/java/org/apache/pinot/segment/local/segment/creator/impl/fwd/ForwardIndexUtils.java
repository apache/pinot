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
package org.apache.pinot.segment.local.segment.creator.impl.fwd;

public class ForwardIndexUtils {
  private static final int TARGET_MIN_CHUNK_SIZE = 4 * 1024;

  private ForwardIndexUtils() {
  }

  /**
   * Get the dynamic target chunk size based on the maximum length of the values, target number of documents per chunk.
   *
   * If targetDocsPerChunk is negative, the target chunk size is the targetMaxChunkSizeBytes and chunk size
   * shall not be dynamically chosen
   * @param maxLength max length of the values
   * @param targetDocsPerChunk target number of documents to store per chunk
   * @param targetMaxChunkSizeBytes target max chunk size in bytes
   */
  public static int getDynamicTargetChunkSize(int maxLength, int targetDocsPerChunk, int targetMaxChunkSizeBytes) {
    if (targetDocsPerChunk < 0 || (long) maxLength * targetDocsPerChunk > Integer.MAX_VALUE) {
      return Math.max(targetMaxChunkSizeBytes, TARGET_MIN_CHUNK_SIZE);
    }
    return Math.max(Math.min(maxLength * targetDocsPerChunk, targetMaxChunkSizeBytes), TARGET_MIN_CHUNK_SIZE);
  }
}
