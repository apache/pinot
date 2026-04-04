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
package org.apache.pinot.segment.spi.index.reader;

import java.util.Collections;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Reader for vector index.
 */
public interface VectorIndexReader extends IndexReader {
  /**
   * Returns the bitmap of top k closest vectors from the given vector.
   * @param vector vector to search
   * @param topK number of closest vectors to return
   * @return bitmap of top k closest vectors
   */
  ImmutableRoaringBitmap getDocIds(float[] vector, int topK);

  /**
   * Returns the bitmap of top k closest vectors with runtime search parameters.
   * Default implementation ignores searchParams and delegates to {@link #getDocIds(float[], int)}.
   *
   * @param vector vector to search
   * @param topK number of closest vectors to return
   * @param searchParams runtime search parameters (e.g., vectorNprobe, vectorExactRerank)
   * @return bitmap of top k closest vectors
   */
  default ImmutableRoaringBitmap getDocIds(float[] vector, int topK, @Nullable Map<String, String> searchParams) {
    return getDocIds(vector, topK);
  }

  /**
   * Returns metadata about the index backend for explain/debug purposes.
   * Default implementation returns an empty map.
   */
  default Map<String, String> getIndexDebugInfo() {
    return Collections.emptyMap();
  }
}
