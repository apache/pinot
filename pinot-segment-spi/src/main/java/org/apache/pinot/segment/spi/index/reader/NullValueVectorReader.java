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

import java.io.IOException;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Reader interface to read from an underlying Null value vector. This is
 * primarily used to check if a particular column value corresponding to
 * a document ID is null or not.
 */
public interface NullValueVectorReader extends IndexReader {

  /**
   * Check if the given docId has a null value in the corresponding column
   *
   * @param docId specifies ID to check for nullability
   * @return true if docId is absent (null). False otherwise
   */
  boolean isNull(int docId);

  /**
   * Return the underlying null bitmap (used in query execution)
   */
  ImmutableRoaringBitmap getNullBitmap();

  @Override
  default void close()
      throws IOException {
  }
}
