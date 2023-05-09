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

import org.apache.pinot.segment.spi.index.IndexReader;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * Reader of the H3 index.
 */
public interface H3IndexReader extends IndexReader {

  /**
   * Gets the matching Doc IDs of the given H3 index ID as bitmaps.
   * @param h3IndexId the H3 index ID to match
   * @return the matched DocIDs
   */
  ImmutableRoaringBitmap getDocIds(long h3IndexId);

  /**
   * @return the H3 index resolutions
   */
  H3IndexResolution getH3IndexResolution();
}
