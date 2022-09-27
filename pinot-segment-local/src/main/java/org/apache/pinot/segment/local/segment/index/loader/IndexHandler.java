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
package org.apache.pinot.segment.local.segment.index.loader;

import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.store.SegmentDirectory;


/**
 * Interface for index handlers, which update the corresponding type of indices,
 * like adding, removing or converting the format.
 */
public interface IndexHandler {
  /**
   * Adds new indices and removes obsolete indices.
   */
  void updateIndices(SegmentDirectory.Writer segmentWriter, IndexCreatorProvider indexCreatorProvider)
      throws Exception;

  /**
   * Check if there is a need to add new indices or removes obsolete indices.
   * @return true if there is a need to update.
   */
  boolean needUpdateIndices(SegmentDirectory.Reader segmentReader)
      throws Exception;
}
