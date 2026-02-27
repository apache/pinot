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
package org.apache.pinot.segment.local.indexsegment.immutable;

import java.io.File;
import javax.annotation.Nullable;
import org.apache.pinot.common.metadata.segment.SegmentZKMetadata;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;

/**
 * {@code SegmentMetadataPreProcessor} provides a hook to preprocess a segment's metadata
 * before the segment is fully loaded by the server.
 */
public interface SegmentMetadataPreProcessor {
  /**
   * Preprocesses the metadata for a segment located at the given index directory.
   *
   * @param indexDir
   *        The root directory of the segment index containing the segment metadata files.
   * @param indexLoadingConfig
   *        The {@link IndexLoadingConfig} used for loading the segment, which may influence
   *        preprocessing behavior (e.g. table name, segment type, or server-level settings).
   * @param zkMetadata
   *        Optional {@link SegmentZKMetadata} associated with the segment, if available.
   *        This may be {@code null} when the segment is loaded outside of Helix or Zookeeper
   *        context.
   *
   * @throws Exception
   *         If preprocessing fails and the segment should not be loaded. Throwing an
   *         exception will abort the segment loading process.
   */
  void process(File indexDir, IndexLoadingConfig indexLoadingConfig,
      @Nullable SegmentZKMetadata zkMetadata)
      throws Exception;
}
