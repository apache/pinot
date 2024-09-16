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
package org.apache.pinot.spi.ingestion.segment.uploader;

import java.net.URI;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.spi.annotations.InterfaceStability;
import org.apache.pinot.spi.auth.AuthProvider;
import org.apache.pinot.spi.config.table.TableConfig;


/**
 * Interface for uploading segments to Pinot
 */
@InterfaceStability.Evolving
public interface SegmentUploader {

  /**
   * @see #init(TableConfig, Map)
   */
  void init(TableConfig tableConfig)
      throws Exception;

  /**
   * Initializes the {@link SegmentUploader}
   * @param tableConfig The table config for the segment upload
   * @param batchConfigOverride The config override on top of tableConfig
   */
  void init(TableConfig tableConfig, Map<String, String> batchConfigOverride)
      throws Exception;

  /**
   * Uploads the segment tar file to the cluster
   * @param segmentTarFile URI of segment tar file
   * @param authProvider auth provider
   */
  void uploadSegment(URI segmentTarFile, @Nullable AuthProvider authProvider)
      throws Exception;

  /**
   * Uploads the segments from the segmentDir to the cluster.
   * Looks for segmentTar files recursively, with suffix .tar.gz
   * @param segmentDir URI of directory containing segment tar files
   * @param authProvider auth auth provider
   */
  void uploadSegmentsFromDir(URI segmentDir, @Nullable AuthProvider authProvider)
      throws Exception;
}
