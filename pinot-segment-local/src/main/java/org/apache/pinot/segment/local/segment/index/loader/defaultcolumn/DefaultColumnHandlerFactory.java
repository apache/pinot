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
package org.apache.pinot.segment.local.segment.index.loader.defaultcolumn;

import java.io.File;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.segment.spi.creator.SegmentVersion;
import org.apache.pinot.spi.data.Schema;


public class DefaultColumnHandlerFactory {
  private DefaultColumnHandlerFactory() {
  }

  public static DefaultColumnHandler getDefaultColumnHandler(File indexDir, SegmentMetadataImpl segmentMetadata,
      IndexLoadingConfig indexLoadingConfig, Schema schema, SegmentDirectory.Writer segmentWriter) {
    if (SegmentVersion.valueOf(segmentMetadata.getVersion()) == SegmentVersion.v3) {
      return new V3DefaultColumnHandler(indexDir, segmentMetadata, indexLoadingConfig, schema, segmentWriter);
    } else {
      return new V1DefaultColumnHandler(indexDir, segmentMetadata, indexLoadingConfig, schema, segmentWriter);
    }
  }
}
