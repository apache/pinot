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

import java.io.File;
import org.apache.pinot.segment.local.segment.index.loader.bloomfilter.BloomFilterHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.H3IndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.InvertedIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.JsonIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.LuceneFSTIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.RangeIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.TextIndexHandler;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;


public class IndexHandlerFactory {
  private static final IndexHandler _defaultHandler = () -> {
    // noop by default.
  };

  public static IndexHandler newHandler(ColumnIndexType type, File indexDir, SegmentMetadataImpl segmentMetadata,
      IndexLoadingConfig indexLoadingConfig, SegmentDirectory.Writer segmentWriter) {
    switch (type) {
      case INVERTED_INDEX:
        return new InvertedIndexHandler(indexDir, segmentMetadata, indexLoadingConfig, segmentWriter);
      case RANGE_INDEX:
        return new RangeIndexHandler(indexDir, segmentMetadata, indexLoadingConfig, segmentWriter);
      case TEXT_INDEX:
        return new TextIndexHandler(indexDir, segmentMetadata, indexLoadingConfig, segmentWriter);
      case FST_INDEX:
        return new LuceneFSTIndexHandler(indexDir, segmentMetadata, indexLoadingConfig, segmentWriter);
      case JSON_INDEX:
        return new JsonIndexHandler(indexDir, segmentMetadata, indexLoadingConfig, segmentWriter);
      case H3_INDEX:
        return new H3IndexHandler(indexDir, segmentMetadata, indexLoadingConfig, segmentWriter);
      case BLOOM_FILTER:
        return new BloomFilterHandler(indexDir, segmentMetadata, indexLoadingConfig, segmentWriter);
      default:
        return _defaultHandler;
    }
  }
}
