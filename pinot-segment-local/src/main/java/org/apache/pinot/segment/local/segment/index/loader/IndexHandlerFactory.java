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

import org.apache.pinot.segment.local.segment.index.loader.bloomfilter.BloomFilterHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.FSTIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.H3IndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.InvertedIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.JsonIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.RangeIndexHandler;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.TextIndexHandler;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.store.ColumnIndexType;
import org.apache.pinot.segment.spi.store.SegmentDirectory;


public class IndexHandlerFactory {
  private IndexHandlerFactory() {
  }

  private static final IndexHandler NO_OP_HANDLER = new IndexHandler() {
    @Override
    public void updateIndices(SegmentDirectory.Writer segmentWriter, IndexCreatorProvider indexCreatorProvider) {
    }

    @Override
    public boolean needUpdateIndices(SegmentDirectory.Reader segmentReader) {
      return false;
    }
  };

  public static IndexHandler getIndexHandler(ColumnIndexType type, SegmentMetadataImpl segmentMetadata,
      IndexLoadingConfig indexLoadingConfig) {
    switch (type) {
      case INVERTED_INDEX:
        return new InvertedIndexHandler(segmentMetadata, indexLoadingConfig);
      case RANGE_INDEX:
        return new RangeIndexHandler(segmentMetadata, indexLoadingConfig);
      case TEXT_INDEX:
        return new TextIndexHandler(segmentMetadata, indexLoadingConfig);
      case FST_INDEX:
        return new FSTIndexHandler(segmentMetadata, indexLoadingConfig);
      case JSON_INDEX:
        return new JsonIndexHandler(segmentMetadata, indexLoadingConfig);
      case H3_INDEX:
        return new H3IndexHandler(segmentMetadata, indexLoadingConfig);
      case BLOOM_FILTER:
        return new BloomFilterHandler(segmentMetadata, indexLoadingConfig);
      default:
        return NO_OP_HANDLER;
    }
  }
}
