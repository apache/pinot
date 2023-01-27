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
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.data.Schema;


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

    @Override
    public void postUpdateIndicesCleanup(SegmentDirectory.Writer segmentWriter) {
    }
  };

  public static IndexHandler getIndexHandler(IndexType<?, ?, ?> type, SegmentDirectory segmentDirectory,
      IndexLoadingConfig indexLoadingConfig, Schema schema) {
    // TODO(index-spi): this will change once index handler creation is moved to IndexType
    if (type.equals(StandardIndexes.inverted())) {
      return new InvertedIndexHandler(segmentDirectory, indexLoadingConfig);
    }
    if (type.equals(StandardIndexes.range())) {
      return new RangeIndexHandler(segmentDirectory, indexLoadingConfig);
    }
    if (type.equals(StandardIndexes.text())) {
      return new TextIndexHandler(segmentDirectory, indexLoadingConfig);
    }
    if (type.equals(StandardIndexes.fst())) {
      return new FSTIndexHandler(segmentDirectory, indexLoadingConfig);
    }
    if (type.equals(StandardIndexes.json())) {
      return new JsonIndexHandler(segmentDirectory, indexLoadingConfig);
    }
    if (type.equals(StandardIndexes.h3())) {
      return new H3IndexHandler(segmentDirectory, indexLoadingConfig);
    }
    if (type.equals(StandardIndexes.bloomFilter())) {
      return new BloomFilterHandler(segmentDirectory, indexLoadingConfig);
    }
    if (type.equals(StandardIndexes.forward())) {
      return new ForwardIndexHandler(segmentDirectory, indexLoadingConfig, schema);
    }
    return NO_OP_HANDLER;
  }
}
