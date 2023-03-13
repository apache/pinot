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

package org.apache.pinot.segment.local.segment.index.inverted;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OnHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.index.forward.ForwardIndexType;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.InvertedIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.BitmapInvertedIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.SortedIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.IndexConfig;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.Schema;


public class InvertedIndexType
    extends AbstractIndexType<IndexConfig, InvertedIndexReader, DictionaryBasedInvertedIndexCreator>
    implements ConfigurableFromIndexLoadingConfig<IndexConfig> {
  public static final InvertedIndexType INSTANCE = new InvertedIndexType();

  private InvertedIndexType() {
  }

  @Override
  public String getId() {
    return "inverted_index";
  }

  @Override
  public Class<IndexConfig> getIndexConfigClass() {
    return IndexConfig.class;
  }

  @Override
  public Map<String, IndexConfig> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    return indexLoadingConfig.getInvertedIndexColumns().stream()
        .collect(Collectors.toMap(Function.identity(), v -> IndexConfig.ENABLED));
  }

  @Override
  public IndexConfig getDefaultConfig() {
    return IndexConfig.DISABLED;
  }

  @Override
  public ColumnConfigDeserializer<IndexConfig> getDeserializer() {
    ColumnConfigDeserializer<IndexConfig> fromInvertedCols = IndexConfigDeserializer.fromCollection(
        tableConfig -> tableConfig.getIndexingConfig().getInvertedIndexColumns(),
        (acum, column) -> acum.put(column, IndexConfig.ENABLED));
    return IndexConfigDeserializer.fromIndexes("inverted", getIndexConfigClass())
        .withExclusiveAlternative(IndexConfigDeserializer.ifIndexingConfig(fromInvertedCols));
  }

  public DictionaryBasedInvertedIndexCreator createIndexCreator(IndexCreationContext context)
      throws IOException {
    if (context.isOnHeap()) {
      return new OnHeapBitmapInvertedIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(),
          context.getCardinality());
    } else {
      return new OffHeapBitmapInvertedIndexCreator(context.getIndexDir(), context.getFieldSpec(),
          context.getCardinality(), context.getTotalDocs(), context.getTotalNumberOfEntries());
    }
  }

  @Override
  public DictionaryBasedInvertedIndexCreator createIndexCreator(IndexCreationContext context,
      IndexConfig indexConfig)
      throws IOException {
    return createIndexCreator(context);
  }

  @Override
  public IndexReaderFactory<InvertedIndexReader> getReaderFactory() {
    return new ReaderFactory();
  }

  @Override
  @Nullable
  public InvertedIndexReader getIndexReader(ColumnIndexContainer indexContainer) {
    InvertedIndexReader reader = super.getIndexReader(indexContainer);
    if (reader != null) {
      return reader;
    }
    ForwardIndexReader fwdReader = indexContainer.getIndex(StandardIndexes.forward());
    if (fwdReader != null && fwdReader instanceof SortedIndexReader) {
      return (SortedIndexReader) fwdReader;
    }
    return null;
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return V1Constants.Indexes.BITMAP_INVERTED_INDEX_FILE_EXTENSION;
  }

  @Override
  public String toString() {
    return getId();
  }

  public InvertedIndexReader<?> read(SegmentDirectory.Reader segmentReader, ColumnMetadata columnMetadata)
      throws IOException {
    return new ReaderFactory().createIndexReader(segmentReader, columnMetadata);
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new InvertedIndexHandler(segmentDirectory, configsByCol, tableConfig);
  }

  public static class ReaderFactory implements IndexReaderFactory<InvertedIndexReader> {
    @Override
    public InvertedIndexReader createIndexReader(SegmentDirectory.Reader segmentReader,
        FieldIndexConfigs fieldIndexConfigs, ColumnMetadata metadata)
        throws IOException {
      if (fieldIndexConfigs == null || !fieldIndexConfigs.getConfig(StandardIndexes.inverted()).isEnabled()) {
        return null;
      }
      return createIndexReader(segmentReader, metadata);
    }


    public InvertedIndexReader createIndexReader(SegmentDirectory.Reader segmentReader, ColumnMetadata metadata)
        throws IOException {
      if (!metadata.hasDictionary()) {
        throw new IllegalStateException("Column " + metadata.getColumnName() + " cannot be indexed by an inverted "
            + "index if it has no dictionary");
      }
      if (metadata.isSorted() && metadata.isSingleValue()) {
        // TODO (index-spi): Check if it is worth to catch the forward index
        ForwardIndexReader fwdReader = ForwardIndexType.INSTANCE.read(segmentReader, metadata);
        Preconditions.checkState(fwdReader instanceof SortedIndexReader);
        return (SortedIndexReader) fwdReader;
      } else {
        PinotDataBuffer dataBuffer = segmentReader.getIndexFor(metadata.getColumnName(), StandardIndexes.inverted());
        return new BitmapInvertedIndexReader(dataBuffer, metadata.getCardinality());
      }
    }
  }
}
