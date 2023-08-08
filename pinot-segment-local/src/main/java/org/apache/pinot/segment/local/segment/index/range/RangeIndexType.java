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

package org.apache.pinot.segment.local.segment.index.range;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.RangeIndexCreator;
import org.apache.pinot.segment.local.segment.index.loader.ConfigurableFromIndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.segment.local.segment.index.loader.invertedindex.RangeIndexHandler;
import org.apache.pinot.segment.local.segment.index.readers.BitSlicedRangeIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.RangeIndexReaderImpl;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.V1Constants;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.AbstractIndexType;
import org.apache.pinot.segment.spi.index.ColumnConfigDeserializer;
import org.apache.pinot.segment.spi.index.FieldIndexConfigs;
import org.apache.pinot.segment.spi.index.IndexConfigDeserializer;
import org.apache.pinot.segment.spi.index.IndexHandler;
import org.apache.pinot.segment.spi.index.IndexReaderConstraintException;
import org.apache.pinot.segment.spi.index.IndexReaderFactory;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.RangeIndexConfig;
import org.apache.pinot.segment.spi.index.StandardIndexes;
import org.apache.pinot.segment.spi.index.creator.CombinedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.config.table.TableConfig;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;


public class RangeIndexType extends AbstractIndexType<RangeIndexConfig, RangeIndexReader, CombinedInvertedIndexCreator>
  implements ConfigurableFromIndexLoadingConfig<RangeIndexConfig> {
  public static final String INDEX_DISPLAY_NAME = "range";

  protected RangeIndexType() {
    super(StandardIndexes.RANGE_ID);
  }

  @Override
  public Class<RangeIndexConfig> getIndexConfigClass() {
    return RangeIndexConfig.class;
  }

  @Override
  public Map<String, RangeIndexConfig> fromIndexLoadingConfig(IndexLoadingConfig indexLoadingConfig) {
    int rangeVersion = indexLoadingConfig.getRangeIndexVersion();
    return indexLoadingConfig.getRangeIndexColumns().stream()
        .collect(Collectors.toMap(
            Function.identity(),
            c -> new RangeIndexConfig(rangeVersion)));
  }

  @Override
  public RangeIndexConfig getDefaultConfig() {
    return RangeIndexConfig.DISABLED;
  }

  @Override
  public String getPrettyName() {
    return INDEX_DISPLAY_NAME;
  }

  @Override
  public ColumnConfigDeserializer<RangeIndexConfig> createDeserializer() {
    return IndexConfigDeserializer.fromIndexes(getPrettyName(), getIndexConfigClass())
        .withExclusiveAlternative((tableConfig, schema) -> {
          if (tableConfig.getIndexingConfig() == null) {
            return Collections.emptyMap();
          }
          List<String> rangeIndexColumns = tableConfig.getIndexingConfig().getRangeIndexColumns();
          if (rangeIndexColumns == null) {
            return Collections.emptyMap();
          }
          int rangeVersion = tableConfig.getIndexingConfig().getRangeIndexVersion();
          if (rangeVersion == 0) {
            rangeVersion = RangeIndexConfig.DEFAULT.getVersion();
          }
          Map<String, RangeIndexConfig> result = new HashMap<>();
          for (String col : rangeIndexColumns) {
            result.put(col, new RangeIndexConfig(rangeVersion));
          }
          return result;
        });
  }

  @Override
  public CombinedInvertedIndexCreator createIndexCreator(IndexCreationContext context, RangeIndexConfig indexConfig)
      throws IOException {
    if (indexConfig.getVersion() == BitSlicedRangeIndexCreator.VERSION && context.getFieldSpec().isSingleValueField()) {
      if (context.hasDictionary()) {
        return new BitSlicedRangeIndexCreator(context.getIndexDir(), context.getFieldSpec(), context.getCardinality());
      }
      return new BitSlicedRangeIndexCreator(context.getIndexDir(), context.getFieldSpec(), context.getMinValue(),
          context.getMaxValue());
    }
    // default to RangeIndexCreator for the time being
    return new RangeIndexCreator(context.getIndexDir(), context.getFieldSpec(),
        context.hasDictionary() ? FieldSpec.DataType.INT : context.getFieldSpec().getDataType(), -1,
        -1, context.getTotalDocs(), context.getTotalNumberOfEntries());
  }

  @Override
  protected IndexReaderFactory<RangeIndexReader> createReaderFactory() {
    return ReaderFactory.INSTANCE;
  }

  public static RangeIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
      throws IndexReaderConstraintException {
    return ReaderFactory.read(dataBuffer, metadata);
  }

  @Override
  public String getFileExtension(ColumnMetadata columnMetadata) {
    return V1Constants.Indexes.BITMAP_RANGE_INDEX_FILE_EXTENSION;
  }

  @Override
  public IndexHandler createIndexHandler(SegmentDirectory segmentDirectory, Map<String, FieldIndexConfigs> configsByCol,
      @Nullable Schema schema, @Nullable TableConfig tableConfig) {
    return new RangeIndexHandler(segmentDirectory, configsByCol, tableConfig);
  }

  private static class ReaderFactory extends IndexReaderFactory.Default<RangeIndexConfig, RangeIndexReader> {

    public static final ReaderFactory INSTANCE = new ReaderFactory();

    private ReaderFactory() {
    }

    @Override
    protected IndexType<RangeIndexConfig, RangeIndexReader, ?> getIndexType() {
      return StandardIndexes.range();
    }

    @Override
    protected RangeIndexReader createIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata metadata,
        RangeIndexConfig indexConfig)
        throws IndexReaderConstraintException {
      return read(dataBuffer, metadata);
    }

    public static RangeIndexReader read(PinotDataBuffer dataBuffer, ColumnMetadata metadata)
        throws IndexReaderConstraintException {
      int version = dataBuffer.getInt(0);
      if (version == RangeIndexCreator.VERSION) {
        return new RangeIndexReaderImpl(dataBuffer);
      } else if (version == BitSlicedRangeIndexCreator.VERSION) {
        return new BitSlicedRangeIndexReader(dataBuffer, metadata);
      }
      throw new IndexReaderConstraintException(metadata.getColumnName(), StandardIndexes.range(),
          "Unknown range index version " + version);
    }
  }

  @Override
  protected void handleIndexSpecificCleanup(TableConfig tableConfig) {
    tableConfig.getIndexingConfig().setRangeIndexColumns(null);
  }
}
