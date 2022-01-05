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
package org.apache.pinot.segment.local.segment.creator.impl;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import org.apache.pinot.segment.local.io.writer.impl.BaseChunkSVForwardIndexWriter;
import org.apache.pinot.segment.local.segment.creator.impl.bloom.OnHeapGuavaBloomFilterCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueUnsortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OffHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.OnHeapBitmapInvertedIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.RangeIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.geospatial.OffHeapH3IndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.geospatial.OnHeapH3IndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OffHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.json.OnHeapJsonIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.text.LuceneFSTIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.text.LuceneTextIndexCreator;
import org.apache.pinot.segment.local.utils.nativefst.NativeFSTIndexCreator;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.creator.IndexCreatorProvider;
import org.apache.pinot.segment.spi.index.creator.BloomFilterCreator;
import org.apache.pinot.segment.spi.index.creator.CombinedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.creator.DictionaryBasedInvertedIndexCreator;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.segment.spi.index.creator.GeoSpatialIndexCreator;
import org.apache.pinot.segment.spi.index.creator.JsonIndexCreator;
import org.apache.pinot.segment.spi.index.creator.TextIndexCreator;
import org.apache.pinot.segment.spi.index.reader.H3IndexResolution;
import org.apache.pinot.spi.config.table.FSTType;
import org.apache.pinot.spi.config.table.FieldConfig;
import org.apache.pinot.spi.data.FieldSpec;


/**
 * This class centralizes logic for how to create indexes. It can be overridden
 * by SPI {@see IndexCreatorProviders} and should not be constructed directly, but
 * accessed only via {@see IndexCreatorProviders#getIndexCreatorProvider}. Unless
 * a user provides an override, this is the logic which will be used to create
 * each index type.
 */
public final class DefaultIndexCreatorProvider implements IndexCreatorProvider {

  @Override
  public ForwardIndexCreator newForwardIndexCreator(IndexCreationContext.Forward context)
      throws Exception {
    if (!context.hasDictionary()) {
      boolean deriveNumDocsPerChunk =
          shouldDeriveNumDocsPerChunk(context.getFieldSpec().getName(), context.getColumnProperties());
      int writerVersion = getRawIndexWriterVersion(context.getFieldSpec().getName(), context.getColumnProperties());
      if (context.getFieldSpec().isSingleValueField()) {
        return getRawIndexCreatorForSVColumn(context.getIndexDir(), context.getChunkCompressionType(),
            context.getFieldSpec().getName(), context.getFieldSpec().getDataType().getStoredType(),
            context.getTotalDocs(), context.getLengthOfLongestEntry(), deriveNumDocsPerChunk, writerVersion);
      } else {
        return getRawIndexCreatorForMVColumn(context.getIndexDir(), context.getChunkCompressionType(),
            context.getFieldSpec().getName(), context.getFieldSpec().getDataType().getStoredType(),
            context.getTotalDocs(), context.getMaxNumberOfMultiValueElements(), deriveNumDocsPerChunk, writerVersion,
            context.getMaxRowLengthInBytes());
      }
    } else {
      if (context.getFieldSpec().isSingleValueField()) {
        if (context.isSorted()) {
          return new SingleValueSortedForwardIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(),
              context.getCardinality());
        } else {
          return new SingleValueUnsortedForwardIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(),
              context.getCardinality(), context.getTotalDocs());
        }
      } else {
        return new MultiValueUnsortedForwardIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(),
            context.getCardinality(), context.getTotalDocs(), context.getTotalNumberOfEntries());
      }
    }
  }

  @Override
  public DictionaryBasedInvertedIndexCreator newInvertedIndexCreator(IndexCreationContext.Inverted context)
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
  public JsonIndexCreator newJsonIndexCreator(IndexCreationContext.Json context)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().isSingleValueField(),
        "Json index is currently only supported on single-value columns");
    Preconditions.checkState(context.getFieldSpec().getDataType().getStoredType() == FieldSpec.DataType.STRING,
        "Json index is currently only supported on STRING columns");
    return context.isOnHeap() ? new OnHeapJsonIndexCreator(context.getIndexDir(), context.getFieldSpec().getName())
        : new OffHeapJsonIndexCreator(context.getIndexDir(), context.getFieldSpec().getName());
  }

  @Override
  public TextIndexCreator newTextIndexCreator(IndexCreationContext.Text context)
      throws IOException {
    if (context.isFst()) {
      Preconditions.checkState(context.getFieldSpec().isSingleValueField(),
          "FST index is currently only supported on single-value columns");
      Preconditions.checkState(context.getFieldSpec().getDataType().getStoredType() == FieldSpec.DataType.STRING,
          "FST index is currently only supported on STRING type columns");
      Preconditions.checkState(context.hasDictionary(),
          "FST index is currently only supported on dictionary-encoded columns");
      String[] sortedValues = context.getSortedUniqueElementsArray();
      if (context.getFstType() == FSTType.NATIVE) {
        return new NativeFSTIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(), sortedValues);
      } else {
        return new LuceneFSTIndexCreator(context.getIndexDir(), context.getFieldSpec().getName(), sortedValues);
      }
    } else {
      Preconditions.checkState(context.getFieldSpec().getDataType().getStoredType() == FieldSpec.DataType.STRING,
          "Text index is currently only supported on STRING type columns");
      return new LuceneTextIndexCreator(context.getFieldSpec().getName(), context.getIndexDir(),
          context.isCommitOnClose());
    }
  }

  @Override
  public GeoSpatialIndexCreator newGeoSpatialIndexCreator(IndexCreationContext.Geospatial context)
      throws IOException {
    Preconditions.checkState(context.getFieldSpec().isSingleValueField(),
        "H3 index is currently only supported on single-value columns");
    Preconditions.checkState(context.getFieldSpec().getDataType().getStoredType() == FieldSpec.DataType.BYTES,
        "H3 index is currently only supported on BYTES columns");
    H3IndexResolution resolution = Objects.requireNonNull(context.getH3IndexConfig()).getResolution();
    return context.isOnHeap() ? new OnHeapH3IndexCreator(context.getIndexDir(), context.getFieldSpec().getName(),
        resolution) : new OffHeapH3IndexCreator(context.getIndexDir(), context.getFieldSpec().getName(), resolution);
  }

  public static boolean shouldDeriveNumDocsPerChunk(String columnName,
      Map<String, Map<String, String>> columnProperties) {
    if (columnProperties != null) {
      Map<String, String> properties = columnProperties.get(columnName);
      return properties != null && Boolean.parseBoolean(
          properties.get(FieldConfig.DERIVE_NUM_DOCS_PER_CHUNK_RAW_INDEX_KEY));
    }
    return false;
  }

  public static int getRawIndexWriterVersion(String columnName, Map<String, Map<String, String>> columnProperties) {
    if (columnProperties != null && columnProperties.get(columnName) != null) {
      Map<String, String> properties = columnProperties.get(columnName);
      String version = properties.get(FieldConfig.RAW_INDEX_WRITER_VERSION);
      if (version == null) {
        return BaseChunkSVForwardIndexWriter.DEFAULT_VERSION;
      }
      return Integer.parseInt(version);
    }
    return BaseChunkSVForwardIndexWriter.DEFAULT_VERSION;
  }

  /**
   * Helper method to build the raw index creator for the column.
   * Assumes that column to be indexed is single valued.
   *
   * @param file Output index file
   * @param column Column name
   * @param totalDocs Total number of documents to index
   * @param lengthOfLongestEntry Length of longest entry
   * @param deriveNumDocsPerChunk true if varbyte writer should auto-derive the number of rows per chunk
   * @param writerVersion version to use for the raw index writer
   * @return raw index creator
   */
  public static ForwardIndexCreator getRawIndexCreatorForSVColumn(File file, ChunkCompressionType compressionType,
      String column, FieldSpec.DataType dataType, int totalDocs, int lengthOfLongestEntry,
      boolean deriveNumDocsPerChunk,
      int writerVersion)
      throws IOException {
    switch (dataType.getStoredType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new SingleValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, dataType,
            writerVersion);
      case STRING:
      case BYTES:
        return new SingleValueVarByteRawIndexCreator(file, compressionType, column, totalDocs, dataType,
            lengthOfLongestEntry, deriveNumDocsPerChunk, writerVersion);
      default:
        throw new UnsupportedOperationException("Data type not supported for raw indexing: " + dataType);
    }
  }

  /**
   * Helper method to build the raw index creator for the column.
   * Assumes that column to be indexed is single valued.
   *
   * @param file Output index file
   * @param column Column name
   * @param totalDocs Total number of documents to index
   * @param deriveNumDocsPerChunk true if varbyte writer should auto-derive the number of rows
   *     per chunk
   * @param writerVersion version to use for the raw index writer
   * @param maxRowLengthInBytes the length of the longest row in bytes
   * @return raw index creator
   */
  public static ForwardIndexCreator getRawIndexCreatorForMVColumn(File file, ChunkCompressionType compressionType,
      String column, FieldSpec.DataType dataType, final int totalDocs, int maxNumberOfMultiValueElements,
      boolean deriveNumDocsPerChunk, int writerVersion, int maxRowLengthInBytes)
      throws IOException {
    switch (dataType.getStoredType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new MultiValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, dataType,
            maxNumberOfMultiValueElements, deriveNumDocsPerChunk, writerVersion);
      case STRING:
      case BYTES:
        return new MultiValueVarByteRawIndexCreator(file, compressionType, column, totalDocs, dataType, writerVersion,
            maxRowLengthInBytes, maxNumberOfMultiValueElements);
      default:
        throw new UnsupportedOperationException("Data type not supported for raw indexing: " + dataType);
    }
  }

  @Override
  public BloomFilterCreator newBloomFilterCreator(IndexCreationContext.BloomFilter context)
      throws IOException {
    return new OnHeapGuavaBloomFilterCreator(context.getIndexDir(), context.getFieldSpec().getName(),
        context.getCardinality(), Objects.requireNonNull(context.getBloomFilterConfig()));
  }

  @Override
  public CombinedInvertedIndexCreator newRangeIndexCreator(IndexCreationContext.Range context)
      throws IOException {
    if (context.getRangeIndexVersion() == BitSlicedRangeIndexCreator.VERSION && context.getFieldSpec()
        .isSingleValueField()) {
      if (context.hasDictionary()) {
        return new BitSlicedRangeIndexCreator(context.getIndexDir(), context.getFieldSpec(), context.getCardinality());
      }
      return new BitSlicedRangeIndexCreator(context.getIndexDir(), context.getFieldSpec(), context.getMin(),
          context.getMax());
    }
    // default to RangeIndexCreator for the time being
    return new RangeIndexCreator(context.getIndexDir(), context.getFieldSpec(),
        context.hasDictionary() ? FieldSpec.DataType.INT : context.getFieldSpec().getDataType(), -1,
        -1, context.getTotalDocs(), context.getTotalNumberOfEntries());
  }
}
