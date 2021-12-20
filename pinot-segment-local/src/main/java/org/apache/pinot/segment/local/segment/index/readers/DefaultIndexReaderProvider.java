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
package org.apache.pinot.segment.local.segment.index.readers;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.io.writer.impl.VarByteChunkSVForwardIndexWriterV4;
import org.apache.pinot.segment.local.segment.creator.impl.inv.BitSlicedRangeIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.inv.RangeIndexCreator;
import org.apache.pinot.segment.local.segment.index.readers.bloom.BloomFilterReaderFactory;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitMVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBitSVForwardIndexReaderV2;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkMVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.FixedBytePower2ChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkMVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkSVForwardIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.forward.VarByteChunkSVForwardIndexReaderV4;
import org.apache.pinot.segment.local.segment.index.readers.geospatial.ImmutableH3IndexReader;
import org.apache.pinot.segment.local.segment.index.readers.json.ImmutableJsonIndexReader;
import org.apache.pinot.segment.local.segment.index.readers.sorted.SortedIndexReaderImpl;
import org.apache.pinot.segment.local.segment.index.readers.text.LuceneTextIndexReader;
import org.apache.pinot.segment.local.utils.nativefst.FSTHeader;
import org.apache.pinot.segment.local.utils.nativefst.NativeFSTIndexReader;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.index.reader.BloomFilterReader;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.H3IndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.RangeIndexReader;
import org.apache.pinot.segment.spi.index.reader.SortedIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.reader.provider.IndexReaderProvider;
import org.apache.pinot.segment.spi.memory.PinotDataBuffer;
import org.apache.pinot.spi.data.FieldSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Default implementations of index reader provision logic. This class should not be
 * instantiated but accessed via {@see IndexReaderProviders#getIndexReaderProvider} so
 * this logic may be overridden by users of the SPI. Unless an override is specified,
 * this is the logic which will be used to construct readers for data buffers.
 */
public class DefaultIndexReaderProvider implements IndexReaderProvider {

  private static final Logger LOGGER = LoggerFactory.getLogger(DefaultIndexReaderProvider.class);

  @Override
  public BloomFilterReader newBloomFilterReader(PinotDataBuffer dataBuffer, boolean onHeap)
      throws IOException {
    return BloomFilterReaderFactory.getBloomFilterReader(dataBuffer, onHeap);
  }

  @Override
  public ForwardIndexReader<?> newForwardIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata)
      throws IOException {
    if (columnMetadata.hasDictionary()) {
      if (columnMetadata.isSingleValue()) {
        if (columnMetadata.isSorted()) {
          return new SortedIndexReaderImpl(dataBuffer, columnMetadata.getCardinality());
        } else {
          return new FixedBitSVForwardIndexReaderV2(dataBuffer, columnMetadata.getTotalDocs(),
              columnMetadata.getBitsPerElement());
        }
      } else {
        return new FixedBitMVForwardIndexReader(dataBuffer, columnMetadata.getTotalDocs(),
            columnMetadata.getTotalNumberOfEntries(), columnMetadata.getBitsPerElement());
      }
    } else {
      FieldSpec.DataType storedType = columnMetadata.getDataType().getStoredType();
      if (columnMetadata.isSingleValue()) {
        int version = dataBuffer.getInt(0);
        if (storedType.isFixedWidth()) {
          return version >= FixedBytePower2ChunkSVForwardIndexReader.VERSION
              ? new FixedBytePower2ChunkSVForwardIndexReader(dataBuffer, storedType)
              : new FixedByteChunkSVForwardIndexReader(dataBuffer, storedType);
        }
        if (version >= VarByteChunkSVForwardIndexWriterV4.VERSION) {
          return new VarByteChunkSVForwardIndexReaderV4(dataBuffer, storedType);
        }
        return new VarByteChunkSVForwardIndexReader(dataBuffer, storedType);
      } else {
        return storedType.isFixedWidth() ? new FixedByteChunkMVForwardIndexReader(dataBuffer, storedType)
            : new VarByteChunkMVForwardIndexReader(dataBuffer, storedType);
      }
    }
  }

  @Override
  public H3IndexReader newGeospatialIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata)
      throws IOException {
    return new ImmutableH3IndexReader(dataBuffer);
  }

  @Override
  public InvertedIndexReader<?> newInvertedIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata)
      throws IOException {
    return new BitmapInvertedIndexReader(dataBuffer, columnMetadata.getCardinality());
  }

  @Override
  public JsonIndexReader newJsonIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata)
      throws IOException {
    return new ImmutableJsonIndexReader(dataBuffer, columnMetadata.getTotalDocs());
  }

  @Override
  public RangeIndexReader<?> newRangeIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata)
      throws IOException {
    int version = dataBuffer.getInt(0);
    if (version == RangeIndexCreator.VERSION) {
      return new RangeIndexReaderImpl(dataBuffer);
    } else if (version == BitSlicedRangeIndexCreator.VERSION) {
      return new BitSlicedRangeIndexReader(dataBuffer, columnMetadata);
    }
    LOGGER.warn("Unknown range index version: {}, skip loading range index for column: {}", version,
        columnMetadata.getColumnName());
    return null;
  }

  @Override
  public SortedIndexReader<?> newSortedIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata)
      throws IOException {
    return new SortedIndexReaderImpl(dataBuffer, columnMetadata.getCardinality());
  }

  @Override
  public TextIndexReader newFSTIndexReader(PinotDataBuffer dataBuffer, ColumnMetadata columnMetadata)
      throws IOException {
    int magicHeader = dataBuffer.getInt(0);
    if (magicHeader == FSTHeader.FST_MAGIC) {
      return new NativeFSTIndexReader(dataBuffer);
    } else {
      return new LuceneFSTIndexReader(dataBuffer);
    }
  }

  @Override
  public TextIndexReader newTextIndexReader(File file, ColumnMetadata columnMetadata, @Nullable
      Map<String, String> textIndexProperties) {
    return new LuceneTextIndexReader(columnMetadata.getColumnName(), file, columnMetadata.getTotalDocs(),
        textIndexProperties);
  }
}
