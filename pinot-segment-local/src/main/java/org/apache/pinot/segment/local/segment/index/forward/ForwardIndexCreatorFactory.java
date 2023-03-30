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

package org.apache.pinot.segment.local.segment.index.forward;

import java.io.File;
import java.io.IOException;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueUnsortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.MultiValueVarByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueFixedByteRawIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueSortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueUnsortedForwardIndexCreator;
import org.apache.pinot.segment.local.segment.creator.impl.fwd.SingleValueVarByteRawIndexCreator;
import org.apache.pinot.segment.spi.compression.ChunkCompressionType;
import org.apache.pinot.segment.spi.creator.IndexCreationContext;
import org.apache.pinot.segment.spi.index.ForwardIndexConfig;
import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;
import org.apache.pinot.spi.data.FieldSpec;


public class ForwardIndexCreatorFactory {
  private ForwardIndexCreatorFactory() {
  }

  public static ForwardIndexCreator createIndexCreator(IndexCreationContext context, ForwardIndexConfig indexConfig)
      throws Exception {
    String colName = context.getFieldSpec().getName();

    if (!context.hasDictionary()) {
      ChunkCompressionType chunkCompressionType = indexConfig.getChunkCompressionType();
      if (chunkCompressionType == null) {
        chunkCompressionType = ForwardIndexType.getDefaultCompressionType(context.getFieldSpec().getFieldType());
      }

      // Dictionary disabled columns
      boolean deriveNumDocsPerChunk = indexConfig.isDeriveNumDocsPerChunk();
      int writerVersion = indexConfig.getRawIndexWriterVersion();
      if (context.getFieldSpec().isSingleValueField()) {
        return getRawIndexCreatorForSVColumn(context.getIndexDir(), chunkCompressionType, colName,
            context.getFieldSpec().getDataType().getStoredType(),
            context.getTotalDocs(), context.getLengthOfLongestEntry(), deriveNumDocsPerChunk, writerVersion);
      } else {
        return getRawIndexCreatorForMVColumn(context.getIndexDir(), chunkCompressionType, colName,
            context.getFieldSpec().getDataType().getStoredType(),
            context.getTotalDocs(), context.getMaxNumberOfMultiValueElements(), deriveNumDocsPerChunk, writerVersion,
            context.getMaxRowLengthInBytes());
      }
    } else {
      // Dictionary enabled columns
      if (context.getFieldSpec().isSingleValueField()) {
        if (context.isSorted()) {
          return new SingleValueSortedForwardIndexCreator(context.getIndexDir(), colName,
              context.getCardinality());
        } else {
          return new SingleValueUnsortedForwardIndexCreator(context.getIndexDir(), colName,
              context.getCardinality(), context.getTotalDocs());
        }
      } else {
        return new MultiValueUnsortedForwardIndexCreator(context.getIndexDir(), colName,
            context.getCardinality(), context.getTotalDocs(), context.getTotalNumberOfEntries());
      }
    }
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
      boolean deriveNumDocsPerChunk, int writerVersion)
      throws IOException {
    switch (dataType.getStoredType()) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new SingleValueFixedByteRawIndexCreator(file, compressionType, column, totalDocs, dataType,
            writerVersion);
      case BIG_DECIMAL:
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
   * Assumes that column to be indexed is multi-valued.
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
}
