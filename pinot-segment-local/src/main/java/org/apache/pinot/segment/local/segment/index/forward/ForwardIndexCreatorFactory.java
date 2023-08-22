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
import org.apache.pinot.spi.data.FieldSpec.DataType;


public class ForwardIndexCreatorFactory {
  private ForwardIndexCreatorFactory() {
  }

  public static ForwardIndexCreator createIndexCreator(IndexCreationContext context, ForwardIndexConfig indexConfig)
      throws Exception {
    File indexDir = context.getIndexDir();
    FieldSpec fieldSpec = context.getFieldSpec();
    String columnName = fieldSpec.getName();
    int numTotalDocs = context.getTotalDocs();

    if (context.hasDictionary()) {
      // Dictionary enabled columns
      int cardinality = context.getCardinality();
      if (fieldSpec.isSingleValueField()) {
        if (context.isSorted()) {
          return new SingleValueSortedForwardIndexCreator(indexDir, columnName, cardinality);
        } else {
          return new SingleValueUnsortedForwardIndexCreator(indexDir, columnName, cardinality, numTotalDocs);
        }
      } else {
        return new MultiValueUnsortedForwardIndexCreator(indexDir, columnName, cardinality, numTotalDocs,
            context.getTotalNumberOfEntries());
      }
    } else {
      // Dictionary disabled columns
      DataType storedType = fieldSpec.getDataType().getStoredType();
      ChunkCompressionType chunkCompressionType = indexConfig.getChunkCompressionType();
      if (chunkCompressionType == null) {
        chunkCompressionType = ForwardIndexType.getDefaultCompressionType(fieldSpec.getFieldType());
      }
      boolean deriveNumDocsPerChunk = indexConfig.isDeriveNumDocsPerChunk();
      int writerVersion = indexConfig.getRawIndexWriterVersion();
      if (fieldSpec.isSingleValueField()) {
        return getRawIndexCreatorForSVColumn(indexDir, chunkCompressionType, columnName, storedType, numTotalDocs,
            context.getLengthOfLongestEntry(), deriveNumDocsPerChunk, writerVersion);
      } else {
        return getRawIndexCreatorForMVColumn(indexDir, chunkCompressionType, columnName, storedType, numTotalDocs,
            context.getMaxNumberOfMultiValueElements(), deriveNumDocsPerChunk, writerVersion,
            context.getMaxRowLengthInBytes());
      }
    }
  }

  /**
   * Helper method to build the raw index creator for the column.
   * Assumes that column to be indexed is single valued.
   */
  public static ForwardIndexCreator getRawIndexCreatorForSVColumn(File indexDir, ChunkCompressionType compressionType,
      String column, DataType storedType, int numTotalDocs, int lengthOfLongestEntry, boolean deriveNumDocsPerChunk,
      int writerVersion)
      throws IOException {
    switch (storedType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new SingleValueFixedByteRawIndexCreator(indexDir, compressionType, column, numTotalDocs, storedType,
            writerVersion);
      case BIG_DECIMAL:
      case STRING:
      case BYTES:
      case VECTOR:
        return new SingleValueVarByteRawIndexCreator(indexDir, compressionType, column, numTotalDocs, storedType,
            lengthOfLongestEntry, deriveNumDocsPerChunk, writerVersion);
      default:
        throw new IllegalStateException("Unsupported stored type: " + storedType);
    }
  }

  /**
   * Helper method to build the raw index creator for the column.
   * Assumes that column to be indexed is multi-valued.
   */
  public static ForwardIndexCreator getRawIndexCreatorForMVColumn(File indexDir, ChunkCompressionType compressionType,
      String column, DataType storedType, int numTotalDocs, int maxNumberOfMultiValueElements,
      boolean deriveNumDocsPerChunk, int writerVersion, int maxRowLengthInBytes)
      throws IOException {
    switch (storedType) {
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
        return new MultiValueFixedByteRawIndexCreator(indexDir, compressionType, column, numTotalDocs, storedType,
            maxNumberOfMultiValueElements, deriveNumDocsPerChunk, writerVersion);
      case STRING:
      case BYTES:
        return new MultiValueVarByteRawIndexCreator(indexDir, compressionType, column, numTotalDocs, storedType,
            writerVersion, maxRowLengthInBytes, maxNumberOfMultiValueElements);
      default:
        throw new IllegalStateException("Unsupported stored type: " + storedType);
    }
  }
}
