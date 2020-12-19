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
package org.apache.pinot.core.segment.index.loader.invertedindex;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.geospatial.serde.GeometrySerializer;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.creator.impl.geospatial.H3IndexCreator;
import org.apache.pinot.core.segment.creator.impl.inv.RangeIndexCreator;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.core.segment.index.loader.LoaderUtils;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReaderContext;
import org.apache.pinot.core.segment.index.readers.forward.FixedBitMVForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.forward.FixedBitSVForwardIndexReaderV2;
import org.apache.pinot.core.segment.index.readers.forward.FixedByteChunkSVForwardIndexReader;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.segment.store.ColumnIndexType;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.spi.data.FieldSpec;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings({"rawtypes", "unchecked"})
public class H3IndexHandler {
  private static final Logger LOGGER = LoggerFactory.getLogger(H3IndexHandler.class);

  private final File _indexDir;
  private final SegmentDirectory.Writer _segmentWriter;
  private final String _segmentName;
  private final SegmentVersion _segmentVersion;
  private final Set<ColumnMetadata> _h3IndexColumns = new HashSet<>();

  public H3IndexHandler(File indexDir, SegmentMetadataImpl segmentMetadata, IndexLoadingConfig indexLoadingConfig,
      SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentWriter = segmentWriter;
    _segmentName = segmentMetadata.getName();
    _segmentVersion = SegmentVersion.valueOf(segmentMetadata.getVersion());

    // Only create H3 index on non-dictionary-encoded columns
    for (String column : indexLoadingConfig.getH3IndexColumns()) {
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null && !columnMetadata.hasDictionary()) {
        _h3IndexColumns.add(columnMetadata);
      }
    }
  }

  public void createH3Indices()
      throws Exception {
    for (ColumnMetadata columnMetadata : _h3IndexColumns) {
      createH3IndexForColumn(columnMetadata);
    }
  }

  private void createH3IndexForColumn(ColumnMetadata columnMetadata)
      throws Exception {
    String column = columnMetadata.getColumnName();
    File inProgress = new File(_indexDir, column + ".h3.inprogress");
    File h3IndexFile = new File(_indexDir, column + V1Constants.Indexes.BITMAP_H3_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.

      if (_segmentWriter.hasIndexFor(column, ColumnIndexType.H3_INDEX)) {
        // Skip creating range index if already exists.

        LOGGER.info("Found h3 index for segment: {}, column: {}", _segmentName, column);
        return;
      }

      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.
      // Remove range index if exists.
      // For v1 and v2, it's the actual range index. For v3, it's the temporary range index.
      FileUtils.deleteQuietly(h3IndexFile);
    }

    // Create new range index for the column.
    LOGGER.info("Creating new h3 index for segment: {}, column: {}", _segmentName, column);
    if (columnMetadata.hasDictionary()) {
//      handleDictionaryBasedColumn(columnMetadata);
    } else {
      handleNonDictionaryBasedColumn(columnMetadata);
    }

    // For v3, write the generated range index file into the single file and remove it.
    if (_segmentVersion == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, h3IndexFile, ColumnIndexType.H3_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created range index for segment: {}, column: {}", _segmentName, column);
  }

  //TODO: add later
  private void handleDictionaryBasedColumn(ColumnMetadata columnMetadata)
      throws IOException {
//    int numDocs = columnMetadata.getTotalDocs();
//    try (ForwardIndexReader forwardIndexReader = getForwardIndexReader(columnMetadata, _segmentWriter);
//        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
//        H3IndexCreator h3IndexCreator = new H3IndexCreator(_indexDir, columnMetadata.getFieldSpec(), 5)) {
//      if (columnMetadata.isSingleValue()) {
//        // Single-value column
//        for (int i = 0; i < numDocs; i++) {
//          forwardIndexReader.getDictId(i, readerContext);
////          h3IndexCreator.add();
//        }
//      } else {
//        // Multi-value column
////        int[] dictIds = new int[columnMetadata.getMaxNumberOfMultiValues()];
////        for (int i = 0; i < numDocs; i++) {
////          int length = forwardIndexReader.getDictIdMV(i, dictIds, readerContext);
////          rangeIndexCreator.add(dictIds, length);
////        }
//      }
//      h3IndexCreator.seal();
//    }
  }

  private void handleNonDictionaryBasedColumn(ColumnMetadata columnMetadata)
      throws Exception {
    int numDocs = columnMetadata.getTotalDocs();
    try (ForwardIndexReader forwardIndexReader = getForwardIndexReader(columnMetadata, _segmentWriter);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        H3IndexCreator h3IndexCreator = new H3IndexCreator(_indexDir, columnMetadata.getFieldSpec(), 5)) {
      if (columnMetadata.isSingleValue()) {
        // Single-value column.
        switch (columnMetadata.getDataType()) {
          case BYTES:
            for (int i = 0; i < numDocs; i++) {
              byte[] bytes = forwardIndexReader.getBytes(i, readerContext);
              Geometry geometry = GeometrySerializer.deserialize(bytes);
              Coordinate coordinate = geometry.getCoordinate();
              h3IndexCreator.add(i, coordinate.x, coordinate.y);
            }
            break;
          default:
            throw new IllegalStateException("Unsupported data type: " + columnMetadata.getDataType());
        }
      } else {
        // Multi-value column
        //TODO
        throw new IllegalStateException(
            "H3 indexing is not supported for Multivalue column : " + columnMetadata.getDataType());
      }
      h3IndexCreator.seal();
    }
  }

  private ForwardIndexReader<?> getForwardIndexReader(ColumnMetadata columnMetadata,
      SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer buffer = segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.FORWARD_INDEX);
    int numRows = columnMetadata.getTotalDocs();
    int numBitsPerValue = columnMetadata.getBitsPerElement();
    if (columnMetadata.isSingleValue()) {
      if (columnMetadata.hasDictionary()) {
        return new FixedBitSVForwardIndexReaderV2(buffer, numRows, numBitsPerValue);
      } else {
        return new FixedByteChunkSVForwardIndexReader(buffer, columnMetadata.getDataType());
      }
    } else {
      if (columnMetadata.hasDictionary()) {
        return new FixedBitMVForwardIndexReader(buffer, numRows, columnMetadata.getTotalNumberOfEntries(),
            numBitsPerValue);
      } else {
        throw new IllegalStateException("Raw index on multi-value column is not supported");
      }
    }
  }
}
