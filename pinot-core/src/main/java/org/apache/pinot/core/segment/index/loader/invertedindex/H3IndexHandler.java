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

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.commons.io.FileUtils;
import org.apache.pinot.core.geospatial.serde.GeometrySerializer;
import org.apache.pinot.core.indexsegment.generator.SegmentVersion;
import org.apache.pinot.core.segment.creator.impl.V1Constants;
import org.apache.pinot.core.segment.creator.impl.inv.geospatial.H3IndexConfig;
import org.apache.pinot.core.segment.creator.impl.inv.geospatial.OffHeapH3IndexCreator;
import org.apache.pinot.core.segment.index.column.PhysicalColumnIndexContainer;
import org.apache.pinot.core.segment.index.loader.IndexLoadingConfig;
import org.apache.pinot.core.segment.index.loader.LoaderUtils;
import org.apache.pinot.core.segment.index.metadata.ColumnMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.ForwardIndexReaderContext;
import org.apache.pinot.core.segment.index.readers.forward.FixedBitSVForwardIndexReaderV2;
import org.apache.pinot.core.segment.index.readers.forward.VarByteChunkSVForwardIndexReader;
import org.apache.pinot.core.segment.index.readers.sorted.SortedIndexReaderImpl;
import org.apache.pinot.core.segment.memory.PinotDataBuffer;
import org.apache.pinot.core.segment.store.ColumnIndexType;
import org.apache.pinot.core.segment.store.SegmentDirectory;
import org.apache.pinot.spi.data.FieldSpec.DataType;
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
  private final Map<String, H3IndexConfig> _h3IndexConfigs = new HashMap<>();

  public H3IndexHandler(File indexDir, SegmentMetadataImpl segmentMetadata, IndexLoadingConfig indexLoadingConfig,
      SegmentDirectory.Writer segmentWriter) {
    _indexDir = indexDir;
    _segmentWriter = segmentWriter;
    _segmentName = segmentMetadata.getName();
    _segmentVersion = SegmentVersion.valueOf(segmentMetadata.getVersion());

    for (Map.Entry<String, H3IndexConfig> entry : indexLoadingConfig.getH3IndexConfigs().entrySet()) {
      String column = entry.getKey();
      ColumnMetadata columnMetadata = segmentMetadata.getColumnMetadataFor(column);
      if (columnMetadata != null) {
        _h3IndexColumns.add(columnMetadata);
        _h3IndexConfigs.put(column, entry.getValue());
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
    File inProgress = new File(_indexDir, column + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION + ".inprogress");
    File h3IndexFile = new File(_indexDir, column + V1Constants.Indexes.H3_INDEX_FILE_EXTENSION);

    if (!inProgress.exists()) {
      // Marker file does not exist, which means last run ended normally.

      if (_segmentWriter.hasIndexFor(column, ColumnIndexType.H3_INDEX)) {
        // Skip creating H3 index if already exists.

        LOGGER.info("Found H3 index for segment: {}, column: {}", _segmentName, column);
        return;
      }

      // Create a marker file.
      FileUtils.touch(inProgress);
    } else {
      // Marker file exists, which means last run gets interrupted.

      // Remove H3 index if exists.
      // For v1 and v2, it's the actual range index. For v3, it's the temporary range index.
      FileUtils.deleteQuietly(h3IndexFile);
    }

    // Create new H3 index for the column.
    LOGGER.info("Creating new H3 index for segment: {}, column: {}", _segmentName, column);
    Preconditions
        .checkState(columnMetadata.getDataType() == DataType.BYTES, "H3 index can only be applied to BYTES columns");
    if (columnMetadata.hasDictionary()) {
      handleDictionaryBasedColumn(columnMetadata);
    } else {
      handleNonDictionaryBasedColumn(columnMetadata);
    }

    // For v3, write the generated H3 index file into the single file and remove it.
    if (_segmentVersion == SegmentVersion.v3) {
      LoaderUtils.writeIndexToV3Format(_segmentWriter, column, h3IndexFile, ColumnIndexType.H3_INDEX);
    }

    // Delete the marker file.
    FileUtils.deleteQuietly(inProgress);

    LOGGER.info("Created H3 index for segment: {}, column: {}", _segmentName, column);
  }

  private void handleDictionaryBasedColumn(ColumnMetadata columnMetadata)
      throws IOException {
    String columnName = columnMetadata.getColumnName();
    try (ForwardIndexReader forwardIndexReader = getForwardIndexReader(columnMetadata, _segmentWriter);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        Dictionary dictionary = getDictionary(columnMetadata, _segmentWriter);
        OffHeapH3IndexCreator h3IndexCreator = new OffHeapH3IndexCreator(_indexDir, columnName,
            _h3IndexConfigs.get(columnName).getResolution())) {
      int numDocs = columnMetadata.getTotalDocs();
      for (int i = 0; i < numDocs; i++) {
        int dictId = forwardIndexReader.getDictId(i, readerContext);
        h3IndexCreator.add(GeometrySerializer.deserialize(dictionary.getBytesValue(dictId)));
      }
      h3IndexCreator.seal();
    }
  }

  private void handleNonDictionaryBasedColumn(ColumnMetadata columnMetadata)
      throws Exception {
    String columnName = columnMetadata.getColumnName();
    try (ForwardIndexReader forwardIndexReader = getForwardIndexReader(columnMetadata, _segmentWriter);
        ForwardIndexReaderContext readerContext = forwardIndexReader.createContext();
        OffHeapH3IndexCreator h3IndexCreator = new OffHeapH3IndexCreator(_indexDir, columnName,
            _h3IndexConfigs.get(columnName).getResolution())) {
      int numDocs = columnMetadata.getTotalDocs();
      for (int i = 0; i < numDocs; i++) {
        h3IndexCreator.add(GeometrySerializer.deserialize(forwardIndexReader.getBytes(i, readerContext)));
      }
      h3IndexCreator.seal();
    }
  }

  private ForwardIndexReader<?> getForwardIndexReader(ColumnMetadata columnMetadata,
      SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer dataBuffer =
        segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.FORWARD_INDEX);
    if (columnMetadata.hasDictionary()) {
      if (columnMetadata.isSorted()) {
        return new SortedIndexReaderImpl(dataBuffer, columnMetadata.getCardinality());
      } else {
        return new FixedBitSVForwardIndexReaderV2(dataBuffer, columnMetadata.getTotalDocs(),
            columnMetadata.getBitsPerElement());
      }
    } else {
      return new VarByteChunkSVForwardIndexReader(dataBuffer, DataType.BYTES);
    }
  }

  private Dictionary getDictionary(ColumnMetadata columnMetadata, SegmentDirectory.Writer segmentWriter)
      throws IOException {
    PinotDataBuffer dataBuffer = segmentWriter.getIndexFor(columnMetadata.getColumnName(), ColumnIndexType.DICTIONARY);
    return PhysicalColumnIndexContainer.loadDictionary(dataBuffer, columnMetadata, false);
  }
}
