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
package org.apache.pinot.segment.local.indexsegment.immutable;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.EmptyDataSource;
import org.apache.pinot.segment.spi.ColumnMetadata;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.IndexReader;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.mutable.ThreadSafeMutableRoaringBitmap;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.InvertedIndexReader;
import org.apache.pinot.segment.spi.index.reader.TextIndexReader;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.segment.spi.store.SegmentDirectory;
import org.apache.pinot.spi.data.FieldSpec;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Immutable segment impl for empty segment
 * Such an IndexSegment contains only the metadata, and no indexes
 */
public class EmptyIndexSegment implements ImmutableSegment {
  private static final Logger LOGGER = LoggerFactory.getLogger(EmptyIndexSegment.class);

  private final SegmentMetadataImpl _segmentMetadata;
  // The directory this empty segment was loaded from, if any. An empty (0-doc) segment holds no index buffers, but the
  // directory may still own resources and post-registration work that a non-empty segment gets via
  // ImmutableSegmentImpl. Null when loaded without a directory.
  @Nullable
  private final SegmentDirectory _segmentDirectory;

  public EmptyIndexSegment(SegmentMetadataImpl segmentMetadata) {
    this(segmentMetadata, null);
  }

  public EmptyIndexSegment(SegmentMetadataImpl segmentMetadata, @Nullable SegmentDirectory segmentDirectory) {
    _segmentMetadata = segmentMetadata;
    _segmentDirectory = segmentDirectory;
  }

  @Override
  public String getSegmentName() {
    return _segmentMetadata.getName();
  }

  @Override
  public SegmentMetadataImpl getSegmentMetadata() {
    return _segmentMetadata;
  }

  @Override
  public Set<String> getColumnNames() {
    return _segmentMetadata.getSchema().getColumnNames();
  }

  @Override
  public Set<String> getPhysicalColumnNames() {
    return _segmentMetadata.getSchema().getPhysicalColumnNames();
  }

  @Override
  public void onSegmentAdded() {
    // Best-effort: this fires after the segment is already serving, so a failure cannot roll back the registration.
    if (_segmentDirectory == null) {
      return;
    }
    try {
      _segmentDirectory.onSegmentAdded();
    } catch (Exception e) {
      LOGGER.warn("Caught exception in onSegmentAdded for empty segment: {}. Continuing with error.", getSegmentName(),
          e);
    }
  }

  @Override
  public void offload() {
  }

  @Override
  public void destroy() {
    if (_segmentDirectory != null) {
      try {
        _segmentDirectory.close();
      } catch (Exception e) {
        LOGGER.warn("Failed to close segment directory for empty segment: {}. Continuing with error.",
            getSegmentName(), e);
      }
    }
  }

  @Nullable
  @Override
  public DataSource getDataSourceNullable(String column) {
    ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
    return columnMetadata != null ? new EmptyDataSource(columnMetadata.getFieldSpec()) : null;
  }

  @Override
  public DataSource getDataSource(String column, Schema schema) {
    DataSource dataSource = getDataSourceNullable(column);
    if (dataSource != null) {
      return dataSource;
    }
    FieldSpec fieldSpec = schema.getFieldSpecFor(column);
    Preconditions.checkState(fieldSpec != null, "Failed to find column: %s in schema: %s", column,
        schema.getSchemaName());
    return new EmptyDataSource(fieldSpec);
  }

  @Nullable
  @Override
  public List<StarTreeV2> getStarTrees() {
    return null;
  }

  @Nullable
  @Override
  public TextIndexReader getMultiColumnTextIndex() {
    return null;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getValidDocIds() {
    return null;
  }

  @Nullable
  @Override
  public ThreadSafeMutableRoaringBitmap getQueryableDocIds() {
    return null;
  }

  @Override
  public GenericRow getRecord(int docId, GenericRow reuse) {
    throw new UnsupportedOperationException("Cannot read record from empty segment");
  }

  @Override
  public Object getValue(int docId, String column) {
    throw new UnsupportedOperationException("Cannot read value from empty segment");
  }

  @Override
  public <I extends IndexReader> I getIndex(String column, IndexType<?, I, ?> type) {
    return null;
  }

  @Override
  public Dictionary getDictionary(String column) {
    return null;
  }

  @Override
  public ForwardIndexReader getForwardIndex(String column) {
    return null;
  }

  @Override
  public InvertedIndexReader getInvertedIndex(String column) {
    return null;
  }

  @Override
  public long getSegmentSizeBytes() {
    return 0;
  }

  @Nullable
  @Override
  public String getTier() {
    return null;
  }
}
