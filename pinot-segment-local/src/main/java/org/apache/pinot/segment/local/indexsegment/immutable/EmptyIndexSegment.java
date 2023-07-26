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
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * Immutable segment impl for empty segment
 * Such an IndexSegment contains only the metadata, and no indexes
 */
public class EmptyIndexSegment implements ImmutableSegment {
  private final SegmentMetadataImpl _segmentMetadata;

  public EmptyIndexSegment(SegmentMetadataImpl segmentMetadata) {
    _segmentMetadata = segmentMetadata;
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
  public DataSource getDataSource(String column) {
    ColumnMetadata columnMetadata = _segmentMetadata.getColumnMetadataFor(column);
    Preconditions.checkNotNull(columnMetadata,
        "ColumnMetadata for " + column + " should not be null. " + "Potentially invalid column name specified.");
    return new EmptyDataSource(columnMetadata);
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
  public void destroy() {
  }

  @Override
  public List<StarTreeV2> getStarTrees() {
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
