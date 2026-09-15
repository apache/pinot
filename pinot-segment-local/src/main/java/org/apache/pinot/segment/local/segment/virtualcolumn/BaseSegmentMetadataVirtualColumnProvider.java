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
package org.apache.pinot.segment.local.segment.virtualcolumn;

import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.index.datasource.ImmutableDataSource;
import org.apache.pinot.segment.local.segment.index.readers.AllNullValueVectorReader;
import org.apache.pinot.segment.spi.SegmentMetadata;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.column.ColumnIndexContainer;
import org.apache.pinot.segment.spi.index.metadata.ColumnMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;


/// Base class for the built-in virtual columns that expose a piece of the segment metadata (creation time, time range,
/// CRC, etc.) as a constant single-value column.
///
/// The value is read from [SegmentMetadata] when the column is built rather than baked into the field spec, so that a
/// mutable segment - which rebuilds its virtual data sources on every access - picks up metadata that was not yet
/// available when the segment was created. Within a single build the value is resolved exactly once (see
/// [BaseConstantValueVirtualColumnProvider#buildColumnIndexContainer]), so the dictionary and the null value vector
/// can never disagree about whether the column has a value.
///
/// When the metadata is not available at all, or the specific piece of metadata is not set (e.g. the time range of a
/// CONSUMING segment), the column stores the default null value of its data type *and* reports every document as null,
/// so that the placeholder is not mistaken for a real value once null handling is enabled.
public abstract class BaseSegmentMetadataVirtualColumnProvider extends BaseConstantValueVirtualColumnProvider {

  @Override
  protected Object getValue(VirtualColumnContext context) {
    return valueOrPlaceholder(context, extractValueOrNull(context));
  }

  @Nullable
  @Override
  public NullValueVectorReader buildNullValueVector(VirtualColumnContext context) {
    return extractValueOrNull(context) == null ? new AllNullValueVectorReader(context.getTotalDocCount()) : null;
  }

  @Override
  public ColumnIndexContainer buildColumnIndexContainer(VirtualColumnContext context) {
    return buildColumnIndexContainer(context, extractValueOrNull(context));
  }

  @Override
  public ColumnMetadataImpl buildMetadata(VirtualColumnContext context) {
    Object extracted = extractValueOrNull(context);
    return buildMetadata(context, valueOrPlaceholder(context, extracted), extracted != null);
  }

  @Override
  public DataSource buildDataSource(VirtualColumnContext context) {
    // Resolve the metadata exactly once for the whole data source, so the dictionary, the column metadata's min/max
    // and the null value vector all agree about whether this column has a value. buildDataSource is the path a
    // mutable segment takes, and it rebuilds its virtual data sources on every access.
    Object extracted = extractValueOrNull(context);
    return new ImmutableDataSource(buildMetadata(context, valueOrPlaceholder(context, extracted), extracted != null),
        buildColumnIndexContainer(context, extracted));
  }

  private ColumnIndexContainer buildColumnIndexContainer(VirtualColumnContext context, @Nullable Object extracted) {
    return buildColumnIndexContainer(context, valueOrPlaceholder(context, extracted),
        extracted == null ? new AllNullValueVectorReader(context.getTotalDocCount()) : null);
  }

  /// Falls back to the column's default null value, which is only ever a placeholder: whenever it is used, the column
  /// also reports every document as null.
  private static Object valueOrPlaceholder(VirtualColumnContext context, @Nullable Object extracted) {
    return extracted != null ? extracted : context.getFieldSpec().getDefaultNullValue();
  }

  @Nullable
  private Object extractValueOrNull(VirtualColumnContext context) {
    SegmentMetadata segmentMetadata = context.getSegmentMetadata();
    return segmentMetadata != null ? extractValue(segmentMetadata) : null;
  }

  /// Extracts the value for this column from the given segment metadata, or `null` when it is not available.
  @Nullable
  protected abstract Object extractValue(SegmentMetadata segmentMetadata);
}
