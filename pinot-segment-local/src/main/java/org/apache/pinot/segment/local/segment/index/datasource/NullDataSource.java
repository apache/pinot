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
package org.apache.pinot.segment.local.segment.index.datasource;

import org.apache.pinot.segment.local.segment.index.column.DefaultNullValueVirtualColumnProvider;
import org.apache.pinot.segment.local.segment.index.readers.AllNullValueVectorReader;
import org.apache.pinot.segment.local.segment.virtualcolumn.VirtualColumnContext;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.FieldSpec;


/// Data source for a column that holds no value in any document, such as a key that is absent from a complex column.
///
/// The column is built the same way as a default column, i.e. a schema column that is missing from the segment: a
/// single-entry dictionary holding the field's default null value, a constant sorted forward index for a single-value
/// field or constant multi-value forward and inverted indexes otherwise, and metadata reporting a cardinality of one
/// with the default null value as both min and max. On top of that every document is marked null in the null value
/// vector, so with null handling enabled the column reads as null rather than as the default value.
///
/// Immutable and safe to share across threads: every reader is stateless, and the null value vector materializes its
/// bitmap lazily under a benign race.
public class NullDataSource extends ImmutableDataSource {
  private static final AllNullColumnProvider PROVIDER = new AllNullColumnProvider();

  public NullDataSource(FieldSpec fieldSpec, int numDocs) {
    this(new VirtualColumnContext(fieldSpec, numDocs));
  }

  private NullDataSource(VirtualColumnContext context) {
    super(PROVIDER.buildMetadata(context), PROVIDER.buildColumnIndexContainer(context));
  }

  /// Default column provider that additionally marks every document null.
  private static class AllNullColumnProvider extends DefaultNullValueVirtualColumnProvider {

    @Override
    public NullValueVectorReader buildNullValueVector(VirtualColumnContext context) {
      return new AllNullValueVectorReader(context.getTotalDocCount());
    }
  }
}
