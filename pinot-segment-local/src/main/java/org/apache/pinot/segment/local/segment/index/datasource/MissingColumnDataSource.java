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

import java.util.Set;
import javax.annotation.Nullable;
import org.apache.pinot.segment.local.segment.missing.MissingColumnReaderFactory;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSourceMetadata;
import org.apache.pinot.segment.spi.partition.PartitionFunction;
import org.apache.pinot.spi.data.DimensionFieldSpec;
import org.apache.pinot.spi.data.FieldSpec;


public class MissingColumnDataSource extends BaseDataSource {

  public MissingColumnDataSource(IndexSegment indexSegment, String column) {
    this(indexSegment, createMissingFieldSpec(column, FieldSpec.DataType.STRING));
  }

  public MissingColumnDataSource(IndexSegment indexSegment, String column, FieldSpec.DataType dataType) {
    this(indexSegment, createMissingFieldSpec(column, dataType));
  }

  public MissingColumnDataSource(IndexSegment indexSegment, FieldSpec fieldSpec) {
    super(new MissingColumnDataSourceMetadata(indexSegment.getSegmentMetadata().getTotalDocs(), fieldSpec),
        MissingColumnReaderFactory.createMissingColumnReader(fieldSpec),
        null, null, null, null, null, null, null,
        null, null);
  }

  private static FieldSpec createMissingFieldSpec(String column, FieldSpec.DataType dataType) {
    return new DimensionFieldSpec(column, dataType, true);
  }

  private static final class MissingColumnDataSourceMetadata implements DataSourceMetadata {

    private final int _numDocs;
    private final FieldSpec _fieldSpec;

    private MissingColumnDataSourceMetadata(int numDocs, FieldSpec fieldSpec) {
      _numDocs = numDocs;
      _fieldSpec = fieldSpec;
    }

    @Override
    public FieldSpec getFieldSpec() {
      return _fieldSpec;
    }

    @Override
    public boolean isSorted() {
      return false;
    }

    @Override
    public int getNumDocs() {
      return _numDocs;
    }

    @Override
    public int getNumValues() {
      return _numDocs;
    }

    @Override
    public int getMaxNumValuesPerMVEntry() {
      return 0;
    }

    @Nullable
    @Override
    public Comparable getMinValue() {
      return null;
    }

    @Nullable
    @Override
    public Comparable getMaxValue() {
      return null;
    }

    @Nullable
    @Override
    public PartitionFunction getPartitionFunction() {
      return null;
    }

    @Nullable
    @Override
    public Set<Integer> getPartitions() {
      return null;
    }
  }


}
