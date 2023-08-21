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
package org.apache.pinot.core.operator.docvalsets;

import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.data.readers.Vector;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.InvocationScope;
import org.apache.pinot.spi.trace.Tracing;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/**
 * This class represents the BlockValSet for a projection block.
 * It provides api's to access data for a specified projection column.
 * It uses {@link DataBlockCache} to cache the projection data.
 */
public class ProjectionBlockValSet implements BlockValSet {
  private final DataBlockCache _dataBlockCache;
  private final String _column;
  private final DataSource _dataSource;

  private boolean _nullBitmapSet;
  private RoaringBitmap _nullBitmap;

  /**
   * Constructor for the class.
   * The dataBlockCache is initialized in {@link ProjectionOperator} so that it can be reused across multiple calls to
   * {@link ProjectionOperator#nextBlock()}.
   */
  public ProjectionBlockValSet(DataBlockCache dataBlockCache, String column, DataSource dataSource) {
    _dataBlockCache = dataBlockCache;
    _column = column;
    _dataSource = dataSource;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    if (!_nullBitmapSet) {
      NullValueVectorReader nullValueReader = _dataSource.getNullValueVector();
      ImmutableRoaringBitmap nullBitmap = nullValueReader != null ? nullValueReader.getNullBitmap() : null;
      if (nullBitmap != null && !nullBitmap.isEmpty()) {
        // Project null bitmap.
        RoaringBitmap projectedNullBitmap = new RoaringBitmap();
        int[] docIds = _dataBlockCache.getDocIds();
        for (int i = 0; i < _dataBlockCache.getNumDocs(); i++) {
          if (nullBitmap.contains(docIds[i])) {
            projectedNullBitmap.add(i);
          }
        }
        _nullBitmap = projectedNullBitmap;
      } else {
        _nullBitmap = null;
      }
      _nullBitmapSet = true;
    }
    return _nullBitmap;
  }

  @Override
  public DataType getValueType() {
    return _dataSource.getDataSourceMetadata().getDataType();
  }

  @Override
  public boolean isSingleValue() {
    return _dataSource.getDataSourceMetadata().isSingleValue();
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return _dataSource.getDictionary();
  }

  @Override
  public int[] getDictionaryIdsSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.INT, true);
      return _dataBlockCache.getDictIdsForSVColumn(_column);
    }
  }

  @Override
  public int[] getIntValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.INT, true);
      return _dataBlockCache.getIntValuesForSVColumn(_column);
    }
  }

  @Override
  public long[] getLongValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.LONG, true);
      return _dataBlockCache.getLongValuesForSVColumn(_column);
    }
  }

  @Override
  public float[] getFloatValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.FLOAT, true);
      return _dataBlockCache.getFloatValuesForSVColumn(_column);
    }
  }

  @Override
  public double[] getDoubleValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.DOUBLE, true);
      return _dataBlockCache.getDoubleValuesForSVColumn(_column);
    }
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.BIG_DECIMAL, true);
      return _dataBlockCache.getBigDecimalValuesForSVColumn(_column);
    }
  }

  @Override
  public Vector[] getVectorValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.VECTOR, true);
      return _dataBlockCache.getVectorValuesForSVColumn(_column);
    }
  }

  @Override
  public String[] getStringValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.STRING, true);
      return _dataBlockCache.getStringValuesForSVColumn(_column);
    }
  }

  @Override
  public byte[][] getBytesValuesSV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.BYTES, true);
      return _dataBlockCache.getBytesValuesForSVColumn(_column);
    }
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.INT, false);
      return _dataBlockCache.getDictIdsForMVColumn(_column);
    }
  }

  @Override
  public int[][] getIntValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.INT, false);
      return _dataBlockCache.getIntValuesForMVColumn(_column);
    }
  }

  @Override
  public long[][] getLongValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.LONG, false);
      return _dataBlockCache.getLongValuesForMVColumn(_column);
    }
  }

  @Override
  public float[][] getFloatValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.FLOAT, false);
      return _dataBlockCache.getFloatValuesForMVColumn(_column);
    }
  }

  @Override
  public double[][] getDoubleValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.DOUBLE, false);
      return _dataBlockCache.getDoubleValuesForMVColumn(_column);
    }
  }

  @Override
  public String[][] getStringValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.STRING, false);
      return _dataBlockCache.getStringValuesForMVColumn(_column);
    }
  }

  @Override
  public byte[][][] getBytesValuesMV() {
    try (InvocationScope scope = Tracing.getTracer().createScope(ProjectionBlockValSet.class)) {
      recordReadValues(scope, DataType.BYTES, false);
      return _dataBlockCache.getBytesValuesForMVColumn(_column);
    }
  }

  @Override
  public int[] getNumMVEntries() {
    return _dataBlockCache.getNumValuesForMVColumn(_column);
  }

  private void recordReadValues(InvocationRecording recording, DataType dataType, boolean singleValue) {
    if (recording.isEnabled()) {
      int numDocs = _dataBlockCache.getNumDocs();
      recording.setNumDocsScanned(numDocs);
      recording.setColumnName(_column);
      recording.setOutputDataType(dataType, singleValue);
    }
  }
}
