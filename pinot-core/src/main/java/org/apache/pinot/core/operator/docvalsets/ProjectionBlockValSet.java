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

import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.DataBlockCache;
import org.apache.pinot.core.operator.ProjectionOperator;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.apache.pinot.spi.trace.InvocationRecording;
import org.apache.pinot.spi.trace.InvocationSpan;
import org.apache.pinot.spi.trace.Tracing;


/**
 * This class represents the BlockValSet for a projection block.
 * It provides api's to access data for a specified projection column.
 * It uses {@link DataBlockCache} to cache the projection data.
 */
public class ProjectionBlockValSet implements BlockValSet {
  private final DataBlockCache _dataBlockCache;
  private final String _column;
  private final DataSource _dataSource;

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
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadDictIds(span, true);
      return _dataBlockCache.getDictIdsForSVColumn(_column);
    }
  }

  @Override
  public int[] getIntValuesSV() {
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadValues(span, DataType.INT, true);
      return _dataBlockCache.getIntValuesForSVColumn(_column);
    }
  }

  @Override
  public long[] getLongValuesSV() {
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadValues(span, DataType.LONG, true);
      return _dataBlockCache.getLongValuesForSVColumn(_column);
    }
  }

  @Override
  public float[] getFloatValuesSV() {
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadValues(span, DataType.FLOAT, true);
      return _dataBlockCache.getFloatValuesForSVColumn(_column);
    }
  }

  @Override
  public double[] getDoubleValuesSV() {
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadValues(span, DataType.DOUBLE, true);
      return _dataBlockCache.getDoubleValuesForSVColumn(_column);
    }
  }

  @Override
  public String[] getStringValuesSV() {
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadValues(span, DataType.STRING, true);
      return _dataBlockCache.getStringValuesForSVColumn(_column);
    }
  }

  @Override
  public byte[][] getBytesValuesSV() {
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadValues(span, DataType.BYTES, true);
      return _dataBlockCache.getBytesValuesForSVColumn(_column);
    }
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadDictIds(span, false);
      return _dataBlockCache.getDictIdsForMVColumn(_column);
    }
  }

  @Override
  public int[][] getIntValuesMV() {
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadValues(span, DataType.INT, false);
      return _dataBlockCache.getIntValuesForMVColumn(_column);
    }
  }

  @Override
  public long[][] getLongValuesMV() {
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadValues(span, DataType.LONG, false);
      return _dataBlockCache.getLongValuesForMVColumn(_column);
    }
  }

  @Override
  public float[][] getFloatValuesMV() {
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadValues(span, DataType.FLOAT, false);
      return _dataBlockCache.getFloatValuesForMVColumn(_column);
    }
  }

  @Override
  public double[][] getDoubleValuesMV() {
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadValues(span, DataType.DOUBLE, false);
      return _dataBlockCache.getDoubleValuesForMVColumn(_column);
    }
  }

  @Override
  public String[][] getStringValuesMV() {
    try (InvocationSpan span = Tracing.getTracer().beginInvocation(ProjectionBlockValSet.class)) {
      recordReadValues(span, DataType.STRING, false);
      return _dataBlockCache.getStringValuesForMVColumn(_column);
    }
  }

  @Override
  public int[] getNumMVEntries() {
    return _dataBlockCache.getNumValuesForMVColumn(_column);
  }

  private void recordReadValues(InvocationRecording recording, DataType targetDataType, boolean targetSV) {
    if (recording.isEnabled()) {
      int numDocs = _dataBlockCache.getNumDocs();
      DataType sourceDataType = _dataSource.getDataSourceMetadata().getDataType();
      boolean isSourceSV = _dataSource.getDataSourceMetadata().isSingleValue();
      recording.setDataTypes(sourceDataType, isSourceSV, targetDataType, targetSV);
      recording.setDocsScanned(numDocs);
      recording.setColumnName(_column);
    }
  }

  private void recordReadDictIds(InvocationRecording recording, boolean singleValue) {
    if (recording.isEnabled()) {
      int numDocs = _dataBlockCache.getNumDocs();
      String dataType = singleValue ? "DICT" : "DICT[]";
      recording.setDataTypes(dataType, dataType);
      recording.setDocsScanned(numDocs);
      recording.setColumnName(_column);
    }
  }
}
