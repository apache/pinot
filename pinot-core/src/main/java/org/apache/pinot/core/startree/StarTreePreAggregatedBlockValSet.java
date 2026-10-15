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
package org.apache.pinot.core.startree;

import java.math.BigDecimal;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;


/// Marks a [BlockValSet] as reading a star-tree pre-aggregated column. Aggregation functions receive this wrapper
/// (applied in `AggregationFunctionUtils.getBlockValSetMap(AggregationFunctionColumnPair, ValueBlock)`) whenever a
/// query is served from a star-tree, so they can distinguish a pre-aggregated cell from a raw column value by
/// provenance instead of inferring it from the block's value type.
///
/// The distinction matters when the two are structurally identical: a distinct `arrayAgg` over a BYTES source column
/// reads raw BYTES values on the scan path and serialized-set BYTES cells on the star-tree path, and no inspection of
/// the bytes themselves can safely tell them apart. Checking `instanceof StarTreePreAggregatedBlockValSet` is the
/// reliable signal.
public class StarTreePreAggregatedBlockValSet implements BlockValSet {
  private final BlockValSet _delegate;

  public StarTreePreAggregatedBlockValSet(BlockValSet delegate) {
    _delegate = delegate;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    return _delegate.getNullBitmap();
  }

  @Override
  public DataType getValueType() {
    return _delegate.getValueType();
  }

  @Override
  public boolean isSingleValue() {
    return _delegate.isSingleValue();
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return _delegate.getDictionary();
  }

  @Override
  public boolean isDictionaryEncoded() {
    return _delegate.isDictionaryEncoded();
  }

  @Override
  public int[] getDictionaryIdsSV() {
    return _delegate.getDictionaryIdsSV();
  }

  @Override
  public int[] getIntValuesSV() {
    return _delegate.getIntValuesSV();
  }

  @Override
  public long[] getLongValuesSV() {
    return _delegate.getLongValuesSV();
  }

  @Override
  public float[] getFloatValuesSV() {
    return _delegate.getFloatValuesSV();
  }

  @Override
  public double[] getDoubleValuesSV() {
    return _delegate.getDoubleValuesSV();
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    return _delegate.getBigDecimalValuesSV();
  }

  @Override
  public String[] getStringValuesSV() {
    return _delegate.getStringValuesSV();
  }

  @Override
  public byte[][] getBytesValuesSV() {
    return _delegate.getBytesValuesSV();
  }

  @Override
  public int[] get32BitsMurmur3HashValuesSV() {
    return _delegate.get32BitsMurmur3HashValuesSV();
  }

  @Override
  public long[] get64BitsMurmur3HashValuesSV() {
    return _delegate.get64BitsMurmur3HashValuesSV();
  }

  @Override
  public long[][] get128BitsMurmur3HashValuesSV() {
    return _delegate.get128BitsMurmur3HashValuesSV();
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    return _delegate.getDictionaryIdsMV();
  }

  @Override
  public int[][] getIntValuesMV() {
    return _delegate.getIntValuesMV();
  }

  @Override
  public long[][] getLongValuesMV() {
    return _delegate.getLongValuesMV();
  }

  @Override
  public float[][] getFloatValuesMV() {
    return _delegate.getFloatValuesMV();
  }

  @Override
  public double[][] getDoubleValuesMV() {
    return _delegate.getDoubleValuesMV();
  }

  @Override
  public BigDecimal[][] getBigDecimalValuesMV() {
    return _delegate.getBigDecimalValuesMV();
  }

  @Override
  public String[][] getStringValuesMV() {
    return _delegate.getStringValuesMV();
  }

  @Override
  public byte[][][] getBytesValuesMV() {
    return _delegate.getBytesValuesMV();
  }

  @Override
  public int[] getNumMVEntries() {
    return _delegate.getNumMVEntries();
  }
}
