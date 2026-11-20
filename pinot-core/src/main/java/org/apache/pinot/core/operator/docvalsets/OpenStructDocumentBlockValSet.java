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
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;


/// The whole of an OPEN_STRUCT column for one block, as JSON text: one document per row, already assembled from
/// the column's per-key sources.
///
/// The parent has no forward index of its own -- it is a handle for per-key resolution -- so `SELECT open_struct_col`
/// cannot go through [ProjectionBlockValSet] like an ordinary column. Assembling in the query layer is what the
/// storage layer's contract asks for, and it keeps the parent free of a synthetic index that would then have to
/// behave like a real one everywhere else.
///
/// Values are STRING because that is what a reassembled document is. A caller wanting a typed value asks for the key.
public class OpenStructDocumentBlockValSet implements BlockValSet {

  private final String[] _documents;

  public OpenStructDocumentBlockValSet(String[] documents) {
    _documents = documents;
  }

  @Nullable
  @Override
  public RoaringBitmap getNullBitmap() {
    // A row always has a document, even when every key is absent from it: an empty object is a value.
    return null;
  }

  @Override
  public DataType getValueType() {
    return DataType.STRING;
  }

  @Override
  public boolean isSingleValue() {
    return true;
  }

  @Nullable
  @Override
  public Dictionary getDictionary() {
    return null;
  }

  @Override
  public String[] getStringValuesSV() {
    return _documents;
  }

  @Override
  public int[] getDictionaryIdsSV() {
    throw unsupported("dictionary ids");
  }

  @Override
  public int[] getIntValuesSV() {
    throw unsupported("INT");
  }

  @Override
  public long[] getLongValuesSV() {
    throw unsupported("LONG");
  }

  @Override
  public float[] getFloatValuesSV() {
    throw unsupported("FLOAT");
  }

  @Override
  public double[] getDoubleValuesSV() {
    throw unsupported("DOUBLE");
  }

  @Override
  public BigDecimal[] getBigDecimalValuesSV() {
    throw unsupported("BIG_DECIMAL");
  }

  @Override
  public byte[][] getBytesValuesSV() {
    throw unsupported("BYTES");
  }

  @Override
  public int[][] getDictionaryIdsMV() {
    throw unsupported("multi-value dictionary ids");
  }

  @Override
  public int[][] getIntValuesMV() {
    throw unsupported("multi-value INT");
  }

  @Override
  public long[][] getLongValuesMV() {
    throw unsupported("multi-value LONG");
  }

  @Override
  public float[][] getFloatValuesMV() {
    throw unsupported("multi-value FLOAT");
  }

  @Override
  public double[][] getDoubleValuesMV() {
    throw unsupported("multi-value DOUBLE");
  }

  @Override
  public BigDecimal[][] getBigDecimalValuesMV() {
    throw unsupported("multi-value BIG_DECIMAL");
  }

  @Override
  public String[][] getStringValuesMV() {
    throw unsupported("multi-value STRING");
  }

  @Override
  public byte[][][] getBytesValuesMV() {
    throw unsupported("multi-value BYTES");
  }

  @Override
  public int[] getNumMVEntries() {
    throw unsupported("multi-value entry counts");
  }

  private static UnsupportedOperationException unsupported(String what) {
    return new UnsupportedOperationException(
        "An OPEN_STRUCT column reads as its JSON document, so it cannot be read as " + what
            + ". Select a key with col['key'] to read a typed value.");
  }
}
