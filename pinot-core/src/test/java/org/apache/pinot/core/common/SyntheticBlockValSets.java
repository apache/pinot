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
package org.apache.pinot.core.common;

import com.google.common.base.Preconditions;
import java.math.BigDecimal;
import java.util.function.DoubleSupplier;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.RoaringBitmap;

/// Synthetic [BlockValSet] for testing and benchmarking.
public class SyntheticBlockValSets {
  private SyntheticBlockValSets() {
  }

  /// Base class for synthetic [BlockValSet].
  ///
  /// Most of its methods throw [UnsupportedOperationException] and should be overridden by subclasses if they
  /// need to be used.
  public static abstract class Base implements BlockValSet {
    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      throw new UnsupportedOperationException();
    }

    @Nullable
    @Override
    public Dictionary getDictionary() {
      return null;
    }

    @Override
    public int[] getDictionaryIdsSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getIntValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[] getLongValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[] getFloatValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[] getDoubleValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal[] getBigDecimalValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[] getStringValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[][] getBytesValuesSV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[][] getDictionaryIdsMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[][] getIntValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long[][] getLongValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public float[][] getFloatValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public double[][] getDoubleValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public BigDecimal[][] getBigDecimalValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public String[][] getStringValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public byte[][][] getBytesValuesMV() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int[] getNumMVEntries() {
      throw new UnsupportedOperationException();
    }
  }

  /// A simple [BlockValSet] for nullable, not dictionary-encoded long values.
  public static class Long extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final long[] _values;

    private Long(@Nullable RoaringBitmap nullBitmap, long[] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static Long create(LongSupplier supplier) {
      return create(DocIdSetPlanNode.MAX_DOC_PER_CALL, null, supplier);
    }

    public static Long create(@Nullable RoaringBitmap nullBitmap, LongSupplier supplier) {
      return create(DocIdSetPlanNode.MAX_DOC_PER_CALL, nullBitmap, supplier);
    }

    public static Long create(int numDocs, @Nullable RoaringBitmap nullBitmap, LongSupplier supplier) {
      Preconditions.checkArgument(nullBitmap == null || nullBitmap.last() < numDocs,
          "null bitmap larger than numDocs");
      long[] values = new long[numDocs];
      for (int i = 0; i < numDocs; i++) {
        values[i] = supplier.getAsLong();
      }

      return new Long(nullBitmap, values);
    }

    public static Long create(@Nullable RoaringBitmap nullBitmap, long[] values) {
      return new Long(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return FieldSpec.DataType.LONG;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public long[] getLongValuesSV() {
      return _values;
    }
  }

  /// A simple [BlockValSet] for nullable, not dictionary-encoded double values.
  public static class Double extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final double[] _values;

    private Double(@Nullable RoaringBitmap nullBitmap, double[] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static Double create(DoubleSupplier supplier) {
      return create(DocIdSetPlanNode.MAX_DOC_PER_CALL, null, supplier);
    }

    public static Double create(@Nullable RoaringBitmap nullBitmap, DoubleSupplier supplier) {
      return create(DocIdSetPlanNode.MAX_DOC_PER_CALL, nullBitmap, supplier);
    }

    public static Double create(int numDocs, @Nullable RoaringBitmap nullBitmap, DoubleSupplier supplier) {
      Preconditions.checkArgument(nullBitmap == null || nullBitmap.last() < numDocs,
          "null bitmap larger than numDocs");
      double[] values = new double[numDocs];
      for (int i = 0; i < numDocs; i++) {
        values[i] = supplier.getAsDouble();
      }

      return new Double(nullBitmap, values);
    }

    public static Double create(@Nullable RoaringBitmap nullBitmap, double[] values) {
      return new Double(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return FieldSpec.DataType.DOUBLE;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public double[] getDoubleValuesSV() {
      return _values;
    }
  }

  /// A simple [BlockValSet] for nullable, not dictionary-encoded string values.
  ///
  /// Named `Str` rather than `String`: a nested class called `String` shadows `java.lang.String` across
  /// the whole of the enclosing class, which silently changes the signature of every `getString*` method
  /// declared here so that it no longer implements [BlockValSet].
  public static class Str extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final String[] _values;

    private Str(@Nullable RoaringBitmap nullBitmap, String[] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static Str create(int numDocs, @Nullable RoaringBitmap nullBitmap, Supplier<String> supplier) {
      Preconditions.checkArgument(nullBitmap == null || nullBitmap.last() < numDocs,
          "null bitmap larger than numDocs");
      String[] values = new String[numDocs];
      for (int i = 0; i < numDocs; i++) {
        values[i] = supplier.get();
      }
      return new Str(nullBitmap, values);
    }

    public static Str create(@Nullable RoaringBitmap nullBitmap, String[] values) {
      return new Str(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return FieldSpec.DataType.STRING;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public String[] getStringValuesSV() {
      return _values;
    }
  }

  /// A simple [BlockValSet] for nullable, not dictionary-encoded byte array values.
  public static class Bytes extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final byte[][] _values;

    private Bytes(@Nullable RoaringBitmap nullBitmap, byte[][] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static Bytes create(int numDocs, @Nullable RoaringBitmap nullBitmap, Supplier<byte[]> supplier) {
      Preconditions.checkArgument(nullBitmap == null || nullBitmap.last() < numDocs,
          "null bitmap larger than numDocs");
      byte[][] values = new byte[numDocs][];
      for (int i = 0; i < numDocs; i++) {
        values[i] = supplier.get();
      }
      return new Bytes(nullBitmap, values);
    }

    public static Bytes create(@Nullable RoaringBitmap nullBitmap, byte[][] values) {
      return new Bytes(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return FieldSpec.DataType.BYTES;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public byte[][] getBytesValuesSV() {
      return _values;
    }
  }

  /// A simple [BlockValSet] for nullable, not dictionary-encoded int values.
  public static class Int extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final int[] _values;

    private Int(@Nullable RoaringBitmap nullBitmap, int[] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static Int create(int numDocs, @Nullable RoaringBitmap nullBitmap, IntSupplier supplier) {
      Preconditions.checkArgument(nullBitmap == null || nullBitmap.last() < numDocs,
          "null bitmap larger than numDocs");
      int[] values = new int[numDocs];
      for (int i = 0; i < numDocs; i++) {
        values[i] = supplier.getAsInt();
      }
      return new Int(nullBitmap, values);
    }

    public static Int create(@Nullable RoaringBitmap nullBitmap, int[] values) {
      return new Int(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public FieldSpec.DataType getValueType() {
      return FieldSpec.DataType.INT;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public int[] getIntValuesSV() {
      return _values;
    }
  }
}
