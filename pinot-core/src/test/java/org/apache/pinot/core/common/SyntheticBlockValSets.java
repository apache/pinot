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
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;


/// Synthetic [BlockValSet] for testing and benchmarking.
///
/// There is one fixture per stored type, single- and multi-value: `Int`/`IntMV`, `Long`/`LongMV`, `Float`/`FloatMV`,
/// `Double`/`DoubleMV`, `BigDec`/`BigDecMV`, `Str`/`StrMV` and `Bytes`/`BytesMV`, plus `DictIds`/`DictIdsMV` for
/// dictionary-encoded columns. Each takes an optional null bitmap and the raw values.
///
/// Two of them are named for a JDK type they must not shadow. A nested class called `String` or `BigDecimal` hides
/// [String] or [BigDecimal] across the whole of this class, which silently changes the signature of every `getString*`
/// or `getBigDecimal*` method declared here so that it no longer implements [BlockValSet]. Hence `Str` and `BigDec`;
/// keep any future fixture clear of the same collision.
///
/// **The null bitmap is independent of the values.** A row marked null still holds whatever the values array puts
/// at that index, and these fixtures have no notion of a column's `defaultNullValue`. That matches what a caller
/// with null handling enabled sees, since it reads the bitmap and skips those rows. It does **not** match a real
/// segment with null handling disabled, where a null row reads as the column default rather than as a neighbouring
/// value. So a test that wants "nothing was aggregated" in the disabled mode cannot get there with an all-null
/// bitmap - the values are still aggregated - and needs a holder that was never touched instead.
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

  /// A [BlockValSet] for a dictionary-encoded multi-value column, which exposes dictionary ids rather than values.
  ///
  /// Functions that collect dictionary ids take a different path from the one that reads values, and resolve the ids
  /// A simple [BlockValSet] for nullable, dictionary-encoded single-value columns.
  public static class DictIds extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final int[] _dictIds;
    final Dictionary _dictionary;
    final DataType _valueType;

    private DictIds(@Nullable RoaringBitmap nullBitmap, int[] dictIds, Dictionary dictionary, DataType valueType) {
      _nullBitmap = nullBitmap;
      _dictIds = dictIds;
      _dictionary = dictionary;
      _valueType = valueType;
    }

    public static DictIds create(@Nullable RoaringBitmap nullBitmap, int[] dictIds, Dictionary dictionary,
        DataType valueType) {
      return new DictIds(nullBitmap, dictIds, dictionary, valueType);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return _valueType;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Nullable
    @Override
    public Dictionary getDictionary() {
      return _dictionary;
    }

    @Override
    public int[] getDictionaryIdsSV() {
      return _dictIds;
    }
  }

  /// against the dictionary only when the result is extracted.
  public static class DictIdsMV extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final int[][] _dictIds;
    final Dictionary _dictionary;
    final DataType _valueType;

    private DictIdsMV(@Nullable RoaringBitmap nullBitmap, int[][] dictIds, Dictionary dictionary, DataType valueType) {
      _nullBitmap = nullBitmap;
      _dictIds = dictIds;
      _dictionary = dictionary;
      _valueType = valueType;
    }

    public static DictIdsMV create(@Nullable RoaringBitmap nullBitmap, int[][] dictIds, Dictionary dictionary,
        DataType valueType) {
      return new DictIdsMV(nullBitmap, dictIds, dictionary, valueType);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return _valueType;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Nullable
    @Override
    public Dictionary getDictionary() {
      return _dictionary;
    }

    @Override
    public int[][] getDictionaryIdsMV() {
      return _dictIds;
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
      Preconditions.checkArgument(nullBitmap == null || nullBitmap.last() < numDocs, "null bitmap larger than numDocs");
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
    public DataType getValueType() {
      return DataType.INT;
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

  /// A simple [BlockValSet] for nullable, not dictionary-encoded multi-value int values.
  public static class IntMV extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final int[][] _values;

    private IntMV(@Nullable RoaringBitmap nullBitmap, int[][] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static IntMV create(@Nullable RoaringBitmap nullBitmap, int[][] values) {
      return new IntMV(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return DataType.INT;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public int[][] getIntValuesMV() {
      return _values;
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
      Preconditions.checkArgument(nullBitmap == null || nullBitmap.last() < numDocs, "null bitmap larger than numDocs");
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
    public DataType getValueType() {
      return DataType.LONG;
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

  /// A simple [BlockValSet] for nullable, not dictionary-encoded multi-value long values.
  public static class LongMV extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final long[][] _values;

    private LongMV(@Nullable RoaringBitmap nullBitmap, long[][] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static LongMV create(@Nullable RoaringBitmap nullBitmap, long[][] values) {
      return new LongMV(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return DataType.LONG;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public long[][] getLongValuesMV() {
      return _values;
    }
  }

  /// A simple [BlockValSet] for nullable, not dictionary-encoded float values.
  public static class Float extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final float[] _values;

    private Float(@Nullable RoaringBitmap nullBitmap, float[] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static Float create(@Nullable RoaringBitmap nullBitmap, float[] values) {
      return new Float(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return DataType.FLOAT;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public float[] getFloatValuesSV() {
      return _values;
    }
  }

  /// A simple [BlockValSet] for nullable, not dictionary-encoded multi-value float values.
  public static class FloatMV extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final float[][] _values;

    private FloatMV(@Nullable RoaringBitmap nullBitmap, float[][] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static FloatMV create(@Nullable RoaringBitmap nullBitmap, float[][] values) {
      return new FloatMV(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return DataType.FLOAT;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public float[][] getFloatValuesMV() {
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
      Preconditions.checkArgument(nullBitmap == null || nullBitmap.last() < numDocs, "null bitmap larger than numDocs");
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
    public DataType getValueType() {
      return DataType.DOUBLE;
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

  /// A simple [BlockValSet] for nullable, not dictionary-encoded multi-value double values.
  public static class DoubleMV extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final double[][] _values;

    private DoubleMV(@Nullable RoaringBitmap nullBitmap, double[][] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static DoubleMV create(@Nullable RoaringBitmap nullBitmap, double[][] values) {
      return new DoubleMV(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return DataType.DOUBLE;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public double[][] getDoubleValuesMV() {
      return _values;
    }
  }

  /// A simple [BlockValSet] for nullable, not dictionary-encoded BigDecimal values.
  ///
  /// Named `BigDec` rather than `BigDecimal` to avoid shadowing [BigDecimal]; see the class comment.
  public static class BigDec extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final BigDecimal[] _values;

    private BigDec(@Nullable RoaringBitmap nullBitmap, BigDecimal[] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static BigDec create(@Nullable RoaringBitmap nullBitmap, BigDecimal[] values) {
      return new BigDec(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return DataType.BIG_DECIMAL;
    }

    @Override
    public boolean isSingleValue() {
      return true;
    }

    @Override
    public BigDecimal[] getBigDecimalValuesSV() {
      return _values;
    }
  }

  /// A simple [BlockValSet] for nullable, not dictionary-encoded multi-value BigDecimal values.
  public static class BigDecMV extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final BigDecimal[][] _values;

    private BigDecMV(@Nullable RoaringBitmap nullBitmap, BigDecimal[][] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static BigDecMV create(@Nullable RoaringBitmap nullBitmap, BigDecimal[][] values) {
      return new BigDecMV(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return DataType.BIG_DECIMAL;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public BigDecimal[][] getBigDecimalValuesMV() {
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
      Preconditions.checkArgument(nullBitmap == null || nullBitmap.last() < numDocs, "null bitmap larger than numDocs");
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
    public DataType getValueType() {
      return DataType.STRING;
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

  /// A simple [BlockValSet] for nullable, not dictionary-encoded multi-value string values.
  public static class StrMV extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final String[][] _values;

    private StrMV(@Nullable RoaringBitmap nullBitmap, String[][] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static StrMV create(@Nullable RoaringBitmap nullBitmap, String[][] values) {
      return new StrMV(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return DataType.STRING;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public String[][] getStringValuesMV() {
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
      Preconditions.checkArgument(nullBitmap == null || nullBitmap.last() < numDocs, "null bitmap larger than numDocs");
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
    public DataType getValueType() {
      return DataType.BYTES;
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

  /// A simple [BlockValSet] for nullable, not dictionary-encoded multi-value byte array values.
  public static class BytesMV extends Base {

    @Nullable
    final RoaringBitmap _nullBitmap;
    final byte[][][] _values;

    private BytesMV(@Nullable RoaringBitmap nullBitmap, byte[][][] values) {
      _nullBitmap = nullBitmap;
      _values = values;
    }

    public static BytesMV create(@Nullable RoaringBitmap nullBitmap, byte[][][] values) {
      return new BytesMV(nullBitmap, values);
    }

    @Nullable
    @Override
    public RoaringBitmap getNullBitmap() {
      return _nullBitmap;
    }

    @Override
    public DataType getValueType() {
      return DataType.BYTES;
    }

    @Override
    public boolean isSingleValue() {
      return false;
    }

    @Override
    public byte[][][] getBytesValuesMV() {
      return _values;
    }
  }
}
