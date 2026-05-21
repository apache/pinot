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
package org.apache.pinot.core.query.aggregation.function;

import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.DoubleAggregationResultHolder;
import org.apache.pinot.nativeengine.agg.PinotNativeAgg;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec.DataType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.SkipException;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/**
 * Integration test for the native (Rust+JNI) aggregation path.
 *
 * <p>The test bootstraps {@code PinotNativeAgg} by setting {@code pinot.native.lib.path} in
 * a static block <em>before</em> any reference to the class is made, so the library is loaded
 * from the dev-build location ({@code ../pinot-native/native/target/release/libpinot_native.*}).
 * If the library can't be found, the entire suite is skipped — running this test requires
 * {@code mvn -pl pinot-native package} to have produced the binary.
 */
public class NativeSumAggregationFunctionTest {

  private static final String NATIVE_FLAG = NativeAggregationRouter.ENABLED_PROPERTY;
  private static final String LIB_PATH_PROP = "pinot.native.lib.path";

  // Set the lib path system property before PinotNativeAgg is touched anywhere in this test.
  // Static blocks of a class run when the class is first loaded.
  static {
    String resolved = resolveDevLibPath();
    if (resolved != null && System.getProperty(LIB_PATH_PROP) == null) {
      System.setProperty(LIB_PATH_PROP, resolved);
    }
  }

  @BeforeClass
  public void enableNativeFlag() {
    if (!PinotNativeAgg.isAvailable()) {
      throw new SkipException("pinot-native library not loadable. Build it with "
          + "'./mvnw -pl pinot-native package' first. Searched at "
          + System.getProperty(LIB_PATH_PROP));
    }
    System.setProperty(NATIVE_FLAG, "true");
  }

  @AfterClass(alwaysRun = true)
  public void clearNativeFlag() {
    System.clearProperty(NATIVE_FLAG);
  }

  @Test
  public void factoryReturnsNativeImplWhenFlagOnAndEligible() {
    FunctionContext fc = sumOfColumn("longCol");
    AggregationFunction fn = AggregationFunctionFactory.getAggregationFunction(fc, false);
    assertTrue(fn instanceof NativeSumAggregationFunction,
        "expected NativeSumAggregationFunction, got " + fn.getClass().getName());
  }

  @Test
  public void factoryReturnsJavaImplWhenFlagOff() {
    System.clearProperty(NATIVE_FLAG);
    try {
      FunctionContext fc = sumOfColumn("longCol");
      AggregationFunction fn = AggregationFunctionFactory.getAggregationFunction(fc, false);
      assertEquals(fn.getClass(), SumAggregationFunction.class,
          "expected plain SumAggregationFunction when flag is off, got " + fn.getClass().getName());
    } finally {
      System.setProperty(NATIVE_FLAG, "true");
    }
  }

  @Test
  public void factoryReturnsJavaImplWhenNullHandlingEnabled() {
    FunctionContext fc = sumOfColumn("longCol");
    AggregationFunction fn = AggregationFunctionFactory.getAggregationFunction(fc, true);
    assertEquals(fn.getClass(), SumAggregationFunction.class,
        "null handling currently disqualifies the native path");
  }

  @Test
  public void aggregateLongMatchesJavaReference() {
    Random rng = new Random(7);
    int n = 100_000;
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      values[i] = rng.nextLong() % 1_000_000L;
    }

    double nativeResult = runAggregate(new NativeSumAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("longCol")), false),
        values, DataType.LONG, n);
    double javaResult = runAggregate(new SumAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("longCol")), false),
        values, DataType.LONG, n);

    double tolerance = Math.max(1.0, Math.abs(javaResult) * 1e-15);
    assertTrue(Math.abs(nativeResult - javaResult) <= tolerance,
        "native=" + nativeResult + " java=" + javaResult);
  }

  @Test
  public void aggregateFallsThroughForIntColumn() {
    // INT is out of POC scope (only LONG is wired natively). The native subclass MUST
    // delegate to the Java parent rather than throwing or returning a wrong value.
    int[] intValues = new int[1000];
    for (int i = 0; i < 1000; i++) {
      intValues[i] = i + 1;
    }
    double expected = 0.0;
    for (int v : intValues) {
      expected += v;
    }

    NativeSumAggregationFunction nativeFn = new NativeSumAggregationFunction(
        Collections.singletonList(ExpressionContext.forIdentifier("intCol")), false);
    StubBlockValSet bvs = new StubBlockValSet(DataType.INT, intValues, null);
    AggregationResultHolder holder = new DoubleAggregationResultHolder(0.0);
    nativeFn.aggregate(intValues.length, holder,
        Collections.singletonMap(nativeFn._expression, bvs));

    assertEquals(holder.getDoubleResult(), expected);
  }

  // --- helpers ----------------------------------------------------------------

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static double runAggregate(SumAggregationFunction fn, long[] values, DataType type,
      int length) {
    StubBlockValSet bvs = new StubBlockValSet(type, null, values);
    AggregationResultHolder holder = new DoubleAggregationResultHolder(0.0);
    fn.aggregate(length, holder, Collections.singletonMap(fn._expression, bvs));
    return holder.getDoubleResult();
  }

  private static FunctionContext sumOfColumn(String column) {
    return new FunctionContext(FunctionContext.Type.AGGREGATION, "SUM",
        Collections.singletonList(ExpressionContext.forIdentifier(column)));
  }

  @Nullable
  private static String resolveDevLibPath() {
    String os = System.getProperty("os.name", "").toLowerCase();
    String libFile;
    if (os.contains("mac") || os.contains("darwin")) {
      libFile = "libpinot_native.dylib";
    } else if (os.contains("windows")) {
      libFile = "pinot_native.dll";
    } else {
      libFile = "libpinot_native.so";
    }
    // Surefire CWD for a module test is the module root (pinot-core). The sibling pinot-native
    // module hosts the Cargo build output.
    Path candidate =
        Paths.get("..", "pinot-native", "native", "target", "release", libFile).toAbsolutePath();
    if (Files.exists(candidate)) {
      return candidate.toString();
    }
    return null;
  }

  /**
   * Minimal {@link BlockValSet} stub for primitive single-value columns. Only the methods
   * the aggregation kernel actually calls are implemented; the rest throw, which would surface
   * any unintended use in tests.
   */
  private static final class StubBlockValSet implements BlockValSet {
    private final DataType _type;
    @Nullable
    private final int[] _intValues;
    @Nullable
    private final long[] _longValues;

    StubBlockValSet(DataType type, @Nullable int[] intValues, @Nullable long[] longValues) {
      _type = type;
      _intValues = intValues;
      _longValues = longValues;
    }

    @Nullable
    @Override public RoaringBitmap getNullBitmap() { return null; }
    @Override public DataType getValueType() { return _type; }
    @Override public boolean isSingleValue() { return true; }
    @Nullable
    @Override public Dictionary getDictionary() { return null; }
    @Override public int[] getDictionaryIdsSV() { throw new UnsupportedOperationException(); }
    @Override public int[] getIntValuesSV() {
      if (_intValues == null) {
        throw new UnsupportedOperationException("no int values configured");
      }
      return _intValues;
    }
    @Override public long[] getLongValuesSV() {
      if (_longValues == null) {
        throw new UnsupportedOperationException("no long values configured");
      }
      return _longValues;
    }
    @Override public float[] getFloatValuesSV() { throw new UnsupportedOperationException(); }
    @Override public double[] getDoubleValuesSV() { throw new UnsupportedOperationException(); }
    @Override public BigDecimal[] getBigDecimalValuesSV() { throw new UnsupportedOperationException(); }
    @Override public String[] getStringValuesSV() { throw new UnsupportedOperationException(); }
    @Override public byte[][] getBytesValuesSV() { throw new UnsupportedOperationException(); }
    @Override public int[][] getDictionaryIdsMV() { throw new UnsupportedOperationException(); }
    @Override public int[][] getIntValuesMV() { throw new UnsupportedOperationException(); }
    @Override public long[][] getLongValuesMV() { throw new UnsupportedOperationException(); }
    @Override public float[][] getFloatValuesMV() { throw new UnsupportedOperationException(); }
    @Override public double[][] getDoubleValuesMV() { throw new UnsupportedOperationException(); }
    @Override public BigDecimal[][] getBigDecimalValuesMV() { throw new UnsupportedOperationException(); }
    @Override public String[][] getStringValuesMV() { throw new UnsupportedOperationException(); }
    @Override public byte[][][] getBytesValuesMV() { throw new UnsupportedOperationException(); }
    @Override public int[] getNumMVEntries() { throw new UnsupportedOperationException(); }

    /** Java's {@code SumAggregationFunction.aggregateSV} reads {@link List} arguments
     * — we never go through that path, but include the helper anyway for completeness. */
    public List<?> getValuesAsList() {
      return Collections.emptyList();
    }
  }
}
