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

import it.unimi.dsi.fastutil.doubles.DoubleArrayList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FunctionContext;
import org.apache.pinot.common.request.context.RequestContextUtils;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.SyntheticBlockValSets;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.roaringbitmap.RoaringBitmap;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.testng.Assert.*;


/// Enforces the null contract documented on [AggregationFunction] across every aggregation function that can be
/// constructed generically, so that a new function cannot quietly opt out of it.
///
/// The case this guards is the one that has repeatedly reached production: a query whose segments are all pruned
/// produces no intermediate result at all, and the broker still has to render a row for it. Both `EmptyResponseUtils`
/// (via an untouched result holder) and the multi-stage executors (via an uninitialized `Object[]` slot) hand that to
/// [AggregationFunction#extractFinalResult], and a function that dereferences the argument throws instead of returning
/// the SQL answer for zero rows.
@SuppressWarnings({"rawtypes", "unchecked"})
public class AggregationFunctionNullContractTest {

  private static final int NUM_DOCS = 10;

  /// Argument shapes tried in order until the factory accepts one. Functions needing an argument list that none of
  /// these match are reported by [#testEverySkippedTypeIsAccountedFor] rather than silently skipped.
  private static final String[] ARGUMENT_SHAPES = {
      "(column)", "(*)", "(column, 50)", "(column, column2)", "(column, 50, 100)", "(column, column2, column3)",
      // arrayAgg(dataColumn, 'dataType')
      "(column, 'LONG')",
      // firstWithTime / lastWithTime(dataColumn, timeColumn, 'dataType')
      "(column, column2, 'LONG')",
      // histogram(column, lower, upper, numBins)
      "(column, 0, 1000, 10)",
      // The funnel family: (timestampColumn, windowMillis, numSteps, stepPredicate...)
      "(column, '1000', 2, column2 = 'a', column2 = 'b')",
      // funnelStepDurationStats takes a trailing settings literal
      "(column, '1000', 2, column2 = 'a', column2 = 'b', 'durationFunctions=count')",
      // funnelEventsFunctionEval takes a trailing (numExtraFields, extraColumns...) tail, where the count must match
      // the number of columns that follow it
      "(column, '1000', 2, column2 = 'a', column2 = 'b', 2, column, column2)",
      // funnelCount uses named options rather than positional arguments
      "(STEPS(column2 = 'a', column2 = 'b'), CORRELATE_BY(column))"
  };

  /// Types that are not user-facing aggregates, so "what does this return when nothing was aggregated" is not a
  /// meaningful question. Keep this list short and justified: anything added here stops being checked.
  ///
  /// - `FOURTHMOMENT` is the shared accumulator behind `SKEWNESS` and `KURTOSIS` and throws from `extractFinalResult`
  ///   by design. The same class is still covered through those two types.
  /// - The parent/child `EXPRMIN` / `EXPRMAX` types are produced by the query rewriter, not written by users. The
  ///   parent returns a nested data block rather than a scalar, and the child always returns `0`.
  private static final Set<AggregationFunctionType> INTERNAL_TYPES = Set.of(
      AggregationFunctionType.FOURTHMOMENT,
      AggregationFunctionType.PINOTPARENTAGGEXPRMIN,
      AggregationFunctionType.PINOTPARENTAGGEXPRMAX,
      AggregationFunctionType.PINOTCHILDAGGEXPRMIN,
      AggregationFunctionType.PINOTCHILDAGGEXPRMAX
  );

  /// Types that cannot be constructed from a bare [FunctionContext] at all, and so cannot be checked here.
  ///
  /// - `EXPRMIN` / `EXPRMAX` are rejected by the factory outright; they are only legal in a selection without an alias,
  ///   and are rewritten into the parent/child pair above before execution. Covered by `ExprMinMaxNullHandlingTest`,
  ///   which drives the parent directly.
  /// - `TIMESERIESAGGREGATE` needs time-series plan context that a bare function context cannot supply. Covered by
  ///   `TimeSeriesAggregationNullHandlingTest`.
  ///
  /// Being here means the contract is checked by the named test instead, not that it goes unchecked. A new entry
  /// needs a test of its own before it is added.
  ///
  /// [#testEverySkippedTypeIsAccountedFor] pins this exactly, in both directions, so a newly added function cannot drop
  /// out of the contract unnoticed.
  private static final Set<AggregationFunctionType> EXPECTED_UNCONSTRUCTIBLE = Set.of(
      AggregationFunctionType.EXPRMIN,
      AggregationFunctionType.EXPRMAX,
      AggregationFunctionType.TIMESERIESAGGREGATE
  );

  @DataProvider(name = "aggregationFunctions")
  public Object[][] aggregationFunctions() {
    List<Object[]> cases = new ArrayList<>();
    for (AggregationFunctionType type : AggregationFunctionType.values()) {
      if (INTERNAL_TYPES.contains(type)) {
        continue;
      }
      for (boolean nullHandlingEnabled : new boolean[]{false, true}) {
        AggregationFunction function = tryCreate(type, nullHandlingEnabled);
        if (function != null) {
          cases.add(new Object[]{type, nullHandlingEnabled, function});
        }
      }
    }
    return cases.toArray(new Object[0][]);
  }

  /// An empty result holder must survive the whole extract path, which is what an all-pruned query does.
  @Test(dataProvider = "aggregationFunctions")
  public void testEmptyHolderExtractsWithoutThrowing(AggregationFunctionType type, boolean nullHandlingEnabled,
      AggregationFunction function) {
    Object intermediate;
    try {
      intermediate = function.extractAggregationResult(function.createAggregationResultHolder());
    } catch (Exception e) {
      throw new AssertionError(
          describe(type, nullHandlingEnabled) + ": extractAggregationResult failed on an empty holder", e);
    }
    try {
      render(function, function.extractFinalResult(intermediate));
    } catch (Exception e) {
      throw new AssertionError(
          describe(type, nullHandlingEnabled) + ": extractFinalResult failed on the empty intermediate result", e);
    }
  }

  /// A `null` intermediate result means nothing was aggregated and must be resolved, never propagated by dereference.
  @Test(dataProvider = "aggregationFunctions")
  public void testNullIntermediateResultIsResolved(AggregationFunctionType type, boolean nullHandlingEnabled,
      AggregationFunction function) {
    try {
      render(function, function.extractFinalResult(null));
    } catch (Exception e) {
      throw new AssertionError(
          describe(type, nullHandlingEnabled) + ": extractFinalResult(null) must return the answer for no input", e);
    }
  }

  /// Forces the final result through both steps the broker response performs on it.
  ///
  /// Returning is not enough to prove the `null` was resolved: a function can wrap the `null` in a serializer that only
  /// dereferences it when the value is rendered, which moves the failure out of this method and into the response path.
  /// [org.apache.pinot.common.utils.DataSchema.ColumnDataType#convert] is the step that threw in production, so it is
  /// exercised alongside `toString`; a serializer wrapping a `null` can survive one and fail the other.
  private static void render(AggregationFunction function, @Nullable Object finalResult) {
    if (finalResult != null) {
      finalResult.toString();
      function.getFinalResultColumnType().convert(finalResult);
    }
  }

  /// `null` is the identity of merging, settled once by the caller so no implementation needs its own null branch.
  ///
  /// Both helpers resolve the `null` operand without delegating, so this holds for every function, including those that
  /// do not support merging final results at all.
  @Test(dataProvider = "aggregationFunctions")
  public void testNullIsTheMergeIdentity(AggregationFunctionType type, boolean nullHandlingEnabled,
      AggregationFunction function) {
    String description = describe(type, nullHandlingEnabled);
    Comparable value = "value";
    assertSame(AggregationFunctionUtils.merge(function, null, value), value, description);
    assertSame(AggregationFunctionUtils.merge(function, value, null), value, description);
    assertNull(AggregationFunctionUtils.merge(function, null, null), description);
    assertSame(AggregationFunctionUtils.mergeFinalResult(function, null, value), value, description);
    assertSame(AggregationFunctionUtils.mergeFinalResult(function, value, null), value, description);
    assertNull(AggregationFunctionUtils.mergeFinalResult(function, null, null), description);
  }

  /// The functions that SQL fixes at `0` rather than `NULL` when nothing was aggregated.
  @Test
  public void testCountingFunctionsReturnZeroWhenNothingAggregated() {
    assertEquals(create("COUNT", "(*)", true).extractFinalResult(null), 0L);
    assertEquals(create("DISTINCTCOUNT", "(column)", true).extractFinalResult(null), 0);
    assertEquals(create("DISTINCTCOUNTHLL", "(column)", true).extractFinalResult(null), 0L);
    assertEquals(create("DISTINCTCOUNTBITMAP", "(column)", true).extractFinalResult(null), 0);
  }

  /// A funnel over no events completed no steps, so every one of them answers zero rather than `NULL`.
  ///
  /// They belong with the counting functions above rather than the value functions below: the answer is a count, or
  /// a per-step vector of counts, and zero is the meaningful value for it. Pinned because the whole family renders
  /// this from an initial accumulator rather than from a branch in `extractFinalResult`, so it would move quietly.
  @Test
  public void testFunnelFunctionsReturnZeroWhenNothingAggregated() {
    String steps = "(column, '1000', 2, column2 = 'a', column2 = 'b')";
    assertEquals(create("FUNNELMAXSTEP", steps, true).extractFinalResult(null), 0);
    assertEquals(create("FUNNELCOMPLETECOUNT", steps, true).extractFinalResult(null), 0);
    assertEquals(create("FUNNELSTEPDURATIONSTATS",
            "(column, '1000', 2, column2 = 'a', column2 = 'b', 'durationFunctions=count')", true)
        .extractFinalResult(null), new DoubleArrayList());
    assertEquals(create("FUNNELMATCHSTEP", steps, true).extractFinalResult(null), new IntArrayList(new int[]{0, 0}));
    assertEquals(create("FUNNELEVENTSFUNCTIONEVAL",
            "(column, '1000', 2, column2 = 'a', column2 = 'b', 2, column, column2)", true)
        .extractFinalResult(null), new ObjectArrayList<String>());
    assertEquals(create("FUNNELCOUNT", "(STEPS(column2 = 'a', column2 = 'b'), CORRELATE_BY(column))", true)
        .extractFinalResult(null), new LongArrayList(new long[]{0L, 0L}));
  }

  /// The functions that return SQL `NULL` when nothing was aggregated.
  ///
  /// Both representations of that state are checked. A `null` intermediate result is the obvious one, but several of
  /// these substitute an empty accumulator for an untouched holder and never produce a `null` at all on the
  /// single-stage engine, so testing only the `null` would pass without exercising them.
  @Test
  public void testValueFunctionsReturnNullWhenNothingAggregated() {
    for (AggregationFunctionType type : new AggregationFunctionType[]{
        AggregationFunctionType.SUM, AggregationFunctionType.MIN, AggregationFunctionType.MAX,
        AggregationFunctionType.AVG, AggregationFunctionType.MINMAXRANGE, AggregationFunctionType.VARPOP,
        AggregationFunctionType.STDDEVPOP, AggregationFunctionType.PERCENTILE,
        AggregationFunctionType.PERCENTILEEST, AggregationFunctionType.PERCENTILETDIGEST,
        AggregationFunctionType.PERCENTILEKLL, AggregationFunctionType.PERCENTILESMARTTDIGEST,
        // These carry a legacy sentinel with the option disabled - NaN, the empty point, an all-zero histogram, an
        // empty id set - so only the enabled answer is NULL, and only the enabled answer is checked here
        AggregationFunctionType.IDSET, AggregationFunctionType.HISTOGRAM, AggregationFunctionType.SKEWNESS,
        AggregationFunctionType.KURTOSIS, AggregationFunctionType.STUNION,
        // These two answer NULL in both modes, which is why neither needs a mode-aware branch
        AggregationFunctionType.SUMARRAYLONG, AggregationFunctionType.SUMARRAYDOUBLE}) {
      // Built through the shared argument shapes: the percentile families disagree on whether the percentile is a
      // name suffix or an argument, and only some accept both
      AggregationFunction function = tryCreate(type, true);
      assertNotNull(function, "Could not construct " + type.getName());
      String name = type.getName();
      assertNull(function.extractFinalResult(null), name + " over a null intermediate result must be NULL");
      Object empty = function.extractAggregationResult(function.createAggregationResultHolder());
      assertNull(function.extractFinalResult(empty), name + " over an empty accumulator must be NULL");
    }
  }

  /// Pins exactly which functions answer differently once null handling is enabled.
  ///
  /// Aggregating a block whose rows are all null separates the two modes: enabled skips every row and reports
  /// nothing aggregated, disabled reads each one as the column default and aggregates it. A function shows up here
  /// only if it both has null-aware aggregation and receives the query's option, so the set is a direct read-out of
  /// which functions the option actually reaches.
  ///
  /// The raw variants reach the option through the function they delegate to, so threading it into one of those
  /// changes the raw variant alongside it.
  ///
  /// Absence no longer means a function was left out of the contract. Every user-facing aggregation now receives the
  /// option, so a function missing from here is one this harness cannot drive — see
  /// [#NOT_EXERCISABLE_BY_SYNTHETIC_BLOCK], which lists where each of those is covered instead — or one that
  /// genuinely answers the same in both modes, as the counting functions do when the empty answer is `0` either
  /// way.
  ///
  /// Two bounds on how much this set proves. It only covers what the harness can drive — see
  /// [#NOT_EXERCISABLE_BY_SYNTHETIC_BLOCK] — and it compares the **rendered** answers, because several functions
  /// return a serializer that implements no `equals`; comparing those objects would compare identities and report
  /// every one of them as honouring the option.
  private static final Set<AggregationFunctionType> HONOURS_NULL_HANDLING = Set.of(
      AggregationFunctionType.COUNT, AggregationFunctionType.MIN, AggregationFunctionType.MAX,
      AggregationFunctionType.SUM, AggregationFunctionType.SUM0, AggregationFunctionType.AVG,
      AggregationFunctionType.MODE, AggregationFunctionType.ANYVALUE, AggregationFunctionType.MINMAXRANGE,
      AggregationFunctionType.DISTINCTCOUNT, AggregationFunctionType.DISTINCTCOUNTOFFHEAP,
      AggregationFunctionType.DISTINCTSUM, AggregationFunctionType.DISTINCTAVG, AggregationFunctionType.PERCENTILE,
      AggregationFunctionType.PERCENTILEEST, AggregationFunctionType.PERCENTILERAWEST,
      AggregationFunctionType.PERCENTILETDIGEST, AggregationFunctionType.PERCENTILERAWTDIGEST,
      AggregationFunctionType.PERCENTILESMARTTDIGEST, AggregationFunctionType.PERCENTILEKLL,
      AggregationFunctionType.PERCENTILERAWKLL, AggregationFunctionType.VARPOP, AggregationFunctionType.VARSAMP,
      AggregationFunctionType.STDDEVPOP, AggregationFunctionType.STDDEVSAMP,
      // Both are the same PinotFourthMoment accumulator, so threading the option into it moves the pair
      AggregationFunctionType.SKEWNESS, AggregationFunctionType.KURTOSIS, AggregationFunctionType.MINMV,
      AggregationFunctionType.MAXMV, AggregationFunctionType.SUMMV, AggregationFunctionType.AVGMV,
      AggregationFunctionType.MINMAXRANGEMV, AggregationFunctionType.DISTINCTCOUNTMV,
      AggregationFunctionType.DISTINCTSUMMV, AggregationFunctionType.DISTINCTAVGMV,
      AggregationFunctionType.PERCENTILEMV, AggregationFunctionType.PERCENTILEESTMV,
      AggregationFunctionType.PERCENTILERAWESTMV, AggregationFunctionType.PERCENTILEKLLMV,
      AggregationFunctionType.PERCENTILETDIGESTMV, AggregationFunctionType.PERCENTILERAWTDIGESTMV,
      AggregationFunctionType.PERCENTILERAWKLLMV, AggregationFunctionType.MINSTRING, AggregationFunctionType.MAXSTRING,
      AggregationFunctionType.MINLONG, AggregationFunctionType.MAXLONG, AggregationFunctionType.SUMINT,
      AggregationFunctionType.SUMLONG, AggregationFunctionType.SUMPRECISION, AggregationFunctionType.FIRSTWITHTIME,
      AggregationFunctionType.LASTWITHTIME, AggregationFunctionType.ARRAYAGG, AggregationFunctionType.LISTAGG,
      AggregationFunctionType.IDSET, AggregationFunctionType.HISTOGRAM,
      // Given the option so they can skip null rows; a row counts only when both input columns are non-null
      AggregationFunctionType.COVARPOP, AggregationFunctionType.COVARSAMP,
      // Given the option so they can skip null rows. These two were in this set once before, on the strength of an
      // identity comparison that reported every serializer-valued function as honouring it; they belong here now
      // because they genuinely do.
      AggregationFunctionType.FREQUENTSTRINGSSKETCH, AggregationFunctionType.FREQUENTLONGSSKETCH,
      // Given the option so they can skip null rows. A distinct count over nothing is 0 in every one of these, so
      // the empty-input answer is unchanged; what changed is that a null row no longer contributes the column default.
      AggregationFunctionType.SEGMENTPARTITIONEDDISTINCTCOUNT, AggregationFunctionType.DISTINCTCOUNTBITMAP,
      AggregationFunctionType.DISTINCTCOUNTHLL, AggregationFunctionType.DISTINCTCOUNTRAWHLL,
      AggregationFunctionType.DISTINCTCOUNTSMARTHLL, AggregationFunctionType.DISTINCTCOUNTHLLPLUS,
      AggregationFunctionType.DISTINCTCOUNTRAWHLLPLUS, AggregationFunctionType.DISTINCTCOUNTSMARTHLLPLUS,
      AggregationFunctionType.DISTINCTCOUNTULL, AggregationFunctionType.DISTINCTCOUNTRAWULL,
      AggregationFunctionType.DISTINCTCOUNTSMARTULL, AggregationFunctionType.DISTINCTCOUNTTHETASKETCH,
      AggregationFunctionType.DISTINCTCOUNTRAWTHETASKETCH, AggregationFunctionType.DISTINCTCOUNTCPCSKETCH,
      AggregationFunctionType.DISTINCTCOUNTRAWCPCSKETCH, AggregationFunctionType.DISTINCTCOUNTBITMAPMV,
      AggregationFunctionType.DISTINCTCOUNTHLLMV, AggregationFunctionType.DISTINCTCOUNTRAWHLLMV,
      AggregationFunctionType.DISTINCTCOUNTHLLPLUSMV, AggregationFunctionType.DISTINCTCOUNTRAWHLLPLUSMV,
      // Reached once the multi-value shapes were added below; the array sums take only an array column
      AggregationFunctionType.SUMARRAYLONG, AggregationFunctionType.SUMARRAYDOUBLE
  );

  /// Functions this test cannot drive with a one-column synthetic block, pinned so that a silent drop-out is always a
  /// reviewed decision. Derived from a run rather than predicted.
  ///
  /// [#BLOCK_SHAPES] supplies one column at a time — `double`, `long`, `int`, `String` or `byte[]`, single-value,
  /// plus multi-value `long`, `double` and `int` — and gives every input expression the same shape. A function
  /// lands here when it needs a value type outside that list, a payload the shape cannot fabricate (a serialized
  /// sketch or geometry rather than an empty `byte[]`), a dictionary, or two input columns of different types. That
  /// last one is what rules out the funnels: their timestamp is a `long` and their steps are `int` predicates, and
  /// no single shape is both.
  ///
  /// The exclusion is scoped to this one check. The rest of the contract is still enforced against these functions
  /// here, since the other cases construct them and call [AggregationFunction#extractFinalResult] without
  /// aggregating first, and their null-row skipping is covered by a test built for the shape each one needs:
  /// `DistinctCountSketchNullHandlingTest`, `FrequentSketchNullHandlingTest`, `ValueAggregationNullHandlingTest`
  /// and `FunnelNullHandlingTest`. So membership here means "checked elsewhere", not "unchecked".
  private static final Set<AggregationFunctionType> NOT_EXERCISABLE_BY_SYNTHETIC_BLOCK = Set.of(
      AggregationFunctionType.FASTHLL, AggregationFunctionType.DISTINCTCOUNTTUPLESKETCH,
      AggregationFunctionType.DISTINCTCOUNTRAWINTEGERSUMTUPLESKETCH,
      AggregationFunctionType.SUMVALUESINTEGERSUMTUPLESKETCH, AggregationFunctionType.AVGVALUEINTEGERSUMTUPLESKETCH,
      AggregationFunctionType.STUNION, AggregationFunctionType.BOOLAND, AggregationFunctionType.BOOLOR,
      AggregationFunctionType.FUNNELMAXSTEP, AggregationFunctionType.FUNNELCOMPLETECOUNT,
      AggregationFunctionType.FUNNELSTEPDURATIONSTATS, AggregationFunctionType.FUNNELMATCHSTEP,
      AggregationFunctionType.FUNNELEVENTSFUNCTIONEVAL, AggregationFunctionType.FUNNELCOUNT
  );

  /// Checks every function rather than a fixed list, so that a function gaining or losing null awareness is caught.
  ///
  /// Failing with something extra means a function started honouring the option and should be added to
  /// [#HONOURS_NULL_HANDLING]; failing with something missing means one stopped, which is a regression unless the
  /// entry is stale. Functions the synthetic block cannot feed — because they read a value type it does not
  /// implement, or reject the column outright — are not counted either way.
  @Test
  public void testNullHandlingOptionReachesEveryFunctionThatHonoursIt() {
    Set<AggregationFunctionType> honours = new TreeSet<>();
    Set<AggregationFunctionType> notExercisable = new TreeSet<>();
    for (AggregationFunctionType type : AggregationFunctionType.values()) {
      if (INTERNAL_TYPES.contains(type) || tryCreate(type, false) == null) {
        continue;
      }
      Object enabled = null;
      Object disabled = null;
      boolean driven = false;
      for (Supplier<BlockValSet> shape : BLOCK_SHAPES) {
        try {
          // The same shape drives both modes, so that the comparison below is between the modes and not the inputs
          enabled = aggregateAllNulls(type, true, shape);
          disabled = aggregateAllNulls(type, false, shape);
          driven = true;
          break;
        } catch (RuntimeException e) {
          // This shape reads a value type the function rejects; fall through to the next
        }
      }
      if (!driven) {
        notExercisable.add(type);
        continue;
      }
      // Deep, because a rendered array compares by identity otherwise and every array-valued function would look
      // like it honours the option
      if (!Objects.deepEquals(enabled, disabled)) {
        honours.add(type);
      }
    }

    Set<AggregationFunctionType> newlySkipped = new TreeSet<>(notExercisable);
    newlySkipped.removeAll(NOT_EXERCISABLE_BY_SYNTHETIC_BLOCK);
    Set<AggregationFunctionType> staleSkip = new TreeSet<>(NOT_EXERCISABLE_BY_SYNTHETIC_BLOCK);
    staleSkip.removeAll(notExercisable);
    assertTrue(newlySkipped.isEmpty() && staleSkip.isEmpty(),
        "The set of functions this test cannot drive has changed.\n  Newly undrivable, so they stopped being checked "
            + "and need the same review as an entry in EXPECTED_UNCONSTRUCTIBLE: " + newlySkipped
            + "\n  Drivable again, so remove them from NOT_EXERCISABLE_BY_SYNTHETIC_BLOCK: " + staleSkip);

    Set<AggregationFunctionType> gained = new TreeSet<>(honours);
    gained.removeAll(HONOURS_NULL_HANDLING);
    Set<AggregationFunctionType> lost = new TreeSet<>(HONOURS_NULL_HANDLING);
    lost.removeAll(honours);
    assertTrue(gained.isEmpty() && lost.isEmpty(),
        "The null handling option now reaches a different set of functions.\n  Newly honouring it, so add to "
            + "HONOURS_NULL_HANDLING once the behaviour change is intended: " + gained
            + "\n  No longer honouring it, which is a regression unless the entry is stale: " + lost);
  }

  /// Aggregates a block whose rows are all null and returns the final result **as the broker would render it**.
  ///
  /// The rendered form is what the two modes are compared on, not the object itself. Several functions answer with a
  /// serializer — `SerializedKLL`, `SerializedTDigest`, the sketch wrappers — and none of those implement `equals`,
  /// so comparing the objects compares identities: two runs are never equal, every such function looks like it
  /// honours the option, and the check that one has *stopped* honouring it can never fail. Converting through
  /// [AggregationFunction#getFinalResultColumnType] gives the value a client would see, which compares properly.
  ///
  /// Every input expression the function declares is given a block, not just the first, so that the functions taking
  /// more than one column are driven rather than failing on a missing entry.
  private static Object aggregateAllNulls(AggregationFunctionType type, boolean nullHandlingEnabled,
      Supplier<BlockValSet> blockShape) {
    AggregationFunction function = tryCreate(type, nullHandlingEnabled);
    assertNotNull(function, "Could not construct " + type.getName());
    Map<ExpressionContext, BlockValSet> blockValSetMap = new HashMap<>();
    for (Object inputExpression : function.getInputExpressions()) {
      // Every declared input gets a block, literals included: a function that took a literal for one of its columns
      // still looks that expression up in the map, and a missing entry makes it throw and drop out of the census
      blockValSetMap.put((ExpressionContext) inputExpression, blockShape.get());
    }
    // A function whose only argument is a literal, such as COUNT(*), still needs one block to read a length from
    if (blockValSetMap.isEmpty()) {
      blockValSetMap.put(ExpressionContext.forIdentifier("column"), blockShape.get());
    }
    AggregationResultHolder resultHolder = function.createAggregationResultHolder();
    function.aggregate(NUM_DOCS, resultHolder, blockValSetMap);
    Object finalResult = function.extractFinalResult(function.extractAggregationResult(resultHolder));
    return finalResult != null ? function.getFinalResultColumnType().convert(finalResult) : null;
  }

  /// All-null block shapes, tried in order until one drives the function.
  ///
  /// Probed rather than pinned per function: which value type an accumulator reads is an implementation detail that
  /// changes, and a hard-coded mapping silently drops a function out of the census when it feeds the wrong width.
  private static int[][] mvInts() {
    int[][] rows = new int[NUM_DOCS][];
    for (int i = 0; i < NUM_DOCS; i++) {
      rows[i] = new int[]{0};
    }
    return rows;
  }

  private static long[][] mvLongs() {
    long[][] rows = new long[NUM_DOCS][];
    for (int i = 0; i < NUM_DOCS; i++) {
      rows[i] = new long[]{0L};
    }
    return rows;
  }

  private static double[][] mvDoubles() {
    double[][] rows = new double[NUM_DOCS][];
    for (int i = 0; i < NUM_DOCS; i++) {
      rows[i] = new double[]{0.0};
    }
    return rows;
  }

  private static final List<Supplier<BlockValSet>> BLOCK_SHAPES = List.of(
      () -> SyntheticBlockValSets.Int.create(NUM_DOCS, allNullBitmap(), () -> 0),
      () -> SyntheticBlockValSets.Long.create(NUM_DOCS, allNullBitmap(), () -> 0L),
      () -> SyntheticBlockValSets.Double.create(NUM_DOCS, allNullBitmap(), () -> 0.0),
      () -> SyntheticBlockValSets.Str.create(NUM_DOCS, allNullBitmap(), () -> ""),
      () -> SyntheticBlockValSets.Bytes.create(NUM_DOCS, allNullBitmap(), () -> new byte[0]),
      () -> SyntheticBlockValSets.IntMV.create(allNullBitmap(), mvInts()),
      () -> SyntheticBlockValSets.LongMV.create(allNullBitmap(), mvLongs()),
      () -> SyntheticBlockValSets.DoubleMV.create(allNullBitmap(), mvDoubles())
  );

  private static RoaringBitmap allNullBitmap() {
    RoaringBitmap allNull = new RoaringBitmap();
    allNull.add(0L, NUM_DOCS);
    return allNull;
  }

  /// Pins exactly which types go unchecked, so a skip is always a reviewed decision rather than a silent one.
  ///
  /// Failing in the "not accounted for" direction means a function could not be built from [#ARGUMENT_SHAPES] and is
  /// therefore not being checked at all: either add the argument shape it needs, or classify it. Failing in the "no
  /// longer skipped" direction means an entry here is stale and should be deleted.
  @Test
  public void testEverySkippedTypeIsAccountedFor() {
    Set<AggregationFunctionType> skipped = new TreeSet<>();
    for (AggregationFunctionType type : AggregationFunctionType.values()) {
      if (!INTERNAL_TYPES.contains(type) && tryCreate(type, false) == null) {
        skipped.add(type);
      }
    }

    Set<AggregationFunctionType> unaccounted = new TreeSet<>(skipped);
    unaccounted.removeAll(EXPECTED_UNCONSTRUCTIBLE);
    assertTrue(unaccounted.isEmpty(),
        "These types are silently excluded from the null contract; extend ARGUMENT_SHAPES or classify them: "
            + unaccounted);

    Set<AggregationFunctionType> stale = new TreeSet<>(EXPECTED_UNCONSTRUCTIBLE);
    stale.removeAll(skipped);
    assertTrue(stale.isEmpty(), "These types can now be constructed and should be removed from "
        + "EXPECTED_UNCONSTRUCTIBLE so they are checked: " + stale);
  }

  private static AggregationFunction create(String name, String args, boolean nullHandlingEnabled) {
    FunctionContext context = RequestContextUtils.getExpression(name + args).getFunction();
    AggregationFunction function = AggregationFunctionFactory.getAggregationFunction(context, nullHandlingEnabled);
    if (function == null) {
      fail("Could not construct " + name + args);
    }
    return function;
  }

  @Nullable
  private static AggregationFunction tryCreate(AggregationFunctionType type, boolean nullHandlingEnabled) {
    for (String args : ARGUMENT_SHAPES) {
      try {
        FunctionContext context = RequestContextUtils.getExpression(type.getName() + args).getFunction();
        AggregationFunction function = AggregationFunctionFactory.getAggregationFunction(context, nullHandlingEnabled);
        if (function != null) {
          return function;
        }
      } catch (Exception e) {
        // Wrong argument shape for this function, or a function that cannot be built outside a real query plan (e.g.
        // the parent/child and time-series functions). Try the next shape.
      }
    }
    return null;
  }

  private static String describe(AggregationFunctionType type, boolean nullHandlingEnabled) {
    return type.getName() + (nullHandlingEnabled ? " [nullHandlingEnabled]" : " [nullHandlingDisabled]");
  }
}
