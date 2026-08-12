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

import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.pinot.common.CustomObject;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;


/// Interface for aggregation functions.
///
/// The implementation should be stateless, and can be shared among multiple segments in multiple threads. The result
/// for each segment should be stored and passed in via the result holder.
///
/// ## Null contract
///
/// Null handling is a per-query flag, and the two modes place different requirements on an implementation.
///
/// ### Null handling disabled
///
/// Null values are read as the column's default, so no input value is ever null. An implementation keeps a primitive
/// result holder, performs no null tracking, and needs no null check while aggregating.
///
/// A row that aggregated the column's default is indistinguishable from one that aggregated nothing, so this mode has
/// no answer to give for the empty input other than the accumulator's identity: `0` for `SUM`, `+Infinity` for `MIN`.
/// Those answers are a backward-compatibility constraint rather than an attempt at SQL conformance, and must not
/// change.
///
/// The identity is rendered by [#extractFinalResult], not carried by the accumulator. An untouched holder yields a
/// `null` **intermediate result** in this mode too, and [#extractFinalResult] turns that `null` into the identity.
/// Doing it there rather than substituting an empty accumulator upstream keeps one decision point, and is the only
/// option for the types that have no empty value to substitute — `MAXSTRING`, `MINSTRING` and `ANYVALUE` are
/// object-backed and have none.
///
/// ### Null handling enabled
///
/// SQL evaluates an aggregate over its **non-null** input values only, and separately defines what the result is when
/// there are none. This mode models those as two distinct things:
/// - A `null` **intermediate result** means nothing was aggregated, either because no row matched or because every
///   matching value was null. It carries no per-function meaning, which is what makes it correct for the aggregation
///   methods to skip null rows outright rather than fold them in.
///   `null` is not the only way that state is represented, though: an empty accumulator — an empty set, value list,
///   digest, sketch or map — means the same thing, and still arrives. A peer that serialized one sends it, and so do
///   the functions in the first deviation below, which never receive the option and substitute one rather than
///   returning `null`. So [#extractFinalResult] accepts both.
///   `null` is the representation to produce where there is a choice, because it is the only one always available:
///   `MAXSTRING`, `MINSTRING` and `ANYVALUE` are object-backed and have no empty value to substitute.
/// - [#extractFinalResult] decides what that means for this aggregation, and is the only method that does. `COUNT`
///   and the distinct counts return `0`; `SUM`, `MIN`, `MAX`, `AVG` and the percentiles return `null`.
///
/// An implementation switches to a nullable result holder only in this mode, which keeps the boxing cost on the opt-in
/// path.
///
/// ### Both modes
///
/// A `null` operand is the identity of merging, and that carries no per-function meaning, so it is resolved once by the
/// caller rather than in every implementation: merge intermediate results through [AggregationFunctionUtils#merge] and
/// final results through [AggregationFunctionUtils#mergeFinalResult], which settle `null` operands before delegating.
/// [#merge] and [#mergeFinalResult] are therefore handed two real values and must not be called with `null`.
///
/// A `null` is safe to put on the wire in either mode. The data table writer supports nulls for every column type
/// independent of the query's option: types with an in-band null encoding keep it, and the rest are written as the
/// column's null placeholder with the row recorded in a per-column null bitmap. So returning `null` rather than
/// substituting an empty accumulator costs nothing in fidelity.
///
/// Nullability is declared rather than discovered. Whether an intermediate result can be `null` is decided by
/// [#extractAggregationResult] and [#extractGroupByResult]; whether a final result can be is decided by
/// [#extractFinalResult]. The `@Nullable` annotation on each is the statement of it, and everything downstream —
/// merging, serialization, rendering — may rely on it: a function whose [#extractFinalResult] is not `@Nullable`
/// never produces a `null` final result, so no caller needs to handle one for it. Adding `@Nullable` to one of
/// these is therefore a behavioural change and not a documentation fix, because it widens what the rest of the
/// engine must carry.
///
/// TODO: Known deviations from the above.
///   1. Several aggregation functions never receive the query's null handling option at all, so they cannot skip
///      null rows and fold the column's default value into the aggregate whatever the query asked for: the
///      sketch-backed distinct counts (`DISTINCTCOUNTBITMAP`, `DISTINCTCOUNTHLL`, `DISTINCTCOUNTTHETASKETCH`,
///      `DISTINCTCOUNTCPCSKETCH`, `FASTHLL`, `SEGMENTPARTITIONEDDISTINCTCOUNT` and the raw and smart variants of
///      each), `HISTOGRAM`, `IDSET`, `STUNION`, the array sums, and the funnel family. Whether a function takes the
///      option is visible at its construction site, which is the reliable way to tell.
///      The family names are not a safe shorthand for this, in either direction. The exact distinct functions
///      (`DISTINCTCOUNT`, `DISTINCTSUM`, `DISTINCTAVG`, `DISTINCTCOUNTOFFHEAP`) do take the option and skip null
///      rows through their shared base, as do the variance, standard-deviation and covariance functions and the
///      first/last-with-time functions, so "the distinct-count family" and "the statistical functions" both include
///      members that honour the option and members that cannot.
///      These same functions also substitute an empty accumulator in [#extractAggregationResult],
///      [#extractGroupByResult] or both, rather than returning `null` and letting [#extractFinalResult] render the
///      disabled-mode value. That is not a separate defect: without the option [#extractFinalResult] cannot tell the
///      two modes apart, so it has nothing to decide with and the substitution is the only thing holding the answer.
///      Which of the two paths substitutes is inconsistent across them and sometimes within one, so such a function
///      can answer differently depending on whether the query groups — a filtered aggregation makes that observable,
///      since one group key space is shared by every aggregation in a query and a group created by one of them can
///      hold no rows for another.
///      Conforming them alongside the option is more than moving a branch. The value to preserve is often not a
///      constant, and the function that substitutes is frequently not the one that renders: the raw variants
///      delegate extraction to the plain function and serialize what it returns, so conforming the plain function
///      alone changes the raw variant's answer from a serialized empty sketch to `NULL` without touching the raw
///      variant at all.
///   2. The multi-stage engine constructs every aggregation function with null handling enabled and never consults the
///      query's null handling option, so a query that disables it still gets enabled-mode semantics there. The two
///      engines can therefore answer the same query differently: with null handling disabled, `SUM` over a query
///      whose segments are all pruned is `NULL` on the multi-stage engine and `0` on the single-stage engine. This
///      may be intended, the multi-stage engine being the SQL-conformant one, but it means the mode described above
///      is not actually per-query everywhere.
///
/// @param <IntermediateResult> Intermediate result generated from segment
/// @param <FinalResult> Final result used in broker response
@ThreadSafe
@SuppressWarnings("rawtypes")
public interface AggregationFunction<IntermediateResult, FinalResult extends Comparable> {

  /// Returns the type of the aggregation function.
  AggregationFunctionType getType();

  /// Returns the column name to be used in the data schema of results.
  /// e.g. 'MINMAXRANGEMV( foo)' -> 'minmaxrangemv(foo)', 'PERCENTILE75(bar)' -> 'percentile75(bar)'
  String getResultColumnName();

  /// Returns a list of input expressions needed for performing aggregation.
  List<ExpressionContext> getInputExpressions();

  /// Returns an aggregation result holder for this function (aggregation only).
  ///
  /// A nullable holder is only warranted with null handling enabled, so that the boxing cost stays on the opt-in path.
  /// See the null contract on [AggregationFunction].
  AggregationResultHolder createAggregationResultHolder();

  /// Returns a group-by result holder with the given initial capacity and max capacity for this function (aggregation
  /// group-by).
  ///
  /// A nullable holder is only warranted with null handling enabled, so that the boxing cost stays on the opt-in path.
  /// See the null contract on [AggregationFunction].
  GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity);

  /// Performs aggregation on the given block value sets (aggregation only).
  ///
  /// With null handling enabled, null rows must be skipped rather than folded in as the column's default. See the null
  /// contract on [AggregationFunction].
  void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap);

  /// Performs aggregation on the given group key array and block value sets (aggregation group-by on single-value
  /// columns).
  ///
  /// With null handling enabled, null rows must be skipped rather than folded in as the column's default. See the null
  /// contract on [AggregationFunction].
  void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap);

  /// Performs aggregation on the given group keys array and block value sets (aggregation group-by on multi-value
  /// columns).
  ///
  /// With null handling enabled, null rows must be skipped rather than folded in as the column's default. See the null
  /// contract on [AggregationFunction].
  void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap);

  /// Extracts the intermediate result from the aggregation result holder (aggregation only).
  ///
  /// The holder may be untouched, which is how a query that matched no row arrives here. With null handling enabled
  /// that means nothing was aggregated and this returns `null`. With it disabled this returns the accumulator's
  /// identity instead, unless the accumulator is object-backed and its type has no identity to render, in which case it
  /// is `null` there too. See the null contract on [AggregationFunction].
  @Nullable
  IntermediateResult extractAggregationResult(AggregationResultHolder aggregationResultHolder);

  /// Extracts the intermediate result from the group-by result holder for the given group key (aggregation group-by).
  ///
  /// With null handling enabled, returns `null` when nothing was aggregated into the group. With it disabled a group
  /// normally holds at least one value, but an object-backed accumulator still returns `null` for a group this
  /// aggregation never wrote to: group keys are shared across the aggregations of a query, so a filtered aggregation
  /// whose predicate matched no row in the group leaves its slot untouched. See the null contract on
  /// [AggregationFunction].
  @Nullable
  IntermediateResult extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey);

  /// Merges two intermediate results, neither of which is `null`.
  ///
  /// Call [AggregationFunctionUtils#merge] instead of calling this directly: it settles the `null` operand, which is
  /// the identity of this operation and means the same thing for every implementation. See the null contract on
  /// [AggregationFunction].
  IntermediateResult merge(IntermediateResult intermediateResult1, IntermediateResult intermediateResult2);

  /// Returns the [ColumnDataType] of the intermediate result.
  ///
  /// This column data type is used for transferring data in data table.
  ColumnDataType getIntermediateResultColumnType();

  /// Serializes the intermediate result into a custom object. This method should be implemented if the intermediate
  /// result type is OBJECT.
  default SerializedIntermediateResult serializeIntermediateResult(IntermediateResult intermediateResult) {
    throw new UnsupportedOperationException("Cannot serialize intermediate result for function: " + getType());
  }

  /// Serialized intermediate result. Type can be used to identify the intermediate result type when deserializing it.
  class SerializedIntermediateResult {
    private final int _type;
    private final byte[] _bytes;

    public SerializedIntermediateResult(int type, byte[] buffer) {
      _type = type;
      _bytes = buffer;
    }

    public int getType() {
      return _type;
    }

    public byte[] getBytes() {
      return _bytes;
    }
  }

  /// Deserializes the intermediate result from the custom object. This method should be implemented if the intermediate
  /// result type is OBJECT.
  default IntermediateResult deserializeIntermediateResult(CustomObject customObject) {
    throw new UnsupportedOperationException("Cannot deserialize intermediate result for function: " + getType());
  }

  /// Returns the [ColumnDataType] of the final result.
  ///
  /// This column data type is used for constructing the result table
  ColumnDataType getFinalResultColumnType();

  /// Extracts the final result used in the broker response from the given intermediate result.
  ///
  /// A `null` intermediate result means nothing was aggregated, and this method decides what that means for this
  /// particular aggregation. It is the only place where that per-function answer is expressed, so it must never
  /// propagate the `null` blindly: `COUNT` and the distinct counts return `0`, while `SUM`, `MIN`, `MAX`, `AVG` and the
  /// percentiles return `null`.
  ///
  /// That is the null-handling-enabled answer. With null handling disabled that state is normally rendered as the
  /// accumulator's identity, but an object-backed accumulator still hands this method a `null`, and for `MAXSTRING`,
  /// `MINSTRING` and `ANYVALUE` there is no identity to render in the first place. A `null` argument is therefore part
  /// of the contract in both modes, not a defensive case. See the null contract on [AggregationFunction].
  @Nullable
  FinalResult extractFinalResult(@Nullable IntermediateResult intermediateResult);

  /// Merges two final results, neither of which is `null`. This can be used to optimized certain functions (e.g.
  /// DISTINCT_COUNT) when data is partitioned on each server, where we may directly request servers to return final
  /// result and merge them on broker.
  ///
  /// Call [AggregationFunctionUtils#mergeFinalResult] instead of calling this directly, on the same terms as [#merge].
  default FinalResult mergeFinalResult(FinalResult finalResult1, FinalResult finalResult2) {
    throw new UnsupportedOperationException("Cannot merge final results for function: " + getType());
  }

  /// Returns whether a star-tree index with the specified properties can be used for this aggregation function.
  default boolean canUseStarTree(Map<String, Object> functionParameters) {
    // Implementations can override this method to perform additional checks on the function parameters
    return true;
  }

  /// @return Description of this operator for Explain Plan
  String toExplainString();
}
