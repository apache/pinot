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

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectOpenHashSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.concurrent.ThreadSafe;
import org.apache.commons.lang3.StringUtils;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperationBuilder;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.theta.UpdateSketchBuilder;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.utils.DataSchema.ColumnDataType;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.query.aggregation.AggregationResultHolder;
import org.apache.pinot.core.query.aggregation.ObjectAggregationResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.GroupByResultHolder;
import org.apache.pinot.core.query.aggregation.groupby.ObjectGroupByResultHolder;
import org.apache.pinot.segment.spi.AggregationFunctionType;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.PeekableIntIterator;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code FunnelCountAggregationFunction} calculates the number of conversions for a given correlation column and
 * a list of steps as boolean expressions.
 *
 * Example:
 *   SELECT
 *    dateTrunc('day', timestamp) AS ts,
 *    FUNNEL_COUNT(
 *      STEPS(url = '/addToCart', url = '/checkout', url = '/orderConfirmation'),
 *      CORRELATED_BY(user_id)
 *    ) as step_counts
 *    FROM user_log
 *    WHERE url in ('/addToCart', '/checkout', '/orderConfirmation')
 *    GROUP BY 1
 *
 *  Counting strategies can be controlled via optional SETTINGS options, for example:
 *
 *  FUNNEL_COUNT(
 *    STEPS(url = '/addToCart', url = '/checkout', url = '/orderConfirmation'),
 *    CORRELATED_BY(user_id),
 *    SETTINGS('theta_sketch','nominalEntries=4096')
 *  )
 *
 *  There are 5 strategies available, mirroring the corresponding distinct count implementations as per below.
 *  <p><ul>
 *  <li>'set': See DISTINCTCOUNT at {@link DistinctCountAggregationFunction}
 *  <li>'bitmap' (default): See DISTINCTCOUNTBITMAP at {@link DistinctCountBitmapAggregationFunction}
 *  <li>'theta_sketch': See DISTINCTCOUNTTHETASKETCH at {@link DistinctCountThetaSketchAggregationFunction}
 *  <li>'partitioned': See SEGMENTPARTITIONEDDISTINCTCOUNT {@link SegmentPartitionedDistinctCountAggregationFunction}
 *  <li>'sorted': sorted counts per segment then sums up. Only availabe in combination with 'partitioned'.
 *  <li>'nominalEntries=4096': theta sketch configuration, default is 4096.
 *  </ul><p>
 */
public class FunnelCountAggregationFunction implements AggregationFunction<Object, LongArrayList> {
  private static final Sketch EMPTY_SKETCH = new UpdateSketchBuilder().build().compact();
  final List<ExpressionContext> _expressions;
  final List<ExpressionContext> _stepExpressions;
  final List<ExpressionContext> _correlateByExpressions;
  final ExpressionContext _primaryCorrelationCol;
  final int _numSteps;
  final int _nominalEntries;
  final boolean _partitionSetting;
  final boolean _sortingSetting;
  final boolean _thetaSketchSetting;
  final boolean _setSetting;

  final AggregationStrategy _thetaSketchAggregationStrategy;
  final AggregationStrategy _bitmapAggregationStrategy;
  final AggregationStrategy _sortedAggregationStrategy;

  final MergeStrategy _thetaSketchMergeStrategy;
  final MergeStrategy _setMergeStrategy;
  final MergeStrategy _bitmapMergeStrategy;
  final MergeStrategy _partitionedMergeStrategy;

  final ResultExtractionStrategy _thetaSketchResultExtractionStrategy;
  final ResultExtractionStrategy _setResultExtractionStrategy;
  final ResultExtractionStrategy _bitmapResultExtractionStrategy;
  final ResultExtractionStrategy _sortedPartitionedResultExtractionStrategy;
  final ResultExtractionStrategy _bitmapPartitionedResultExtractionStrategy;
  final ResultExtractionStrategy _thetaSketchPartitionedResultExtractionStrategy;

  public FunnelCountAggregationFunction(List<ExpressionContext> expressions) {
    _expressions = expressions;
    Option.validate(expressions);
    _correlateByExpressions = Option.CORRELATE_BY.getInputExpressions(expressions);
    _primaryCorrelationCol = Option.CORRELATE_BY.getFirstInputExpression(expressions);
    _stepExpressions = Option.STEPS.getInputExpressions(expressions);
    _numSteps = _stepExpressions.size();

    final List<String> settings = Option.SETTINGS.getLiterals(expressions);
    Setting.validate(settings);
    _setSetting = Setting.SET.isSet(settings);
    _partitionSetting = Setting.PARTITIONED.isSet(settings);
    _sortingSetting = Setting.SORTED.isSet(settings);
    _thetaSketchSetting = Setting.THETA_SKETCH.isSet(settings);
    _nominalEntries = Setting.NOMINAL_ENTRIES.getInteger(settings).orElse(ThetaUtil.DEFAULT_NOMINAL_ENTRIES);

    _thetaSketchAggregationStrategy = new ThetaSketchAggregationStrategy();
    _bitmapAggregationStrategy = new BitmapAggregationStrategy();
    _sortedAggregationStrategy = new SortedAggregationStrategy();

    _setMergeStrategy = new SetMergeStrategy();
    _thetaSketchMergeStrategy = new ThetaSketchMergeStrategy();
    _bitmapMergeStrategy = new BitmapMergeStrategy();
    _partitionedMergeStrategy = new PartitionedMergeStrategy();

    _thetaSketchResultExtractionStrategy = new ThetaSketchResultExtractionStrategy();
    _setResultExtractionStrategy = new SetResultExtractionStrategy();
    _bitmapResultExtractionStrategy = new BitmapResultExtractionStrategy();
    _sortedPartitionedResultExtractionStrategy = (ResultExtractionStrategy<SortedAggregationResult, List<Long>>)
        SortedAggregationResult::extractResult;
    _bitmapPartitionedResultExtractionStrategy = (ResultExtractionStrategy<DictIdsWrapper, List<Long>>)
        dictIdsWrapper -> _bitmapMergeStrategy.extractFinalResult(Arrays.asList(dictIdsWrapper._stepsBitmaps));
    _thetaSketchPartitionedResultExtractionStrategy = (ResultExtractionStrategy<UpdateSketch[], List<Long>>)
        sketches -> _thetaSketchMergeStrategy.extractFinalResult(Arrays.asList(sketches));
  }

  @Override
  public String getResultColumnName() {
    return getType().getName().toLowerCase() + "(" + _expressions.stream().map(ExpressionContext::toString)
        .collect(Collectors.joining(",")) + ")";
  }

  @Override
  public List<ExpressionContext> getInputExpressions() {
    final List<ExpressionContext> inputs = new ArrayList<>();
    inputs.addAll(_correlateByExpressions);
    inputs.addAll(_stepExpressions);
    return inputs;
  }

  @Override
  public AggregationFunctionType getType() {
    return AggregationFunctionType.FUNNELCOUNT;
  }

  @Override
  public AggregationResultHolder createAggregationResultHolder() {
    return new ObjectAggregationResultHolder();
  }

  @Override
  public GroupByResultHolder createGroupByResultHolder(int initialCapacity, int maxCapacity) {
    return new ObjectGroupByResultHolder(initialCapacity, maxCapacity);
  }

  @Override
  public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    getAggregationStrategy(blockValSetMap)
        .aggregate(length, aggregationResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    getAggregationStrategy(blockValSetMap)
        .aggregateGroupBySV(length, groupKeyArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
      Map<ExpressionContext, BlockValSet> blockValSetMap) {
    getAggregationStrategy(blockValSetMap)
        .aggregateGroupByMV(length, groupKeysArray, groupByResultHolder, blockValSetMap);
  }

  @Override
  public Object extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
    return getResultExtractionStrategy(aggregationResultHolder.getResult())
        .extractAggregationResult(aggregationResultHolder);
  }
  @Override
  public Object extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
    return getResultExtractionStrategy(groupByResultHolder.getResult(groupKey))
        .extractGroupByResult(groupByResultHolder, groupKey);
  }

  @Override
  public Object merge(Object a, Object b) {
    if (a == null) {
      return b;
    }
    if (b == null) {
      return a;
    }
    return getMergeStrategy().merge(a, b);
  }

  @Override
  public LongArrayList extractFinalResult(Object intermediateResult) {
    if (intermediateResult == null) {
      return new LongArrayList(_numSteps);
    }
    return getMergeStrategy().extractFinalResult(intermediateResult);
  }

  @Override
  public ColumnDataType getIntermediateResultColumnType() {
    return ColumnDataType.OBJECT;
  }

  @Override
  public ColumnDataType getFinalResultColumnType() {
    return ColumnDataType.LONG_ARRAY;
  }

  @Override
  public String toExplainString() {
    StringBuilder stringBuilder = new StringBuilder(getType().getName()).append('(');
    int numArguments = getInputExpressions().size();
    if (numArguments > 0) {
      stringBuilder.append(getInputExpressions().get(0).toString());
      for (int i = 1; i < numArguments; i++) {
        stringBuilder.append(", ").append(getInputExpressions().get(i).toString());
      }
    }
    return stringBuilder.append(')').toString();
  }

  private Dictionary getDictionary(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final Dictionary primaryCorrelationDictionary = blockValSetMap.get(_primaryCorrelationCol).getDictionary();
    Preconditions.checkArgument(primaryCorrelationDictionary != null,
        "CORRELATE_BY column in FUNNELCOUNT aggregation function not supported, please use a dictionary encoded "
            + "column.");
    return primaryCorrelationDictionary;
  }

  private int[] getCorrelationIds(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    return blockValSetMap.get(_primaryCorrelationCol).getDictionaryIdsSV();
  }

  private int[][] getSteps(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    final int[][] steps = new int[_numSteps][];
    for (int n = 0; n < _numSteps; n++) {
      final BlockValSet stepBlockValSet = blockValSetMap.get(_stepExpressions.get(n));
      steps[n] = stepBlockValSet.getIntValuesSV();
    }
    return steps;
  }

  private boolean isSorted(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    return getDictionary(blockValSetMap).isSorted();
  }

  private AggregationStrategy getAggregationStrategy(Map<ExpressionContext, BlockValSet> blockValSetMap) {
    if (_partitionSetting && _sortingSetting && isSorted(blockValSetMap)) {
      return _sortedAggregationStrategy;
    }
    if (_thetaSketchSetting) {
      return _thetaSketchAggregationStrategy;
    }
    // default
    return _bitmapAggregationStrategy;
  }

  private ResultExtractionStrategy getResultExtractionStrategy(Object aggResult) {
    if (_partitionSetting) {
        if (_sortingSetting && aggResult instanceof SortedAggregationResult) {
          return _sortedPartitionedResultExtractionStrategy;
        }
        if (_thetaSketchSetting) {
          return _thetaSketchPartitionedResultExtractionStrategy;
        }
        return _bitmapPartitionedResultExtractionStrategy;
    }
    if (_thetaSketchSetting) {
      return _thetaSketchResultExtractionStrategy;
    }
    if (_setSetting) {
      return _setResultExtractionStrategy;
    }
    // default
    return _bitmapResultExtractionStrategy;
  }

  private MergeStrategy getMergeStrategy() {
    if (_partitionSetting) {
      return _partitionedMergeStrategy;
    }
    if (_thetaSketchSetting) {
      return _thetaSketchMergeStrategy;
    }
    if (_setSetting) {
      return _setMergeStrategy;
    }
    // default
    return _bitmapMergeStrategy;
  }

  enum Option {
    STEPS("steps"),
    CORRELATE_BY("correlateby"),
    SETTINGS("settings");

    final String _name;

    Option(String name) {
      _name = name;
    }

    boolean matches(ExpressionContext expression) {
      if (expression.getType() != ExpressionContext.Type.FUNCTION) {
        return false;
      }
      return _name.equals(expression.getFunction().getFunctionName());
    }

    Optional<ExpressionContext> find(List<ExpressionContext> expressions) {
      return expressions.stream().filter(this::matches).findFirst();
    }

    public List<ExpressionContext> getInputExpressions(List<ExpressionContext> expressions) {
      return this.find(expressions).map(exp -> exp.getFunction().getArguments())
          .orElseThrow(() -> new IllegalArgumentException("FUNNELCOUNT requires " + _name));
    }

    public ExpressionContext getFirstInputExpression(List<ExpressionContext> expressions) {
      return this.find(expressions)
          .flatMap(exp -> exp.getFunction().getArguments().stream().findFirst())
          .orElseThrow(() -> new IllegalArgumentException("FUNNELCOUNT: " + _name + " requires an argument."));
    }

    public List<String> getLiterals(List<ExpressionContext> expressions) {
      List<ExpressionContext> inputExpressions = find(expressions).map(exp -> exp.getFunction().getArguments())
          .orElseGet(Collections::emptyList);
      Preconditions.checkArgument(
          inputExpressions.stream().allMatch(exp -> exp.getType() == ExpressionContext.Type.LITERAL),
          "FUNNELCOUNT: " + _name + " parameters must be literals");
      return inputExpressions.stream().map(exp -> exp.getLiteral().getStringValue()).collect(Collectors.toList());
    }

    public static void validate(List<ExpressionContext> expressions) {
      final List<String> invalidOptions = expressions.stream()
          .filter(expression -> !Arrays.stream(Option.values()).anyMatch(option -> option.matches(expression)))
          .map(ExpressionContext::toString)
          .collect(Collectors.toList());

      if (!invalidOptions.isEmpty()) {
        throw new IllegalArgumentException("Invalid FUNNELCOUNT options: " + String.join(", ", invalidOptions));
      }
    }
  }

  enum Setting {
    SET("set"),
    BITMAP("bitmap"),
    PARTITIONED("partitioned"),
    SORTED("sorted"),
    THETA_SKETCH("theta_sketch"),
    NOMINAL_ENTRIES("nominalEntries");

    private static final char KEY_VALUE_SEPARATOR = '=';
    final String _name;

    Setting(String name) {
      _name = name.toLowerCase();
    }

    boolean matchesKV(String setting) {
      return StringUtils.deleteWhitespace(setting).toLowerCase().startsWith(_name + KEY_VALUE_SEPARATOR);
    }
    boolean matches(String setting) {
      return StringUtils.deleteWhitespace(setting).toLowerCase().equals(_name);
    }
    public Optional<String> getString(List<String> settings) {
      return settings.stream().filter(this::matchesKV).findFirst()
          .map(setting -> setting.substring(_name.length() + 1));
    }
    public Optional<Integer> getInteger(List<String> settings) {
      return getString(settings).map(Integer::parseInt);
    }
    public boolean isSet(List<String> settings) {
      return settings.stream().anyMatch(this::matches)
          || getString(settings).map(Boolean::parseBoolean).orElse(false);
    }

    public static void validate(List<String> settings) {
      final List<String> invalidSettings = settings.stream()
          .filter(param -> !Arrays.stream(Setting.values())
              .anyMatch(setting -> setting.matchesKV(param) || setting.matches(param))
          ).collect(Collectors.toList());

      if (!invalidSettings.isEmpty()) {
        throw new IllegalArgumentException("Invalid FUNNELCOUNT SETTINGS: " + String.join(", ", invalidSettings));
      }
    }
  }

  /**
   * Interface for within segment aggregation strategy.
   *
   * <p>The implementation should be stateless, and can be shared among multiple segments in multiple threads. The
   * result for each segment should be stored and passed in via the result holder.
   * There should be no assumptions beyond segment boundaries, different aggregation strategies may be utilized
   * across different segments for a given query.
   *
   * @param <A> Aggregation result accumulated across blocks within segment, kept by result holder.
   */
  @ThreadSafe
  abstract class AggregationStrategy<A> {

    /**
     * Returns an aggregation result for this aggregation strategy to be kept in a result holder (aggregation only).
     */
    abstract A createAggregationResult(Dictionary dictionary);

    public A getAggregationResultGroupBy(Dictionary dictionary, GroupByResultHolder groupByResultHolder, int groupKey) {
      A aggResult = groupByResultHolder.getResult(groupKey);
      if (aggResult == null) {
        aggResult = createAggregationResult(dictionary);
        groupByResultHolder.setValueForKey(groupKey, aggResult);
      }
      return aggResult;
    }

    public A getAggregationResult(Dictionary dictionary, AggregationResultHolder aggregationResultHolder) {
      A aggResult = aggregationResultHolder.getResult();
      if (aggResult == null) {
        aggResult = createAggregationResult(dictionary);
        aggregationResultHolder.setValue(aggResult);
      }
      return aggResult;
    }

    /**
     * Performs aggregation on the given block value sets (aggregation only).
     */
    public void aggregate(int length, AggregationResultHolder aggregationResultHolder,
        Map<ExpressionContext, BlockValSet> blockValSetMap) {
      final Dictionary dictionary = getDictionary(blockValSetMap);
      final int[] correlationIds = getCorrelationIds(blockValSetMap);
      final int[][] steps = getSteps(blockValSetMap);

      final A aggResult = getAggregationResult(dictionary, aggregationResultHolder);
      for (int i = 0; i < length; i++) {
        for (int n = 0; n < _numSteps; n++) {
          if (steps[n][i] > 0) {
            add(dictionary, aggResult, n, correlationIds[i]);
          }
        }
      }
    }

    /**
     * Performs aggregation on the given group key array and block value sets (aggregation group-by on single-value
     * columns).
     */
    public void aggregateGroupBySV(int length, int[] groupKeyArray, GroupByResultHolder groupByResultHolder,
        Map<ExpressionContext, BlockValSet> blockValSetMap) {
      final Dictionary dictionary = getDictionary(blockValSetMap);
      final int[] correlationIds = getCorrelationIds(blockValSetMap);
      final int[][] steps = getSteps(blockValSetMap);

      for (int i = 0; i < length; i++) {
        for (int n = 0; n < _numSteps; n++) {
          final int groupKey = groupKeyArray[i];
          final A aggResult = getAggregationResultGroupBy(dictionary, groupByResultHolder, groupKey);
          if (steps[n][i] > 0) {
            add(dictionary, aggResult, n, correlationIds[i]);
          }
        }
      }
    }

    /**
     * Performs aggregation on the given group keys array and block value sets (aggregation group-by on multi-value
     * columns).
     */
    public void aggregateGroupByMV(int length, int[][] groupKeysArray, GroupByResultHolder groupByResultHolder,
        Map<ExpressionContext, BlockValSet> blockValSetMap) {
      final Dictionary dictionary = getDictionary(blockValSetMap);
      final int[] correlationIds = getCorrelationIds(blockValSetMap);
      final int[][] steps = getSteps(blockValSetMap);

      for (int i = 0; i < length; i++) {
        for (int n = 0; n < _numSteps; n++) {
          for (int groupKey : groupKeysArray[i]) {
            final A aggResult = getAggregationResultGroupBy(dictionary, groupByResultHolder, groupKey);
            if (steps[n][i] > 0) {
              add(dictionary, aggResult, n, correlationIds[i]);
            }
          }
        }
      }
    }

    /**
     * Adds a correlation id to the aggregation counter for a given step in the funnel.
     */
    abstract void add(Dictionary dictionary, A aggResult, int step, int correlationId);
  }

  /**
   * Interface for segment aggregation result extraction strategy.
   *
   * <p>The implementation should be stateless, and can be shared among multiple segments in multiple threads.
   *
   * @param <A> Aggregation result accumulated across blocks within segment, kept by result holder.
   * @param <I> Intermediate result at segment level (extracted from aforementioned aggregation result).
   */
  @ThreadSafe
  interface ResultExtractionStrategy<A, I> {

    /**
     * Extracts the intermediate result from the aggregation result holder (aggregation only).
     */
    default I extractAggregationResult(AggregationResultHolder aggregationResultHolder) {
      return extractIntermediateResult(aggregationResultHolder.getResult());
    }

    /**
     * Extracts the intermediate result from the group-by result holder for the given group key (aggregation group-by).
     */
    default I extractGroupByResult(GroupByResultHolder groupByResultHolder, int groupKey) {
      return extractIntermediateResult(groupByResultHolder.getResult(groupKey));
    }

    I extractIntermediateResult(A aggregationResult);
  }

  /**
   * Interface for cross-segment merge strategy.
   *
   * <p>The implementation should be stateless, and can be shared among multiple segments in multiple threads.
   *
   * @param <I> Intermediate result at segment level (extracted from aggregation strategy result).
   */
  @ThreadSafe
  interface MergeStrategy<I> {
    I merge(I intermediateResult1, I intermediateResult2);
    LongArrayList extractFinalResult(I intermediateResult);
  }

  /**
   * Aggregation strategy leveraging theta sketch algebra (unions/intersections).
   */
  class ThetaSketchAggregationStrategy extends AggregationStrategy<UpdateSketch[]> {
    final UpdateSketchBuilder _updateSketchBuilder = new UpdateSketchBuilder();

    @Override
    public UpdateSketch[] createAggregationResult(Dictionary dictionary) {
      final UpdateSketch[] stepsSketches = new UpdateSketch[_numSteps];
      for (int n = 0; n < _numSteps; n++) {
        stepsSketches[n] = _updateSketchBuilder.build();
      }
      return stepsSketches;
    }

    @Override
    void add(Dictionary dictionary, UpdateSketch[] stepsSketches, int step, int correlationId) {
      final UpdateSketch sketch = stepsSketches[step];
      switch (dictionary.getValueType()) {
        case INT:
          sketch.update(dictionary.getIntValue(correlationId));
          break;
        case LONG:
          sketch.update(dictionary.getLongValue(correlationId));
          break;
        case FLOAT:
          sketch.update(dictionary.getFloatValue(correlationId));
          break;
        case DOUBLE:
          sketch.update(dictionary.getDoubleValue(correlationId));
          break;
        case STRING:
          sketch.update(dictionary.getStringValue(correlationId));
          break;
        default:
          throw new IllegalStateException(
              "Illegal CORRELATED_BY column data type for FUNNEL_COUNT aggregation function: "
                  + dictionary.getValueType());
      }
    }
  }

  class ThetaSketchResultExtractionStrategy implements ResultExtractionStrategy<UpdateSketch[], List<Sketch>> {
    @Override
    public List<Sketch> extractIntermediateResult(UpdateSketch[] stepsSketches) {
      if (stepsSketches == null) {
        return Collections.nCopies(_numSteps, EMPTY_SKETCH);
      }
      return Arrays.asList(stepsSketches);
    }
  }

  class ThetaSketchMergeStrategy implements MergeStrategy<List<Sketch>> {
    final SetOperationBuilder _setOperationBuilder = new SetOperationBuilder();
    @Override
    public List<Sketch> merge(List<Sketch> sketches1, List<Sketch> sketches2) {
      final List<Sketch> mergedSketches = new ArrayList<>(_numSteps);
      for (int i = 0; i < _numSteps; i++) {
        // NOTE: Compact the sketch in unsorted, on-heap fashion for performance concern.
        //       See https://datasketches.apache.org/docs/Theta/ThetaSize.html for more details.
        mergedSketches.add(_setOperationBuilder.buildUnion().union(sketches1.get(i), sketches2.get(i), false, null));
      }
      return mergedSketches;
    }

    @Override
    public LongArrayList extractFinalResult(List<Sketch> sketches) {
      long[] result = new long[_numSteps];

      Sketch sketch = sketches.get(0);
      result[0] = Math.round(sketch.getEstimate());
      for (int i = 1; i < _numSteps; i++) {
        Intersection intersection = _setOperationBuilder.buildIntersection();
        sketch = intersection.intersect(sketch, sketches.get(i));
        result[i] = Math.round(sketch.getEstimate());
      }
      return LongArrayList.wrap(result);
    }
  }

  /**
   * Aggregation strategy leveraging roaring bitmap algebra (unions/intersections).
   */
  class BitmapAggregationStrategy extends AggregationStrategy<DictIdsWrapper> {
    @Override
    public DictIdsWrapper createAggregationResult(Dictionary dictionary) {
      return new DictIdsWrapper(_numSteps, dictionary);
    }

    @Override
    protected void add(Dictionary dictionary, DictIdsWrapper dictIdsWrapper, int step, int correlationId) {
      dictIdsWrapper._stepsBitmaps[step].add(correlationId);
    }
  }

  private static final class DictIdsWrapper {
    final Dictionary _dictionary;
    final RoaringBitmap[] _stepsBitmaps;

    private DictIdsWrapper(int numSteps, Dictionary dictionary) {
      _dictionary = dictionary;
      _stepsBitmaps = new RoaringBitmap[numSteps];
      for (int n = 0; n < numSteps; n++) {
        _stepsBitmaps[n] = new RoaringBitmap();
      }
    }
  }

  class BitmapResultExtractionStrategy implements ResultExtractionStrategy<DictIdsWrapper, List<RoaringBitmap>> {
    @Override
    public List<RoaringBitmap> extractIntermediateResult(DictIdsWrapper dictIdsWrapper) {
      Dictionary dictionary = dictIdsWrapper._dictionary;
      List<RoaringBitmap> result = new ArrayList<>(_numSteps);
      for (RoaringBitmap dictIdBitmap : dictIdsWrapper._stepsBitmaps) {
        result.add(convertToValueBitmap(dictionary, dictIdBitmap));
      }
      return result;
    }

    /**
     * Helper method to read dictionary and convert dictionary ids to hash code of the values for dictionary-encoded
     * expression.
     */
    private RoaringBitmap convertToValueBitmap(Dictionary dictionary, RoaringBitmap dictIdBitmap) {
      RoaringBitmap valueBitmap = new RoaringBitmap();
      PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
      FieldSpec.DataType storedType = dictionary.getValueType();
      switch (storedType) {
        case INT:
          while (iterator.hasNext()) {
            valueBitmap.add(dictionary.getIntValue(iterator.next()));
          }
          break;
        case LONG:
          while (iterator.hasNext()) {
            valueBitmap.add(Long.hashCode(dictionary.getLongValue(iterator.next())));
          }
          break;
        case FLOAT:
          while (iterator.hasNext()) {
            valueBitmap.add(Float.hashCode(dictionary.getFloatValue(iterator.next())));
          }
          break;
        case DOUBLE:
          while (iterator.hasNext()) {
            valueBitmap.add(Double.hashCode(dictionary.getDoubleValue(iterator.next())));
          }
          break;
        case STRING:
          while (iterator.hasNext()) {
            valueBitmap.add(dictionary.getStringValue(iterator.next()).hashCode());
          }
          break;
        default:
          throw new IllegalArgumentException("Illegal data type for FUNNEL_COUNT aggregation function: " + storedType);
      }
      return valueBitmap;
    }
  }

  /**
   * Aggregation strategy leveraging set algebra (unions/intersections).
   */
  class SetResultExtractionStrategy implements ResultExtractionStrategy<DictIdsWrapper, List<Set>> {
    @Override
    public List<Set> extractIntermediateResult(DictIdsWrapper dictIdsWrapper) {
      Dictionary dictionary = dictIdsWrapper._dictionary;
      List<Set> result = new ArrayList<>(_numSteps);
      for (RoaringBitmap dictIdBitmap : dictIdsWrapper._stepsBitmaps) {
        result.add(convertToValueSet(dictionary, dictIdBitmap));
      }
      return result;
    }

    private Set convertToValueSet(Dictionary dictionary, RoaringBitmap dictIdBitmap) {
      int numValues = dictIdBitmap.getCardinality();
      PeekableIntIterator iterator = dictIdBitmap.getIntIterator();
      FieldSpec.DataType storedType = dictionary.getValueType();
      switch (storedType) {
        case INT:
          IntOpenHashSet intSet = new IntOpenHashSet(numValues);
          while (iterator.hasNext()) {
            intSet.add(dictionary.getIntValue(iterator.next()));
          }
          return intSet;
        case LONG:
          LongOpenHashSet longSet = new LongOpenHashSet(numValues);
          while (iterator.hasNext()) {
            longSet.add(dictionary.getLongValue(iterator.next()));
          }
          return longSet;
        case FLOAT:
          FloatOpenHashSet floatSet = new FloatOpenHashSet(numValues);
          while (iterator.hasNext()) {
            floatSet.add(dictionary.getFloatValue(iterator.next()));
          }
          return floatSet;
        case DOUBLE:
          DoubleOpenHashSet doubleSet = new DoubleOpenHashSet(numValues);
          while (iterator.hasNext()) {
            doubleSet.add(dictionary.getDoubleValue(iterator.next()));
          }
          return doubleSet;
        case STRING:
          ObjectOpenHashSet<String> stringSet = new ObjectOpenHashSet<>(numValues);
          while (iterator.hasNext()) {
            stringSet.add(dictionary.getStringValue(iterator.next()));
          }
          return stringSet;
        default:
          throw new IllegalArgumentException("Illegal data type for FUNNEL_COUNT aggregation function: " + storedType);
      }
    }
  }

  class SetMergeStrategy implements MergeStrategy<List<Set>> {
    @Override
    public List<Set> merge(List<Set> intermediateResult1, List<Set> intermediateResult2) {
      for (int i = 0; i < _numSteps; i++) {
        intermediateResult1.get(i).addAll(intermediateResult2.get(i));
      }
      return intermediateResult1;
    }

    @Override
    public LongArrayList extractFinalResult(List<Set> stepsSets) {
      long[] result = new long[_numSteps];
      result[0] = stepsSets.get(0).size();
      for (int i = 1; i < _numSteps; i++) {
        // intersect this step with previous step
        stepsSets.get(i).retainAll(stepsSets.get(i - 1));
        result[i] = stepsSets.get(i).size();
      }
      return LongArrayList.wrap(result);
    }
  }

  class BitmapMergeStrategy implements MergeStrategy<List<RoaringBitmap>> {
    @Override
    public List<RoaringBitmap> merge(List<RoaringBitmap> intermediateResult1, List<RoaringBitmap> intermediateResult2) {
      for (int i = 0; i < _numSteps; i++) {
        intermediateResult1.get(i).or(intermediateResult2.get(i));
      }
      return intermediateResult1;
    }

    @Override
    public LongArrayList extractFinalResult(List<RoaringBitmap> stepsBitmaps) {
      long[] result = new long[_numSteps];
      result[0] = stepsBitmaps.get(0).getCardinality();
      for (int i = 1; i < _numSteps; i++) {
        // intersect this step with previous step
        stepsBitmaps.get(i).and(stepsBitmaps.get(i - 1));
        result[i] = stepsBitmaps.get(i).getCardinality();
      }
      return LongArrayList.wrap(result);
    }
  }

  class PartitionedMergeStrategy implements MergeStrategy<List<Long>> {
    @Override
    public List<Long> merge(List<Long> a, List<Long> b) {
      int length = a.size();
      Preconditions.checkState(length == b.size(), "The two operand arrays are not of the same size! provided %s, %s",
          length, b.size());

      LongArrayList result = toLongArrayList(a);
      long[] elements = result.elements();
      for (int i = 0; i < length; i++) {
        elements[i] += b.get(i);
      }
      return result;
    }

    @Override
    public LongArrayList extractFinalResult(List<Long> intermediateResult) {
      return toLongArrayList(intermediateResult);
    }

    private LongArrayList toLongArrayList(List<Long> longList) {
      return longList instanceof LongArrayList ? ((LongArrayList) longList).clone() : new LongArrayList(longList);
    }
  }

  /**
   * Aggregation strategy for segments partitioned and sorted by the main correlation column.
   */
  class SortedAggregationStrategy extends AggregationStrategy<SortedAggregationResult> {

    @Override
    public SortedAggregationResult createAggregationResult(Dictionary dictionary) {
      return new SortedAggregationResult(_numSteps);
    }

    @Override
    void add(Dictionary dictionary, SortedAggregationResult aggResult, int step, int correlationId) {
      aggResult.add(step, correlationId);
    }
  }

  /**
   * Aggregation result data structure leveraged by sorted aggregation strategy.
   */
  static class SortedAggregationResult {
    final int _numSteps;
    final long[] _stepCounters;
    final boolean[] _correlatedSteps;
    int _lastCorrelationId = Integer.MIN_VALUE;

    SortedAggregationResult(int numSteps) {
      _numSteps = numSteps;
      _stepCounters = new long[_numSteps];
      _correlatedSteps = new boolean[_numSteps];
    }

    public void add(int step, int correlationId) {
      if (correlationId != _lastCorrelationId) {
        // End of correlation group, calculate funnel conversion counts
        incrStepCounters();

        // initialize next correlation group
        for (int n = 0; n < _numSteps; n++) {
          _correlatedSteps[n] = false;
        }
        _lastCorrelationId = correlationId;
      }
      _correlatedSteps[step] = true;
    }

    void incrStepCounters() {
      for (int n = 0; n < _numSteps; n++) {
        if (!_correlatedSteps[n]) {
          break;
        }
        _stepCounters[n]++;
      }
    }

    public LongArrayList extractResult() {
      // count last correlation id left open
      incrStepCounters();
      return LongArrayList.wrap(_stepCounters);
    }
  }
}
