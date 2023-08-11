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
package org.apache.pinot.core.query.aggregation.function.funnel;

import com.google.common.base.Preconditions;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.UpdateSketch;
import org.apache.datasketches.thetacommon.ThetaUtil;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.core.query.aggregation.function.AggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctCountAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctCountBitmapAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.DistinctCountThetaSketchAggregationFunction;
import org.apache.pinot.core.query.aggregation.function.SegmentPartitionedDistinctCountAggregationFunction;
import org.roaringbitmap.RoaringBitmap;


/**
 * The {@code FunnelCountAggregationFunctionFactory} builds a {@code FunnelCountAggregationFunction}.
 * Primary role is to validate inputs and select the appropriate aggregation strategy to use based on settings.
 *
 * There are 5 strategies available, mirroring the corresponding distinct count implementations as per below.
 *  <p><ul>
 *  <li>'set': See DISTINCTCOUNT at {@link DistinctCountAggregationFunction}
 *  <li>'bitmap' (default): See DISTINCTCOUNTBITMAP at {@link DistinctCountBitmapAggregationFunction}
 *  <li>'theta_sketch': See DISTINCTCOUNTTHETASKETCH at {@link DistinctCountThetaSketchAggregationFunction}
 *  <li>'partitioned': See SEGMENTPARTITIONEDDISTINCTCOUNT {@link SegmentPartitionedDistinctCountAggregationFunction}
 *  <li>'sorted': sorted counts per segment then sums up. Only availabe in combination with 'partitioned'.
 *  <li>'nominalEntries=4096': theta sketch configuration, default is 4096.
 *  </ul><p>
 */
public class FunnelCountAggregationFunctionFactory implements Supplier<AggregationFunction> {
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

  final AggregationStrategy<UpdateSketch[]> _thetaSketchAggregationStrategy;
  final AggregationStrategy<DictIdsWrapper> _bitmapAggregationStrategy;
  final AggregationStrategy<SortedAggregationResult> _sortedAggregationStrategy;

  final MergeStrategy<List<Sketch>> _thetaSketchMergeStrategy;
  final MergeStrategy<List<Set>> _setMergeStrategy;
  final MergeStrategy<List<RoaringBitmap>> _bitmapMergeStrategy;
  final MergeStrategy<List<Long>> _partitionedMergeStrategy;

  final ResultExtractionStrategy<UpdateSketch[], List<Sketch>> _thetaSketchResultExtractionStrategy;
  final ResultExtractionStrategy<DictIdsWrapper, List<Set>> _setResultExtractionStrategy;
  final ResultExtractionStrategy<DictIdsWrapper, List<RoaringBitmap>> _bitmapResultExtractionStrategy;
  final ResultExtractionStrategy<SortedAggregationResult, List<Long>> _sortedPartitionedResultExtractionStrategy;
  final ResultExtractionStrategy<DictIdsWrapper, List<Long>> _bitmapPartitionedResultExtractionStrategy;
  final ResultExtractionStrategy<UpdateSketch[], List<Long>> _thetaSketchPartitionedResultExtractionStrategy;

  public FunnelCountAggregationFunctionFactory(List<ExpressionContext> expressions) {
    _expressions = expressions;
    Option.validate(expressions);
    _correlateByExpressions = Option.CORRELATE_BY.getInputExpressions(expressions);
    _primaryCorrelationCol = _correlateByExpressions.get(0);
    _stepExpressions = Option.STEPS.getInputExpressions(expressions);
    _numSteps = _stepExpressions.size();

    final List<String> settings = Option.SETTINGS.getLiterals(expressions);
    Setting.validate(settings);
    _setSetting = Setting.SET.isSet(settings);
    _partitionSetting = Setting.PARTITIONED.isSet(settings);
    _sortingSetting = Setting.SORTED.isSet(settings);
    _thetaSketchSetting = Setting.THETA_SKETCH.isSet(settings);
    _nominalEntries = Setting.NOMINAL_ENTRIES.getInteger(settings).orElse(ThetaUtil.DEFAULT_NOMINAL_ENTRIES);

    _thetaSketchAggregationStrategy = new ThetaSketchAggregationStrategy(_stepExpressions, _correlateByExpressions,
        _nominalEntries);
    _bitmapAggregationStrategy = new BitmapAggregationStrategy(_stepExpressions, _correlateByExpressions);
    _sortedAggregationStrategy = new SortedAggregationStrategy(_stepExpressions, _correlateByExpressions);

    _setMergeStrategy = new SetMergeStrategy(_numSteps);
    _thetaSketchMergeStrategy = new ThetaSketchMergeStrategy(_numSteps, _nominalEntries);
    _bitmapMergeStrategy = new BitmapMergeStrategy(_numSteps);
    _partitionedMergeStrategy = new PartitionedMergeStrategy(_numSteps);

    _thetaSketchResultExtractionStrategy = new ThetaSketchResultExtractionStrategy(_numSteps);
    _setResultExtractionStrategy = new SetResultExtractionStrategy(_numSteps);
    _bitmapResultExtractionStrategy = new BitmapResultExtractionStrategy(_numSteps);
    _sortedPartitionedResultExtractionStrategy = SortedAggregationResult::extractResult;
    _bitmapPartitionedResultExtractionStrategy =
        dictIdsWrapper -> _bitmapMergeStrategy.extractFinalResult(Arrays.asList(dictIdsWrapper._stepsBitmaps));
    _thetaSketchPartitionedResultExtractionStrategy =
        sketches -> _thetaSketchMergeStrategy.extractFinalResult(Arrays.asList(sketches));
  }

  public AggregationFunction get() {
    if (_partitionSetting) {
      if (_thetaSketchSetting) {
        if (_sortingSetting) {
          // theta_sketch && partitioned && sorted
          return new FunnelCountSortedAggregationFunction<>(_expressions, _stepExpressions, _correlateByExpressions,
              _thetaSketchAggregationStrategy, _thetaSketchPartitionedResultExtractionStrategy,
              _partitionedMergeStrategy, _sortedAggregationStrategy, _sortedPartitionedResultExtractionStrategy);
        } else {
          // theta_sketch && partitioned && !sorted
          return new FunnelCountAggregationFunction<>(_expressions, _stepExpressions, _correlateByExpressions,
              _thetaSketchAggregationStrategy, _thetaSketchPartitionedResultExtractionStrategy,
              _partitionedMergeStrategy);
        }
      } else {
        if (_sortingSetting) {
          // partitioned && sorted && !tetha_sketch
          return new FunnelCountSortedAggregationFunction<>(_expressions, _stepExpressions, _correlateByExpressions,
              _bitmapAggregationStrategy, _bitmapPartitionedResultExtractionStrategy, _partitionedMergeStrategy,
              _sortedAggregationStrategy, _sortedPartitionedResultExtractionStrategy);
        } else {
          // partitioned && !sorted && !tetha_sketch
          return new FunnelCountAggregationFunction<>(_expressions, _stepExpressions, _correlateByExpressions,
              _bitmapAggregationStrategy, _bitmapPartitionedResultExtractionStrategy, _partitionedMergeStrategy);
        }
      }
    } else {
      if (_thetaSketchSetting) {
        // theta_sketch && !partitioned && !sorted
        return new FunnelCountAggregationFunction<>(_expressions, _stepExpressions, _correlateByExpressions,
            _thetaSketchAggregationStrategy, _thetaSketchResultExtractionStrategy, _thetaSketchMergeStrategy);
      } else if (_setSetting) {
        // set && !partitioned && !sorted && !theta_sketch
        return new FunnelCountAggregationFunction<>(_expressions, _stepExpressions, _correlateByExpressions,
            _bitmapAggregationStrategy, _setResultExtractionStrategy, _setMergeStrategy);
      } else {
        // default (bitmap)
        // !partitioned && !sorted && !theta_sketch && !set
        return new FunnelCountAggregationFunction<>(_expressions, _stepExpressions, _correlateByExpressions,
            _bitmapAggregationStrategy, _bitmapResultExtractionStrategy, _bitmapMergeStrategy);
      }
    }
  }

  enum Option {
    STEPS("steps"), CORRELATE_BY("correlateby"), SETTINGS("settings");

    final String _name;

    Option(String name) {
      _name = name;
    }

    public static void validate(List<ExpressionContext> expressions) {
      final List<String> invalidOptions = expressions.stream()
          .filter(expression -> !Arrays.stream(Option.values()).anyMatch(option -> option.matches(expression)))
          .map(ExpressionContext::toString).collect(Collectors.toList());

      if (!invalidOptions.isEmpty()) {
        throw new IllegalArgumentException("Invalid FUNNELCOUNT options: " + String.join(", ", invalidOptions));
      }
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
      final List<ExpressionContext> inputExpressions =
          this.find(expressions).map(exp -> exp.getFunction().getArguments())
              .orElseThrow(() -> new IllegalArgumentException("FUNNELCOUNT requires " + _name));
      Preconditions.checkArgument(!inputExpressions.isEmpty(), "FUNNELCOUNT: " + _name + " requires an argument.");
      return inputExpressions;
    }

    public List<String> getLiterals(List<ExpressionContext> expressions) {
      List<ExpressionContext> inputExpressions =
          find(expressions).map(exp -> exp.getFunction().getArguments()).orElseGet(Collections::emptyList);
      Preconditions.checkArgument(
          inputExpressions.stream().allMatch(exp -> exp.getType() == ExpressionContext.Type.LITERAL),
          "FUNNELCOUNT: " + _name + " parameters must be literals");
      return inputExpressions.stream().map(exp -> exp.getLiteral().getStringValue()).collect(Collectors.toList());
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

    public static void validate(List<String> settings) {
      final List<String> invalidSettings = settings.stream().filter(param -> !Arrays.stream(Setting.values())
          .anyMatch(setting -> setting.matchesKV(param) || setting.matches(param))).collect(Collectors.toList());

      if (!invalidSettings.isEmpty()) {
        throw new IllegalArgumentException("Invalid FUNNELCOUNT SETTINGS: " + String.join(", ", invalidSettings));
      }
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
      return settings.stream().anyMatch(this::matches) || getString(settings).map(Boolean::parseBoolean).orElse(false);
    }
  }
}
