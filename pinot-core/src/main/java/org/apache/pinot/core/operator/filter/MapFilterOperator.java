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
package org.apache.pinot.core.operator.filter;

import com.google.common.base.CaseFormat;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.common.request.context.predicate.EqPredicate;
import org.apache.pinot.common.request.context.predicate.InPredicate;
import org.apache.pinot.common.request.context.predicate.NotEqPredicate;
import org.apache.pinot.common.request.context.predicate.NotInPredicate;
import org.apache.pinot.common.request.context.predicate.Predicate;
import org.apache.pinot.common.request.context.predicate.RangePredicate;
import org.apache.pinot.core.common.BlockDocIdSet;
import org.apache.pinot.core.common.Operator;
import org.apache.pinot.core.operator.ExplainAttributeBuilder;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.segment.local.segment.index.openstruct.OpenStructNullDataSource;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.datasource.DataSource;
import org.apache.pinot.segment.spi.datasource.OpenStructDataSource;
import org.apache.pinot.segment.spi.index.IndexService;
import org.apache.pinot.segment.spi.index.IndexType;
import org.apache.pinot.segment.spi.index.reader.ForwardIndexReader;
import org.apache.pinot.segment.spi.index.reader.JsonIndexReader;
import org.apache.pinot.segment.spi.index.reader.NullValueVectorReader;
import org.apache.pinot.spi.data.FieldSpec;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;


/// Filter operator for Map/OPEN_STRUCT matching. Dispatches in priority order:
/// 1. Per-key materialized index (OPEN_STRUCT columns with per-key inverted/range/bloom indexes)
/// 2. Sparse virtual scan / manifest short-circuit (unmaterialized keys with a sparse blob tier)
/// 3. JSON index (JsonMatchFilterOperator)
/// 4. Expression scan fallback (ExpressionFilterOperator)
public class MapFilterOperator extends BaseFilterOperator {
  private static final String EXPLAIN_NAME = "FILTER_MAP";

  private enum DelegateType {
    PER_KEY_INDEX, JSON_MATCH, EXPRESSION_FILTER
  }

  private final BaseFilterOperator _delegate;
  private DelegateType _delegateType = DelegateType.PER_KEY_INDEX;
  private final String _columnName;
  private final String _keyName;
  private final Predicate _predicate;

  public MapFilterOperator(IndexSegment indexSegment, Predicate predicate, QueryContext queryContext,
      int numDocs) {
    super(numDocs, queryContext.isNullHandlingEnabled());
    _predicate = predicate;

    List<ExpressionContext> arguments = predicate.getLhs().getFunction().getArguments();
    if (arguments.size() != 2) {
      throw new IllegalStateException("Expected two arguments (column name and key name), found: " + arguments.size());
    }

    _columnName = arguments.get(0).getIdentifier();
    _keyName = arguments.get(1).getLiteral().getStringValue();

    // Try dispatch paths in priority order
    DataSource columnDs = indexSegment.getDataSourceNullable(_columnName);

    BaseFilterOperator perKey = tryPerKeyIndex(columnDs, queryContext, numDocs);
    if (perKey != null) {
      _delegate = perKey;
      return;
    }

    JsonMatchFilterOperator jsonOp = tryJsonIndex(columnDs, numDocs);
    if (jsonOp != null) {
      _delegate = jsonOp;
      _delegateType = DelegateType.JSON_MATCH;
      return;
    }

    _delegate = new ExpressionFilterOperator(indexSegment, queryContext, predicate, numDocs);
    _delegateType = DelegateType.EXPRESSION_FILTER;
  }

  @Nullable
  private BaseFilterOperator tryPerKeyIndex(@Nullable DataSource columnDs, QueryContext queryContext, int numDocs) {
    if (!(columnDs instanceof OpenStructDataSource)) {
      return null;
    }
    OpenStructDataSource osDs = (OpenStructDataSource) columnDs;

    if (osDs.isMaterialized(_keyName)) {
      DataSource keyDs = osDs.getDataSource(_keyName);
      return buildPerKeyFilterOperator(keyDs, queryContext, numDocs);
    }

    // Key not materialized but the segment is fully materialized — definitive absence.
    if (osDs.isFullyMaterialized()) {
      return buildAbsentKeyFilterOperator(osDs, queryContext, numDocs);
    }

    // Sparse tier: virtual source from blob, or null if manifest proves absence.
    DataSource sparseKeyDs = osDs.getDataSource(_keyName);
    if (sparseKeyDs == null) {
      return buildAbsentKeyFilterOperator(osDs, queryContext, numDocs);
    }
    BaseFilterOperator jsonFastPath = trySparseJsonIndex(osDs, sparseKeyDs, queryContext, numDocs);
    if (jsonFastPath != null) {
      return jsonFastPath;
    }
    return buildPerKeyFilterOperator(sparseKeyDs, queryContext, numDocs);
  }

  /// Builds the filter for a key that is definitively absent from a fully materialized segment.
  /// Every document answers the predicate identically, so it is folded to a match-all / match-none
  /// once instead of scanning an all-null column.
  ///
  /// With null handling on, three-valued logic makes every value predicate UNKNOWN and nothing
  /// matches. With it off the key reads as its type's default null value, so the predicate is
  /// evaluated against that single value — notably NOT_EQ / NOT_IN then match every document.
  ///
  /// The value comes from {@link OpenStructNullDataSource#forAbsentKey}, the same source the
  /// projection path feeds to `item()`, so filter and projection cannot disagree. For a key with no
  /// declared child spec that factory falls back to STRING, which means a numeric range over an
  /// undeclared key compares lexicographically — the same answer `item()` yields for it.
  @Nullable
  private BaseFilterOperator buildAbsentKeyFilterOperator(OpenStructDataSource osDs, QueryContext queryContext,
      int numDocs) {
    Predicate.Type type = _predicate.getType();
    if (type == Predicate.Type.IS_NULL) {
      return new MatchAllFilterOperator(numDocs);
    }
    if (type == Predicate.Type.IS_NOT_NULL) {
      return EmptyFilterOperator.getInstance();
    }
    Predicate rewritten = rewritePredicateForKey(_predicate);
    if (rewritten == null) {
      return null;
    }
    if (queryContext.isNullHandlingEnabled()) {
      return EmptyFilterOperator.getInstance();
    }
    DataSource keyDs = OpenStructNullDataSource.forAbsentKey(osDs, _keyName);
    PredicateEvaluator evaluator = PredicateEvaluatorProvider.getPredicateEvaluator(rewritten, keyDs, queryContext);
    Boolean matches = matchesDefaultNullValue(evaluator, keyDs.getForwardIndex());
    if (matches == null) {
      return null;
    }
    return matches ? new MatchAllFilterOperator(numDocs) : EmptyFilterOperator.getInstance();
  }

  /// Applies the evaluator to the default null value every document of an absent key reads as.
  /// Returns `null` for a stored type the all-null forward index cannot serve, so the caller falls
  /// through to the expression path rather than failing the query.
  @Nullable
  private static Boolean matchesDefaultNullValue(PredicateEvaluator evaluator, ForwardIndexReader<?> forwardIndex) {
    switch (forwardIndex.getStoredType()) {
      case INT:
        return evaluator.applySV(forwardIndex.getInt(0, null));
      case LONG:
        return evaluator.applySV(forwardIndex.getLong(0, null));
      case FLOAT:
        return evaluator.applySV(forwardIndex.getFloat(0, null));
      case DOUBLE:
        return evaluator.applySV(forwardIndex.getDouble(0, null));
      case BIG_DECIMAL:
        return evaluator.applySV(forwardIndex.getBigDecimal(0, null));
      case STRING:
        return evaluator.applySV(forwardIndex.getString(0, null));
      case BYTES:
        return evaluator.applySV(forwardIndex.getBytes(0, null));
      default:
        return null;
    }
  }

  @Nullable
  private BaseFilterOperator buildPerKeyFilterOperator(DataSource keyDs, QueryContext queryContext, int numDocs) {
    switch (_predicate.getType()) {
      case IS_NULL:
      case IS_NOT_NULL: {
        NullValueVectorReader nullReader = keyDs.getNullValueVector();
        if (nullReader == null) {
          // No null vector — all values are non-null
          return _predicate.getType() == Predicate.Type.IS_NULL
              ? EmptyFilterOperator.getInstance()
              : new MatchAllFilterOperator(numDocs);
        }
        ImmutableRoaringBitmap nullBitmap = nullReader.getNullBitmap();
        boolean exclusive = _predicate.getType() == Predicate.Type.IS_NOT_NULL;
        return new BitmapBasedFilterOperator(nullBitmap, exclusive, numDocs);
      }
      case EQ:
      case NOT_EQ:
      case IN:
      case NOT_IN:
      case RANGE: {
        Predicate rewritten = rewritePredicateForKey(_predicate);
        if (rewritten == null) {
          return null;
        }
        PredicateEvaluator evaluator = PredicateEvaluatorProvider.getPredicateEvaluator(rewritten, keyDs, queryContext);
        return FilterOperatorUtils.getLeafFilterOperator(queryContext, evaluator, keyDs, numDocs);
      }
      default:
        return null;
    }
  }

  @Nullable
  private BaseFilterOperator trySparseJsonIndex(OpenStructDataSource osDs, DataSource sparseKeyDs,
      QueryContext queryContext, int numDocs) {
    JsonIndexReader jsonIndex = osDs.getSparseJsonIndex();
    if (jsonIndex == null) {
      return null;
    }
    if (sparseKeyDs.getDataSourceMetadata().getDataType().getStoredType() != FieldSpec.DataType.STRING) {
      return null;
    }
    List<String> values;
    boolean negated;
    switch (_predicate.getType()) {
      case EQ:
        values = List.of(((EqPredicate) _predicate).getValue());
        negated = false;
        break;
      case IN:
        values = ((InPredicate) _predicate).getValues();
        negated = false;
        break;
      case NOT_EQ:
        values = List.of(((NotEqPredicate) _predicate).getValue());
        negated = true;
        break;
      case NOT_IN:
        values = ((NotInPredicate) _predicate).getValues();
        negated = true;
        break;
      default:
        return null;
    }
    if (values.contains(FieldSpec.DEFAULT_DIMENSION_NULL_VALUE_OF_STRING)) {
      return null;
    }
    if (negated && queryContext.isNullHandlingEnabled()) {
      return null;
    }
    ExpressionContext keyLhs = ExpressionContext.forIdentifier(_keyName);
    Predicate positive = values.size() == 1
        ? new EqPredicate(keyLhs, values.get(0))
        : new InPredicate(keyLhs, values);
    JsonMatchFilterOperator jsonOp =
        new JsonMatchFilterOperator(jsonIndex, FilterContext.forPredicate(positive), numDocs);
    _delegateType = DelegateType.JSON_MATCH;
    return negated ? new NotFilterOperator(jsonOp, numDocs, false) : jsonOp;
  }

  @Nullable
  private Predicate rewritePredicateForKey(Predicate predicate) {
    ExpressionContext keyLhs = ExpressionContext.forIdentifier(_keyName);
    switch (predicate.getType()) {
      case EQ:
        return new EqPredicate(keyLhs, ((EqPredicate) predicate).getValue());
      case NOT_EQ:
        return new NotEqPredicate(keyLhs, ((NotEqPredicate) predicate).getValue());
      case IN:
        return new InPredicate(keyLhs, ((InPredicate) predicate).getValues());
      case NOT_IN:
        return new NotInPredicate(keyLhs, ((NotInPredicate) predicate).getValues());
      case RANGE: {
        RangePredicate rp = (RangePredicate) predicate;
        return new RangePredicate(keyLhs, rp.isLowerInclusive(), rp.getLowerBound(),
            rp.isUpperInclusive(), rp.getUpperBound(), rp.getRangeDataType());
      }
      default:
        return null;
    }
  }

  @Nullable
  private JsonMatchFilterOperator tryJsonIndex(@Nullable DataSource dataSource, int numDocs) {
    if (!canUseJsonIndex(_predicate.getType())) {
      return null;
    }
    if (dataSource == null) {
      return null;
    }
    JsonIndexReader jsonIndex = dataSource.getJsonIndex();
    if (jsonIndex == null) {
      Optional<IndexType<?, ?, ?>> compositeIndex =
          IndexService.getInstance().getOptional("composite_json_index");
      if (compositeIndex.isPresent()) {
        jsonIndex = (JsonIndexReader) dataSource.getIndex(compositeIndex.get());
      }
    }
    if (jsonIndex == null) {
      return null;
    }
    FilterContext filterContext = createFilterContext();
    return new JsonMatchFilterOperator(jsonIndex, filterContext, numDocs);
  }

  private FilterContext createFilterContext() {
    Predicate rewritten = rewritePredicateForKey(_predicate);
    if (rewritten == null) {
      throw new IllegalStateException("Unsupported predicate type for JSON filter context: " + _predicate.getType());
    }
    return FilterContext.forPredicate(rewritten);
  }

  @Override
  protected BlockDocIdSet getTrues() {
    return _delegate.getTrues();
  }

  /// Forwarded so three-valued logic survives the delegation. `NotFilterOperator.getTrues()` reads
  /// `getFalses()`, and `And`/`OrFilterOperator.getFalses()` read `getNulls()`; leaving these on the
  /// base implementation would report an absent key's UNKNOWN docs as FALSE, so
  /// `NOT (col['key'] = v)` would match every doc missing the key.
  @Override
  protected BlockDocIdSet getNulls() {
    return _delegate.getNulls();
  }

  @Override
  protected BlockDocIdSet getFalses() {
    return _delegate.getFalses();
  }

  @Override
  public boolean canOptimizeCount() {
    return _delegate.canOptimizeCount();
  }

  @Override
  public int getNumMatchingDocs() {
    return _delegate.getNumMatchingDocs();
  }

  @Override
  public boolean canProduceBitmaps() {
    return _delegate.canProduceBitmaps();
  }

  @Override
  public BitmapCollection getBitmaps() {
    return _delegate.getBitmaps();
  }

  @Override
  public List<Operator> getChildOperators() {
    return List.of();
  }

  @Override
  public String toExplainString() {
    return new StringBuilder(EXPLAIN_NAME)
        .append("(column:").append(_columnName)
        .append(",key:").append(_keyName)
        .append(",indexLookUp:map_index")
        .append(",operator:").append(_predicate.getType())
        .append(",predicate:").append(_predicate)
        .append(",delegateTo:").append(_delegateType.name().toLowerCase(Locale.ROOT))
        .append(')')
        .toString();
  }

  @Override
  protected String getExplainName() {
    return CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.UPPER_CAMEL, EXPLAIN_NAME);
  }

  @Override
  protected void explainAttributes(ExplainAttributeBuilder attributeBuilder) {
    super.explainAttributes(attributeBuilder);
    attributeBuilder.putString("column", _columnName);
    attributeBuilder.putString("key", _keyName);
    attributeBuilder.putString("indexLookUp", "map_index");
    attributeBuilder.putString("operator", _predicate.getType().name());
    attributeBuilder.putString("predicate", _predicate.toString());
    attributeBuilder.putString("delegateTo", _delegateType.name().toLowerCase(Locale.ROOT));
  }

  private static boolean canUseJsonIndex(Predicate.Type predicateType) {
    switch (predicateType) {
      case EQ:
      case NOT_EQ:
      case IN:
      case NOT_IN:
        return true;
      default:
        return false;
    }
  }
}
