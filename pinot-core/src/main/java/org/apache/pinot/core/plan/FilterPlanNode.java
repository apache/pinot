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
package org.apache.pinot.core.plan;

import java.math.BigDecimal;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.geospatial.transform.function.StDistanceFunction;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.operator.filter.BaseFilterOperator;
import org.apache.pinot.core.operator.filter.BitmapBasedFilterOperator;
import org.apache.pinot.core.operator.filter.EmptyFilterOperator;
import org.apache.pinot.core.operator.filter.ExpressionFilterOperator;
import org.apache.pinot.core.operator.filter.FilterOperatorUtils;
import org.apache.pinot.core.operator.filter.H3IndexFilterOperator;
import org.apache.pinot.core.operator.filter.JsonMatchFilterOperator;
import org.apache.pinot.core.operator.filter.MatchAllFilterOperator;
import org.apache.pinot.core.operator.filter.TextMatchFilterOperator;
import org.apache.pinot.core.operator.filter.predicate.FSTBasedRegexpPredicateEvaluatorFactory;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluator;
import org.apache.pinot.core.operator.filter.predicate.PredicateEvaluatorProvider;
import org.apache.pinot.core.query.exception.BadQueryRequestException;
import org.apache.pinot.core.query.request.context.ExpressionContext;
import org.apache.pinot.core.query.request.context.FilterContext;
import org.apache.pinot.core.query.request.context.FunctionContext;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.apache.pinot.core.query.request.context.predicate.EqPredicate;
import org.apache.pinot.core.query.request.context.predicate.InPredicate;
import org.apache.pinot.core.query.request.context.predicate.NotEqPredicate;
import org.apache.pinot.core.query.request.context.predicate.JsonMatchPredicate;
import org.apache.pinot.core.query.request.context.predicate.NotInPredicate;
import org.apache.pinot.core.query.request.context.predicate.Predicate;
import org.apache.pinot.core.query.request.context.predicate.RangePredicate;
import org.apache.pinot.core.query.request.context.predicate.RegexpLikePredicate;
import org.apache.pinot.core.query.request.context.predicate.TextMatchPredicate;
import org.apache.pinot.core.segment.index.datasource.MutableDataSource;
import org.apache.pinot.core.segment.index.readers.JsonIndexReader;
import org.apache.pinot.core.segment.index.readers.NullValueVectorReader;
import org.apache.pinot.core.segment.index.readers.ValidDocIndexReader;
import org.apache.pinot.core.util.QueryOptions;
import org.apache.pinot.spi.data.FieldSpec;


public class FilterPlanNode implements PlanNode {
  private final IndexSegment _indexSegment;
  private final QueryContext _queryContext;
  private final int _numDocs;

  public FilterPlanNode(IndexSegment indexSegment, QueryContext queryContext) {
    _indexSegment = indexSegment;
    _queryContext = queryContext;
    // NOTE: Fetch number of documents in the segment when creating the plan node so that it is consistent among all
    //       filter operators. Number of documents will keep increasing for MutableSegment (CONSUMING segment).
    _numDocs = _indexSegment.getSegmentMetadata().getTotalDocs();
  }

  @Override
  public BaseFilterOperator run() {
    FilterContext filter = _queryContext.getFilter();
    ValidDocIndexReader validDocIndexReader = _indexSegment.getValidDocIndex();
    boolean upsertSkipped = false;
    if (_queryContext.getQueryOptions() != null) {
      upsertSkipped = new QueryOptions(_queryContext.getQueryOptions()).isSkipUpsert();
    }
    if (filter != null) {
      BaseFilterOperator filterOperator = constructPhysicalOperator(filter, _queryContext.getDebugOptions());
      if (validDocIndexReader != null && !upsertSkipped) {
        BaseFilterOperator validDocFilter =
            new BitmapBasedFilterOperator(validDocIndexReader.getValidDocBitmap(), false, _numDocs);
        return FilterOperatorUtils.getAndFilterOperator(Arrays.asList(filterOperator, validDocFilter), _numDocs,
            _queryContext.getDebugOptions());
      } else {
        return filterOperator;
      }
    } else if (validDocIndexReader != null && !upsertSkipped) {
      return new BitmapBasedFilterOperator(validDocIndexReader.getValidDocBitmap(), false, _numDocs);
    } else {
      return new MatchAllFilterOperator(_numDocs);
    }
  }

  /**
   * H3 index can be applied iff:
   * <ul>
   *   <li>Predicate is of type RANGE</li>
   *   <li>Left-hand-side of the predicate is an ST_Distance function</li>
   *   <li>One argument of the ST_Distance function is an identifier, the other argument is an literal</li>
   *   <li>The identifier column has H3 index</li>
   * </ul>
   */
  private boolean canApplyH3Index(Predicate predicate, FunctionContext function) {
    if (predicate.getType() != Predicate.Type.RANGE) {
      return false;
    }
    if (!function.getFunctionName().equalsIgnoreCase(StDistanceFunction.FUNCTION_NAME)) {
      return false;
    }
    List<ExpressionContext> arguments = function.getArguments();
    if (arguments.size() != 2) {
      throw new BadQueryRequestException("Expect 2 arguments for function: " + StDistanceFunction.FUNCTION_NAME);
    }
    // TODO: handle nested geography/geometry conversion functions
    String columnName = null;
    boolean findLiteral = false;
    for (ExpressionContext argument : arguments) {
      if (argument.getType() == ExpressionContext.Type.IDENTIFIER) {
        columnName = argument.getIdentifier();
      } else if (argument.getType() == ExpressionContext.Type.LITERAL) {
        findLiteral = true;
      }
    }
    return columnName != null && _indexSegment.getDataSource(columnName).getH3Index() != null && findLiteral;
  }

  /**
   * Helper method to build the operator tree from the filter.
   */
  private BaseFilterOperator constructPhysicalOperator(FilterContext filter,
      @Nullable Map<String, String> debugOptions) {
    switch (filter.getType()) {
      case AND:
        List<FilterContext> childFilters = filter.getChildren();
        List<BaseFilterOperator> childFilterOperators = new ArrayList<>(childFilters.size());
        for (FilterContext childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, debugOptions);
          if (childFilterOperator.isResultEmpty()) {
            // Return empty filter operator if any of the child filter operator's result is empty
            return EmptyFilterOperator.getInstance();
          } else if (!childFilterOperator.isResultMatchingAll()) {
            // Remove child filter operators that match all records
            childFilterOperators.add(childFilterOperator);
          }
        }
        return FilterOperatorUtils.getAndFilterOperator(childFilterOperators, _numDocs, debugOptions);
      case OR:
        childFilters = filter.getChildren();
        childFilterOperators = new ArrayList<>(childFilters.size());
        for (FilterContext childFilter : childFilters) {
          BaseFilterOperator childFilterOperator = constructPhysicalOperator(childFilter, debugOptions);
          if (childFilterOperator.isResultMatchingAll()) {
            // Return match all filter operator if any of the child filter operator matches all records
            return new MatchAllFilterOperator(_numDocs);
          } else if (!childFilterOperator.isResultEmpty()) {
            // Remove child filter operators whose result is empty
            childFilterOperators.add(childFilterOperator);
          }
        }
        return FilterOperatorUtils.getOrFilterOperator(childFilterOperators, _numDocs, debugOptions);
      case PREDICATE:
        Predicate predicate = filter.getPredicate();
        ExpressionContext lhs = predicate.getLhs();
        if (lhs.getType() == ExpressionContext.Type.FUNCTION) {
          if (canApplyH3Index(predicate, lhs.getFunction())) {
            return new H3IndexFilterOperator(_indexSegment, predicate, _numDocs);
          }
          // TODO: ExpressionFilterOperator does not support predicate types without PredicateEvaluator (IS_NULL,
          //       IS_NOT_NULL, TEXT_MATCH)
          return new ExpressionFilterOperator(_indexSegment, predicate, _numDocs);
        } else {
          String column = lhs.getIdentifier();
          DataSource dataSource = _indexSegment.getDataSource(column);
          switch (predicate.getType()) {
            case TEXT_MATCH:
              return new TextMatchFilterOperator(dataSource.getTextIndex(), ((TextMatchPredicate) predicate).getValue(),
                  _numDocs);
            case REGEXP_LIKE:
              // FST Index is available only for rolled out segments. So, we use different evaluator for rolled out and
              // consuming segments.
              //
              // Rolled out segments (immutable): FST Index reader is available use FSTBasedEvaluator
              // else use regular flow of getting predicate evaluator.
              //
              // Consuming segments: When FST is enabled, use AutomatonBasedEvaluator so that regexp matching logic is
              // similar to that of FSTBasedEvaluator, else use regular flow of getting predicate evaluator.
              PredicateEvaluator evaluator;
              if (dataSource.getFSTIndex() != null) {
                evaluator = FSTBasedRegexpPredicateEvaluatorFactory
                    .newFSTBasedEvaluator(dataSource.getFSTIndex(), dataSource.getDictionary(),
                        ((RegexpLikePredicate) predicate).getValue());
              } else if (dataSource instanceof MutableDataSource && ((MutableDataSource) dataSource).isFSTEnabled()) {
                evaluator = FSTBasedRegexpPredicateEvaluatorFactory
                    .newAutomatonBasedEvaluator(dataSource.getDictionary(),
                        ((RegexpLikePredicate) predicate).getValue());
              } else {
                evaluator = PredicateEvaluatorProvider.getPredicateEvaluator(predicate, dataSource.getDictionary(),
                    dataSource.getDataSourceMetadata().getDataType());
              }
              return FilterOperatorUtils.getLeafFilterOperator(evaluator, dataSource, _numDocs);
            case JSON_MATCH:
              JsonIndexReader jsonIndex = dataSource.getJsonIndex();
              Preconditions
                  .checkState(jsonIndex != null, "Cannot apply JSON_MATCH on column: %s without json index", column);
              return new JsonMatchFilterOperator(jsonIndex, ((JsonMatchPredicate) predicate).getValue(), _numDocs);
            case IS_NULL:
              NullValueVectorReader nullValueVector = dataSource.getNullValueVector();
              if (nullValueVector != null) {
                return new BitmapBasedFilterOperator(nullValueVector.getNullBitmap(), false, _numDocs);
              } else {
                return EmptyFilterOperator.getInstance();
              }
            case IS_NOT_NULL:
              nullValueVector = dataSource.getNullValueVector();
              if (nullValueVector != null) {
                return new BitmapBasedFilterOperator(nullValueVector.getNullBitmap(), true, _numDocs);
              } else {
                return new MatchAllFilterOperator(_numDocs);
              }
            default:
              FieldSpec.DataType columnType = dataSource.getDataSourceMetadata().getDataType();
              Predicate castedPredicate = castPredicateToColumnType(predicate, columnType);
              PredicateEvaluator predicateEvaluator = PredicateEvaluatorProvider
                  .getPredicateEvaluator(castedPredicate, dataSource.getDictionary(),
                      columnType);
              return FilterOperatorUtils.getLeafFilterOperator(predicateEvaluator, dataSource, _numDocs);
          }
        }
      default:
        throw new IllegalStateException();
    }
  }

  private Predicate castPredicateToColumnType(Predicate predicate, FieldSpec.DataType columnType) {
    if (columnType != FieldSpec.DataType.DOUBLE && columnType != FieldSpec.DataType.FLOAT
        && columnType != FieldSpec.DataType.LONG && columnType != FieldSpec.DataType.INT) {
      // this is not a numerical predicate, hence don't worry about type conversions.
      return predicate;
    }

    switch (predicate.getType())
    {
      case EQ: {
        EqPredicate eqPredicate = (EqPredicate) predicate;
        BigDecimal actualValue = new BigDecimal(eqPredicate.getValue());
        BigDecimal convertedValue = actualValue;
        switch (columnType) {
          case INT:
            convertedValue = new BigDecimal(actualValue.intValue());
            break;
          case LONG:
            convertedValue = new BigDecimal(actualValue.longValue());
            break;
          case FLOAT:
            convertedValue = new BigDecimal(String.valueOf(String.valueOf(actualValue.floatValue())));
            break;
          case DOUBLE:
            convertedValue = new BigDecimal(String.valueOf(String.valueOf(actualValue.floatValue())));
            break;
        }

        int compared = actualValue.compareTo(convertedValue);

        // TODO: Need to adjust all eqPredicate.getLhs() function since we are re-writing the plan.
        EqPredicate castedEqPredicate = new EqPredicate(eqPredicate.getLhs(), convertedValue.toString());
        if (compared != 0) {
          // We already know that this predicate will always evaluate to false; hence, there is no need
          // to evaluate the predicate during runtime.
          castedEqPredicate.setPrecomputed(false);
        }

        return castedEqPredicate;
      }
      case NOT_EQ: {
        NotEqPredicate nEqPredicate = (NotEqPredicate) predicate;
        BigDecimal actualValue = new BigDecimal(nEqPredicate.getValue());
        BigDecimal convertedValue = actualValue;
        switch (columnType) {
          case INT:
            convertedValue = new BigDecimal(actualValue.intValue());
            break;
          case LONG:
            convertedValue = new BigDecimal(actualValue.longValue());
            break;
          case FLOAT:
            convertedValue = new BigDecimal(String.valueOf(String.valueOf(actualValue.floatValue())));
            break;
          case DOUBLE:
            convertedValue = new BigDecimal(String.valueOf(String.valueOf(actualValue.floatValue())));
            break;
        }

        int compared = actualValue.compareTo(convertedValue);
        NotEqPredicate castedEqPredicate = new NotEqPredicate(nEqPredicate.getLhs(), convertedValue.toString());
        if (compared != 0) {
          // We already know that this predicate will always evaluate to true; hence, there is no need
          // to evaluate the predicate during runtime.
          castedEqPredicate.setPrecomputed(true);
        }

        return castedEqPredicate;
      }
      case RANGE: {
        RangePredicate rangePredicate = (RangePredicate) predicate;

        boolean lowerInclusive = rangePredicate.isLowerInclusive(), upperInclusive = rangePredicate.isUpperInclusive();
        if (!rangePredicate.getLowerBound().equals(RangePredicate.UNBOUNDED)) {
          BigDecimal lowerBound = new BigDecimal(rangePredicate.getLowerBound());
          BigDecimal lowerConvertedValue = lowerBound;
          switch (columnType) {
            case INT: {
              lowerConvertedValue = new BigDecimal(lowerBound.intValue());
              break;
            }
            case LONG: {
              lowerConvertedValue = new BigDecimal(lowerBound.longValue());
              break;
            }
            case FLOAT: {
              lowerConvertedValue = new BigDecimal(String.valueOf(lowerBound.floatValue()));
              break;
            }
            case DOUBLE: {
              // check if conversion to LONG is lossless
              lowerConvertedValue = new BigDecimal(String.valueOf(lowerBound.doubleValue()));
              break;
            }
          }

          int compared = lowerBound.compareTo(lowerConvertedValue);
          if (compared != 0) {
            //If value after conversion is less than original value, upper bound has to be exclusive; otherwise,
            //upper bound has to be inclusive.
            lowerInclusive = !(compared > 0);
          }
        }

        if (!rangePredicate.getUpperBound().equals(RangePredicate.UNBOUNDED)) {
          BigDecimal upperBound = new BigDecimal(rangePredicate.getUpperBound());
          BigDecimal upperConvertedValue = upperBound;
          switch (columnType) {
            case INT: {
              upperConvertedValue = new BigDecimal(upperBound.intValue());
              break;
            }
            case LONG: {
              upperConvertedValue = new BigDecimal(upperBound.longValue());
              break;
            }
            case FLOAT: {
              upperConvertedValue = new BigDecimal(String.valueOf(upperBound.floatValue()));
              break;
            }
            case DOUBLE: {
              upperConvertedValue = new BigDecimal(String.valueOf(upperBound.doubleValue()));
              break;
            }
          }

          int compared = upperBound.compareTo(upperConvertedValue);
          if (compared != 0) {
            //If value after conversion is less than original value, upper bound has to be inclusive; otherwise,
            //upper bound has to be exclusive.
            upperInclusive = compared > 0;
          }
        }

        return new RangePredicate(rangePredicate.getLhs(), lowerInclusive, rangePredicate.getLowerBound(),
            upperInclusive, rangePredicate.getUpperBound());
      }
      case IN:
      {
        InPredicate inPredicate = (InPredicate) predicate;
        List<String> values = inPredicate.getValues();
        List<String> castedValues = new ArrayList<>();
        for (String value : values) {
          BigDecimal actualValue = new BigDecimal(value);
          switch (columnType) {
            case INT: {
              BigDecimal convertedValue = new BigDecimal(actualValue.intValue());
              if (actualValue.compareTo(convertedValue) == 0) {
                castedValues.add(String.valueOf(convertedValue.intValue()));
              }
              break;
            }
            case LONG: {
              BigDecimal convertedValue = new BigDecimal(actualValue.longValue());
              if (actualValue.compareTo(convertedValue) == 0) {
                castedValues.add(String.valueOf(convertedValue.longValue()));
              }
              break;
            }
            case FLOAT: {
              BigDecimal convertedValue = new BigDecimal(String.valueOf(actualValue.floatValue()));
              if (actualValue.compareTo(convertedValue) == 0) {
                castedValues.add(String.valueOf(convertedValue.floatValue()));
              }
              break;
            }
            case DOUBLE: {
              BigDecimal convertedValue = new BigDecimal(String.valueOf(actualValue.doubleValue()));
              if (actualValue.compareTo(convertedValue) == 0) {
                castedValues.add(String.valueOf(convertedValue.doubleValue()));
              }
              break;
            }
          }
        }

        return new InPredicate(inPredicate.getLhs(), castedValues);
      }
      case NOT_IN:
      {
        NotInPredicate notInPredicate = (NotInPredicate) predicate;
        List<String> values = notInPredicate.getValues();
        List<String> castedValues = new ArrayList<>();
        for (String value : values) {
          BigDecimal actualValue = new BigDecimal(value);
          switch (columnType) {
            case INT: {
              BigDecimal convertedValue = new BigDecimal(actualValue.intValue());
              if (actualValue.compareTo(convertedValue) == 0) {
                castedValues.add(String.valueOf(convertedValue.intValue()));
              }
              break;
            }
            case LONG: {
              BigDecimal convertedValue = new BigDecimal(actualValue.longValue());
              if (actualValue.compareTo(convertedValue) == 0) {
                castedValues.add(String.valueOf(convertedValue.longValue()));
              }
              break;
            }
            case FLOAT: {
              BigDecimal convertedValue = new BigDecimal(String.valueOf(actualValue.floatValue()));
              if (actualValue.compareTo(convertedValue) == 0) {
                castedValues.add(String.valueOf(convertedValue.floatValue()));
              }
              break;
            }
            case DOUBLE: {
              BigDecimal convertedValue = new BigDecimal(String.valueOf(actualValue.doubleValue()));
              if (actualValue.compareTo(convertedValue) == 0) {
                castedValues.add(String.valueOf(convertedValue.doubleValue()));
              }
              break;
            }
          }
        }

        return new NotInPredicate(notInPredicate.getLhs(), castedValues);
      }
    }

    return predicate;
  }
}
