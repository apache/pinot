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
package org.apache.pinot.controller.recommender.rules.impl;

import com.google.common.util.concurrent.AtomicDouble;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.common.request.context.ExpressionContext;
import org.apache.pinot.common.request.context.FilterContext;
import org.apache.pinot.controller.recommender.exceptions.InvalidInputException;
import org.apache.pinot.controller.recommender.io.ConfigManager;
import org.apache.pinot.controller.recommender.io.InputManager;
import org.apache.pinot.controller.recommender.rules.AbstractRule;
import org.apache.pinot.controller.recommender.rules.io.params.NoDictionaryOnHeapDictionaryJointRuleParams;
import org.apache.pinot.controller.recommender.rules.utils.FixedLenBitset;
import org.apache.pinot.core.query.request.context.QueryContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.pinot.controller.recommender.rules.io.params.RecommenderConstants.REALTIME;


/**
 * Recommend no dictionary columns and on heap dictionary columns
 * Name of the column(s) not to create a dictionary on:
 *    EXCLUDE the columns we will create bitmap/sorted indices on.
 *    If a column (fixed width or variable width) is used in filter and/or group by, create a dictionary.
 *    If a column is not used in filter and group by: there are two cases:
 *      If the column is used heavily in selection, then don't create a dictionary
 *      If the column is not used in selection, then create a dictionary only if by creating a dictionary we can save
 *      > p% of storage
 *
 * Name of the column(s) with dictionary on heap
 *    We want the table’s QPS > Q
 *    The memory footprint should be < M (configurable)
 *    The column is frequently > F queried in filter/group by
 */
public class NoDictionaryOnHeapDictionaryJointRule extends AbstractRule {
  private static final Logger LOGGER = LoggerFactory.getLogger(NoDictionaryOnHeapDictionaryJointRule.class);
  private final NoDictionaryOnHeapDictionaryJointRuleParams _params;

  public NoDictionaryOnHeapDictionaryJointRule(InputManager input, ConfigManager output) {
    super(input, output);
    _params = input.getNoDictionaryOnHeapDictionaryJointRuleParams();
  }

  @Override
  public void run() {
    LOGGER.info("Recommending no dictionary and on-heap dictionaries");

    int numCols = _input.getNumCols();
    Map<String, Double> filterGroupByWeights = new HashMap<>();
    Map<String, Double> selectionWeights = new HashMap<>();
    AtomicDouble totalWeight = new AtomicDouble(0);

    //****Find out columns used in filter&groupby and selection and corresponding frequencies*****/
    _input.getParsedQueries().forEach(query -> {
      Double weight = _input.getQueryWeight(query);
      parseQuery(_input.getQueryContext(query), weight, filterGroupByWeights, selectionWeights);
      totalWeight.addAndGet(weight);
    });

    //**********No dictionary recommendation*******/
    Set<String> noDictCols = new HashSet<>(_input.getColNameToIntMap().keySet());

    // Exclude cols already having index
    noDictCols.removeAll(_output.getIndexConfig().getInvertedIndexColumns());
    noDictCols.remove(_output.getIndexConfig().getSortedColumn());
    noDictCols.removeAll(_output.getIndexConfig().getRangeIndexColumns());
    LOGGER.debug("noDictCols {}", noDictCols);

    // Exclude MV cols TODO: currently no index column is only applicable for SV columns, change this after it's
    //  supported for MV
    noDictCols.removeIf(colName -> !_input.isSingleValueColumn(colName));

    // Exclude columns used in filter&groupby, with frequency > threshold
    // Our study shows: [With a dictionary setup, at segment level the server
    // can early return an empty result if the value specified in the query
    // does not match with any value in the dictionary]
    noDictCols.removeIf(colName -> {
      double filterGroupByFreq = filterGroupByWeights.getOrDefault(colName, 0d) / totalWeight.get();
      return filterGroupByFreq
          > _params._thresholdMinFilterFreqDictionary; // THRESHOLD_MIN_FILTER_FREQ_DICTIONARY is default to 0
    });

    LOGGER.debug("filterGroupByWeights {}, selectionWeights{}, totalWeight{} ", filterGroupByWeights, selectionWeights,
        totalWeight);
    LOGGER.debug("noDictCols {}", noDictCols);

    noDictCols.removeIf(colName -> {
      // For columns frequently used in selection, use no dictionary
      // Our study shows: [for a column heavily used in selection only (not part of filter or group by)
      // making it no dictionary reduces the latency by ~25%]
      double selectionFreq = selectionWeights.getOrDefault(colName, 0d) / totalWeight.get();
      if (selectionFreq > _params._thresholdMinSelectionFreqNoDictionary) {
        return false;
      }

      // For columns NOT frequently used in selection
      // Add dictionary only if doing so can save us > THRESHOLD_MIN_PERCENT_DICTIONARY_STORAGE_SAVE of space
      double noDictSize;
      double withDictSize;

      long svColRawSizePerDoc = 0;
      try {
        svColRawSizePerDoc = _input.getColRawSizePerDoc(colName);
      } catch (InvalidInputException e) {
        return true; // If this column is a MV column, it cannot be used as noDict column
      }

      double numValuesPerEntry = _input.getNumValuesPerEntry(colName);
      int dictionaryEncodedForwardIndexSize = _input.getDictionaryEncodedForwardIndexSize(colName);
      long dictionarySize = _input.getDictionarySize(colName);
      long numRecordsPerPush;
      LOGGER.debug("svColRawSizePerDoc {}", svColRawSizePerDoc);
      LOGGER.debug("dictionaryEncodedForwardIndexSize {}", dictionaryEncodedForwardIndexSize);
      LOGGER.debug("dictionarySize {}", dictionarySize);
      LOGGER.debug("numValuesPerEntry {}", numValuesPerEntry);

      if (_input.getTableType().equalsIgnoreCase(REALTIME)) {
        //TODO: improve this estimation
        numRecordsPerPush = _input.getNumMessagesPerSecInKafkaTopic() * _input.getSegmentFlushTime();
      } else { // For hybrid or offline table, nodictionary follows the offline side
        numRecordsPerPush = _input.getNumRecordsPerPush();
      }

      noDictSize = numRecordsPerPush * svColRawSizePerDoc;
      withDictSize =
          numRecordsPerPush * dictionaryEncodedForwardIndexSize + dictionarySize * _params._dictionaryCoefficient;

      double storageSaved = (noDictSize - withDictSize) / noDictSize;
      LOGGER.debug("colName {}, noDictSize {}, withDictSize{}, storageSaved{}", colName, noDictSize, withDictSize,
          storageSaved);

      return storageSaved > _params._thresholdMinPercentDictionaryStorageSave;
    });

    // Add the no dictionary cols to output config
    _output.getIndexConfig().getNoDictionaryColumns().addAll(noDictCols);

    //**********On heap dictionary recommendation*******/
    // QPS > THRESHOLD_MIN_QPS_ON_HEAP
    if (_input.getQps() > _params._thresholdMinQpsOnHeap) {
      for (String colName : _input.getColNameToIntMap().keySet()) {
        // exclude no dictionary column
        if (!_output.getIndexConfig().getNoDictionaryColumns().contains(colName)) {
          long dictionarySize = _input.getDictionarySize(colName);
          double filterGroupByFreq = filterGroupByWeights.getOrDefault(colName, 0d) / totalWeight.get();
          // frequently used in filter/group by and memory foot print < threshold
          if (filterGroupByFreq > _params._thresholdMinFilterFreqOnHeap
              && dictionarySize < _params._thresholdMaxDictionarySizeOnHeap) {

            _output.getIndexConfig().getOnHeapDictionaryColumns().add(colName);
          }
        }
      }
    }
  }

  public void parseQuery(QueryContext queryContext, double weight, Map<String, Double> filterGroupByWeights,
      Map<String, Double> selectionWeights) {
    if (queryContext.getSelectExpressions() != null) {
      queryContext.getSelectExpressions().forEach(selectExpression -> {
        Set<String> colNames = new HashSet<>();
        selectExpression.getColumns(colNames);
        colNames.forEach(colName -> {
          selectionWeights.merge(colName, weight, Double::sum);
        });
      });
    }

    FixedLenBitset fixedLenBitsetFilterGroupBy = mutableEmptySet();
    if (queryContext.getGroupByExpressions() != null) {
      queryContext.getGroupByExpressions().forEach(groupByExpression -> {
        Set<String> colNames = new HashSet<>();
        groupByExpression.getColumns(colNames);
        colNames.forEach(colName -> fixedLenBitsetFilterGroupBy.add(_input.colNameToInt(colName)));
      });
    }

    FilterContext filter = queryContext.getFilter();
    if (filter != null && !filter.isConstant()) {
      fixedLenBitsetFilterGroupBy.union(parsePredicateList(filter));
    }

    for (Integer colId : fixedLenBitsetFilterGroupBy.getOffsets()) {
      filterGroupByWeights.merge(_input.intToColName(colId), weight, Double::sum);
    }
  }

  public FixedLenBitset parsePredicateList(FilterContext filterContext) {
    FixedLenBitset ret = mutableEmptySet();
    List<FilterContext> children = filterContext.getChildren();
    if (children != null) {
      // AND, OR, NOT
      for (FilterContext child : children) {
        FixedLenBitset childResult = parsePredicateList(child);
        ret.union(childResult);
      }
    } else {
      // PREDICATE
      ExpressionContext lhs = filterContext.getPredicate().getLhs();
      String colName = lhs.toString();
      if (lhs.getType() == ExpressionContext.Type.FUNCTION || _input.isTimeOrDateTimeColumn(colName)) {
        LOGGER.trace("Skipping this column {}", colName);
      } else if (!_input.isDim(colName)) {
        LOGGER.error("Error: Column {} should not appear in filter", colName);
      } else {
        ret.add(_input.colNameToInt(colName));
      }
    }
    return ret;
  }

  private FixedLenBitset mutableEmptySet() {
    return new FixedLenBitset(_input.getNumCols());
  }
}
