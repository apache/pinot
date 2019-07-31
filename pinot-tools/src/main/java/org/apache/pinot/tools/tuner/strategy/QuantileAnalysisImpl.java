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
package org.apache.pinot.tools.tuner.strategy;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.math3.exception.MathArithmeticException;
import org.apache.commons.math3.exception.MathIllegalArgumentException;
import org.apache.commons.math3.stat.descriptive.rank.Percentile;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.pinot.pql.parsers.PQL2Lexer;
import org.apache.pinot.pql.parsers.PQL2Parser;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.IndexSuggestQueryStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * A report generator to show the quantile of numEntriesScanned, and give the estimated time used in filtering if possible
 */
public class QuantileAnalysisImpl implements TuningStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(QuantileAnalysisImpl.class);

  private static final String NUM_QUERIES_COUNT = "PINOT_TUNER_COUNT*";
  public static final float MODEL_R_SQUARE_THRESHOLD = 0.7f;

  private HashSet<String> _tableNamesWithoutType;
  private long _lenBin;

  private QuantileAnalysisImpl(Builder builder) {
    _tableNamesWithoutType = builder._tableNamesWithoutType;
    _lenBin = builder._lenBin;
  }

  public static final class Builder {

    private HashSet<String> _tableNamesWithoutType = new HashSet<>();
    private long _lenBin = 100;

    public Builder() {
    }

    @Nonnull
    public QuantileAnalysisImpl build() {
      return new QuantileAnalysisImpl(this);
    }

    @Nonnull
    public Builder setTableNamesWithoutType(@Nonnull HashSet<String> val) {
      _tableNamesWithoutType = val;
      return this;
    }

    @Nonnull
    public Builder setLenBin(long val) {
      _lenBin = val;
      return this;
    }
  }

  @Override
  public boolean filter(AbstractQueryStats queryStats) {
    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    return (_tableNamesWithoutType == null || _tableNamesWithoutType.isEmpty() || _tableNamesWithoutType.contains(
        indexSuggestQueryStatsImpl.getTableNameWithoutType()));
  }

  @Override
  public void accumulate(AbstractQueryStats queryStats, MetaManager metaManager, Map<String, Map<String, AbstractAccumulator>> accumulatorOut) {

    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    String tableNameWithoutType = indexSuggestQueryStatsImpl.getTableNameWithoutType();
    String time = indexSuggestQueryStatsImpl.getTime();
    String numEntriesScannedInFilter = indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter();
    String numEntriesScannedPostFilter = indexSuggestQueryStatsImpl.getNumEntriesScannedPostFilter();
    String query = indexSuggestQueryStatsImpl.getQuery();
    LOGGER.debug("Accumulator: scoring query {}", query);

    accumulatorOut.putIfAbsent(tableNameWithoutType, new HashMap<>());
    accumulatorOut.get(tableNameWithoutType).putIfAbsent(NUM_QUERIES_COUNT, new QuantileAnalysisAccumulator());
    accumulatorOut.get(tableNameWithoutType).get(NUM_QUERIES_COUNT).increaseCount();

    accumulatorOut.get(tableNameWithoutType).putIfAbsent("*", new QuantileAnalysisAccumulator());
    ((QuantileAnalysisAccumulator) accumulatorOut.get(tableNameWithoutType).get("*")).merge(Long.parseLong(time),
        Long.parseLong(numEntriesScannedInFilter), Long.parseLong(numEntriesScannedPostFilter), 0, _lenBin);
  }

  @Override
  public void merge(AbstractAccumulator p1, AbstractAccumulator p2) {
    ((QuantileAnalysisAccumulator) p1).merge((QuantileAnalysisAccumulator) p2);
  }

  /**
   * Generate a report for recommendation using tableResults:tableName/colName/AbstractMergerObj
   * @param tableResults input
   */
  @Override
  public void report(Map<String, Map<String, AbstractAccumulator>> tableResults) {
    tableResults.forEach((table, map) -> reportTable(table, map));
  }

  public void reportTable(String tableNameWithoutType, Map<String, AbstractAccumulator> columnStats) {
    String reportOut = "\n**********************Report For Table: " + tableNameWithoutType + "**********************\n";

    long totalCount = columnStats.remove(NUM_QUERIES_COUNT).getCount();
    reportOut += MessageFormat.format("\nTotal lines accumulated: {0}\n\n", totalCount);

    if (!columnStats.containsKey("*")) {
      return;
    }

    QuantileAnalysisAccumulator olsMergerObj = (QuantileAnalysisAccumulator) columnStats.get("*");
    LOGGER.debug(olsMergerObj.getMinBin().toString());

    double[] timeAll = new double[olsMergerObj.getTimeList().size()];
    double[] inFilterAll = new double[olsMergerObj.getInFilterList().size()];

    ArrayList<Long> timeList = olsMergerObj.getTimeList();
    ArrayList<Long> inFilterList = olsMergerObj.getInFilterList();
    for (int i = 0; i < timeList.size(); i++) {
      timeAll[i] = timeList.get(i);
      inFilterAll[i] = inFilterList.get(i);
    }

    double[] timePercentile = new double[10];
    Percentile percentile = new Percentile();
    percentile.setData(timeAll);

    timePercentile[0] = percentile.evaluate(10);
    timePercentile[1] = percentile.evaluate(20);
    timePercentile[2] = percentile.evaluate(30);
    timePercentile[3] = percentile.evaluate(40);
    timePercentile[4] = percentile.evaluate(50);
    timePercentile[5] = percentile.evaluate(60);
    timePercentile[6] = percentile.evaluate(70);
    timePercentile[7] = percentile.evaluate(80);
    timePercentile[8] = percentile.evaluate(90);
    timePercentile[9] = percentile.evaluate(95);

    double[] numEntriesScannedInFilterPercentile = new double[10];
    percentile = new Percentile();
    percentile.setData(inFilterAll);

    numEntriesScannedInFilterPercentile[0] = percentile.evaluate(10);
    numEntriesScannedInFilterPercentile[1] = percentile.evaluate(20);
    numEntriesScannedInFilterPercentile[2] = percentile.evaluate(30);
    numEntriesScannedInFilterPercentile[3] = percentile.evaluate(40);
    numEntriesScannedInFilterPercentile[4] = percentile.evaluate(50);
    numEntriesScannedInFilterPercentile[5] = percentile.evaluate(60);
    numEntriesScannedInFilterPercentile[6] = percentile.evaluate(70);
    numEntriesScannedInFilterPercentile[7] = percentile.evaluate(80);
    numEntriesScannedInFilterPercentile[8] = percentile.evaluate(90);
    numEntriesScannedInFilterPercentile[9] = percentile.evaluate(95);

    reportOut += "numEntriesScannedInFilter:\n";
    reportOut += MessageFormat.format(
        "10%:{0} | 20%:{1} | 30%:{2} | 40%:{3} | 50%:{4} | 60%:{5} | 70%:{6} | 80%:{7} | 90%:{8} | 95%:{9}\n", String.format("%.0f", numEntriesScannedInFilterPercentile[0]),
        String.format("%.0f", numEntriesScannedInFilterPercentile[1]), String.format("%.0f", numEntriesScannedInFilterPercentile[2]),
        String.format("%.0f", numEntriesScannedInFilterPercentile[3]), String.format("%.0f", numEntriesScannedInFilterPercentile[4]),
        String.format("%.0f", numEntriesScannedInFilterPercentile[5]), String.format("%.0f", numEntriesScannedInFilterPercentile[6]),
        String.format("%.0f", numEntriesScannedInFilterPercentile[7]), String.format("%.0f", numEntriesScannedInFilterPercentile[8]),
        String.format("%.0f", numEntriesScannedInFilterPercentile[9]));

    reportOut += "\nLatency (ms):\n";
    reportOut += MessageFormat.format(
        "10%:{0} | 20%:{1} | 30%:{2} | 40%:{3} | 50%:{4} | 60%:{5} | 70%:{6} | 80%:{7} | 90%:{8} | 95%:{9}\n", String.format("%.0f", timePercentile[0]), String.format("%.0f", timePercentile[1]),
        String.format("%.0f", timePercentile[2]), String.format("%.0f", timePercentile[3]), String.format("%.0f", timePercentile[4]), String.format("%.0f", timePercentile[5]),
        String.format("%.0f", timePercentile[6]), String.format("%.0f", timePercentile[7]), String.format("%.0f", timePercentile[8]), String.format("%.0f", timePercentile[9]));

    OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
    regression.setNoIntercept(true);

    double[] time = new double[olsMergerObj.getMinBin().size()];
    double[][] x_arr = new double[olsMergerObj.getMinBin().size()][2];
    AtomicInteger iter = new AtomicInteger(0);

    olsMergerObj.getMinBin().forEach((key, val) -> {
      time[iter.get()] = val._2();
      x_arr[iter.get()][0] = key._1() * _lenBin + _lenBin / 2;
      x_arr[iter.get()][1] = key._2() * _lenBin + _lenBin / 2;
      iter.incrementAndGet();
    });

    try {
      regression.newSampleData(time, x_arr);
      double[] params = regression.estimateRegressionParameters();
      double rSquared = regression.calculateRSquared();
      if (rSquared > MODEL_R_SQUARE_THRESHOLD) {
        reportOut += "\nMaximum Optimization(ms):\n";
        reportOut += MessageFormat.format(
            "10%:{0} | 20%:{1} | 30%:{2} | 40%:{3} | 50%:{4} | 60%:{5} | 70%:{6} | 80%:{7} | 90%:{8} | 95%:{9}\n",
            String.format("%.0f", numEntriesScannedInFilterPercentile[0] * params[0]),
            String.format("%.0f", numEntriesScannedInFilterPercentile[1] * params[0]),
            String.format("%.0f", numEntriesScannedInFilterPercentile[2] * params[0]),
            String.format("%.0f", numEntriesScannedInFilterPercentile[3] * params[0]), String.format("%.0f", numEntriesScannedInFilterPercentile[4] * params[0]),
            String.format("%.0f", numEntriesScannedInFilterPercentile[5] * params[0]), String.format("%.0f", numEntriesScannedInFilterPercentile[6] * params[0]),
            String.format("%.0f", numEntriesScannedInFilterPercentile[7] * params[0]), String.format("%.0f", numEntriesScannedInFilterPercentile[8] * params[0]),
            String.format("%.0f", numEntriesScannedInFilterPercentile[9] * params[0]));
        reportOut += MessageFormat.format("\nR-square: {0}\n", rSquared);
        reportOut += String.format("Params: %s %s\n", Double.toString(params[0]), Double.toString(params[1]));
      } else {
        reportOut += "\nunable to predict the optimization boundary of this table!";
      }
    } catch (MathIllegalArgumentException | MathArithmeticException e) {
      reportOut += "\nunable to predict the optimization boundary of this table!";
    }
    LOGGER.info(reportOut);
  }

  /**
   * Parse and count the total bitmaps used in a query
   */
  class DimensionScoring {
    static final String AND = "AND";
    static final String OR = "OR";
    private String _tableNameWithoutType;
    private MetaManager _metaManager;
    private String _queryString;
    private final Logger LOGGER = LoggerFactory.getLogger(DimensionScoring.class);

    DimensionScoring(String tableNameWithoutType, MetaManager metaManager, String queryString) {
      _tableNameWithoutType = tableNameWithoutType;
      _metaManager = metaManager;
      _queryString = queryString;
    }

    /*
     * Navigate from root to predicateListContext of whereClauseContext, where all the filtering happens
     */
    @NotNull int parseQuery() {
      LOGGER.debug("Parsing query: {}", _queryString);
      PQL2Parser.OptionalClauseContext optionalClauseContext = null;
      PQL2Parser.WhereClauseContext whereClauseContext = null;
      if (_queryString == null) {
        return 0;
      }
      try {
        PQL2Lexer lexer = new PQL2Lexer(new ANTLRInputStream(_queryString));
        PQL2Parser parser = new PQL2Parser(new CommonTokenStream(lexer));
        ParseTree selectStatement = parser.root().statement().selectStatement();
        LOGGER.debug("selectStatement: {}", selectStatement.getText());

        for (int i = 0; i < selectStatement.getChildCount(); i++) {
          if (selectStatement.getChild(i) instanceof PQL2Parser.OptionalClauseContext) {
            optionalClauseContext = (PQL2Parser.OptionalClauseContext) selectStatement.getChild(i);
            LOGGER.debug("optionalClauseContext: {}", optionalClauseContext.getText());
            if (optionalClauseContext.getChild(0) instanceof PQL2Parser.WhereClauseContext) {
              whereClauseContext = (PQL2Parser.WhereClauseContext) optionalClauseContext.getChild(0);
              break;
            }
          }
        }
      } catch (Exception e) {
        return 0;
      }
      if (whereClauseContext == null) {
        return 0;
      }
      LOGGER.debug("whereClauseContext: {}", whereClauseContext.getText());

      return parsePredicateList(whereClauseContext.predicateList());
    }

    /*
     * Parse predicate list connected by AND and OR (recursively)
     * Sum the results of children
     */
    int parsePredicateList(PQL2Parser.PredicateListContext predicateListContext) {
      LOGGER.debug("Parsing predicate list: {}", predicateListContext.getText());
      if (predicateListContext.getChildCount() == 1) {
        LOGGER.debug("Parsing parenthesis group");
        return parsePredicate((PQL2Parser.PredicateContext) predicateListContext.getChild(0));
      } else if (predicateListContext.getChild(1).getText().toUpperCase().equals(AND) || predicateListContext.getChild(1).getText().toUpperCase().equals(OR)) {
        LOGGER.debug("Parsing AND/OR list {}", predicateListContext.getText());
        int childResults = 0;
        for (int i = 0; i < predicateListContext.getChildCount(); i += 2) {
          childResults += parsePredicate((PQL2Parser.PredicateContext) predicateListContext.getChild(i));
        }
        LOGGER.debug("AND/OR rank: {}", childResults);
        return childResults;
      } else {
        LOGGER.error("Query: " + _queryString + " parsing exception: " + predicateListContext.getText());
        return 0;
      }
    }

    /*
     * Parse leaf predicates
     * Count the inverted indices used
     */
    int parsePredicate(PQL2Parser.PredicateContext predicateContext) {
      LOGGER.debug("Parsing predicate: {}", predicateContext.getText());
      if (predicateContext instanceof PQL2Parser.PredicateParenthesisGroupContext) {
        PQL2Parser.PredicateParenthesisGroupContext predicateParenthesisGroupContext = (PQL2Parser.PredicateParenthesisGroupContext) predicateContext;
        return parsePredicateList(predicateParenthesisGroupContext.predicateList());
      } else if (predicateContext instanceof PQL2Parser.InPredicateContext) {
        LOGGER.debug("Entering IN clause!");
        String colName = ((PQL2Parser.InPredicateContext) predicateContext).inClause().expression().getText();
        if (_metaManager.hasInvertedIndex(_tableNameWithoutType, colName)) {
          return ((PQL2Parser.InPredicateContext) predicateContext).inClause().literal().size();
        }
        return 0;
      } else if (predicateContext instanceof PQL2Parser.ComparisonPredicateContext) {
        String colName =
            ((PQL2Parser.ComparisonPredicateContext) predicateContext).comparisonClause().expression(0).getText();
        LOGGER.debug("Entering COMP clause!");
        String comparisonOp = ((PQL2Parser.ComparisonPredicateContext) predicateContext).comparisonClause()
            .comparisonOperator()
            .getText();
        LOGGER.debug("COMP operator {}", comparisonOp);
        if (comparisonOp.equals("=") || comparisonOp.equals("!=") || comparisonOp.equals("<>")) {
          if (_metaManager.hasInvertedIndex(_tableNameWithoutType, colName)) {
            return 1;
          }
        }
        return 0;
      } else {
        return 0;
      }
    }
  }
}
