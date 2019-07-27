package org.apache.pinot.tools.tuner.strategy;

import io.vavr.Tuple2;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.math.fraction.BigFraction;
import org.apache.pinot.pql.parsers.PQL2Lexer;
import org.apache.pinot.pql.parsers.PQL2Parser;
import org.apache.pinot.tools.tuner.meta.manager.MetaManager;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.AbstractQueryStats;
import org.apache.pinot.tools.tuner.query.src.stats.wrapper.IndexSuggestQueryStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParserBasedImpl implements Strategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParserBasedImpl.class);

  public final static int FIRST_ORDER = 1;
  public final static int SECOND_ORDER = 2;
  public final static int THIRD_ORDER = 3;

  public final static long NO_IN_FILTER_THRESHOLD = 0;

  public final static long NO_PROCESSED_THRESH = 0;

  public final static int NO_SEL_THRESH = 1;

  private int _algorithmOrder;
  private HashSet<String> _tableNamesWithoutType;
  private long _numEntriesScannedThreshold;
  private long _numProcessedThreshold;
  private int _selectivityThreshold;

  private ParserBasedImpl(Builder builder) {
    _algorithmOrder = builder._algorithmOrder;
    _tableNamesWithoutType = builder._tableNamesWithoutType;
    _numEntriesScannedThreshold = builder._numEntriesScannedThreshold;
    _numProcessedThreshold = builder._numProcessedThreshold;
    _selectivityThreshold = builder._selectivityThreshold;
  }

  public static final class Builder {
    private int _algorithmOrder = FIRST_ORDER;
    private HashSet<String> _tableNamesWithoutType = new HashSet<>();
    private long _numEntriesScannedThreshold = NO_IN_FILTER_THRESHOLD;
    private long _numProcessedThreshold = NO_PROCESSED_THRESH;
    private int _selectivityThreshold = NO_SEL_THRESH;

    public Builder() {
    }

    @Nonnull
    public ParserBasedImpl build() {
      return new ParserBasedImpl(this);
    }

    /**
     * Lower order(FIRST_ORDER) for inverted index, higher order(THIRD_ORDER) for sorted (broad coverage), default to FIRST_ORDER
     * @param val
     * @return
     */
    @Nonnull
    public Builder setAlgorithmOrder(int val) {
      _algorithmOrder = val;
      return this;
    }

    /**
     * Set the tables to work on, other tables will be filtered out
     * @param val set of table names without type
     * @return
     */
    @Nonnull
    public Builder setTableNamesWithoutType(@Nonnull HashSet<String> val) {
      _tableNamesWithoutType = val;
      return this;
    }

    /**
     * Set the threshold for _numEntriesScannedInFilter, the queries with _numEntriesScannedInFilter below this will be filtered out
     * @param val
     * @return
     */
    @Nonnull
    public Builder setNumEntriesScannedThreshold(long val) {
      _numEntriesScannedThreshold = val;
      return this;
    }

    /**
     * Set the minimum number of records scanned to give a recommendation
     * @param val minimum number of records scanned to give a recommendation, default to 0
     * @return
     */
    @Nonnull
    public Builder setNumProcessedThreshold(long val) {
      _numProcessedThreshold = val;
      return this;
    }

    /**
     * Set the selectivity threshold, column with selectivity below this will be ignored;
     * setting a high value will force the system to ignore low selectivity columns
     * @param val selectivity threshold, default to 1
     * @return
     */
    @Nonnull
    public Builder setSelectivityThreshold(int val) {
      _selectivityThreshold = val;
      return this;
    }
  }

  @Override
  public boolean filter(AbstractQueryStats queryStats) {
    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    long numEntriesScannedInFilter = Long.parseLong(indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter());
    return (_tableNamesWithoutType == null || _tableNamesWithoutType.isEmpty() || _tableNamesWithoutType
        .contains(indexSuggestQueryStatsImpl.getTableNameWithoutType())) && (numEntriesScannedInFilter
        >= _numEntriesScannedThreshold);
  }

  @Override
  public void accumulate(AbstractQueryStats queryStats, MetaManager metaManager,
      Map<String, Map<String, AbstractAccumulator>> AccumulatorOut) {

    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    String tableNameWithoutType = indexSuggestQueryStatsImpl.getTableNameWithoutType();
    String numEntriesScannedInFilter = indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter();
    String query = indexSuggestQueryStatsImpl.getQuery();
    LOGGER.debug("Accumulator: scoring query {}", query);

    if (Long.parseLong(numEntriesScannedInFilter) == 0) {
      return;
    }

    DimensionScoring dimensionScoring = new DimensionScoring(tableNameWithoutType, metaManager, query);
    List<Tuple2<List<String>, BigFraction>> columnScores = dimensionScoring.parseQuery();
    LOGGER.debug("Accumulator: query score: {}", columnScores.toString());

    HashSet<String> counted = new HashSet<>();
    //Discard if the effective selectivity is less than _selectivityThreshold
    BigFraction selectivityThresholdFraction = new BigFraction(_selectivityThreshold);
    columnScores.stream().filter(tupleNamesScore -> tupleNamesScore._2().compareTo(selectivityThresholdFraction) > 0)
        .forEach(tupleNamesScore -> {
          //Do not count if already counted
          tupleNamesScore._1().stream().filter(colName -> !counted.contains(colName)).forEach(colName -> {
            counted.add(colName);
            AccumulatorOut.putIfAbsent(tableNameWithoutType, new HashMap<>());
            AccumulatorOut.get(tableNameWithoutType).putIfAbsent(colName, new ParseBasedAccumulator());
            BigFraction weightedScore = BigFraction.ONE.subtract(tupleNamesScore._2().reciprocal())
                .multiply(new BigInteger(numEntriesScannedInFilter));
            ((ParseBasedAccumulator) AccumulatorOut.get(tableNameWithoutType).get(colName))
                .merge(1, weightedScore.bigDecimalValue(RoundingMode.DOWN.ordinal()).toBigInteger());
          });
        });
  }

  @Override
  public void merge(AbstractAccumulator p1, AbstractAccumulator p2) {
    ((ParseBasedAccumulator) p1).merge((ParseBasedAccumulator) p2);
  }

  /**
   * Generate a report for recommendation using tableResults:tableName/colName/AbstractMergerObj
   * @param tableResults input
   */
  public void report(Map<String, Map<String, AbstractAccumulator>> tableResults) {
    tableResults.forEach((table, map) -> {
      reportTable(table, map);
    });
  }

  public void reportTable(String tableNameWithoutType, Map<String, AbstractAccumulator> columnStats) {
    AtomicLong totalCount = new AtomicLong(0);
    columnStats.forEach((k, v) -> totalCount.addAndGet(v.getCount()));
    if (totalCount.longValue() < _numProcessedThreshold) {
      return;
    }

    String reportOut = "\n**********************Report For Table: " + tableNameWithoutType + "**********************\n";
    reportOut += MessageFormat.format("\nTotal lines accumulated: {0}\n\n", totalCount);
    List<Tuple2<String, Long>> sortedPure = new ArrayList<>();
    List<Tuple2<String, BigInteger>> sortedWeighted = new ArrayList<>();
    columnStats.forEach((colName, score) -> {
      sortedPure.add(new Tuple2<>(colName, ((ParseBasedAccumulator) score).getPureScore()));
      sortedWeighted.add(new Tuple2<>(colName, ((ParseBasedAccumulator) score).getWeightedScore()));
    });
    sortedPure.sort((p1, p2) -> (p2._2().compareTo(p1._2())));
    sortedWeighted.sort((p1, p2) -> (p2._2().compareTo(p1._2())));
    for (Tuple2<String, Long> tuple2 : sortedPure) {
      reportOut += "Dimension: " + tuple2._1() + "  " + tuple2._2().toString() + "\n";
    }
    reportOut += "***********************************************************************************\n";
    for (Tuple2<String, BigInteger> tuple2 : sortedWeighted) {
      reportOut += "Dimension: " + tuple2._1() + "  " + tuple2._2().toString() + "\n";
    }
    LOGGER.info(reportOut);
  }

  /**
   * Parse and score the dimensions in a query
   */
  class DimensionScoring {
    static final String AND = "AND";
    static final String OR = "OR";
    private String _tableNameWithoutType;
    private MetaManager _metaManager;
    private String _queryString;
    private final Logger LOGGER = LoggerFactory.getLogger(DimensionScoring.class);

    /*
     * Crop a list to finalLength
     */
    private void cropList(List list, int finalLength) {
      int listSize = list.size();
      int numToReMove = listSize - finalLength;
      for (int i = 1; i <= numToReMove; i++) {
        list.remove(listSize - i);
      }
    }

    DimensionScoring(String tableNameWithoutType, MetaManager metaManager, String queryString) {
      _tableNameWithoutType = tableNameWithoutType;
      _metaManager = metaManager;
      _queryString = queryString;
    }


    /**
     * Navigate from root to predicateListContext of whereClauseContext, where all the filtering happens
     * @return a list of sorted tuples List<Tuple2<List<colName>, Score>>
     */
    @NotNull List<Tuple2<List<String>, BigFraction>> parseQuery() {
      LOGGER.debug("Parsing query: {}", _queryString);
      PQL2Parser.OptionalClauseContext optionalClauseContext;
      PQL2Parser.WhereClauseContext whereClauseContext = null;
      if (_queryString == null) {
        return new ArrayList<>();
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
        return new ArrayList<>();
      }
      if (whereClauseContext == null) {
        return new ArrayList<>();
      }
      LOGGER.debug("whereClauseContext: {}", whereClauseContext.getText());

      List<Tuple2<List<String>, BigFraction>> results = parsePredicateList(whereClauseContext.predicateList());
      cropList(results, _algorithmOrder);
      return results;
    }


    /**
     * Parse predicate list connected by AND and OR (recursively)
     * The score is calculated as:
     *  AND connected: pick the top _algorithmOrder of sorted([([colName],Score(predicate)) for predicate in predicateList])
     *  OR connected: ([colName1]+[colName2]+[colName3], 1/(1/Score(predicate1)+1/Score(predicate2)+1/Score(predicate3))) i.e. Harmonic mean of scores
     * @param predicateListContext the leaf predicate context where the score are generated from selectivity
     * @return a list of sorted tuples List<Tuple2<List<colName>, Score>>
     */
    List<Tuple2<List<String>, BigFraction>> parsePredicateList(PQL2Parser.PredicateListContext predicateListContext) {
      LOGGER.debug("Parsing predicate list: {}", predicateListContext.getText());
      if (predicateListContext.getChildCount() == 1) {
        LOGGER.debug("Parsing parenthesis group");
        return parsePredicate((PQL2Parser.PredicateContext) predicateListContext.getChild(0));
      } else if (predicateListContext.getChild(1).getText().toUpperCase().equals(AND)) {
        LOGGER.debug("Parsing AND list {}", predicateListContext.getText());
        List<Tuple2<List<String>, BigFraction>> childResults = new ArrayList<>();

        for (int i = 0; i < predicateListContext.getChildCount(); i += 2) {
          List<Tuple2<List<String>, BigFraction>> childResult =
              parsePredicate((PQL2Parser.PredicateContext) predicateListContext.getChild(i));
          if (childResult != null) {
            childResults.addAll(childResult);
          }
        }

        childResults.sort(
            Comparator.comparing((Function<Tuple2<List<String>, BigFraction>, BigFraction>) Tuple2::_2).reversed());
        cropList(childResults, _algorithmOrder);
        LOGGER.debug("AND rank: {}", childResults.toString());
        return childResults;
      } else if (predicateListContext.getChild(1).getText().toUpperCase().equals(OR)) {
        LOGGER.debug("Parsing OR list: {}", predicateListContext.getText());
        BigFraction weight = BigFraction.ZERO;
        List<String> colNames = new ArrayList<>();
        List<Tuple2<List<String>, BigFraction>> childResults = new ArrayList<>();

        for (int i = 0; i < predicateListContext.getChildCount(); i += 2) {
          List<Tuple2<List<String>, BigFraction>> childResult =
              parsePredicate((PQL2Parser.PredicateContext) predicateListContext.getChild(i));
          if (childResult != null && childResult.size() > 0
              && childResult.get(0)._2().compareTo(BigFraction.ZERO) > 0) {
            colNames.addAll(childResult.get(0)._1());
            weight = weight.add(childResult.get(0)._2().reciprocal());
          }
        }
        LOGGER.debug("OR rank sum weight: {}", weight);

        if (weight.compareTo(BigFraction.ZERO) <= 0) {
          return childResults;
        }

        weight = weight.reciprocal();
        childResults.add(new Tuple2<>(colNames, weight));
        LOGGER.debug("OR rank: {}", childResults.toString());
        return childResults;
      } else {
        LOGGER.error("Query: " + _queryString + " parsing exception: " + predicateListContext.getText());
        return new ArrayList<>();
      }
    }


    /**
     * Parse leaf predicates
     * The score is calculated as:
     *  IN clause:
     *    IN: selectivity/len(literals to match)
     *    NOT IN: selectivity/(selectivity-len(literals to match))
     *  Comparison clause:
     *    '=': selectivity
     *    '!=' '<>' selectivity/(selectivity-1)
     *
     *  Other Predicates have no scoring for now
     *  TODO:
     *  Range ( <d<, BETWEEN AND) clause:
     *    average_values_hit/selectivity
     *  Moreover, if average_values_hit is made available, prediction for In clause can be optimized
     * @param predicateContext the leaf predicate context where the score are generated from selectivity
     * @return a list of tuples List<Tuple2<List<colName>, Score>>
     */
    List<Tuple2<List<String>, BigFraction>> parsePredicate(PQL2Parser.PredicateContext predicateContext) {
      LOGGER.debug("Parsing predicate: {}", predicateContext.getText());
      if (predicateContext instanceof PQL2Parser.PredicateParenthesisGroupContext) {
        PQL2Parser.PredicateParenthesisGroupContext predicateParenthesisGroupContext =
            (PQL2Parser.PredicateParenthesisGroupContext) predicateContext;
        return parsePredicateList(predicateParenthesisGroupContext.predicateList());
      } else if (predicateContext instanceof PQL2Parser.InPredicateContext) {
        LOGGER.debug("Entering IN clause!");
        String colName = ((PQL2Parser.InPredicateContext) predicateContext).inClause().expression().getText();
        List<String> colNameList = new ArrayList<>();
        colNameList.add(colName);
        ArrayList<Tuple2<List<String>, BigFraction>> ret = new ArrayList<>();

        BigFraction selectivity = _metaManager.getColumnSelectivity(_tableNameWithoutType, colName);
        LOGGER.debug("Final Cardinality: {} {} {}", selectivity, _tableNameWithoutType, colName);
        if (selectivity.compareTo(new BigFraction(_selectivityThreshold)) <= 0) {
          return ret;
        }

        int lenFilter = ((PQL2Parser.InPredicateContext) predicateContext).inClause().literal().size();
        if (((PQL2Parser.InPredicateContext) predicateContext).inClause().NOT() != null) {
          if (selectivity.subtract(lenFilter).compareTo(BigFraction.ZERO) <= 0) {
            ret.add(new Tuple2<>(colNameList, selectivity));
            return ret;
          }
          ret.add(new Tuple2<>(colNameList, selectivity.divide(selectivity.subtract(lenFilter))));
        } else {
          ret.add(new Tuple2<>(colNameList, selectivity.divide(lenFilter)));
        }
        LOGGER.debug("IN clause ret {}", ret.toString());
        return ret;
      } else if (predicateContext instanceof PQL2Parser.ComparisonPredicateContext) {
        LOGGER.debug("Entering COMP clause!");
        String colName =
            ((PQL2Parser.ComparisonPredicateContext) predicateContext).comparisonClause().expression(0).getText();
        List<String> colNameList = new ArrayList<>();
        colNameList.add(colName);
        ArrayList<Tuple2<List<String>, BigFraction>> ret = new ArrayList<>();

        BigFraction selectivity = _metaManager.getColumnSelectivity(_tableNameWithoutType, colName);
        LOGGER.debug("Final Cardinality: {} {} {}", selectivity, _tableNameWithoutType, colName);
        if (selectivity.compareTo(new BigFraction(_selectivityThreshold)) <= 0) {
          return ret;
        }

        String comparisonOp =
            ((PQL2Parser.ComparisonPredicateContext) predicateContext).comparisonClause().comparisonOperator()
                .getText();
        LOGGER.debug("COMP operator {}", comparisonOp);
        if (comparisonOp.equals("=")) {
          ret.add(new Tuple2<>(colNameList, selectivity));
          LOGGER.debug("COMP clause ret {}", ret.toString());
          return ret;
        } else if (comparisonOp.equals("!=") || comparisonOp.equals("<>")) {
          if (selectivity.subtract(BigInteger.ONE).compareTo(BigFraction.ZERO) <= 0) {
            return ret;
          }
          ret.add(new Tuple2<>(colNameList, selectivity.divide(selectivity.subtract(BigFraction.ONE))));
          LOGGER.debug("COMP clause ret {}", ret.toString());
          return ret;
        } else {
          return ret;
        }
      } else {
        return new ArrayList<>();
      }
    }
  }
}
