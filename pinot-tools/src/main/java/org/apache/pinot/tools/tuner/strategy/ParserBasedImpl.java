package org.apache.pinot.tools.tuner.strategy;

import io.vavr.Tuple2;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.math.fraction.BigFraction;
import org.apache.pinot.pql.parsers.PQL2Lexer;
import org.apache.pinot.pql.parsers.PQL2Parser;
import org.apache.pinot.tools.tuner.meta.manager.MetaDataProperties;
import org.apache.pinot.tools.tuner.query.src.BasicQueryStats;
import org.apache.pinot.tools.tuner.query.src.IndexSuggestQueryStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ParserBasedImpl implements BasicStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParserBasedImpl.class);

  public final static int FIRST_ORDER = 1;
  public final static int THIRD_ORDER = 3;

  public final static long NO_IN_FILTER_THRESHOLD = 0;

  public final static int NO_WEIGHT_FOR_VOTE = 0;
  public final static int IN_FILTER_WEIGHT_FOR_VOTE = 1;

  private int _algorithmOrder;
  private HashSet<String> _tableNamesWorkonWithoutType;
  private long _numEntriesScannedThreshold;

  private ParserBasedImpl(Builder builder) {
    _algorithmOrder = builder._algorithmOrder;
    _tableNamesWorkonWithoutType = builder._tableNamesWorkonWithoutType;
    _numEntriesScannedThreshold = builder._numEntriesScannedThreshold;
  }

  public static final class Builder {
    private int _algorithmOrder = FIRST_ORDER;
    private HashSet<String> _tableNamesWorkonWithoutType = new HashSet<>();
    private long _numEntriesScannedThreshold = NO_IN_FILTER_THRESHOLD;

    public Builder() {
    }

    @Nonnull
    public ParserBasedImpl build() {
      return new ParserBasedImpl(this);
    }

    @Nonnull
    public Builder _algorithmOrder(int val) {
      _algorithmOrder = val;
      return this;
    }

    @Nonnull
    public Builder _tableNamesWorkonWithoutType(@Nonnull HashSet<String> val) {
      _tableNamesWorkonWithoutType = val;
      return this;
    }

    @Nonnull
    public Builder _numEntriesScannedThreshold(long val) {
      _numEntriesScannedThreshold = val;
      return this;
    }

    @Nonnull
    public Builder _tableNamesWorkonWithoutType(@Nonnull List<String> val) {
      _tableNamesWorkonWithoutType.addAll(val);
      return this;
    }
  }

  @Override
  public boolean filter(BasicQueryStats queryStats) {
    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    long numEntriesScannedInFilter = Long.parseLong(indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter());
    return (_tableNamesWorkonWithoutType.isEmpty() || _tableNamesWorkonWithoutType
        .contains(indexSuggestQueryStatsImpl.getTableNameWithoutType())) && (numEntriesScannedInFilter
        >= _numEntriesScannedThreshold);
  }

  @Override
  public void accumulator(BasicQueryStats queryStats, MetaDataProperties metaDataProperties,
      Map<String, Map<String, BasicMergerObj>> AccumulatorOut) {

    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    String tableNameWithoutType = indexSuggestQueryStatsImpl.getTableNameWithoutType();
    String numEntriesScannedInFilter = indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter();
    String query = indexSuggestQueryStatsImpl.getQuery();
    LOGGER.debug("Accumulator: scoring query {}", query);

    if (Long.parseLong(numEntriesScannedInFilter) == 0) return; //Early return if the query is not scanning in filter

    DimensionScoring dimensionScoring = new DimensionScoring(tableNameWithoutType, metaDataProperties, query);
    List<Tuple2<List<String>, BigFraction>> columnScores = dimensionScoring.parseQuery();
    LOGGER.debug("Accumulator: query score: {}", columnScores.toString());

    HashSet<String> counted=new HashSet<>();
    //Discard if the effective cardinality is less than one.
    columnScores.stream().filter(tupleNamesScore -> tupleNamesScore._2().compareTo(BigFraction.ONE) > 0)
        .forEach(tupleNamesScore -> {
          //Do not count if already counted
          tupleNamesScore._1().stream().filter(colName -> !counted.contains(colName)).forEach(colName -> {
            counted.add(colName);
            AccumulatorOut.putIfAbsent(tableNameWithoutType, new HashMap<>());
            AccumulatorOut.get(tableNameWithoutType).putIfAbsent(colName, new ParseBasedBasicMergerObj());
            BigFraction weigthedScore = BigFraction.ONE.subtract(tupleNamesScore._2().reciprocal())
                .multiply(new BigInteger(numEntriesScannedInFilter));
            ((ParseBasedBasicMergerObj) AccumulatorOut.get(tableNameWithoutType).get(colName))
                .merge(1, weigthedScore.bigDecimalValue(RoundingMode.DOWN.ordinal()).toBigInteger());
          });
        });
  }

  @Override
  public void merger(BasicMergerObj p1, BasicMergerObj p2) {
    ((ParseBasedBasicMergerObj) p1).merge((ParseBasedBasicMergerObj) p2);
  }

  @Override
  public void reporter(String tableNameWithoutType, Map<String, BasicMergerObj> mergedOut) {
    String tableName = "\n**********************Report For Table: " + tableNameWithoutType + "**********************\n";
    String mergerOut = "";
    List<Tuple2<String, Long>> sortedPure = new ArrayList<>();
    List<Tuple2<String, BigInteger>> sortedWeighted = new ArrayList<>();
    mergedOut.forEach((colName, score) -> {
      sortedPure.add(new Tuple2<>(colName, ((ParseBasedBasicMergerObj) score).getPureScore()));
      sortedWeighted.add(new Tuple2<>(colName, ((ParseBasedBasicMergerObj) score).getWeigtedScore()));
    });
    sortedPure.sort((p1,p2)->(p2._2().compareTo(p1._2())));
    sortedWeighted.sort((p1,p2)->(p2._2().compareTo(p1._2())));
    for(Tuple2<String, Long> tuple2 : sortedPure) {
      mergerOut += "Dimension: " + tuple2._1() + "  " + tuple2._2().toString() + "\n";
    }
    mergerOut += "***********************************************************************************\n";
    for (Tuple2<String, BigInteger> tuple2 : sortedWeighted) {
      mergerOut += "Dimension: " + tuple2._1()+ "  " + tuple2._2().toString() + "\n";
    }
    LOGGER.info(tableName + mergerOut);
  }

  /*
   * Parse and score the dimensions in a query
   */
  class DimensionScoring {
    static final String AND = "AND";
    static final String OR = "OR";
    private String _tableNameWithoutType;
    private MetaDataProperties _metaDataProperties;
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

    DimensionScoring(String tableNameWithoutType, MetaDataProperties metaDataProperties, String queryString) {
      _tableNameWithoutType = tableNameWithoutType;
      _metaDataProperties = metaDataProperties;
      _queryString = queryString;
    }

    /*
     * Navigate from root to predicateListContext of whereClauseContext, where all the filtering happens
     */
    @NotNull
    List<Tuple2<List<String>, BigFraction>> parseQuery() {
      LOGGER.debug("Parsing query: {}", _queryString);
      PQL2Parser.OptionalClauseContext optionalClauseContext = null;
      PQL2Parser.WhereClauseContext whereClauseContext = null;

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

    /*
     * Parse predicate list connected by AND and OR (recursively)
     * The score is calculated as:
     *  AND connected: pick the top _algorithmOrder of sorted([([colName],Score(predicate)) for predicate in predicateList])
     *  OR connected: ([colName1]+[colName2]+[colName3], 1/(1/Score(predicate1)+1/Score(predicate2)+1/Score(predicate3))) i.e. Harmonic mean of scores
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

    /*
     * Parse leaf predicates
     * The score is calculated as:
     *  IN clause:
     *    IN: cardinality/len(literals to match)
     *    NOT IN: cardinality/(cardinality-len(literals to match))
     *  Comparison clause:
     *    '=': cardinality
     *    '!=' '<>' cardinality/(cardinality-1)
     *
     *  Other Predicates have no scoring for now
     *  TODO:
     *  Range ( <d<, BETWEEN AND) clause:
     *    average_values_hit/cardinality
     *  Moreover, if average_values_hit is made available, prediction for In clause can be optimized
     *
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

        BigFraction cardinality = _metaDataProperties.getAverageCardinality(_tableNameWithoutType, colName);
        LOGGER.debug("Final Cardinality: {} {} {}", cardinality, _tableNameWithoutType, colName);
        if (cardinality.compareTo(BigFraction.ONE) <= 0) {
          return ret;
        }

        int lenFilter = ((PQL2Parser.InPredicateContext) predicateContext).inClause().literal().size();
        if (((PQL2Parser.InPredicateContext) predicateContext).inClause().NOT() != null) {
          if (cardinality.subtract(lenFilter).compareTo(BigFraction.ZERO) <= 0) {
            ret.add(new Tuple2<>(colNameList, cardinality));
            return ret;
          }
          ret.add(new Tuple2<>(colNameList, cardinality.divide(cardinality.subtract(lenFilter))));
        }
        {
          ret.add(new Tuple2<>(colNameList, cardinality.divide(lenFilter)));
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

        BigFraction cardinality = _metaDataProperties.getAverageCardinality(_tableNameWithoutType, colName);
        LOGGER.debug("Final Cardinality: {} {} {}", cardinality, _tableNameWithoutType, colName);
        if (cardinality.compareTo(BigFraction.ONE) <= 0) {
          return ret;
        }

        String comparisonOp =
            ((PQL2Parser.ComparisonPredicateContext) predicateContext).comparisonClause().comparisonOperator()
                .getText();
        LOGGER.debug("COMP operator {}", comparisonOp);
        if (comparisonOp.equals("=")) {
          ret.add(new Tuple2<>(colNameList, cardinality));
          LOGGER.debug("COMP clause ret {}", ret.toString());
          return ret;
        }
        else if (comparisonOp.equals("!=") || comparisonOp.equals("<>")) {
          if (cardinality.subtract(BigInteger.ONE).compareTo(BigFraction.ZERO) <= 0) {
            ret.add(new Tuple2<>(colNameList, BigFraction.ONE));
            return ret;
          }
          ret.add(new Tuple2<>(colNameList, cardinality.divide(cardinality.subtract(BigFraction.ONE))));
          LOGGER.debug("COMP clause ret {}", ret.toString());
          return ret;
        }
        else {
          return ret;
        }
      } else {
        return new ArrayList<>();
      }
    }
  }
}
