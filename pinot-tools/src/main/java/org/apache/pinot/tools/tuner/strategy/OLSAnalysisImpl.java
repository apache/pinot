package org.apache.pinot.tools.tuner.strategy;

import io.vavr.Tuple2;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.validation.constraints.NotNull;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.math3.stat.regression.OLSMultipleLinearRegression;
import org.apache.pinot.pql.parsers.PQL2Lexer;
import org.apache.pinot.pql.parsers.PQL2Parser;
import org.apache.pinot.tools.tuner.meta.manager.MetaDataProperties;
import org.apache.pinot.tools.tuner.query.src.BasicQueryStats;
import org.apache.pinot.tools.tuner.query.src.IndexSuggestQueryStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class OLSAnalysisImpl implements BasicStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(OLSAnalysisImpl.class);



  public final static long NO_IN_FILTER_THRESHOLD = 0;

  public final static int NO_WEIGHT_FOR_VOTE = 0;
  public final static int IN_FILTER_WEIGHT_FOR_VOTE = 1;

  private HashSet<String> _tableNamesWorkonWithoutType;
  private long _numEntriesScannedThreshold;
  private long _len_bin;

  private OLSAnalysisImpl(Builder builder) {
    _tableNamesWorkonWithoutType = builder._tableNamesWorkonWithoutType;
    _numEntriesScannedThreshold = builder._numEntriesScannedThreshold;
    _len_bin = builder._len_bin;
  }

  public static final class Builder {

    private HashSet<String> _tableNamesWorkonWithoutType = new HashSet<>();
    private long _numEntriesScannedThreshold = NO_IN_FILTER_THRESHOLD;
    private long _len_bin = 100;

    public Builder() {
    }

    @Nonnull
    public OLSAnalysisImpl build() {
      return new OLSAnalysisImpl(this);
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
    public Builder _len_bin(long val) {
      _len_bin = val;
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
    String time=indexSuggestQueryStatsImpl.getTime();
    String numEntriesScannedInFilter = indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter();
    String numEntriesScannedPostFilter = indexSuggestQueryStatsImpl.getNumEntriesScannedPostFilter();
    String query = indexSuggestQueryStatsImpl.getQuery();
    LOGGER.debug("Accumulator: scoring query {}", query);

    DimensionScoring dimensionScoring = new DimensionScoring(tableNameWithoutType, metaDataProperties, query);
    int usedIndexs = dimensionScoring.parseQuery();
    LOGGER.debug("Accumulator: query score: {}", usedIndexs);

    AccumulatorOut.putIfAbsent(tableNameWithoutType, new HashMap<>());
    AccumulatorOut.get(tableNameWithoutType).putIfAbsent("*", new OLSMergerObj());
    ((OLSMergerObj)AccumulatorOut.get(tableNameWithoutType).get("*")).merge(Long.parseLong(time),
        Long.parseLong(numEntriesScannedInFilter),
        Long.parseLong(numEntriesScannedPostFilter),
        usedIndexs,
        _len_bin);

  }

  @Override
  public void merger(BasicMergerObj p1, BasicMergerObj p2) {
    ((OLSMergerObj) p1).merge((OLSMergerObj) p2);
  }

  @Override
  public void reporter(String tableNameWithoutType, Map<String, BasicMergerObj> mergedOut) {
    String tableName = "\n**********************Report For Table: " + tableNameWithoutType + "**********************\n";
    LOGGER.info(tableName);

    if(!mergedOut.containsKey("*")){
      return;
    }
    OLSMergerObj olsMergerObj=(OLSMergerObj)mergedOut.get("*");
    LOGGER.info(olsMergerObj.getMinBin().toString());
    OLSMultipleLinearRegression regression = new OLSMultipleLinearRegression();
    regression.setNoIntercept(true);

    double[] time = new double[olsMergerObj.getMinBin().size()];
    double[][] x_arr = new double[olsMergerObj.getMinBin().size()][3];
    int iter=0;

    for (Map.Entry<Tuple2<Long, Long>, Tuple2<Long, Long>> entry : olsMergerObj.getMinBin().entrySet()) {
      Tuple2<Long, Long> key = entry.getKey();
      Tuple2<Long, Long> val = entry.getValue();
      time[iter]=val._2();
      x_arr[iter][0]=key._1()*_len_bin+_len_bin/2;
      x_arr[iter][1]=key._2()*_len_bin+_len_bin/2;
      x_arr[iter][2]=val._1();
      //LOGGER.info("time:{} inFilter:{} postFilter:{} usedIndex:{}",time[iter], x_arr[iter][0], x_arr[iter][1], x_arr[iter][2]);
      iter++;
    }

    try {
      regression.newSampleData(time, x_arr);
      double[] para = regression.estimateRegressionParameters();
      double rSquared = regression.calculateRSquared();
      LOGGER.info("r-square: {}", rSquared);
      LOGGER.info("params: {}", para);
    }
    catch(Exception e){
      LOGGER.info("unable to predict this table!");
      return;
    }
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

    DimensionScoring(String tableNameWithoutType, MetaDataProperties metaDataProperties, String queryString) {
      _tableNameWithoutType = tableNameWithoutType;
      _metaDataProperties = metaDataProperties;
      _queryString = queryString;
    }

    /*
     * Navigate from root to predicateListContext of whereClauseContext, where all the filtering happens
     */
    @NotNull
    int parseQuery() {
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
     * Sum the results of childs
     */
    int parsePredicateList(PQL2Parser.PredicateListContext predicateListContext) {
      LOGGER.debug("Parsing predicate list: {}", predicateListContext.getText());
      if (predicateListContext.getChildCount() == 1) {
        LOGGER.debug("Parsing parenthesis group");
        return parsePredicate((PQL2Parser.PredicateContext) predicateListContext.getChild(0));
      } else if (predicateListContext.getChild(1).getText().toUpperCase().equals(AND)||predicateListContext.getChild(1).getText().toUpperCase().equals(OR)) {
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
        PQL2Parser.PredicateParenthesisGroupContext predicateParenthesisGroupContext =
            (PQL2Parser.PredicateParenthesisGroupContext) predicateContext;
        return parsePredicateList(predicateParenthesisGroupContext.predicateList());
      } else if (predicateContext instanceof PQL2Parser.InPredicateContext) {
        LOGGER.debug("Entering IN clause!");
        String colName = ((PQL2Parser.InPredicateContext) predicateContext).inClause().expression().getText();
        if(_metaDataProperties.hasInvertedIndex(_tableNameWithoutType,colName)){
          return ((PQL2Parser.InPredicateContext) predicateContext).inClause().literal().size();
        }
        return 0;
      } else if (predicateContext instanceof PQL2Parser.ComparisonPredicateContext) {
        String colName =
            ((PQL2Parser.ComparisonPredicateContext) predicateContext).comparisonClause().expression(0).getText();
        LOGGER.debug("Entering COMP clause!");
        String comparisonOp =
            ((PQL2Parser.ComparisonPredicateContext) predicateContext).comparisonClause().comparisonOperator()
                .getText();
        LOGGER.debug("COMP operator {}", comparisonOp);
        if (comparisonOp.equals("=") || comparisonOp.equals("!=") || comparisonOp.equals("<>")) {
          if(_metaDataProperties.hasInvertedIndex(_tableNameWithoutType,colName)){
            return 1;
          }
        }
        return  0;
      } else {
        return 0;
      }
    }
  }
}
