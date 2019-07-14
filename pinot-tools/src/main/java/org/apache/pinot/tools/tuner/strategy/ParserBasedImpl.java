package org.apache.pinot.tools.tuner.strategy;

import io.vavr.Tuple2;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;
import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.atn.SemanticContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.commons.math.fraction.BigFraction;
import org.apache.pinot.pql.parsers.PQL2Lexer;
import org.apache.pinot.pql.parsers.PQL2Parser;
import org.apache.pinot.tools.tuner.meta.manager.MetaDataProperties;
import org.apache.pinot.tools.tuner.query.src.BasicQueryStats;
import org.apache.pinot.tools.tuner.query.src.IndexSuggestQueryStatsImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.plugin.dom.exception.InvalidStateException;


public class ParserBasedImpl implements BasicStrategy {
  private static final Logger LOGGER = LoggerFactory.getLogger(ParserBasedImpl.class);

  private final static int FIRST_ORDER = 1;
  private final static int THIRD_ORDER = 3;

  private int _algorithmOrder;
  private HashSet<String> _tableNamesWithType;

  private ParserBasedImpl(Builder builder) {
    _algorithmOrder = builder._algorithmOrder;
    _tableNamesWithType = builder._tableNamesWithType;
  }

  public static final class Builder {
    private int _algorithmOrder = FIRST_ORDER;
    private HashSet<String> _tableNamesWithType = new HashSet<>();

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
    public Builder _tableNamesWithType(@Nonnull List<String> val) {
      _tableNamesWithType.addAll(val);
      return this;
    }
  }

  @Override
  public boolean filter(BasicQueryStats queryStats) {
    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    return _tableNamesWithType.isEmpty() ||
        _tableNamesWithType.contains(indexSuggestQueryStatsImpl.getTableNameWithType());
  }

  @Override
  public void accumulator(BasicQueryStats queryStats, MetaDataProperties metaDataProperties, Map<String, Map<String, ColumnStatsObj>> MapperOut) {
    IndexSuggestQueryStatsImpl indexSuggestQueryStatsImpl = (IndexSuggestQueryStatsImpl) queryStats;
    String tableNameWithType = indexSuggestQueryStatsImpl.getTableNameWithType();
    long numEntriesScannedInFilter = indexSuggestQueryStatsImpl.getNumEntriesScannedInFilter();
    String query = indexSuggestQueryStatsImpl.getQuery();

    DimensionScoring dimensionScoring=new DimensionScoring(tableNameWithType, metaDataProperties);
  }

  @Override
  public void merger(ColumnStatsObj p1, ColumnStatsObj p2) {
  }

  @Override
  public void reporter(String tableNameWithType, Map<String, ColumnStatsObj> MergedOut) {

  }

  class DimensionScoring {
    private String _tableNameWithType;
    private MetaDataProperties _metaDataProperties;

    DimensionScoring(String tableNameWithType, MetaDataProperties metaDataProperties) {
      _tableNameWithType = tableNameWithType;
      _metaDataProperties = metaDataProperties;
    }

    /*
     * Navigate from root to predicateListContext of whereClauseContext, where all the filtering happens
     */
    public List<Tuple2<List<String>, BigFraction>> parseQuery(String queryString) {
      PQL2Lexer lexer = new PQL2Lexer(new ANTLRInputStream(queryString));
      PQL2Parser parser = new PQL2Parser(new CommonTokenStream(lexer));
      ParseTree selectStatement = parser.root().statement().selectStatement();

      PQL2Parser.OptionalClauseContext optionalClauseContext = null;
      for (int i = 0; i < selectStatement.getChildCount(); i++) {
        if (selectStatement.getChild(i) instanceof PQL2Parser.OptionalClauseContext) {
          optionalClauseContext = (PQL2Parser.OptionalClauseContext) selectStatement.getChild(i);
        }
      }

      if (optionalClauseContext == null) {
        return null;
      }

      PQL2Parser.WhereClauseContext whereClauseContext = null;
      for (int i = 0; i < optionalClauseContext.getChildCount(); i++) {
        if (optionalClauseContext.getChild(i) instanceof PQL2Parser.WhereClauseContext) {
          whereClauseContext = (PQL2Parser.WhereClauseContext) optionalClauseContext.getChild(i);
        }
      }

      if (whereClauseContext == null) {
        return null;
      }

      PQL2Parser.PredicateListContext predicateListContext = null;
      for (int i = 0; i < whereClauseContext.getChildCount(); i++) {
        if (whereClauseContext.getChild(i) instanceof PQL2Parser.PredicateListContext) {
          predicateListContext = (PQL2Parser.PredicateListContext) whereClauseContext.getChild(i);
        }
      }

      return parsePredicateList(predicateListContext);
    }

    /*
     * Parse predicate list connected by AND and OR (recursively)
     * The score is calculated as:
     *  AND connected: pick the top _algorithmOrder of sorted([([colName],Score(predicate)) for predicate in predicateList])
     *  OR connected: ([colName1]+[colName2]+[colName3], 1/(1/Score(predicate1)+1/Score(predicate2)+1/Score(predicate3))) i.e. Harmonic mean of scores
     */
    List<Tuple2<List<String>, BigFraction>> parsePredicateList(PQL2Parser.PredicateListContext predicateListContext) {
      if (predicateListContext.getChildCount() == 1) {
        return parsePredicate((PQL2Parser.PredicateContext) predicateListContext.getChild(0));
      } else if (predicateListContext.getChild(1) instanceof SemanticContext.AND) {
        List<Tuple2<List<String>, BigFraction>> childResults = new ArrayList<>();
        for (int i = 0; i < predicateListContext.getChildCount(); i += 2) {
          List<Tuple2<List<String>, BigFraction>> childResult =
              parsePredicate((PQL2Parser.PredicateContext) predicateListContext.getChild(i));
          if (childResult != null) {
            childResults.addAll(childResult);
          }
        }

        childResults.sort(Comparator.comparing((Tuple2<List<String>, BigFraction> o) -> o._2()).reversed());
        int childResultsSize = childResults.size();
        int numToReMove = childResultsSize - _algorithmOrder;
        for (int i = 1; i <= numToReMove; i++) {
          childResults.remove(childResultsSize - i);
        }
        return childResults;
      } else if (predicateListContext.getChild(1) instanceof SemanticContext.OR) {
        BigFraction weight = BigFraction.ZERO;
        List<String> colNames = new ArrayList<>();
        for (int i = 0; i < predicateListContext.getChildCount(); i += 2) {
          List<Tuple2<List<String>, BigFraction>> childResult =
              parsePredicate((PQL2Parser.PredicateContext) predicateListContext.getChild(i));
          if (childResult != null) {
            colNames.addAll(childResult.get(0)._1());
            weight.add(childResult.get(0)._2().reciprocal());
          }
        }
        weight = weight.reciprocal();
        List<Tuple2<List<String>, BigFraction>> ret = new ArrayList<>();
        ret.add(new Tuple2<>(colNames, weight));
        return ret;
      } else {
        throw new InvalidStateException("Should not happen");
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
      if (predicateContext instanceof PQL2Parser.PredicateParenthesisGroupContext) {
        PQL2Parser.PredicateParenthesisGroupContext predicateParenthesisGroupContext =
            (PQL2Parser.PredicateParenthesisGroupContext) predicateContext;
        return parsePredicateList((PQL2Parser.PredicateListContext) predicateParenthesisGroupContext.predicateList());
      } else if (predicateContext instanceof PQL2Parser.InPredicateContext) {
        String colName = ((PQL2Parser.InPredicateContext) predicateContext).inClause().expression().toString();
        List<String> colNameList = new ArrayList<>();
        colNameList.add(colName);
        BigFraction cardinality = _metaDataProperties.getAverageCardinality(_tableNameWithType, colName);
        int lenFilter = ((PQL2Parser.InPredicateContext) predicateContext).inClause().literal().size();
        ArrayList<Tuple2<List<String>, BigFraction>> ret = new ArrayList<>();

        if (((PQL2Parser.InPredicateContext) predicateContext).inClause().NOT() != null) {
          ret.add(new Tuple2<>(colNameList, cardinality.divide(cardinality.subtract(lenFilter))));
        } else {
          ret.add(new Tuple2<>(colNameList, cardinality.divide(lenFilter)));
        }
        return ret;
      } else if (predicateContext instanceof PQL2Parser.ComparisonPredicateContext) {
        String colName =
            ((PQL2Parser.ComparisonPredicateContext) predicateContext).comparisonClause().expression(0).toString();
        List<String> colNameList = new ArrayList<>();
        colNameList.add(colName);
        BigFraction cardinality = _metaDataProperties.getAverageCardinality(_tableNameWithType, colName);
        ArrayList<Tuple2<List<String>, BigFraction>> ret = new ArrayList<>();
        String comparisonOp =
            ((PQL2Parser.ComparisonPredicateContext) predicateContext).comparisonClause().comparisonOperator()
                .toString();

        if (comparisonOp.equals("=")) {
          ret.add(new Tuple2<>(colNameList, cardinality));
          return ret;
        } else if (comparisonOp.equals("!=") || comparisonOp.equals("<>")) {
          ret.add(new Tuple2<>(colNameList, cardinality.divide(cardinality.subtract(BigFraction.ONE))));
          return ret;
        } else {
          return null;
        }
      } else {
        return null;
      }
    }
  }
}
