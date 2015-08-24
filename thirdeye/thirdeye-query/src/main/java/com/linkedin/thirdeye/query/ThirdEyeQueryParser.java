package com.linkedin.thirdeye.query;

import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.TimeGranularity;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import org.joda.time.format.ISODateTimeFormat;

import java.io.FileInputStream;
import java.io.StringReader;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class ThirdEyeQueryParser implements
    SelectVisitor,
    SelectItemVisitor,
    FromItemVisitor,
    ExpressionVisitor,
    ItemsListVisitor {

  private enum TerminalState {
    BETWEEN_LEFT,
    BETWEEN_START,
    BETWEEN_END,
    SELECT_ITEM,
    DIMENSION_EQUALS_LEFT,
    DIMENSION_EQUALS_RIGHT,
    OR,
    GROUP_BY,
    FUNCTION_MOVING_AVERAGE,
    FUNCTION_AGGREGATE,
    FUNCTION_SUM,
    FUNCTION_RATIO
  }

  private final String sql;
  private final ThirdEyeQuery query = new ThirdEyeQuery();
  private final Stack<TerminalState> terminalState = new Stack<>();

  private String currentDimension;
  private String currentValue;

  private List<String> functionMetrics;

  public ThirdEyeQueryParser(String sql) throws Exception {
    this.sql = sql;

    CCJSqlParserManager parserManager = new CCJSqlParserManager();
    Statement statement = parserManager.parse(new StringReader(sql));
    ((Select) statement).getSelectBody().accept(this);

    if (query.getStart() == null || query.getEnd() == null) {
      throw new Exception("Must provide BETWEEN clause");
    }
  }

  public ThirdEyeQuery getQuery() {
    return query;
  }

  @Override
  public void visit(PlainSelect plainSelect) {
    // Collection
    plainSelect.getFromItem().accept(this);

    // Metrics
    for (SelectItem selectItem : plainSelect.getSelectItems()) {
      selectItem.accept(this);
    }

    // Time / dimensions
    plainSelect.getWhere().accept(this);

    // Group by
    if (plainSelect.getGroupByColumnReferences() != null) {
      terminalState.push(TerminalState.GROUP_BY);
      for (Expression groupBy : plainSelect.getGroupByColumnReferences()) {
        groupBy.accept(this);
      }
      terminalState.pop();
    }
  }

  @Override
  public void visit(Table table) {
    query.setCollection(table.getName());
  }

  @Override
  public void visit(SubJoin subJoin) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(LateralSubSelect lateralSubSelect) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(ValuesList valuesList) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(SetOperationList setOperationList) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(WithItem withItem) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(NullValue nullValue) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(Function function) {
    functionMetrics = new ArrayList<>();
    if (function.getName().startsWith("MOVING_AVERAGE")) {
      terminalState.push(TerminalState.FUNCTION_MOVING_AVERAGE);
      function.getParameters().accept(this);
      TimeGranularity window = parseTimeGranularity(function.getName());
      query.addFunction(new ThirdEyeMovingAverageFunction(window));
    } else if (function.getName().startsWith("AGGREGATE_")) {
      terminalState.push(TerminalState.FUNCTION_AGGREGATE);
      function.getParameters().accept(this);
      TimeGranularity window = parseTimeGranularity(function.getName());
      query.addFunction(new ThirdEyeAggregateFunction(window));
    } else if ("SUM".equals(function.getName())) {
      terminalState.push(TerminalState.FUNCTION_SUM);
      function.getParameters().accept(this);
      query.addFunction(new ThirdEyeSumFunction(functionMetrics));
    } else if ("RATIO".equals(function.getName())) {
      terminalState.push(TerminalState.FUNCTION_RATIO);
      function.getParameters().accept(this);
      query.addDerivedMetric(new ThirdEyeRatioFunction(functionMetrics));
    } else {
      throw new IllegalStateException("Invalid SQL: " + sql);
    }
    terminalState.pop();
  }

  @Override
  public void visit(SignedExpression signedExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(JdbcParameter jdbcParameter) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(JdbcNamedParameter jdbcNamedParameter) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(DoubleValue doubleValue) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(LongValue longValue) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(DateValue dateValue) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(TimeValue timeValue) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(TimestampValue timestampValue) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(Parenthesis parenthesis) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(StringValue stringValue) {
    switch (terminalState.peek()) {
      case DIMENSION_EQUALS_RIGHT:
        currentValue = stringValue.getValue();
        return;
      case BETWEEN_START:
        query.setStart(ISODateTimeFormat.dateTimeParser().parseDateTime(stringValue.getValue()));
        return;
      case BETWEEN_END:
        query.setEnd(ISODateTimeFormat.dateTimeParser().parseDateTime(stringValue.getValue()));
        return;
      case SELECT_ITEM:
        query.addMetricName(stringValue.getValue());
        return;
      case GROUP_BY:
        query.addGroupByColumn(stringValue.getValue());
        return;
      case FUNCTION_AGGREGATE:
      case FUNCTION_MOVING_AVERAGE:
        query.addMetricName(stringValue.getValue());
        return;
      case FUNCTION_SUM:
      case FUNCTION_RATIO:
        functionMetrics.add(stringValue.getValue());
        return;
    }
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(Addition addition) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(Division division) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(Multiplication multiplication) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(Subtraction subtraction) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(AndExpression andExpression) {
    andExpression.getLeftExpression().accept(this);
    andExpression.getRightExpression().accept(this);
  }

  @Override
  public void visit(OrExpression orExpression) {
    terminalState.push(TerminalState.OR);
    orExpression.getLeftExpression().accept(this);
    orExpression.getRightExpression().accept(this);
    terminalState.pop();
  }

  @Override
  public void visit(Between between) {
    terminalState.push(TerminalState.BETWEEN_LEFT);
    between.getLeftExpression().accept(this);
    terminalState.pop();

    terminalState.push(TerminalState.BETWEEN_START);
    between.getBetweenExpressionStart().accept(this);
    terminalState.pop();

    terminalState.push(TerminalState.BETWEEN_END);
    between.getBetweenExpressionEnd().accept(this);
    terminalState.pop();
  }

  @Override
  public void visit(EqualsTo equalsTo) {
    terminalState.push(TerminalState.DIMENSION_EQUALS_LEFT);
    equalsTo.getLeftExpression().accept(this);
    terminalState.pop();

    terminalState.push(TerminalState.DIMENSION_EQUALS_RIGHT);
    equalsTo.getRightExpression().accept(this);
    terminalState.pop();

    query.addDimensionValue(currentDimension, currentValue);
  }

  @Override
  public void visit(GreaterThan greaterThan) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(GreaterThanEquals greaterThanEquals) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(InExpression inExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(IsNullExpression isNullExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(LikeExpression likeExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(MinorThan minorThan) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(MinorThanEquals minorThanEquals) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(NotEqualsTo notEqualsTo) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(Column column) {
    switch (terminalState.peek()) {
      case BETWEEN_LEFT:
        if (!"time".equals(column.getColumnName())) {
          throw new IllegalStateException("Invalid time column " + column.getColumnName() + " (must be 'time')");
        }
        return;
      case SELECT_ITEM:
        query.addMetricName(column.getColumnName());
        return;
      case DIMENSION_EQUALS_LEFT:
        currentDimension = column.getColumnName();
        return;
      case GROUP_BY:
        query.addGroupByColumn(column.getColumnName());
        return;
      case FUNCTION_MOVING_AVERAGE:
      case FUNCTION_AGGREGATE:
        query.addMetricName(column.getColumnName());
      case FUNCTION_SUM:
      case FUNCTION_RATIO:
        functionMetrics.add(column.getColumnName());
        return;
    }
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(SubSelect subSelect) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(CaseExpression caseExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(WhenClause whenClause) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(ExistsExpression existsExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(AllComparisonExpression allComparisonExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(AnyComparisonExpression anyComparisonExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(Concat concat) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(Matches matches) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(BitwiseAnd bitwiseAnd) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(BitwiseOr bitwiseOr) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(BitwiseXor bitwiseXor) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(CastExpression castExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(Modulo modulo) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(AnalyticExpression analyticExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(ExtractExpression extractExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(IntervalExpression intervalExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(RegExpMatchOperator regExpMatchOperator) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(JsonExpression jsonExpression) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  @Override
  public void visit(RegExpMySQLOperator regExpMySQLOperator) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  // SELECT item handlers

  @Override
  public void visit(AllColumns allColumns) {
    throw new IllegalStateException("Invalid SQL (cannot specify all): " + sql);
  }

  @Override
  public void visit(AllTableColumns allTableColumns) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }



  @Override
  public void visit(SelectExpressionItem selectExpressionItem) {
    terminalState.push(TerminalState.SELECT_ITEM);
    selectExpressionItem.getExpression().accept(this);
    terminalState.pop();
  }

  @Override
  public void visit(ExpressionList expressionList) {
    for (Expression expression : expressionList.getExpressions()) {
      expression.accept(this);
    }
  }

  @Override
  public void visit(MultiExpressionList multiExprList) {
    throw new IllegalStateException("Invalid SQL: " + sql);
  }

  private static TimeGranularity parseTimeGranularity(String functionName) {
    String[] tokens = functionName.split("_");
    return new TimeGranularity(Integer.valueOf(tokens[tokens.length - 2]), TimeUnit.valueOf(tokens[tokens.length - 1]));
  }
}
