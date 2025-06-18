package org.apache.pinot.query.runtime.operator;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.sql.SqlKind;
import org.apache.pinot.common.datablock.DataBlock;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.logical.RexExpression;
import org.apache.pinot.query.planner.plannode.EnrichedJoinNode;
import org.apache.pinot.query.planner.plannode.JoinNode;
import org.apache.pinot.query.planner.plannode.PlanNode;
import org.apache.pinot.query.routing.VirtualServerAddress;
import org.apache.pinot.query.runtime.blocks.MseBlock;
import org.mockito.Mock;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static org.mockito.Mockito.when;
import static org.mockito.MockitoAnnotations.openMocks;
import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertTrue;


public class EnrichedHashJoinOperatorTest {
  private AutoCloseable _mocks;
  private MultiStageOperator _leftInput;
  private MultiStageOperator _rightInput;
  @Mock
  private VirtualServerAddress _serverAddress;

  private static final DataSchema DEFAULT_CHILD_SCHEMA = new DataSchema(new String[]{"int_col", "string_col"},
      new DataSchema.ColumnDataType[] {DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
  @BeforeMethod
  public void setUp() {
    _mocks = openMocks(this);
    when(_serverAddress.toString()).thenReturn(new VirtualServerAddress("mock", 80, 0).toString());
  }

  @AfterMethod
  public void tearDown()
      throws Exception {
    _mocks.close();
  }

  @Test
  public void shouldHandleBasicInnerJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});
    HashJoinOperator operator = getBasicOperator(resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of());
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(2), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleInnerJoinWithTrueFilter() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, RexExpression.Literal.TRUE, null, null, 0, 0);
    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, "Aa", 2, "Aa"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(2), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleInnerJoinWithFalseFilter() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, RexExpression.Literal.FALSE, null, null, 0, 0);
    assertTrue(operator.nextBlock().isSuccess());
  }

  @Test
  public void shouldHandleInnerJoinWithFuncCallFilter() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(1), new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "B")));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, startsWith, null, null, 0, 0);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 3, "BB"});
  }

  // project tests ----
  @Test
  public void shouldHandleRefProject() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    DataSchema projectedSchema = new DataSchema(
        new String[]{"int_col1", "int_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});

    List<RexExpression> projects = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));

    HashJoinOperator operator = getProjectedOperator(DEFAULT_CHILD_SCHEMA, resultSchema, projectedSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, null, projects, null, 0, 0);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{1, 2});
    assertEquals(resultRows.get(1), new Object[]{2, 2});
    assertEquals(resultRows.get(2), new Object[]{2, 3});
  }

  @Test
  public void shouldHandlePlusMinusTransform() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    DataSchema projectSchema = new DataSchema(
        new String[]{"sum", "diff"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.INT});

    List<RexExpression> operands = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));

    List<RexExpression> projects =
        List.of(new RexExpression.FunctionCall(DataSchema.ColumnDataType.DOUBLE, SqlKind.PLUS.name(), operands),
            new RexExpression.FunctionCall(DataSchema.ColumnDataType.DOUBLE, SqlKind.MINUS.name(), operands));

    HashJoinOperator operator = getProjectedOperator(DEFAULT_CHILD_SCHEMA, resultSchema, projectSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, null, projects, null, 0, 0);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{3.0, -1.0});
    assertEquals(resultRows.get(1), new Object[]{4.0, 0.0});
    assertEquals(resultRows.get(2), new Object[]{5.0, -1.0});
  }

  @Test
  public void shouldHandleInnerJoinWithSort() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // sort limit test
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST), new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, null, null, collations, 0, 0);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{2, "BB", 3, "BB"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(2), new Object[]{1, "Aa", 2, "Aa"});
  }

  @Test
  public void shouldHandleInnerJoinWithSortLimit() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // sort limit test
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST), new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, null, null, collations, 2, 0);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2, "BB", 3, "BB"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 2, "BB"});
  }

  @Test
  public void shouldHandleInnerJoinWithSortLimitOffset() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // sort limit test
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST), new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, null, null, collations, 2, 1);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(1), new Object[]{1, "Aa", 2, "Aa"});
  }

  // mixed filter - project - sortLimit
  @Test
  public void shouldHandleInnerJoinWithSortLimitOffsetOnProjectedColumn() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(3, "BB")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // project
    List<RexExpression> operands = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));
    List<RexExpression> projects =
        List.of(new RexExpression.FunctionCall(DataSchema.ColumnDataType.DOUBLE, SqlKind.PLUS.name(), operands),
            new RexExpression.FunctionCall(DataSchema.ColumnDataType.DOUBLE, SqlKind.MINUS.name(), operands));

    // sort limit test
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST), new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, null, projects, collations, 2, 1);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{4.0, 0.0});
    assertEquals(resultRows.get(1), new Object[]{3.0, -1.0});
  }

  @Test
  public void shouldHandleInnerJoinFilteredProjected() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // filter
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(1), new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "B")));

    // project
    List<RexExpression> projects = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, startsWith, projects, null, -1, -1);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 3);
  }

  @Test
  public void shouldHandleInnerJoinWithSortLimitOffsetOnFilteredColumn() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // filter
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(1), new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "B")));

    // sort limit test
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST), new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, startsWith, null, collations, 2, 0);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{3, "Bc", 2, "Bc"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 3, "BB"});
  }

  @Test
  public void shouldHandleInnerJoinWithSortLimitOffsetOnFilteredProjectedColumn() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // filter
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(1), new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "B")));

    // project
    List<RexExpression> projects = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));

    // sort limit, note that the RelFieldCollation's fieldIndex should be acc. to the projected row
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST), new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, startsWith, projects, collations, 2, 0);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{3, 2});
    assertEquals(resultRows.get(1), new Object[]{2, 3});
  }

  @Test
  public void shouldHandleRightTableUniqueInnerJoinWithSortLimitOffsetOnFilteredProjectedColumn() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // filter
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(1), new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "B")));

    // project
    List<RexExpression> projects = List.of(new RexExpression.InputRef(0), new RexExpression.InputRef(2));

    // sort limit, note that the RelFieldCollation's fieldIndex should be acc. to the projected row
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST), new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.INNER, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, startsWith, projects, collations, 0, 0);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{3, 2});
    assertEquals(resultRows.get(1), new Object[]{2, 2});
  }

  @Test
  public void shouldHandleLeftOuterJoinWithSortOnPreserveSide() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(4, "Bd")
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // filter
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(1), new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "B")));

    // sort limit test
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.LEFT, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, startsWith, null, collations, 2, 0);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{4, "Bd", null, null});
    assertEquals(resultRows.get(1), new Object[]{3, "Bc", 2, "Bc"});
  }

  @Test
  public void shouldHandleLeftOuterJoinWithSortOnNonPreserveSide() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(4, "Bd")
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // filter
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(1), new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "B")));

    // sort limit test
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(3, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST),
        new RelFieldCollation(2, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.LEFT, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, startsWith, null, collations, -1, -1);

    List<Object[]> resultRows = ((MseBlock.Data) operator.nextBlock()).asRowHeap().getRows();
    assertEquals(resultRows.size(), 4);
    assertEquals(resultRows.get(0), new Object[]{3, "Bc", 2, "Bc"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB", 3, "BB"});
    assertEquals(resultRows.get(2), new Object[]{2, "BB", 2, "BB"});
    assertEquals(resultRows.get(3), new Object[]{4, "Bd", null, null});
  }

  @Test
  public void shouldHandleBasicRightOuterJoin() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(4, "Bd")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.RIGHT, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, null, null, null, -1, -1);
    List<Object[]> resultRows = new ArrayList<>();
    MseBlock resultBlock = operator.nextBlock();
    while (!resultBlock.isEos()) {
      resultRows.addAll(((MseBlock.Data) resultBlock).asRowHeap().getRows());
      resultBlock = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 5);
  }

  @Test
  public void shouldHandleRightOuterJoinWithSortOnNonPreserveSide() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(4, "Bd")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // filter
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(3), new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "B")));

    // sort limit test
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST),
        new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.RIGHT, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, startsWith, null, collations, -1, -1);

    List<Object[]> resultRows = new ArrayList<>();
    MseBlock resultBlock = operator.nextBlock();
    while (!resultBlock.isEos()) {
      resultRows.addAll(((MseBlock.Data) resultBlock).asRowHeap().getRows());
      resultBlock = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 4);
    assertEquals(resultRows.get(0), new Object[]{3, "Bc", 2, "Bc"});
    assertEquals(resultRows.get(3), new Object[]{null, null, 4, "Bd"});
  }

  @Test
  public void shouldHandleRightOuterJoinWithFilterOnNonPreserveSide() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(1, "Aa")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(4, "Bd")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // filter
    RexExpression.FunctionCall startsWith =
        new RexExpression.FunctionCall(DataSchema.ColumnDataType.BOOLEAN, SqlKind.STARTS_WITH.name(),
            List.of(new RexExpression.InputRef(1), new RexExpression.Literal(DataSchema.ColumnDataType.STRING, "B")));

    // sort limit test
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST),
        new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.RIGHT, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, startsWith, null, collations, -1, -1);

    List<Object[]> resultRows = new ArrayList<>();
    MseBlock resultBlock = operator.nextBlock();
    while (!resultBlock.isEos()) {
      resultRows.addAll(((MseBlock.Data) resultBlock).asRowHeap().getRows());
      resultBlock = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{3, "Bc", 2, "Bc"});
  }

  @Test
  public void shouldHandleFullOuterJoinWithFilterOnNonPreserveSide() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(1, "Aa")
        .addRow(4, "Be")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(4, "Bd")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // sort limit test
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST),
        new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.FULL, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, null, null, collations, -1, -1);

    List<Object[]> resultRows = new ArrayList<>();
    MseBlock resultBlock = operator.nextBlock();
    while (!resultBlock.isEos()) {
      resultRows.addAll(((MseBlock.Data) resultBlock).asRowHeap().getRows());
      resultBlock = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 6);
    assertEquals(resultRows.get(0), new Object[]{4, "Be", null, null});
    assertEquals(resultRows.get(5), new Object[]{null, null, 4, "Bd"});
  }

  @Test
  public void shouldHandleAntiJoinWithSort() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(1, "Aa")
        .addRow(4, "Be")
        .addRow(2, "Bf")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(4, "Bd")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // sort limit test
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST),
        new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.ANTI, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, null, null, collations, -1, -1);

    List<Object[]> resultRows = new ArrayList<>();
    MseBlock resultBlock = operator.nextBlock();
    while (!resultBlock.isEos()) {
      resultRows.addAll(((MseBlock.Data) resultBlock).asRowHeap().getRows());
      resultBlock = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 2);
    assertEquals(resultRows.get(0), new Object[]{2, "Bf"});
    assertEquals(resultRows.get(1), new Object[]{4, "Be"});
  }

  @Test
  public void shouldHandleSemiJoinWithSort() {
    _leftInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "Bc")
        .addRow(1, "Aa")
        .addRow(4, "Be")
        .addRow(2, "BB")
        .buildWithEos();

    _rightInput = new BlockListMultiStageOperator.Builder(DEFAULT_CHILD_SCHEMA)
        .addRow(3, "BB")
        .addRow(4, "Bd")
        .addRow(2, "Aa")
        .addRow(2, "BB")
        .addRow(2, "Bc")
        .buildWithEos();
    DataSchema resultSchema = new DataSchema(
        new String[]{"int_col1", "string_col1", "int_col2", "string_col2"},
        new DataSchema.ColumnDataType[]{DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING, DataSchema.ColumnDataType.INT, DataSchema.ColumnDataType.STRING});

    // sort limit test
    List<RelFieldCollation> collations = List.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST),
        new RelFieldCollation(0, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST));

    HashJoinOperator operator = getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, JoinRelType.SEMI, List.of(1), List.of(1), List.of(),
        PlanNode.NodeHint.EMPTY, null, null, null, collations, -1, -1);

    List<Object[]> resultRows = new ArrayList<>();
    MseBlock resultBlock = operator.nextBlock();
    while (!resultBlock.isEos()) {
      resultRows.addAll(((MseBlock.Data) resultBlock).asRowHeap().getRows());
      resultBlock = operator.nextBlock();
    }
    assertEquals(resultRows.size(), 3);
    assertEquals(resultRows.get(0), new Object[]{3, "Bc"});
    assertEquals(resultRows.get(1), new Object[]{2, "BB"});
    assertEquals(resultRows.get(2), new Object[]{1, "Aa"});
  }

  // utils ----
  private EnrichedHashJoinOperator getOperatorNoProject(DataSchema leftSchema, DataSchema resultSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions, PlanNode.NodeHint nodeHint,
      RexExpression matchCondition, RexExpression filterCondition, List<RexExpression> projects, List<RelFieldCollation> collations,
      int fetch, int offset
  ) {
    return new EnrichedHashJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, leftSchema, _rightInput,
        new EnrichedJoinNode(-1, resultSchema, resultSchema, nodeHint, List.of(), joinType, leftKeys, rightKeys, nonEquiConditions,
            JoinNode.JoinStrategy.HASH, matchCondition, filterCondition, projects, collations, fetch, offset));
  }

  private EnrichedHashJoinOperator getProjectedOperator(DataSchema leftSchema, DataSchema joinSchema, DataSchema projectSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions, PlanNode.NodeHint nodeHint,
      RexExpression matchCondition, RexExpression filterCondition, List<RexExpression> projects, List<RelFieldCollation> collations,
      int fetch, int offset
  ) {
    return new EnrichedHashJoinOperator(OperatorTestUtil.getTracingContext(), _leftInput, leftSchema, _rightInput,
        new EnrichedJoinNode(-1, joinSchema, projectSchema, nodeHint, List.of(), joinType, leftKeys, rightKeys, nonEquiConditions,
            JoinNode.JoinStrategy.HASH, matchCondition, filterCondition, projects, collations, fetch, offset));
  }

  private HashJoinOperator getBasicOperator(DataSchema resultSchema, JoinRelType joinType,
      List<Integer> leftKeys, List<Integer> rightKeys, List<RexExpression> nonEquiConditions) {
    return getOperatorNoProject(DEFAULT_CHILD_SCHEMA, resultSchema, joinType, leftKeys, rightKeys, nonEquiConditions,
        PlanNode.NodeHint.EMPTY, null, null, null, null, 0, 0);
  }

}
