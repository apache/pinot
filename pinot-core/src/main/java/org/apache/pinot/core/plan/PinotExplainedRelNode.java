package org.apache.pinot.core.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rel.type.StructKind;
import org.apache.pinot.common.utils.DataSchema;


/**
 * This class is a hackish way to adapt Pinot explain plan into Calcite explain plan.
 *
 * While in Calcite both logical and physical operators are represented by classes that extend RelNode, in multi-stage
 * Pinot plans we first optimize {@link RelNode RelNodes} at logical level in the broker and then we transform send
 * these RelNodes to the server (using {@code org.apache.pinot.query.planner.plannode.PlanNode} as intermediate format).
 *
 * Servers then extract leaf nodes from the stage, trying to execute as much as possible in a leaf operator. In that
 * leaf operator is where nodes are compiled into single-stage physical operators
 * (aka {@link org.apache.pinot.core.operator.BaseOperator}), which include important information like whether indexes
 * are being used or not.
 *
 * {@link PinotExplainedRelNode} is a way to represent these single-stage operators in Calcite.
 * <b>They are not meant to be executed</b>.
 * Instead, when physical explain is required, the broker ask for the final plan to each server. They return
 * a PlanNode and the broker then converts these PlanNodes into a PinotExplainedRelNode. The logical plan is then
 * modified to substitute the nodes that were converted into single-stage physical plans with the corresponding
 * PinotExplainedRelNode.
 */
public class PinotExplainedRelNode extends AbstractRelNode {

  /**
   * The name of this node, whose role is like a title in the explain plan.
   */
  private final String _type;
  private final Map<String, ?> _attributes;
  private final List<RelNode> _inputs;
  private final DataSchema _dataSchema;

  public PinotExplainedRelNode(RelOptCluster cluster, RelTraitSet traitSet, String type,
      Map<String, ?> attributes, DataSchema dataSchema, RelNode input) {
    this(cluster, traitSet, type, attributes, dataSchema, Lists.newArrayList(input));
  }

  public PinotExplainedRelNode(RelOptCluster cluster, RelTraitSet traitSet, String type,
      Map<String, ?> attributes, DataSchema dataSchema, List<? extends RelNode> inputs) {
    super(cluster, traitSet);
    _type = type;
    _attributes = attributes;
    _inputs = new ArrayList<>(inputs);
    _dataSchema = dataSchema;
  }

  @Override
  protected RelDataType deriveRowType() {
    int size = _dataSchema.size();
    List<RelDataTypeField> fields = new ArrayList<>(size);
    RelDataTypeFactory typeFactory = getCluster().getTypeFactory();
    for (int i = 0; i < size; i++) {
      String columnName = _dataSchema.getColumnName(i);

      DataSchema.ColumnDataType columnDataType = _dataSchema.getColumnDataType(i);
      RelDataType type = columnDataType.toType(typeFactory);

      fields.add(new RelDataTypeFieldImpl(columnName, i, type));
    }
    return new RelRecordType(StructKind.FULLY_QUALIFIED, fields, false);
  }

  @Override
  public List<RelNode> getInputs() {
    return Collections.unmodifiableList(_inputs);
  }

  @Override
  public void replaceInput(int ordinalInParent, RelNode p) {
    _inputs.set(ordinalInParent, p);
  }

  @Override
  public String getRelTypeName() {
    return _type;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    RelWriter relWriter = super.explainTerms(pw);
    relWriter.item("type", _type);
    for (Map.Entry<String, ?> entry : _attributes.entrySet()) {
      relWriter.item(entry.getKey(), entry.getValue());
    }
    return relWriter;
  }

  public static class Info {
    private final String _type;
    private final Map<String, Object> _attributes;
    private final List<Info> _inputs;

    public Info(String type) {
      _type = type;
      _attributes = Collections.emptyMap();
      _inputs = Collections.emptyList();
    }

    @JsonCreator
    public Info(String type, Map<String, Object> attributes, List<Info> inputs) {
      _type = type;
      _attributes = attributes;
      _inputs = inputs;
    }

    public String getType() {
      return _type;
    }

    public Map<String, Object> getAttributes() {
      return _attributes;
    }

    public List<Info> getInputs() {
      return _inputs;
    }
  }
}
