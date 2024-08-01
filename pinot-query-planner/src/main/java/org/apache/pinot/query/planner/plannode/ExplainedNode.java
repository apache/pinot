package org.apache.pinot.query.planner.plannode;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;


/**
 * The {@link PlanNode} version for the {@link org.apache.pinot.core.plan.PinotExplainedRelNode}.
 *
 * Remember that {@link PlanNode} are just the serializable and deserializable version of a
 * {@link org.apache.calcite.rel.RelNode}.
 */
public class ExplainedNode extends BasePlanNode {

  private final String _type;
  private final Map<String, String> _attributes;

  public ExplainedNode(int stageId, DataSchema dataSchema, @Nullable NodeHint nodeHint, List<PlanNode> inputs,
      String type, Map<String, String> attributes) {
    super(stageId, dataSchema, nodeHint, inputs);
    _type = type;
    _attributes = attributes;
  }

  public String getType() {
    return _type;
  }

  public Map<String, String> getAttributes() {
    return _attributes;
  }

  @Override
  public String explain() {
    return _type;
  }

  @Override
  public <T, C> T visit(PlanNodeVisitor<T, C> visitor, C context) {
    return visitor.visitExplained(this, context);
  }

  @Override
  public PlanNode withInputs(List<PlanNode> inputs) {
    return new ExplainedNode(_stageId, _dataSchema, _nodeHint, inputs, _type, _attributes);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ExplainedNode)) {
      return false;
    }
    if (!super.equals(o)) {
      return false;
    }
    ExplainedNode that = (ExplainedNode) o;
    return Objects.equals(_type, that._type) && Objects.equals(_attributes, that._attributes);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), _type, _attributes);
  }
}
