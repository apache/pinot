package org.apache.pinot.query.runtime.operator;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.pinot.common.utils.DataSchema;
import org.apache.pinot.query.planner.PlannerUtils;
import org.apache.pinot.query.runtime.operator.operands.TransformOperand;
import org.apache.pinot.query.runtime.operator.operands.TransformOperandFactory;


public class FilterProjectOperand {

//  public static class ProjectOperandAndResultSchema {
//    private final List<TransformOperand> _operands;
//    private final DataSchema _schema;
//    private ProjectOperandAndResultSchema(List<TransformOperand> operands, DataSchema schema) {
//      _operands = operands;
//      _schema = schema;
//    }
//
//    public DataSchema getSchema() {
//      return _schema;
//    }
//
//    public List<TransformOperand> getOperands() {
//      return _operands;
//    }
//  }
//
  public enum FilterProjectOperandsType {
    FILTER,
    PROJECT
  }

  private final FilterProjectOperandsType _type;
  @Nullable
  final TransformOperand _filter;
  @Nullable
  final List<TransformOperand> _project;

  public FilterProjectOperand(PlannerUtils.FilterProjectRex rex, DataSchema inputSchema) {
    if (rex.getType() == PlannerUtils.FilterProjectRexType.FILTER) {
      _type = FilterProjectOperandsType.FILTER;
      _filter = TransformOperandFactory.getTransformOperand(rex.getFilter(), inputSchema);
      _project = null;
    } else {
      _type = FilterProjectOperandsType.PROJECT;
      _filter = null;
      List<TransformOperand> projects = new ArrayList<>();
      rex.getProjectAndResultSchemas().getProject().forEach((x) ->
          projects.add(TransformOperandFactory.getTransformOperand(x, inputSchema)));
      _project = projects;
    }
  }

  public TransformOperand getFilter() {
    return _filter;
  }

  public List<TransformOperand> getProject() {
    return _project;
  }

  public FilterProjectOperandsType getType() {
    return _type;
  }
}

