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
package org.apache.pinot.core.plan;

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
import org.apache.pinot.common.proto.Plan;
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
  private final Map<String, Plan.ExplainNode.AttributeValue> _attributes;
  private final List<RelNode> _inputs;
  private final DataSchema _dataSchema;

  public PinotExplainedRelNode(RelOptCluster cluster, String type,
      Map<String, Plan.ExplainNode.AttributeValue> attributes, DataSchema dataSchema, List<? extends RelNode> inputs) {
    this(cluster, RelTraitSet.createEmpty(), type, attributes, dataSchema, inputs);
  }

  public PinotExplainedRelNode(RelOptCluster cluster, RelTraitSet traitSet, String type,
      Map<String, Plan.ExplainNode.AttributeValue> attributes, DataSchema dataSchema, List<? extends RelNode> inputs) {
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
    return new RelRecordType(fields);
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
    int inputSize = getInputs().size();
    if (inputSize == 1) {
      relWriter.input("input", getInputs().get(0));
    } else if (inputSize > 1) {
      for (int i = 0; i < inputSize; i++) {
        relWriter.input("input#" + i, getInputs().get(i));
      }
    }
    for (Map.Entry<String, Plan.ExplainNode.AttributeValue> entry : _attributes.entrySet()) {
      Plan.ExplainNode.AttributeValue value = entry.getValue();
      switch (value.getValueCase()) {
        case LONG:
          relWriter.item(entry.getKey(), value.getLong());
          break;
        case STRING:
          relWriter.item(entry.getKey(), value.getString());
          break;
        case BOOL:
          relWriter.item(entry.getKey(), value.getBool());
          break;
        case STRINGLIST:
          relWriter.item(entry.getKey(), value.getStringList().getValuesList());
          break;
        default:
          relWriter.item(entry.getKey(), "unknown value");
          break;
      }
    }
    return relWriter;
  }

  public Map<String, Plan.ExplainNode.AttributeValue> getAttributes() {
    return Collections.unmodifiableMap(_attributes);
  }
}
