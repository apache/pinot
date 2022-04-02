package org.apache.pinot.query.planner.nodes;

import org.apache.pinot.common.proto.Plan;


public interface ProtoSerializable {

  void setFields(Plan.ObjectFields objFields);

  Plan.ObjectFields getFields();
}
