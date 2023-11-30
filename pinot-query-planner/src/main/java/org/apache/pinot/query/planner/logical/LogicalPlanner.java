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
package org.apache.pinot.query.planner.logical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;


/**
 * The {@code LogicalPlanner} is an extended implementation of the Calcite's {@link HepPlanner}.
 */
public class LogicalPlanner extends HepPlanner {

  private List<RelTraitDef> _traitDefs;

  public LogicalPlanner(HepProgram program, Context context) {
    this(program, context, new ArrayList<>());
  }

  public LogicalPlanner(HepProgram program, Context context, List<RelTraitDef> traitDefs) {
    super(program, context);
    _traitDefs = traitDefs;
  }

  @Override
  public boolean addRelTraitDef(RelTraitDef relTraitDef) {
    return !_traitDefs.contains(relTraitDef) && _traitDefs.add(relTraitDef);
  }

  @Override
  public List<RelTraitDef> getRelTraitDefs() {
    return _traitDefs;
  }

  @Override
  public RelTraitSet emptyTraitSet() {
    RelTraitSet traitSet = super.emptyTraitSet();
    for (RelTraitDef traitDef : _traitDefs) {
      if (traitDef.multiple()) {
        // not supported
      }
      traitSet = traitSet.plus(traitDef.getDefault());
    }
    return traitSet;
  }
}
