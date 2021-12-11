package org.apache.pinot.query.logical;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;


public class QueryPlanner extends HepPlanner {

  private List<RelTraitDef> traitDefs;

  public QueryPlanner(HepProgram program, Context context) {
    super(program, context);
    this.traitDefs = new ArrayList();
  }

  @Override
  public boolean addRelTraitDef(RelTraitDef relTraitDef) {
    return !this.traitDefs.contains(relTraitDef) && this.traitDefs.add(relTraitDef);
  }

  @Override
  public List<RelTraitDef> getRelTraitDefs() {
    return traitDefs;
  }

  @Override
  public RelTraitSet emptyTraitSet() {
    RelTraitSet traitSet = super.emptyTraitSet();
    for (RelTraitDef traitDef : traitDefs) {
      if (traitDef.multiple()) {
        // not supported
      }
      traitSet = traitSet.plus(traitDef.getDefault());
    }
    return traitSet;
  }
}
