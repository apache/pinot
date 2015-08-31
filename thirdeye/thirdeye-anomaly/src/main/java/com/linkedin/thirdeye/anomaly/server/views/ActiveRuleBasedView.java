package com.linkedin.thirdeye.anomaly.server.views;

import java.util.List;

import com.linkedin.thirdeye.anomaly.rulebased.RuleBasedFunctionTableRow;

/**
 *
 */
public class ActiveRuleBasedView extends AbstractView {

  private final List<RuleBasedFunctionTableRow> functions;

  public ActiveRuleBasedView(
      String database,
      String functionTable,
      String collection,
      List<RuleBasedFunctionTableRow> functions) {
    super("active-rulebased-view.ftl", database, functionTable, collection);
    this.functions = functions;
  }

  public List<RuleBasedFunctionTableRow> getFunctions() {
    return functions;
  }

}
