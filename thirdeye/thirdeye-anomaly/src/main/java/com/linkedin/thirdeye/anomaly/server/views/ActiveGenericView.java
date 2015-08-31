package com.linkedin.thirdeye.anomaly.server.views;

import java.util.List;

import com.linkedin.thirdeye.anomaly.generic.GenericFunctionTableRow;

/**
 *
 */
public class ActiveGenericView extends AbstractView {

  private final List<GenericFunctionTableRow> functions;

  public ActiveGenericView(
      String database,
      String functionTable,
      String collection,
      List<GenericFunctionTableRow> functions) {
    super("active-generic-view.ftl", database, functionTable, collection);
    this.functions = functions;
  }

  public List<GenericFunctionTableRow> getFunctions() {
    return functions;
  }

}
