package com.linkedin.thirdeye.anomaly.server.views;

import io.dropwizard.views.View;

/**
 *
 */
public abstract class AbstractView extends View {

  private final String database;
  private final String functionTable;
  private final String collection;

  protected AbstractView(
      String templateName,
      String database,
      String functionTable,
      String collection) {
    super(templateName);
    this.collection = collection;
    this.functionTable = functionTable;
    this.database = database;
  }

  public String getDatabase() {
    return database;
  }

  public String getFunctionTable() {
    return functionTable;
  }

  public String getCollection() {
    return collection;
  }

}
