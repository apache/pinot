package com.linkedin.thirdeye.anomaly.server.views;

/**
 *
 */
public class AddGenericView extends AbstractView {

  private final String responseUrl;

  public AddGenericView(
      String database,
      String funtionTable,
      String collection,
      String responseUrl)
  {
    super("add-generic-view.ftl", database, funtionTable, collection);
    this.responseUrl = responseUrl;
  }

  public String getResponseUrl() {
    return responseUrl;
  }

}
