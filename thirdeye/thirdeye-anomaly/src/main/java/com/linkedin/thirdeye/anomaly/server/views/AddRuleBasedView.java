package com.linkedin.thirdeye.anomaly.server.views;


/**
 *
 */
public class AddRuleBasedView extends AbstractView {

  private final String responseUrl;

  public AddRuleBasedView(
      String database,
      String funtionTable,
      String collection,
      String responseUrl)
  {
    super("add-rulebased-view.ftl", database, funtionTable, collection);
    this.responseUrl = responseUrl;
  }

  public String getResponseUrl() {
    return responseUrl;
  }

}
