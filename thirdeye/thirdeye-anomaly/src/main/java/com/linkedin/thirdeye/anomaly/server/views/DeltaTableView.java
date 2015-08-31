package com.linkedin.thirdeye.anomaly.server.views;

import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.anomaly.util.DimensionKeyMatchTable;
import com.linkedin.thirdeye.api.DimensionKey;
import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;

/**
 *
 */
public class DeltaTableView extends AbstractView {

  private final List<String> columnNames;
  private final List<List<String>> rows;
  private final String responseUrl;

  public DeltaTableView(
      StarTreeConfig starTreeConfig,
      DimensionKeyMatchTable<Double> matchTable,
      String responseUrl,
      String database,
      String funtionTable,
      String collection) {
    super("delta-table-view.ftl", database, funtionTable, collection);
    this.responseUrl = responseUrl;

    columnNames = new ArrayList<>(starTreeConfig.getDimensions().size() + 1);
    columnNames.add("delta");
    for (DimensionSpec ds : starTreeConfig.getDimensions()) {
      String name = ds.getName();
      columnNames.add(name);
    }

    rows = new ArrayList<>(matchTable.size());
    for (DimensionKey dimensionKey : matchTable.keySet()) {
      List<String> row = new ArrayList<>(dimensionKey.getDimensionValues().length + 1);
      row.add("" + matchTable.get(dimensionKey));
      for (int i = 0; i < dimensionKey.getDimensionValues().length; i++) {
        row.add(dimensionKey.getDimensionValues()[i]);
      }
      rows.add(row);
    }
  }

  public List<String> getColumnNames() {
    return columnNames;
  }

  public List<List<String>> getRows() {
    return rows;
  }

  public String getResponseUrl() {
    return responseUrl;
  }
}
