package com.linkedin.pinot.query.request;

import org.json.JSONObject;


/**
 * GroupBy query will take an array of columns and the GroupByOrder with the
 * number of groups.
 * 
 * @author xiafu
 *
 */
public class GroupBy {
  private String[] columns;
  private GroupByOrder orderBy;
  private int limit;

  public String[] getColumns() {
    return columns;
  }

  public void setColumns(String[] columns) {
    this.columns = columns;
  }

  public GroupByOrder getOrderBy() {
    return orderBy;
  }

  public void setOrderBy(GroupByOrder orderBy) {
    this.orderBy = orderBy;
  }

  public int getLimit() {
    return limit;
  }

  public void setLimit(int limit) {
    this.limit = limit;
  }

  public static GroupBy fromJson(JSONObject jsonObject) {
    GroupBy groupBy = new GroupBy();
    groupBy.setColumns(jsonObject.getString("groupByColumns").split(","));
    groupBy.setOrderBy(GroupByOrder.valueOf(jsonObject.getString("groupByOrder")));
    groupBy.setLimit(jsonObject.getInt("limit"));
    return groupBy;
  }
}
