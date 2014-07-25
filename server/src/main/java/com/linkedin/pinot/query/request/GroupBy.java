package com.linkedin.pinot.query.request;

import java.util.Arrays;
import java.util.List;

import org.json.JSONException;
import org.json.JSONObject;


/**
 * Group-by field: the field name used for the group-by operation.
 * 
 * @author xiafu
 *
 */
public class GroupBy {
  private List<String> _columns;
  private int _top;

  public GroupBy() {
  }

  public GroupBy(List<String> columns, int top) {
    this._columns = columns;
    this.setTop(top);
  }

  public List<String> getColumns() {
    return _columns;
  }

  public void setColumns(List<String> columns) {
    this._columns = columns;
  }

  public int getTop() {
    return _top;
  }

  public void setTop(int top) {
    this._top = top;
  }

  public static GroupBy fromJson(JSONObject jsonObject) throws JSONException {
    GroupBy groupBy = new GroupBy();
    groupBy.setColumns(Arrays.asList(jsonObject.getString("columns").split(",")));
    groupBy.setTop(jsonObject.getInt("top"));
    return groupBy;
  }
}
