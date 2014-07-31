package com.linkedin.pinot.common.utils.request;

import java.util.List;

import com.linkedin.pinot.common.request.FilterOperator;

public class FilterQueryTree {

  private final int id;
  private final String column;
  private final List<String> value;
  private final FilterOperator operator;
  private final List<FilterQueryTree> children;

  public FilterQueryTree(int id, String column, List<String> value, FilterOperator operator,
      List<FilterQueryTree> children) {
    super();
    this.id = id;
    this.column = column;
    this.value = value;
    this.operator = operator;
    this.children = children;
  }

  public int getId() {
    return id;
  }

  public String getColumn() {
    return column;
  }

  public List<String> getValue() {
    return value;
  }

  public FilterOperator getOperator() {
    return operator;
  }

  public List<FilterQueryTree> getChildren() {
    return children;
  }
}
