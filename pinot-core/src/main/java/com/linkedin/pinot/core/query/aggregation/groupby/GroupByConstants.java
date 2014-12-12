package com.linkedin.pinot.core.query.aggregation.groupby;

public class GroupByConstants {
  public enum GroupByDelimiter {
    groupByMultiDelimeter("\t");

    private final String name;

    private GroupByDelimiter(String name) {
      this.name = name;
    }

    @Override
    public String toString() {
      return name;
    }

  }
}
