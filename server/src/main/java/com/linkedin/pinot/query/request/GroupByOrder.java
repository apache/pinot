package com.linkedin.pinot.query.request;

public enum GroupByOrder {
  // GroupBy will return top groups based on the first aggregation function
  // group by column
  top,
  // GroupBy will return groups ordered by values in the first group by column
  // value. 
  value
}
