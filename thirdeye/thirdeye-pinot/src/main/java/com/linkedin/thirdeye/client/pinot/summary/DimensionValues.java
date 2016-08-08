package com.linkedin.thirdeye.client.pinot.summary;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonProperty;


public class DimensionValues implements Comparable<DimensionValues> {
  @JsonProperty("values")
  List<String> values;

  public DimensionValues() {
    this.values = new ArrayList<String>();
  }

  public DimensionValues(List<String> values) {
    this.values = values;
  }

  public String get(int index) {
    return values.get(index);
  }

  /**
   * Example Results:
   * 1. D1 = {"a"} D2 = {"s"} ==> compare strings "a" and "s" directly
   * 2. D1 = {"a"} D2 = {"a", "b"} ==> D1 > D2
   * 3. D1 = {"s"} D2 = {"a", "b"} ==> compare strings "a" and "s" directly
   * {@inheritDoc}
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(DimensionValues other) {
    if (values.size() == 0)
      return 1;
    Iterator<String> ite = other.values.iterator();
    for (int i = 0; i < values.size(); ++i) {
      if (ite.hasNext()) {
        String oString = ite.next();
        if (values.get(i).equals(oString))
          continue;
        return values.get(i).compareTo(oString);
      } else {
        return -1;
      }
    }
    return 1;
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
  }
}
