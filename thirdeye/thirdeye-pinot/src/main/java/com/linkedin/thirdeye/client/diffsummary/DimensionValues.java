/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.client.diffsummary;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;


public class DimensionValues implements Comparable<DimensionValues> {
  @JsonProperty("values")
  private ImmutableList<String> values;

  public DimensionValues() {
    this.values = ImmutableList.of();
  }

  public DimensionValues(List<String> values) {
    this.values = ImmutableList.copyOf(values);
  }

  public String get(int index) {
    return values.get(index);
  }

  public List<String> values() {
    return values;
  }

  public int size() {
    return values.size();
  }

  /**
   * Example Results:
   * 1. D1 = {"a"} D2 = {"s"} ==> compare strings "a" and "s" directly (i.e., D2 > D1).
   * 2. D1 = {"a"} D2 = {"a", "b"} ==> D1 > D2
   * 3. D1 = {"s"} D2 = {"a", "b"} ==> compare strings "a" and "s" directly (i.e., D1 > D2).
   *
   * {@inheritDoc}
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(DimensionValues other) {
    Iterator<String> thisIte = this.values.iterator();
    Iterator<String> otherIte = other.values.iterator();

    while (thisIte.hasNext()) {
      if (!otherIte.hasNext()) { // other is parents
        return -1;
      }
      String thisName = thisIte.next();
      String otherName = otherIte.next();
      int diff = ObjectUtils.compare(thisName, otherName);
      if (diff != 0) {
        return diff;
      }
    }

    if (otherIte.hasNext()) { // other is a child
      return 1;
    }

    return 0;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DimensionValues that = (DimensionValues) o;
    return Objects.equals(values, that.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(values);
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.SIMPLE_STYLE);
  }
}
