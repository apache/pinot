package com.linkedin.thirdeye.datasource.pinot;

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.builder.ToStringBuilder;


public class PinotThirdEyeResultSetGroup {
  private List<ResultSet> resultSets = Collections.emptyList();

  public PinotThirdEyeResultSetGroup(List<ResultSet> resultSets) {
    if (resultSets != null) {
      this.resultSets = ImmutableList.copyOf(resultSets);
    }
  }

  public int size() {
    return resultSets.size();
  }

  public ResultSet get(int idx) {
    return resultSets.get(idx);
  }

  public List<ResultSet> getResultSets() {
    return resultSets;
  }

  public static PinotThirdEyeResultSetGroup fromResultSetGroup(ResultSetGroup resultSetGroup) {
    List<ResultSet> resultSets = new ArrayList<>();
    for (int i = 0; i < resultSetGroup.getResultSetCount(); i++) {
      resultSets.add(resultSetGroup.getResultSet(i));
    }
    return new PinotThirdEyeResultSetGroup(resultSets);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("resultSets", resultSets).toString();
  }
}
