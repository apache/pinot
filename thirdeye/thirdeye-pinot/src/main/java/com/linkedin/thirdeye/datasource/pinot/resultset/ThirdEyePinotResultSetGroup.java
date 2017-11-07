package com.linkedin.thirdeye.datasource.pinot.resultset;

import com.google.common.collect.ImmutableList;
import com.linkedin.pinot.client.ResultSet;
import com.linkedin.pinot.client.ResultSetGroup;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang.builder.ToStringBuilder;

/**
 * The ThirdEye's own {@link ResultSetGroup} for storing Pinot's results.
 */
public class ThirdEyePinotResultSetGroup {
  private List<ThirdEyePinotResultSet> resultSets = Collections.emptyList();

  public ThirdEyePinotResultSetGroup(List<ThirdEyePinotResultSet> resultSets) {
    if (resultSets != null) {
      this.resultSets = ImmutableList.copyOf(resultSets);
    }
  }

  public int size() {
    return resultSets.size();
  }

  public ThirdEyePinotResultSet get(int idx) {
    return resultSets.get(idx);
  }

  public List<ThirdEyePinotResultSet> getResultSets() {
    return resultSets;
  }

  public static ThirdEyePinotResultSetGroup fromPinotResultSetGroup(ResultSetGroup resultSetGroup) {
    List<ResultSet> resultSets = new ArrayList<>();
    for (int i = 0; i < resultSetGroup.getResultSetCount(); i++) {
      resultSets.add(resultSetGroup.getResultSet(i));
    }
    // Convert Pinot's ResultSet to ThirdEyePinotResultSet
    List<ThirdEyePinotResultSet> thirdEyePinotResultSets = new ArrayList<>();
    for (ResultSet resultSet : resultSets) {
      ThirdEyePinotResultSet thirdEyePinotResultSet = ThirdEyeDataFrameResultSet.fromPinotResultSet(resultSet);
      thirdEyePinotResultSets.add(thirdEyePinotResultSet);
    }

    return new ThirdEyePinotResultSetGroup(thirdEyePinotResultSets);
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this).append("resultSets", resultSets).toString();
  }
}
