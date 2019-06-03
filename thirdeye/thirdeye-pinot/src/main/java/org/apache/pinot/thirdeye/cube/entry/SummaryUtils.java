package org.apache.pinot.thirdeye.cube.entry;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Multimap;
import java.util.List;
import org.apache.pinot.thirdeye.cube.data.dbrow.Dimensions;


public class SummaryUtils {
  /**
   * Checks the arguments of cube algorithm.
   *
   * @param dataset the name of dataset; cannot be null or empty.
   * @param metrics the list of metrics; it needs to contain at least one metric and they cannot be null or empty.
   * @param currentStartInclusive the current start time; needs to be smaller than current end time.
   * @param currentEndExclusive the current end time.
   * @param baselineStartInclusive the baseline start time; needs to be smaller than baseline end time.
   * @param baselineEndExclusive the baseline end time.
   * @param dimensions the dimensions to be explored; it needs to contains at least one dimensions.
   * @param dataFilters the filter to be applied on the Pinot query; cannot be null.
   * @param summarySize the summary size; needs to > 0.
   * @param depth the max depth of dimensions to be drilled down; needs to be >= 0.
   * @param hierarchies the hierarchy among dimensions; cannot be null.
   */
  public static void checkArguments(String dataset, List<String> metrics, long currentStartInclusive,
      long currentEndExclusive, long baselineStartInclusive, long baselineEndExclusive, Dimensions dimensions,
      Multimap<String, String> dataFilters, int summarySize, int depth, List<List<String>> hierarchies) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(dataset));
    Preconditions.checkArgument(!metrics.isEmpty());
    for (String metric : metrics) {
      Preconditions.checkArgument(!Strings.isNullOrEmpty(metric));
    }
    Preconditions.checkArgument(currentStartInclusive < currentEndExclusive);
    Preconditions.checkArgument(baselineStartInclusive < baselineEndExclusive);
    Preconditions.checkNotNull(dimensions);
    Preconditions.checkArgument(dimensions.size() > 0);
    Preconditions.checkNotNull(dataFilters);
    Preconditions.checkArgument(summarySize > 1);
    Preconditions.checkNotNull(hierarchies);
    Preconditions.checkArgument(depth >= 0);
  }
}
