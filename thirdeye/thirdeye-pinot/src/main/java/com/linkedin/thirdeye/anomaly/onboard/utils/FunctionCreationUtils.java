package com.linkedin.thirdeye.anomaly.onboard.utils;

import com.linkedin.thirdeye.datalayer.dto.DatasetConfigDTO;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.StringUtils;


public class FunctionCreationUtils {

  /**
   * Return the valid dimension string
   * @param dataset the dataset configuration
   * @param exploreDimensions the dimensions to be explored
   * @return a dimension string with valid dimensions
   * @throws Exception
   */
  public static String getDimensions(DatasetConfigDTO dataset, String exploreDimensions) throws Exception {
    // Ensure that the explore dimension names are ordered as schema dimension names
    List<String> schemaDimensionNames = dataset.getDimensions();
    Set<String> splitExploreDimensions = new HashSet<>(Arrays.asList(exploreDimensions.trim().split(",")));
    List<String> reorderedDimensions = new ArrayList<>();
    for (String dimensionName : schemaDimensionNames) {
      if (splitExploreDimensions.contains(dimensionName)) {
        reorderedDimensions.add(dimensionName);
      }
    }
    return StringUtils.join(reorderedDimensions, ",");
  }
}
