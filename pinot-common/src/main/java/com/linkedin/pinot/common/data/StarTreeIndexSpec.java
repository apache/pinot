/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.common.data;

import com.google.common.base.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class StarTreeIndexSpec {
  private Logger LOGGER = LoggerFactory.getLogger(StarTreeIndexSpec.class);
  public static final Integer DEFAULT_MAX_LEAF_RECORDS = 100000; // TODO: determine a good number via experiment
  public static final int DEFAULT_SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD = 10000;

  private boolean enableStarTree = false;
  /** The upper bound on the number of leaf records to be scanned for any query */
  private Integer maxLeafRecords = DEFAULT_MAX_LEAF_RECORDS;

  /** Dimension split order (if null or absent, descending w.r.t. dimension cardinality) */
  private List<String> dimensionsSplitOrder = new ArrayList<String>();

  /** Dimensions for which to exclude star nodes at split. */
  private Set<String> skipStarNodeCreationForDimensions = new HashSet<String>();
  private Set<String> skipMaterializationForDimensions = new HashSet<String>();
  private int skipMaterializationCardinalityThreshold = DEFAULT_SKIP_MATERIALIZATION_CARDINALITY_THRESHOLD;

  public StarTreeIndexSpec() {}

  public StarTreeIndexSpec(Map<String, String> starTreeIndexSpecConfigs) {
    if (starTreeIndexSpecConfigs.containsKey("enableStarTree")) {
      enableStarTree = Boolean.parseBoolean(starTreeIndexSpecConfigs.get("enableStarTree"));
    }
    if (starTreeIndexSpecConfigs.containsKey("maxLeafRecords")) {
      maxLeafRecords = Integer.parseInt(starTreeIndexSpecConfigs.get("maxLeafRecords"));
    }
    if (starTreeIndexSpecConfigs.containsKey("dimensionsSplitOrder")) {
      if (!starTreeIndexSpecConfigs.get("dimensionsSplitOrder").isEmpty()) {
        try {
          String[] dimensionsSplitOrderArray = starTreeIndexSpecConfigs.get("dimensionsSplitOrder").split(",");
          for (String column : dimensionsSplitOrderArray) {
            dimensionsSplitOrder.add(column);
          }
        } catch (Exception e) {
          LOGGER.error("Failed to add dimensionsSplitOrder: " + starTreeIndexSpecConfigs.get("dimensionsSplitOrder"));
        }
      }
    }
    if (starTreeIndexSpecConfigs.containsKey("skipStarNodeCreationForDimensions")) {
      if (!starTreeIndexSpecConfigs.get("skipStarNodeCreationForDimensions").isEmpty()) {
        try {
          String[] skipStarNodeCreationForDimensionsArray = starTreeIndexSpecConfigs.get("skipStarNodeCreationForDimensions").split(",");
          for (String column : skipStarNodeCreationForDimensionsArray) {
            skipStarNodeCreationForDimensions.add(column);
          }
        } catch (Exception e) {
          LOGGER.error("Failed to add skipStarNodeCreationForDimensions: " + starTreeIndexSpecConfigs.get("skipStarNodeCreationForDimensions"));
        }
      }
    }
    if (starTreeIndexSpecConfigs.containsKey("skipMaterializationForDimensions")) {
      if (!starTreeIndexSpecConfigs.get("skipMaterializationForDimensions").isEmpty()) {
        try {
          String[] skipMaterializationForDimensionsArray = starTreeIndexSpecConfigs.get("skipMaterializationForDimensions").split(",");
          for (String column : skipMaterializationForDimensionsArray) {
            skipMaterializationForDimensions.add(column);
          }
        } catch (Exception e) {
          LOGGER.error("Failed to add skipMaterializationForDimensions: " + starTreeIndexSpecConfigs.get("skipMaterializationForDimensions"));
        }
      }
    }
    if (starTreeIndexSpecConfigs.containsKey("skipMaterializationCardinalityThreshold")) {
      skipMaterializationCardinalityThreshold = Integer.parseInt(starTreeIndexSpecConfigs.get("skipMaterializationCardinalityThreshold"));
    }
  }

  public boolean enableStarTree() {
    return enableStarTree;
  }

  public void setEnableStarTree(boolean enableStarTree) {
    this.enableStarTree = enableStarTree;
  }

  public Integer getMaxLeafRecords() {
    return maxLeafRecords;
  }

  public void setMaxLeafRecords(Integer maxLeafRecords) {
    this.maxLeafRecords = maxLeafRecords;
  }

  public List<String> getDimensionsSplitOrder() {
    return dimensionsSplitOrder;
  }

  public void setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
    this.dimensionsSplitOrder = dimensionsSplitOrder;
  }

  public void setSkipStarNodeCreationForDimensions(Set<String> skipStarNodeCreationForDimensions) {
    this.skipStarNodeCreationForDimensions = skipStarNodeCreationForDimensions;
  }

  public Set<String> getSkipStarNodeCreationForDimensions() {
    return skipStarNodeCreationForDimensions;
  }

  public Set<String> getskipMaterializationForDimensions() {
    return skipMaterializationForDimensions;
  }

  public void setSkipMaterializationForDimensions(Set<String> skipMaterializationForDimensions) {
    this.skipMaterializationForDimensions = skipMaterializationForDimensions;
  }

  public int getskipMaterializationCardinalityThreshold() {
    return skipMaterializationCardinalityThreshold;
  }

  public void setSkipMaterializationCardinalityThreshold(int skipMaterializationCardinalityThreshold) {
    this.skipMaterializationCardinalityThreshold = skipMaterializationCardinalityThreshold;
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StarTreeIndexSpec)) {
      return false;
    }
    StarTreeIndexSpec s = (StarTreeIndexSpec) o;
    return Objects.equal(enableStarTree, s.enableStarTree())
        && Objects.equal(maxLeafRecords, s.getMaxLeafRecords())
        && Objects.equal(dimensionsSplitOrder, s.getDimensionsSplitOrder());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(maxLeafRecords, dimensionsSplitOrder);
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("maxLeafRecords", maxLeafRecords)
        .add("dimensionsSplitOrder", dimensionsSplitOrder)
        .toString();
  }
}
