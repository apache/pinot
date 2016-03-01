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
package com.linkedin.pinot.core.startree;

import java.io.File;
import java.util.List;

import com.linkedin.pinot.common.data.Schema;
import java.util.Set;


public class StarTreeBuilderConfig {

  public Schema schema;

  public List<String> dimensionsSplitOrder;

  public int maxLeafRecords;

  File outDir;

  private Set<String> skipStarNodeCreationForDimensions;

  public File getOutDir() {
    return outDir;
  }

  public void setOutDir(File outDir) {
    this.outDir = outDir;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public List<String> getDimensionsSplitOrder() {
    return dimensionsSplitOrder;
  }

  public void setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
    this.dimensionsSplitOrder = dimensionsSplitOrder;
  }

  public int getMaxLeafRecords() {
    return maxLeafRecords;
  }

  public void setMaxLeafRecords(int maxLeafRecords) {
    this.maxLeafRecords = maxLeafRecords;
  }

  public void setSkipStarNodeCreationForDimensions(Set<String> excludedStarDimensions) {
    this.skipStarNodeCreationForDimensions = excludedStarDimensions;
  }

  /** Set of dimension names for which not to create star nodes at split. */
  public Set<String> getSkipStarNodeCreationForDimensions() {
    return skipStarNodeCreationForDimensions;
  }
}
