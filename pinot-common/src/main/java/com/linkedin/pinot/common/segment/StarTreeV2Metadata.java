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

package com.linkedin.pinot.common.segment;

import java.util.List;


public class StarTreeV2Metadata {
  private int _docsCount;
  private List<String> _dimensionsSplitOrder;
  private List<String> _met2AggfuncPairs;

  public void setDocsCount(int count) {
    _docsCount = count;
  }

  public int getDocsCount() {
    return _docsCount;
  }

  public void setDimensionsSplitOrder(List<String> dimensionsSplitOrder) {
    _dimensionsSplitOrder = dimensionsSplitOrder;
  }

  public List<String> getDimensionsSplitOrder() {
    return _dimensionsSplitOrder;
  }

  public List<String> getMet2AggfuncPairs() {
    return _met2AggfuncPairs;
  }

  public void setMet2AggfuncPairs(List<String> met2AggfuncPairs) {
    _met2AggfuncPairs = met2AggfuncPairs;
  }
}
