/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.google.common.base.Objects;
import java.io.Serializable;
import java.util.Map;


public class StarTreeIndexNode implements Serializable {

  public static final int ALL = -1;

  private int _childDimension = -1;
  private Map<Integer, StarTreeIndexNode> _children = null;
  private int _startDocumentId = -1;
  private int _endDocumentId = -1;
  private int _aggregatedDocumentId = -1;

  public void setChildDimension(int childDimension) {
    _childDimension = childDimension;
  }

  public int getChildDimension() {
    return _childDimension;
  }

  public void setChildren(Map<Integer, StarTreeIndexNode> children) {
    _children = children;
  }

  public Map<Integer, StarTreeIndexNode> getChildren() {
    return _children;
  }

  public boolean isLeaf() {
    return _children == null;
  }

  public void setStartDocumentId(int docStartId) {
    _startDocumentId = docStartId;
  }

  public int getStartDocumentId() {
    return _startDocumentId;
  }

  public void setEndDocumentId(int endDocumentId) {
    _endDocumentId = endDocumentId;
  }

  public int getEndDocumentId() {
    return _endDocumentId;
  }

  public void setAggregatedDocumentId(int aggregatedDocumentId) {
    _aggregatedDocumentId = aggregatedDocumentId;
  }

  public int getAggregatedDocumentId() {
    return _aggregatedDocumentId;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("childDimension", _childDimension)
        .add("childCount", _children == null ? 0 : _children.size())
        .add("startDocumentId", _startDocumentId)
        .add("endDocumentId", _endDocumentId)
        .add("aggregatedDocumentId", _aggregatedDocumentId)
        .toString();
  }

  public static void printTree(StarTreeIndexNode node, int level) {
    for (int i = 0; i < level; i++) {
      System.out.print("  ");
    }
    System.out.println(node);

    if (!node.isLeaf()) {
      for (StarTreeIndexNode child : node.getChildren().values()) {
        printTree(child, level + 1);
      }
    }
  }
}
