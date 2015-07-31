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

import com.google.common.base.Objects;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;

import java.io.*;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class StarTreeIndexNode implements Serializable {
  private static final int ALL = -1;
  private static final int GROUP = -2;
  private static final long serialVersionUID = 1;

  private Integer nodeId;
  private Integer level;
  private Integer dimensionName;
  private Integer dimensionValue;
  private Integer childDimensionName;
  private Map<Integer, StarTreeIndexNode> children;
  private StarTreeIndexNode parent;

  /**
   * An element in the StarTreeIndex.
   */
  public StarTreeIndexNode() {}

  public Integer getNodeId() {
    return nodeId;
  }

  public void setNodeId(Integer nodeId) {
    this.nodeId = nodeId;
  }

  public Integer getLevel() {
    return level;
  }

  public void setLevel(Integer level) {
    this.level = level;
  }

  public Integer getDimensionName() {
    return dimensionName;
  }

  public void setDimensionName(Integer dimensionName) {
    this.dimensionName = dimensionName;
  }

  public Integer getDimensionValue() {
    return dimensionValue;
  }

  public void setDimensionValue(Integer dimensionValue) {
    this.dimensionValue = dimensionValue;
  }

  public Integer getChildDimensionName() {
    return childDimensionName;
  }

  public void setChildDimensionName(Integer childDimensionName) {
    this.childDimensionName = childDimensionName;
  }

  public Map<Integer, StarTreeIndexNode> getChildren() {
    return children;
  }

  public void setChildren(Map<Integer, StarTreeIndexNode> children) {
    this.children = children;
  }

  public StarTreeIndexNode getParent() {
    return parent;
  }

  public void setParent(StarTreeIndexNode parent) {
    this.parent = parent;
  }

  public boolean isLeaf() {
    return children == null;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(nodeId, dimensionName, dimensionValue, childDimensionName);
  }

  @Override
  public boolean equals(Object o) {
    if (!(o instanceof StarTreeIndexNode)) {
      return false;
    }
    StarTreeIndexNode n = (StarTreeIndexNode) o;
    return Objects.equal(nodeId, n.getNodeId())
        && Objects.equal(level, n.getLevel())
        && Objects.equal(dimensionName, n.getDimensionName())
        && Objects.equal(dimensionValue, n.getDimensionValue())
        && Objects.equal(childDimensionName, n.getChildDimensionName())
        && Objects.equal(children, n.getChildren());
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("nodeId", nodeId)
        .add("level", level)
        .add("dimensionName", dimensionName)
        .add("dimensionValue", dimensionValue)
        .add("childDimensionName", childDimensionName)
        .add("childCount", children == null ? 0 : children.size())
        .toString();
  }

  /**
   * Returns a Java-serialized StarTree structure of this node and all its sub-trees.
   */
  public byte[] toBytes() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    writeTree(baos);
    baos.close();
    return baos.toByteArray();
  }

  public void writeTree(OutputStream outputStream) throws IOException {
    ObjectOutputStream oos = new ObjectOutputStream(outputStream);
    oos.writeObject(this);
  }

  /**
   * Returns the dimension IDs, in order of tree level, to this node.
   */
  public List<Integer> getPathDimensions() {
    StarTreeIndexNode current = this;
    List<Integer> dimensions = new LinkedList<Integer>();
    while (current != null && current.getParent() != null) {
      dimensions.add(0, current.getDimensionName());
      current = current.getParent();
    }
    return dimensions;
  }

  /**
   * Returns the dimension values, in order of tree level, to this node.
   */
  public Map<Integer, Integer> getPathValues() {
    StarTreeIndexNode current = this;
    Map<Integer, Integer> values = new HashMap<Integer, Integer>();
    while (current != null && current.getParent() != null) {
      values.put(current.getDimensionName(), current.getDimensionValue());
      current = current.getParent();
    }
    return values;
  }

  /**
   * Returns the child node that matches dimensions, or null if none matches.
   */
  public StarTreeIndexNode getMatchingNode(List<Integer> dimensions) {
    return getMatchingNode(this, dimensions);
  }

  public StarTreeIndexNode getMatchingNode(StarTreeIndexNode node, List<Integer> dimensions) {
    if (node == null || node.isLeaf()) {
      return node;
    }
    Integer childDimensionName = node.getChildDimensionName();
    Integer childDimensionValue = dimensions.get(childDimensionName);
    StarTreeIndexNode child = node.getChildren().get(childDimensionValue);
    return getMatchingNode(child, dimensions);
  }

  /**
   * De-serializes a StarTree structure generated with {@link #toBytes}.
   */
  public static StarTreeIndexNode fromBytes(InputStream inputStream) throws IOException, ClassNotFoundException {
    ObjectInputStream ois = new ObjectInputStream(inputStream);
    StarTreeIndexNode node = (StarTreeIndexNode) ois.readObject();
    return node;
  }

  public static int all() {
    return ALL;
  }

  public static int group() {
    return GROUP;
  }

  /** Returns true if the dimension combination has the specified prefix */
  public static boolean matchesPrefix(Map<Integer, Integer> prefix, List<Integer> combination) {
    for (Map.Entry<Integer, Integer> entry : prefix.entrySet()) {
      Integer index = entry.getKey();
      Integer value = entry.getValue();
      if (!value.equals(combination.get(index))) {
        return false;
      }
    }
    return true;
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

  public static Object getAllValue(FieldSpec spec) {
    Object allValue;
    switch (spec.getDataType()) {
      case INT:
        allValue = V1Constants.STARTREE_ALL_NUMBER.intValue();
        break;
      case LONG:
        allValue = V1Constants.STARTREE_ALL_NUMBER.longValue();
        break;
      case FLOAT:
        allValue = V1Constants.STARTREE_ALL_NUMBER.floatValue();
        break;
      case DOUBLE:
        allValue = V1Constants.STARTREE_ALL_NUMBER.doubleValue();
        break;
      case STRING:
      case BOOLEAN:
        allValue = V1Constants.STARTREE_ALL;
        break;
      default:
        throw new UnsupportedOperationException("unsupported data type : " + spec.getDataType() + " : " + " for column : "
            + spec.getName());
    }
    return allValue;
  }
}
