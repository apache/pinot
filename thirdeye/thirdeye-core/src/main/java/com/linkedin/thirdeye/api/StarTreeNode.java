package com.linkedin.thirdeye.api;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public interface StarTreeNode extends Serializable {
  /** @return this node's ID */
  UUID getId();

  /**
   * Called after construction to initialize all the appropriate resources
   */
  void init(StarTreeConfig config, StarTreeRecordStoreFactory recordStoreFactory);

  /**
   * @return True if this node is a leaf (i.e. getChildren() == 0)
   */
  boolean isLeaf();

  /**
   * @return A list of all the children of this node, not including other (?)
   *         and star (*) nodes
   */
  Collection<StarTreeNode> getChildren();

  /**
   * @return The specific child node (keyed by dimension value), or null if it
   *         doesn't exist
   */
  StarTreeNode getChild(String dimensionValue);

  /** @return The other (?) node */
  StarTreeNode getOtherNode();

  /** @return The star (*) node */
  StarTreeNode getStarNode();

  /**
   * @return The record store associated with this node if isLeaf() == true,
   *         otherwise null
   */
  StarTreeRecordStore getRecordStore();

  /**
   * Sets the record store at this node, and closes any existing one
   */
  void setRecordStore(StarTreeRecordStore recordStore) throws IOException;

  /**
   * Splits a leaf node on a specific dimension into a parent node whose
   * children are fixed values of that dimension, an "other" (?) node, and a
   * star (*) node.
   *
   * <p>
   * When a split occurs, we first for each dimension value, determine the list
   * of records in the record store that have said value. After this, we apply
   * passesThreshold function to that list. If this returns true, that dimension
   * value will be explicitly represented as a child of this node. Otherwise,
   * that dimension value will be rolled up into the other "?" category.
   * </p>
   *
   * <p>
   * Each record is then placed into either a specific leaf node (if it's
   * dimension value is explicitly represented), or the "other" leaf node. For a
   * getAggregate involving the "?" value, it effectively means every value in
   * the other bucket (i.e. "*" - [specificValues]).
   * </p>
   *
   * <p>
   * In addition to this, we compute the aggregates for each remaining
   * combination in the leaf with dimensionName set to "*", and add this new set
   * of records to the star (*) node.
   * </p>
   *
   * <p>
   * For example, consider a split of a leaf node with the following records. We
   * have dimensions a,b,c, and are splitting on a:
   * <ul>
   * <li>a1,b2,c3 -> 1</li>
   * <li>a2,b1,c2 -> 2</li>
   * <li>a3,b1,c2 -> 3</li>
   * </ul>
   * </p>
   *
   * <p>
   * We would add the following two records to the star (*) node:
   * <ul>
   * <li>*,b2,c3 -> 1</li>
   * <li>*,b1,c2 -> 5</li>
   * </ul>
   * </p>
   *
   * <p>
   * Now, consider subsequently adding a1,b2,c3 -> 5 to the tree. We first
   * traverse to the specific record location for a1,b2,c3 (under the a1
   * specific node), AND we traverse down EACH star node, relaxing the dimension
   * value each time to *, and adding the resulting vector to the two
   * appropriate nodes in the sub tree (i.e. the record for the NEXT branched
   * dimension, and the star tree for that level as well).
   * </p>
   *
   * <p>
   * Calling split on a non-leaf node has no effect.
   * </p>
   *
   * @param dimensionName
   *          The dimension name on which to split
   */
  void split(String dimensionName);

  /**
   * @return The name of the dimension which caused this node to come into
   *         existence.
   */
  String getDimensionName();

  /**
   * @return The name of the dimension on which this node was split (i.e. the
   *         dimension of each child)
   */
  String getChildDimensionName();

  /**
   * @return The set of dimension names corresponding to the dimension name of
   *         each ancestor of this node in the tree
   */
  List<String> getAncestorDimensionNames();

  /**
   * @return The values for each ancestor dimension
   */
  Map<String, String> getAncestorDimensionValues();

  /**
   * @return The specific dimension value for this node among its sisters with
   *         like dimension.
   */
  String getDimensionValue();

  /**
   * Creates a new child for dimensionValue under this node. If a node already exists it will return existing node.
   * @param dimensionValue
   * @return
   */
  StarTreeNode addChildNode(String dimensionValue);

  /**
   * return the path to this node in the tree of the form /(dimName1:dimVal1)/(dimName2:dimVal2)
   * @return
   */
  String getPath();
}
