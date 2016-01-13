package com.linkedin.thirdeye.tools;

import java.io.FileInputStream;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.linkedin.thirdeye.api.DimensionSpec;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreePersistanceUtil;
import com.linkedin.thirdeye.impl.StarTreeUtils;

/**
 * Performs validation checks on the star tree and the leaf data
 * @author kgopalak
 */
public class StarTreeIndexValidator {
  public static void main(String[] args) throws Exception {
    String config = args[0];
    String pathToTreeBinary = args[1];
    String dataDirectory = args[2];

    StarTreeConfig starTreeConfig = StarTreeConfig.decode(new FileInputStream(config));

    StarTreeNode starTreeRootNode =
        StarTreePersistanceUtil.loadStarTree(new FileInputStream(pathToTreeBinary));

    List<StarTreeNode> leafNodes = new LinkedList<StarTreeNode>();
    StarTreeUtils.traverseAndGetLeafNodes(leafNodes, starTreeRootNode);

    List<DimensionSpec> dimensionSpecs = starTreeConfig.getDimensions();

    int totalCount = 0;
    int errorCount = 0;
    for (StarTreeNode node : leafNodes) {
      String[] parentDimValues = new String[node.getAncestorDimensionNames().size() + 1];

      Map<String, Map<String, Integer>> forwardIndex =
          StarTreePersistanceUtil.readForwardIndex(node.getId().toString(), dataDirectory);
      Map<String, Map<Integer, String>> reverseIndex = StarTreeUtils.toReverseIndex(forwardIndex);
      List<int[]> leafRecords = StarTreePersistanceUtil.readLeafRecords(dataDirectory,
          node.getId().toString(), starTreeConfig.getDimensions().size());
      List<String> ancestorDimensionNames = node.getAncestorDimensionNames();
      for (int i = 0; i < ancestorDimensionNames.size(); i++) {
        String name = ancestorDimensionNames.get(i);
        parentDimValues[i] = node.getAncestorDimensionValues().get(name);
      }
      parentDimValues[parentDimValues.length - 1] = node.getDimensionValue();
      String[] childDimValues = new String[dimensionSpecs.size()];
      System.out.println("START: Processing leaf node:" + node.getId() + " " + node.getPath()
          + " numChildren:" + leafRecords.size());
      for (int arr[] : leafRecords) {
        Arrays.fill(childDimValues, "");
        boolean passed = true;
        for (int i = 0; i < dimensionSpecs.size(); i++) {
          String name = dimensionSpecs.get(i).getName();
          childDimValues[i] = reverseIndex.get(name).get(arr[i]);
        }
        for (int i = 0; i < dimensionSpecs.size(); i++) {
          String name = dimensionSpecs.get(i).getName();
          String parentNodeVal = null;
          if (node.getAncestorDimensionValues().containsKey(name)) {
            parentNodeVal = node.getAncestorDimensionValues().get(name);
          }
          if (node.getDimensionName().equals(name)) {
            parentNodeVal = node.getDimensionValue();
          }
          if (parentNodeVal != null && !parentNodeVal.equals(StarTreeConstants.STAR)) {
            if (!parentNodeVal.equals(childDimValues[i])) {
              System.out.println("\t\t\t\t ERROR: " + name + " : parentVal:" + parentNodeVal
                  + " childVal:" + childDimValues[i] + Arrays.toString(childDimValues));
              passed = false;
            }
          }
        }
        totalCount = totalCount + 1;
        if (!passed)
          errorCount = errorCount + 1;
      }
      System.out.println("END: Processing leaf node:" + node.getId());

    }
    System.out.println("total Count: " + totalCount + " failed count: " + errorCount);
  }
}
