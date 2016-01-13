package com.linkedin.thirdeye.tools;

import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreeUtils;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class StarTreeViewer {
  private StarTreeNode root;
  private PrintWriter printWriter;

  public StarTreeViewer(StarTreeNode root, PrintWriter printWriter) {
    this.root = root;
    this.printWriter = printWriter;
  }

  public void print() {
    StarTreeUtils.printNode(printWriter, root, 0);
    printWriter.flush();

    // Collect metadata
    AtomicInteger numNodes = new AtomicInteger();
    AtomicInteger numLeaves = new AtomicInteger();
    Map<Integer, Integer> leafDepthCount = new HashMap<Integer, Integer>();
    Map<Integer, Integer> parentDepthCount = new HashMap<Integer, Integer>();
    countNodes(root, numNodes, numLeaves, leafDepthCount, parentDepthCount, 0);

    // Compute all depths
    Set<Integer> depthSet = new HashSet<Integer>();
    depthSet.addAll(leafDepthCount.keySet());
    depthSet.addAll(parentDepthCount.keySet());
    List<Integer> depths = new ArrayList<Integer>(depthSet);
    Collections.sort(depths);

    // Print metadata
    printWriter.println();
    printWriter.println("--------------------------------------------------");
    printWriter.println();
    printWriter.println("(total=" + numNodes.get() + ", leaves=" + numLeaves.get() + ")");
    Collections.sort(depths);
    for (Integer depth : depths) {
      Integer leafCount = leafDepthCount.get(depth);
      Integer parentCount = parentDepthCount.get(depth);
      Integer totalCount =
          (leafCount == null ? 0 : leafCount) + (parentCount == null ? 0 : parentCount);

      printWriter.println("\tat depth=" + depth + ":");
      printWriter.println("\t\ttotal=" + totalCount);
      if (leafCount != null) {
        printWriter.println("\t\tleaves=" + leafCount);
      }
      if (parentCount != null) {
        printWriter.println("\t\tparents=" + parentCount);
      }
    }
    printWriter.flush();
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      throw new IllegalArgumentException("usage: tree.bin");
    }
    PrintWriter printWriter = new PrintWriter(System.out);

    // Dump tree
    ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(args[0]));
    StarTreeNode root = (StarTreeNode) objectInputStream.readObject();
    StarTreeViewer dumper = new StarTreeViewer(root, printWriter);
    dumper.print();
  }

  private static void countNodes(StarTreeNode node, AtomicInteger numNodes, AtomicInteger numLeaves,
      Map<Integer, Integer> leafDepthCount, Map<Integer, Integer> parentDepthCount,
      int currentDepth) {
    if (node.isLeaf()) {
      numLeaves.incrementAndGet();
      numNodes.incrementAndGet();
      updateDepthCount(leafDepthCount, currentDepth);
    } else {
      numNodes.incrementAndGet();
      updateDepthCount(parentDepthCount, currentDepth);

      for (StarTreeNode child : node.getChildren()) {
        countNodes(child, numNodes, numLeaves, leafDepthCount, parentDepthCount, currentDepth + 1);
      }
      countNodes(node.getOtherNode(), numNodes, numLeaves, leafDepthCount, parentDepthCount,
          currentDepth + 1);
      countNodes(node.getStarNode(), numNodes, numLeaves, leafDepthCount, parentDepthCount,
          currentDepth + 1);
    }
  }

  private static void updateDepthCount(Map<Integer, Integer> depthCount, int depth) {
    Integer count = depthCount.get(depth);
    if (count == null) {
      count = 0;
    }
    depthCount.put(depth, count + 1);
  }
}
