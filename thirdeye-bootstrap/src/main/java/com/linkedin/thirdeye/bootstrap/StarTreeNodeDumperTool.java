package com.linkedin.thirdeye.bootstrap;

import com.linkedin.thirdeye.api.StarTreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.ObjectInputStream;

public class StarTreeNodeDumperTool
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeNodeDumperTool.class);

  public static void main(String[] args) throws Exception
  {
    if (args.length != 1)
    {
      throw new IllegalArgumentException("usage: tree.bin");
    }

    ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(args[0]));

    StarTreeNode root = (StarTreeNode) objectInputStream.readObject();

    printNode(root, 0);
  }

  public static void printNode(StarTreeNode node, int level)
  {
    StringBuilder sb = new StringBuilder();

    for (int i = 0; i < level; i++)
    {
      sb.append("\t");
    }

    sb.append(node.getDimensionName())
      .append(":")
      .append(node.getDimensionValue())
      .append("(")
      .append(node.getId())
      .append(")");

    LOG.info(sb.toString());

    if (!node.isLeaf())
    {
      for (StarTreeNode child : node.getChildren())
      {
        printNode(child, level + 1);
      }
      printNode(node.getOtherNode(), level + 1);
      printNode(node.getStarNode(), level + 1);
    }
  }
}
