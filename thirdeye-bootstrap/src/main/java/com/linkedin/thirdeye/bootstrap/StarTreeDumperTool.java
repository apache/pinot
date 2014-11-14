package com.linkedin.thirdeye.bootstrap;

import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreeUtils;

import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.PrintWriter;

public class StarTreeDumperTool
{
  public static void main(String[] args) throws Exception
  {
    if (args.length != 1)
    {
      throw new IllegalArgumentException("usage: tree.bin");
    }
    ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(args[0]));
    StarTreeNode root = (StarTreeNode) objectInputStream.readObject();
    StarTreeUtils.printNode(new PrintWriter(System.out), root, 0);
  }
}
