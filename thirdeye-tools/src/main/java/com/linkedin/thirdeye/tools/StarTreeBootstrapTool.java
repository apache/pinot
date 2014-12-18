package com.linkedin.thirdeye.tools;

import java.io.File;
import java.io.ObjectInputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import com.linkedin.thirdeye.impl.StarTreeImpl;
import com.linkedin.thirdeye.impl.StarTreeManagerImpl;
import com.linkedin.thirdeye.impl.StarTreeUtils;

/**
 * Does only the bootstrap
 * 
 * @author kgopalak
 * 
 */
public class StarTreeBootstrapTool {
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  public static void main(String[] args) throws Exception{
    FileSystem fs = FileSystem.get(new Configuration());

    Path dataInputPath = new Path(args[0]);
    Path starTreeInputPath = new Path(args[0]);
    Path configPath = new Path(args[1]);
    Path outputPath = new Path(args[2]);
    
    StarTreeConfig config = StarTreeConfig.fromJson(OBJECT_MAPPER.readTree(fs.open(configPath)), new File(""));
    ObjectInputStream objectInputStream = new ObjectInputStream(fs.open(starTreeInputPath));
    StarTreeNode root = (StarTreeNode) objectInputStream.readObject();
    StarTreeImpl starTree = new StarTreeImpl(config, root);

    LinkedList<StarTreeNode> leafNodes= new LinkedList<StarTreeNode>();
    StarTreeUtils.traverseAndGetLeafNodes(leafNodes, root);
    
  }
}
