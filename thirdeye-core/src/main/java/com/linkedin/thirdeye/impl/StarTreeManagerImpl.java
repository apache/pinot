package com.linkedin.thirdeye.impl;

import com.linkedin.thirdeye.api.StarTree;
import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeConstants;
import com.linkedin.thirdeye.api.StarTreeManager;
import com.linkedin.thirdeye.api.StarTreeNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class StarTreeManagerImpl implements StarTreeManager
{
  private static final Logger LOG = LoggerFactory.getLogger(StarTreeManagerImpl.class);

  private final ConcurrentMap<String, StarTree> trees;

  public StarTreeManagerImpl()
  {
    this.trees = new ConcurrentHashMap<String, StarTree>();
  }

  @Override
  public Set<String> getCollections()
  {
    return trees.keySet();
  }

  @Override
  public StarTree getStarTree(String collection)
  {
    return trees.get(collection);
  }

  @Override
  public void restore(File rootDir, String collection) throws Exception
  {
    synchronized (trees)
    {
      LOG.info("Creating new startree for {}", collection);

      File collectionDir = new File(rootDir, collection);

      // Read tree structure
      File treeFile = new File(collectionDir, StarTreeConstants.TREE_FILE_NAME);
      ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(treeFile));
      StarTreeNode root = (StarTreeNode) inputStream.readObject();

      // Read config
      File configFile = new File(collectionDir, StarTreeConstants.CONFIG_FILE_NAME);
      StarTreeConfig config = StarTreeConfig.decode(new FileInputStream(configFile));

      // Create tree
      StarTree starTree = new StarTreeImpl(config, new File(collectionDir, StarTreeConstants.DATA_DIR_NAME), root);
      StarTree previous = trees.put(collection, starTree);
      if (previous != null)
      {
        previous.close();
      }
    }
  }

  private void stubRecordStores(StarTreeNode node, StarTreeConfig config) throws IOException
  {
    if (node.isLeaf())
    {
      node.setRecordStore(new StarTreeRecordStoreBlackHoleImpl(config.getMetrics()));
    }
    else
    {
      for (StarTreeNode child : node.getChildren())
      {
        stubRecordStores(child, config);
      }
      stubRecordStores(node.getOtherNode(), config);
      stubRecordStores(node.getStarNode(), config);
    }
  }

  @Override
  public void remove(String collection) throws IOException
  {
    synchronized (trees)
    {
      StarTree starTree = trees.remove(collection);
      if (starTree != null)
      {
        LOG.info("Closing startree for {}", collection);
        starTree.close();
      }
    }
  }

  @Override
  public void open(String collection) throws IOException
  {
    synchronized (trees)
    {
      StarTree starTree = trees.get(collection);
      if (starTree == null)
      {
        throw new IllegalArgumentException("No star tree for collection " + collection);
      }
      starTree.open();
      LOG.info("Opened tree for collection {}", collection);
    }
  }

  @Override
  public void close(String collection) throws IOException
  {
    synchronized (trees)
    {
      StarTree starTree = trees.get(collection);
      if (starTree != null)
      {
        starTree.close();
        LOG.info("Closed tree for collection {}", collection);
      }
    }
  }
}
