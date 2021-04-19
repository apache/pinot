/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.tools;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.MinMaxPriorityQueue;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.startree.StarTree;
import org.apache.pinot.segment.spi.index.startree.StarTreeNode;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.utils.JsonUtils;
import org.apache.pinot.spi.utils.ReadMode;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StarTreeIndexViewer {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeIndexViewer.class);

  /*
   * MAX num children to show in the UI
   */
  private static final int MAX_CHILDREN = 100;

  public StarTreeIndexViewer(File segmentDir)
      throws Exception {
    IndexSegment indexSegment = ImmutableSegmentLoader.load(segmentDir, ReadMode.heap);
    List<StarTreeV2> starTrees = indexSegment.getStarTrees();
    if (starTrees == null) {
      throw new IllegalStateException("Cannot find star-tree in segment directory: " + segmentDir);
    }

    StarTreeJsonNode rootJsonNode = new StarTreeJsonNode("ROOT");
    int numStarTrees = starTrees.size();
    if (numStarTrees == 1) {
      new SingleTreeViewer(starTrees.get(0)).build(rootJsonNode);
    } else {
      for (int i = 0; i < numStarTrees; i++) {
        StarTreeJsonNode singleTreeRootJsonNode = new StarTreeJsonNode("Star-Tree-" + i);
        new SingleTreeViewer(starTrees.get(i)).build(singleTreeRootJsonNode);
        rootJsonNode.addChild(singleTreeRootJsonNode);
      }
    }

    indexSegment.destroy();
    String rootJsonString = JsonUtils.objectToPrettyString(rootJsonNode);
    LOGGER.info(rootJsonString);
    startServer(rootJsonString);
  }

  private void startServer(String jsonString) {
    GrizzlyHttpServerFactory.createHttpServer(URI.create("http://0.0.0.0:8090/"), new StarTreeResource(jsonString));
    LOGGER.info("Go to http://localhost:8090/ to view the star-trees");
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 1) {
      LOGGER.error("USAGE: StarIndexViewer <segmentDirectory>");
      System.exit(1);
    }
    String segmentDir = args[0];
    if (!new File(segmentDir).isDirectory()) {
      throw new FileNotFoundException("Missing directory: " + segmentDir);
    }
    new StarTreeIndexViewer(new File(segmentDir));
  }

  private static class StarTreeJsonNode {
    String _name;
    List<StarTreeJsonNode> _children;

    StarTreeJsonNode(String name) {
      _name = name;
    }

    public void addChild(StarTreeJsonNode child) {
      if (_children == null) {
        _children = new ArrayList<>();
      }
      _children.add(child);
    }

    public String getName() {
      return _name;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public List<StarTreeJsonNode> getChildren() {
      return _children;
    }
  }

  private static class SingleTreeViewer {
    private final StarTreeNode _rootNode;
    private final List<String> _dimensionNames;
    private final List<Dictionary> _dimensionDictionaries;

    public SingleTreeViewer(StarTreeV2 starTreeV2) {
      StarTree starTree = starTreeV2.getStarTree();
      _rootNode = starTree.getRoot();
      _dimensionNames = starTree.getDimensionNames();
      int numDimensions = _dimensionNames.size();
      _dimensionDictionaries = new ArrayList<>(numDimensions);
      for (String dimension : _dimensionNames) {
        _dimensionDictionaries.add(starTreeV2.getDataSource(dimension).getDictionary());
      }
    }

    public void build(StarTreeJsonNode rootJsonNode) {
      build(_rootNode, rootJsonNode);
    }

    @SuppressWarnings("UnstableApiUsage")
    private long build(StarTreeNode treeNode, StarTreeJsonNode jsonNode) {
      if (treeNode.isLeaf()) {
        return 0;
      }

      int childDimensionId = treeNode.getChildDimensionId();
      String childDimension = _dimensionNames.get(childDimensionId);
      Dictionary dictionary = _dimensionDictionaries.get(childDimensionId);
      int numChildNodes = treeNode.getNumChildren();
      long totalChildNodes = numChildNodes;

      Comparator<Pair<String, Long>> comparator = (o1, o2) -> Long.compare(o2.getRight(), o1.getRight());
      MinMaxPriorityQueue<Pair<String, Long>> queue =
          MinMaxPriorityQueue.orderedBy(comparator).maximumSize(MAX_CHILDREN).create();
      StarTreeJsonNode starJsonNode = null;

      Iterator<? extends StarTreeNode> childrenIterator = treeNode.getChildrenIterator();
      while (childrenIterator.hasNext()) {
        StarTreeNode childTreeNode = childrenIterator.next();
        int childDimensionValueId = childTreeNode.getDimensionValue();
        String childDimensionValue =
            childDimensionValueId != StarTreeNode.ALL ? dictionary.getStringValue(childDimensionValueId) : "ALL";
        String childJsonNodeName = childDimension + ": " + childDimensionValue;
        StarTreeJsonNode childJsonNode = new StarTreeJsonNode(childJsonNodeName);
        long result = build(childTreeNode, childJsonNode);
        totalChildNodes += result;
        if (childDimensionValueId != StarTreeNode.ALL) {
          jsonNode.addChild(childJsonNode);
          queue.add(ImmutablePair.of(childJsonNodeName, result));
        } else {
          starJsonNode = childJsonNode;
        }
      }
      if (numChildNodes > MAX_CHILDREN) {
        // Keep the nodes with most child nodes
        Set<String> nodeNamesToKeep = new HashSet<>();
        for (Pair<String, Long> pair : queue) {
          nodeNamesToKeep.add(pair.getKey());
        }
        jsonNode.getChildren().removeIf(next -> !nodeNamesToKeep.contains(next.getName()));
      }
      // Add star-node at last
      if (starJsonNode != null) {
        jsonNode.addChild(starJsonNode);
      }
      return totalChildNodes;
    }
  }

  private static class StarTreeResource extends ResourceConfig {
    StarTreeResource(String json) {
      StarTreeViewRestResource resource = new StarTreeViewRestResource();
      StarTreeViewRestResource.json = json;
      register(resource.getClass());
    }
  }

  @Path("/")
  public static final class StarTreeViewRestResource {
    public static String json;

    @GET
    @Path("/data")
    @Produces(MediaType.TEXT_PLAIN)
    public String getStarTree() {
      return json;
    }

    @GET
    @Path("/")
    @Produces(MediaType.TEXT_HTML)
    public InputStream getStarTreeHtml() {
      return getClass().getClassLoader().getResourceAsStream("star-tree.html");
    }
  }
}
