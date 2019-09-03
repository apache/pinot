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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.common.utils.JsonUtils;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.BlockSingleValIterator;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.index.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.startree.StarTree;
import org.apache.pinot.core.startree.StarTreeNode;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class StarTreeIndexViewer {
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

  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeIndexViewer.class);

  /*
   * MAX num children to show in the UI
   */
  static int MAX_CHILDREN = 100;
  private List<String> _dimensionNames;
  private Map<String, Dictionary> dictionaries;
  private Map<String, BlockSingleValIterator> valueIterators;

  public StarTreeIndexViewer(File segmentDir)
      throws Exception {
    IndexSegment indexSegment = ImmutableSegmentLoader.load(segmentDir, ReadMode.heap);

    dictionaries = new HashMap<>();
    valueIterators = new HashMap<>();
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);

    for (String columnName : metadata.getAllColumns()) {
      DataSource dataSource = indexSegment.getDataSource(columnName);
      Block block = dataSource.nextBlock();
      BlockValSet blockValSet = block.getBlockValueSet();
      BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();
      valueIterators.put(columnName, itr);
      dictionaries.put(columnName, dataSource.getDictionary());
    }
    StarTree tree = indexSegment.getStarTrees().get(0).getStarTree();
    _dimensionNames = tree.getDimensionNames();
    StarTreeJsonNode jsonRoot = new StarTreeJsonNode("ROOT");
    build(tree.getRoot(), jsonRoot);
    indexSegment.destroy();

    String writeValueAsString = JsonUtils.objectToPrettyString(jsonRoot);
    LOGGER.info(writeValueAsString);
    startServer(segmentDir, writeValueAsString);
  }

  private int build(StarTreeNode indexNode, StarTreeJsonNode json) {
    Iterator<? extends StarTreeNode> childrenIterator = indexNode.getChildrenIterator();
    if (!childrenIterator.hasNext()) {
      return 0;
    }
    int childDimensionId = indexNode.getChildDimensionId();
    String childDimensionName = _dimensionNames.get(childDimensionId);
    Dictionary dictionary = dictionaries.get(childDimensionName);
    int totalChildNodes = indexNode.getNumChildren();

    Comparator<Pair<String, Integer>> comparator = new Comparator<Pair<String, Integer>>() {

      @Override
      public int compare(Pair<String, Integer> o1, Pair<String, Integer> o2) {
        return -1 * Integer.compare(o1.getRight(), o2.getRight());
      }
    };
    MinMaxPriorityQueue<Pair<String, Integer>> queue =
        MinMaxPriorityQueue.orderedBy(comparator).maximumSize(MAX_CHILDREN).create();
    StarTreeJsonNode allNode = null;

    while (childrenIterator.hasNext()) {
      StarTreeNode childIndexNode = childrenIterator.next();
      int childDimensionValueId = childIndexNode.getDimensionValue();
      String childDimensionValue = "ALL";
      if (childDimensionValueId != StarTreeNode.ALL) {
        childDimensionValue = dictionary.get(childDimensionValueId).toString();
      }
      StarTreeJsonNode childJson = new StarTreeJsonNode(childDimensionValue);
      totalChildNodes += build(childIndexNode, childJson);
      if (childDimensionValueId != StarTreeNode.ALL) {
        json.addChild(childJson);
        queue.add(ImmutablePair.of(childDimensionValue, totalChildNodes));
      } else {
        allNode = childJson;
      }
    }
    //put ALL node at the end
    if (allNode != null) {
      json.addChild(allNode);
    }
    if (totalChildNodes > MAX_CHILDREN) {
      Iterator<Pair<String, Integer>> qIterator = queue.iterator();
      Set<String> topKDimensions = new HashSet<>();
      topKDimensions.add("ALL");
      while (qIterator.hasNext()) {
        topKDimensions.add(qIterator.next().getKey());
      }
      Iterator<StarTreeJsonNode> iterator = json.getChildren().iterator();
      while (iterator.hasNext()) {
        StarTreeJsonNode next = iterator.next();
        if (!topKDimensions.contains(next.getName())) {
          iterator.remove();
        }
      }
    }
    return totalChildNodes;
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 1) {
      LOGGER.error("USAGE: StarIndexViewer <segmentDirectory>");
      System.exit(1);
    }
    String segmentDir = args[0];
    if (!new File(segmentDir).exists()) {
      throw new FileNotFoundException("Missing directory:" + segmentDir);
    }
    new StarTreeIndexViewer(new File(segmentDir));
  }

  private static class StarTreeResource extends ResourceConfig {
    StarTreeResource(String json) {
      StarTreeViewRestResource resource = new StarTreeViewRestResource();
      StarTreeViewRestResource.json = json;
      register(resource.getClass());
    }
  }

  private void startServer(final File segmentDirectory, final String json)
      throws Exception {
    int httpPort = 8090;
    URI baseUri = URI.create("http://0.0.0.0:" + Integer.toString(httpPort) + "/");
    HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, new StarTreeResource(json));

    LOGGER.info("Go to http://{}:{}/  to view the star tree", "localhost", httpPort);
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
