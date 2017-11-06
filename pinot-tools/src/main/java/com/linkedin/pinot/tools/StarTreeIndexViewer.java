/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.tools;

import com.google.common.collect.MinMaxPriorityQueue;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.segment.store.SegmentDirectoryPaths;
import com.linkedin.pinot.core.startree.StarTreeIndexNodeInterf;
import com.linkedin.pinot.core.startree.StarTreeInterf;
import com.linkedin.pinot.core.startree.StarTreeSerDe;
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
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.annotate.JsonSerialize.Inclusion;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StarTreeIndexViewer {
  private static final Logger LOGGER = LoggerFactory.getLogger(StarTreeIndexViewer.class);

  /*
   * MAX num children to show in the UI
   */
  static int MAX_CHILDREN = 100;
  private List<String> _dimensionNames;
  private Map<String, Dictionary> dictionaries;
  private Map<String, BlockSingleValIterator> valueIterators;

  public StarTreeIndexViewer(File segmentDir) throws Exception {
    IndexSegment indexSegment = Loaders.IndexSegment.load(segmentDir, ReadMode.heap);

    dictionaries = new HashMap<String, Dictionary>();
    valueIterators = new HashMap<String, BlockSingleValIterator>();
    SegmentMetadataImpl metadata = new SegmentMetadataImpl(segmentDir);

    for (String columnName : metadata.getAllColumns()) {
      DataSource dataSource = indexSegment.getDataSource(columnName);
      dataSource.open();
      Block block = dataSource.nextBlock();
      BlockValSet blockValSet = block.getBlockValueSet();
      BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();
      valueIterators.put(columnName, itr);
      dictionaries.put(columnName, dataSource.getDictionary());
    }
    File starTreeFile = SegmentDirectoryPaths.findStarTreeFile(segmentDir);
    StarTreeInterf tree = StarTreeSerDe.fromFile(starTreeFile, ReadMode.mmap);
    _dimensionNames = tree.getDimensionNames();
    StarTreeJsonNode jsonRoot = new StarTreeJsonNode("ROOT");
    build(tree.getRoot(), jsonRoot);
    ObjectMapper objectMapper = new ObjectMapper();
    objectMapper.getSerializationConfig().setSerializationInclusion(Inclusion.NON_NULL);
    String writeValueAsString =
        objectMapper.defaultPrettyPrintingWriter().writeValueAsString(jsonRoot);
    LOGGER.info(writeValueAsString);
    startServer(segmentDir, writeValueAsString);
  }

  private int build(StarTreeIndexNodeInterf indexNode, StarTreeJsonNode json) {
    Iterator<? extends StarTreeIndexNodeInterf> childrenIterator = indexNode.getChildrenIterator();
    if (!childrenIterator.hasNext()) {
      return 0;
    }
    int childDimensionId = indexNode.getChildDimensionName();
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
      StarTreeIndexNodeInterf childIndexNode = childrenIterator.next();
      int childDimensionValueId = childIndexNode.getDimensionValue();
      String childDimensionValue = "ALL";
      if (childDimensionValueId != StarTreeIndexNodeInterf.ALL) {
        childDimensionValue = dictionary.get(childDimensionValueId).toString();
      }
      StarTreeJsonNode childJson = new StarTreeJsonNode(childDimensionValue);
      totalChildNodes += build(childIndexNode, childJson);
      if (childDimensionValueId != StarTreeIndexNodeInterf.ALL) {
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

  public static void main(String[] args) throws Exception {
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

  private void startServer(final File segmentDirectory, final String json) throws Exception {
    int httpPort = 8090;
    URI baseUri = URI.create("http://0.0.0.0:" + Integer.toString(httpPort) + "/");
    HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(baseUri, new StarTreeResource(json));

    LOGGER.info("Go to http://{}:{}/  to view the star tree", "localhost", httpPort );
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

class StarTreeJsonNode {

  public StarTreeJsonNode(String name) {
    this.name = name;
  }

  String name;
  String parent;
  int size = 0;
  List<StarTreeJsonNode> children;

  public void addChild(StarTreeJsonNode childJsonNode) {
    if (children == null) {
      children = new ArrayList<>();
    }
    children.add(childJsonNode);
  }

  public String getParent() {
    return parent;
  }

  public void setParent(String parent) {
    this.parent = parent;
  }

  public void addChild(String name) {
    if (children == null) {
      children = new ArrayList<>();
    }
    children.add(new StarTreeJsonNode(name));
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<StarTreeJsonNode> getChildren() {
    return children;
  }

  public void setChildren(List<StarTreeJsonNode> children) {
    this.children = children;
  }
}
