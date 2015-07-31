/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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

import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.common.segment.SegmentMetadata;
import com.linkedin.pinot.core.common.Block;
import com.linkedin.pinot.core.common.BlockDocIdIterator;
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.common.Operator;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.segment.creator.impl.V1Constants;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.startree.StarTreeIndexNode;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SegmentDumpTool {
  @Argument
  private String segmentPath;

  @Argument(index = 1, multiValued = true)
  private List<String> columnNames;

  @Option(name="-dumpStarTree")
  private boolean dumpStarTree;

  public void doMain(String[] args) throws Exception {
    CmdLineParser parser = new CmdLineParser(this);
    parser.parseArgument(args);

    File segmentDir = new File(segmentPath);
    File starTreeDir = new File(segmentDir, V1Constants.STARTREE_DIR);

    SegmentMetadata metadata = new SegmentMetadataImpl(segmentDir);

    if (dumpStarTree) {
      // Look at star tree instead
      segmentDir = starTreeDir;
    }

    // All columns by default
    if (columnNames == null) {
      columnNames = new ArrayList<String>(metadata.getSchema().getColumnNames());
      Collections.sort(columnNames);
    }

    IndexSegment indexSegment = Loaders.IndexSegment.load(segmentDir, ReadMode.mmap);

    Map<String, Dictionary> dictionaries = new HashMap<String, Dictionary>();
    Map<String, BlockSingleValIterator> iterators = new HashMap<String, BlockSingleValIterator>();

    for (String columnName : columnNames) {
      DataSource dataSource = indexSegment.getDataSource(columnName);
      dataSource.open();
      Block block = dataSource.nextBlock();
      BlockValSet blockValSet = block.getBlockValueSet();
      BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();
      iterators.put(columnName, itr);
      dictionaries.put(columnName, dataSource.getDictionary());
    }

    for (String columnName : columnNames) {
      System.out.print(columnName);
      System.out.print("\t");
    }
    System.out.println();

    for (int i = 0; i < indexSegment.getTotalDocs(); i++) {
      for (String columnName : columnNames) {
        FieldSpec.DataType columnType = metadata.getSchema().getFieldSpecFor(columnName).getDataType();
        BlockSingleValIterator itr = iterators.get(columnName);
        Integer encodedValue = itr.nextIntVal();
        Object value = dictionaries.get(columnName).get(encodedValue);
        System.out.print(value);
        System.out.print("\t");
      }
      System.out.println();
    }

    if (dumpStarTree) {
      System.out.println();
      File starTreeFile = new File(starTreeDir, V1Constants.STARTREE_FILE);
      StarTreeIndexNode tree = StarTreeIndexNode.fromBytes(new FileInputStream(starTreeFile));
      StarTreeIndexNode.printTree(tree, 0);
    }
  }

  public static void main(String[] args) throws Exception {
    new SegmentDumpTool().doMain(args);
  }
}
