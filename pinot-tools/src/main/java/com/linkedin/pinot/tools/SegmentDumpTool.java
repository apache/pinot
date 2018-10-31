/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
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
import com.linkedin.pinot.core.common.BlockSingleValIterator;
import com.linkedin.pinot.core.common.BlockValSet;
import com.linkedin.pinot.core.common.DataSource;
import com.linkedin.pinot.core.indexsegment.IndexSegment;
import com.linkedin.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import com.linkedin.pinot.core.segment.index.SegmentMetadataImpl;
import com.linkedin.pinot.core.segment.index.readers.Dictionary;
import com.linkedin.pinot.core.startree.v2.StarTreeV2;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;


public class SegmentDumpTool {
  @Argument
  private String segmentPath;

  @Argument(index = 1, multiValued = true)
  private List<String> columnNames;

  @Option(name = "-dumpStarTree")
  private boolean dumpStarTree;

  public void doMain(String[] args) throws Exception {
    CmdLineParser parser = new CmdLineParser(this);
    parser.parseArgument(args);

    File segmentDir = new File(segmentPath);

    SegmentMetadata metadata = new SegmentMetadataImpl(segmentDir);

    // All columns by default
    if (columnNames == null) {
      columnNames = new ArrayList<>(metadata.getSchema().getColumnNames());
      Collections.sort(columnNames);
    }

    IndexSegment indexSegment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);

    Map<String, Dictionary> dictionaries = new HashMap<>();
    Map<String, BlockSingleValIterator> iterators = new HashMap<>();

    for (String columnName : columnNames) {
      DataSource dataSource = indexSegment.getDataSource(columnName);
      Block block = dataSource.nextBlock();
      BlockValSet blockValSet = block.getBlockValueSet();
      BlockSingleValIterator itr = (BlockSingleValIterator) blockValSet.iterator();
      iterators.put(columnName, itr);
      dictionaries.put(columnName, dataSource.getDictionary());
    }

    System.out.print("Doc\t");
    for (String columnName : columnNames) {
      System.out.print(columnName);
      System.out.print("\t");
    }
    System.out.println();

    for (int i = 0; i < indexSegment.getSegmentMetadata().getTotalDocs(); i++) {
      System.out.print(i);
      System.out.print("\t");
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
      List<StarTreeV2> starTrees = indexSegment.getStarTrees();
      if (starTrees != null) {
        for (StarTreeV2 starTree : starTrees) {
          System.out.println();
          starTree.getStarTree().printTree(dictionaries);
        }
      }
    }

    indexSegment.destroy();
  }

  public static void main(String[] args) throws Exception {
    new SegmentDumpTool().doMain(args);
  }
}
