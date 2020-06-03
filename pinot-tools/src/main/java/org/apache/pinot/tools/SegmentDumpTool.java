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

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pinot.common.segment.ReadMode;
import org.apache.pinot.core.common.Block;
import org.apache.pinot.core.common.BlockMultiValIterator;
import org.apache.pinot.core.common.BlockSingleValIterator;
import org.apache.pinot.core.common.BlockValIterator;
import org.apache.pinot.core.common.BlockValSet;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.indexsegment.IndexSegment;
import org.apache.pinot.core.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadata;
import org.apache.pinot.core.segment.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.core.segment.index.readers.Dictionary;
import org.apache.pinot.core.startree.v2.StarTreeV2;
import org.apache.pinot.spi.data.FieldSpec;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;
import org.kohsuke.args4j.spi.StringArrayOptionHandler;


public class SegmentDumpTool extends AbstractBaseCommand implements Command {
  @Argument
  @Option(name = "-path", required = true, metaVar = "<string>", usage = "Path of the folder containing the segment"
      + " file")
  private String segmentPath;

  @Argument(index = 1, multiValued = true)
  @Option(name = "-columns", handler = StringArrayOptionHandler.class, usage = "Columns to dump")
  private List<String> columnNames;

  @Option(name = "-dumpStarTree")
  private boolean dumpStarTree;

  public void doMain(String[] args)
      throws Exception {
    CmdLineParser parser = new CmdLineParser(this);
    parser.parseArgument(args);
    dump();
  }

  private void dump()
      throws Exception {
    File segmentDir = new File(segmentPath);

    SegmentMetadata metadata = new SegmentMetadataImpl(segmentDir);

    // All columns by default
    if (columnNames == null) {
      columnNames = new ArrayList<>(metadata.getSchema().getColumnNames());
      Collections.sort(columnNames);
    }

    IndexSegment indexSegment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);

    Map<String, Dictionary> dictionaries = new HashMap<>();
    Map<String, BlockValIterator> iterators = new HashMap<>();

    for (String columnName : columnNames) {
      DataSource dataSource = indexSegment.getDataSource(columnName);
      Block block = dataSource.nextBlock();
      BlockValSet blockValSet = block.getBlockValueSet();
      BlockValIterator itr = (BlockValIterator) blockValSet.iterator();
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
        BlockValIterator itr = iterators.get(columnName);
        if (itr instanceof BlockSingleValIterator) {
          int encodedValue = ((BlockSingleValIterator) itr).nextIntVal();
          Object value = dictionaries.get(columnName).get(encodedValue);
          System.out.print(value);
          System.out.print("\t");
        } else {
          BlockMultiValIterator mItr = (BlockMultiValIterator) itr;
          int maxNumValuesPerMVEntry =
              indexSegment.getDataSource(columnName).getDataSourceMetadata().getMaxNumValuesPerMVEntry();
          int[] intArray = new int[maxNumValuesPerMVEntry];
          int length = mItr.nextIntVal(intArray);
          System.out.print("[");
          for (int j = 0; j < length; j++) {
            System.out.print(dictionaries.get(columnName).get(intArray[j]));
            if (j != length - 1) {
              System.out.print(",");
            }
          }
          System.out.print("]\t");
        }
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

  public static void main(String[] args)
      throws Exception {
    new SegmentDumpTool().doMain(args);
  }

  public String getName() {
    return getClass().getSimpleName();
  }

  @Override
  public boolean execute()
      throws Exception {
    dump();
    return true;
  }

  @Override
  public String description() {
    return "Dump the segment content of the given path.";
  }

  @Override
  public boolean getHelp() {
    return false;
  }
}
