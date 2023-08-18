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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.local.segment.readers.PinotSegmentRecordReader;
import org.apache.pinot.segment.spi.IndexSegment;
import org.apache.pinot.segment.spi.index.metadata.SegmentMetadataImpl;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.segment.spi.index.startree.StarTreeV2;
import org.apache.pinot.spi.data.Schema;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.utils.ReadMode;
import picocli.CommandLine;


@CommandLine.Command(name = "SegmentDump", description = "Dump the segment content of the given path.",
    mixinStandardHelpOptions = true)
public class SegmentDumpTool extends AbstractBaseCommand implements Command {
  @CommandLine.Option(names = {"-path"}, required = true, description = "Dump the segment content of the given path.")
  private String _segmentDir = null;

  @CommandLine.Option(names = {"-columns"}, arity = "1..*", description = "Columns to dump")
  private List<String> _columnNames;

  @CommandLine.Option(names = {"-dumpStarTree"})
  private boolean _dumpStarTree = false;

  private void dump()
      throws Exception {
    File indexDir = new File(_segmentDir);
    Schema schema = new SegmentMetadataImpl(indexDir).getSchema();
    PinotSegmentRecordReader reader = new PinotSegmentRecordReader(indexDir);
    GenericRow reuse = new GenericRow();

    // All columns by default
    if (_columnNames == null) {
      _columnNames = new ArrayList<>(schema.getColumnNames());
      Collections.sort(_columnNames);
    }

    // Collect MV columns.
    Set<String> mvColumns = new HashSet<>();
    for (String columnName : _columnNames) {
      if (!schema.getFieldSpecFor(columnName).isSingleValueField()) {
        mvColumns.add(columnName);
      }
    }

    dumpHeader(schema);
    dumpRows(reader, reuse, mvColumns);
    if (_dumpStarTree) {
      dumpStarTree();
    }

    reader.close();
  }

  private void dumpHeader(Schema schema) {
    System.out.println("Schema: " + schema);
    System.out.print("Doc\t");

    for (String columnName : _columnNames) {
      System.out.print(columnName);
      System.out.print("\t");
    }
  }

  private void dumpRows(PinotSegmentRecordReader reader, GenericRow reuse, Set<String> mvColumns) {
    int docId = 0;

    while (reader.hasNext()) {
      System.out.print(docId++ + "\t");
      GenericRow row = reader.next(reuse);

      for (String columnName : _columnNames) {
        if (!mvColumns.contains(columnName)) {
          System.out.print(row.getValue(columnName));
          System.out.print("\t");
        } else {
          Object[] values = (Object[]) row.getValue(columnName);
          System.out.print("[");

          for (int i = 0; i < values.length; i++) {
            System.out.print(values[i]);
            if (i < values.length - 1) {
              System.out.print(", ");
            }
          }
          System.out.print("]\t");
        }
      }
      System.out.println();
      row.clear();
    }
  }

  private void dumpStarTree()
      throws Exception {
    File segmentDir = new File(_segmentDir);
    IndexSegment indexSegment = ImmutableSegmentLoader.load(segmentDir, ReadMode.mmap);

    Map<String, Dictionary> dictionaries = new HashMap<>();
    for (String columnName : _columnNames) {
      dictionaries.put(columnName, indexSegment.getDataSource(columnName).getDictionary());
    }

    List<StarTreeV2> starTrees = indexSegment.getStarTrees();
    if (starTrees != null) {
      for (StarTreeV2 starTree : starTrees) {
        System.out.println();
        starTree.getStarTree().printTree(dictionaries);
      }
    }

    indexSegment.destroy();
  }

  public static void main(String[] args)
      throws Exception {
    SegmentDumpTool tool = new SegmentDumpTool();
    CommandLine commandLine = new CommandLine(tool);
    CommandLine.ParseResult result = commandLine.parseArgs(args);
    if (commandLine.isUsageHelpRequested() || result.matchedArgs().size() == 0) {
      commandLine.usage(System.out);
      return;
    }
    tool.execute();
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
}
