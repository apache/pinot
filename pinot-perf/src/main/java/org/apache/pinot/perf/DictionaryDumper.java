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
package org.apache.pinot.perf;

import com.google.common.base.Preconditions;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.pinot.segment.local.indexsegment.immutable.ImmutableSegmentLoader;
import org.apache.pinot.segment.spi.ImmutableSegment;
import org.apache.pinot.segment.spi.index.reader.Dictionary;
import org.apache.pinot.spi.utils.ReadMode;


public class DictionaryDumper {

  public static void main(String[] args) throws Exception {
    if (args.length != 3) {
      System.err.println("Usage: DictionaryDumper <segmentDirectory> <dimensionName> <comma-separated dictionaryIds>");
      System.exit(1);
    }
    File[] indexDirs = new File(args[0]).listFiles();
    Preconditions.checkNotNull(indexDirs);

    for (File indexDir : indexDirs) {
      System.out.println("Loading " + indexDir.getName());

      ImmutableSegment immutableSegment = ImmutableSegmentLoader.load(indexDir, ReadMode.heap);
      Dictionary colDictionary = immutableSegment.getDictionary(args[1]);
      List<String> strIdList = Arrays.asList(args[2].split(","));

      for (String strId : strIdList) {
        int id = Integer.valueOf(strId);
        String s = colDictionary.getStringValue(id);
        System.out.println(String.format("%d -> %s", id, s));
      }
    }
  }
}
