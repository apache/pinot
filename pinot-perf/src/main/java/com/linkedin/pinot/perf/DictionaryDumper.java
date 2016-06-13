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
package com.linkedin.pinot.perf;

import com.linkedin.pinot.common.segment.ReadMode;
import com.linkedin.pinot.core.segment.index.IndexSegmentImpl;
import com.linkedin.pinot.core.segment.index.loader.Loaders;
import com.linkedin.pinot.core.segment.index.readers.ImmutableDictionaryReader;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DictionaryDumper {
  private static final Logger LOGGER = LoggerFactory.getLogger(DictionaryDumper.class);
  public static void main(String[] args)
      throws Exception {
    if (args.length != 3) {
      LOGGER.error("Usage: DictionaryDumper <segmentDirectory> <dimensionName> <comma-separated dictionaryIds>");
      System.exit(1);
    }
    File[] segmentDirs = new File(args[0]).listFiles();

    for (int i = 0; i < segmentDirs.length; i++) {
      File indexSegmentDir = segmentDirs[i];
      System.out.println("Loading " + indexSegmentDir.getName());

      IndexSegmentImpl indexSegmentImpl =
          (IndexSegmentImpl) Loaders.IndexSegment.load(indexSegmentDir, ReadMode.heap);
      ImmutableDictionaryReader colDictionary = indexSegmentImpl.getDictionaryFor(args[1]);
      List<String> strIdList = Arrays.asList(args[2].split(","));

      for (String strId: strIdList) {
        int id = Integer.valueOf(strId);
        String s = colDictionary.getStringValue(id);
        System.out.println(String.format("%d -> %s", id, s));
      }
    }
  }
}
