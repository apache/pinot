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

package com.linkedin.pinot.core.startree.v2;

import java.io.File;
import java.util.Map;
import java.util.HashMap;
import java.io.FileReader;
import java.io.IOException;
import java.io.BufferedReader;


public class OnHeapStarTreeV2LoaderHelper {

  /**
   * read meta data for star tree indexes.
   */
  public static Map<String, Integer> readMetaData(File indexMapFile) {

    Map<String, Integer> metadata = new HashMap<>();
    try {
      FileReader fileReader = new FileReader(indexMapFile);
      BufferedReader bufferedReader = new BufferedReader(fileReader);
      String line;

      while ((line = bufferedReader.readLine()) != null) {
        String s = line.toString();
        String[] parts = s.split(":");
        metadata.put(parts[0], Integer.parseInt(parts[1]));
      }
      fileReader.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
    return metadata;
  }
}
