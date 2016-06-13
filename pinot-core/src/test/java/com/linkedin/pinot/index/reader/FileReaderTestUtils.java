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
package com.linkedin.pinot.index.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;


public class FileReaderTestUtils {

  public static int getNumOpenFiles(File f) throws IOException {
    Process plsof = new ProcessBuilder(new String[] { "lsof" }).start();
    BufferedReader plsofReader = new BufferedReader(new InputStreamReader(plsof.getInputStream()));
    String line;
    int numOpenFiles = 0;
    while ((line = plsofReader.readLine()) != null) {
      if (line.contains(f.getAbsolutePath())) {
        //System.out.println(line);
        numOpenFiles++;
      }
    }
    plsofReader.close();
    plsof.destroy();
    return numOpenFiles;
  }
}
