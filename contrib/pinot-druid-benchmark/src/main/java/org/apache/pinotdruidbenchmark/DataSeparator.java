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
package org.apache.pinotdruidbenchmark;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;


/**
 * Separate data set into multiple chunks according to <code>l_shipdate</code>.
 */
public final class DataSeparator {
  private DataSeparator() {
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 2) {
      System.err.println("2 arguments required: INPUT_FILE_PATH, OUTPUT_DIR.");
      return;
    }

    File inputFile = new File(args[0]);
    File outputDir = new File(args[1]);
    if (!outputDir.exists()) {
      if (!outputDir.mkdirs()) {
        throw new RuntimeException("Failed to create output directory: " + outputDir);
      }
    }

    BufferedReader reader = new BufferedReader(new FileReader(inputFile));
    Map<String, BufferedWriter> writerMap = new HashMap<>();
    String line;
    while ((line = reader.readLine()) != null) {
      String shipDate = line.split("\\|")[10];
      BufferedWriter writer = writerMap.get(shipDate);
      if (writer == null) {
        writer = new BufferedWriter(new FileWriter(new File(outputDir, shipDate + ".csv")));
        writerMap.put(shipDate, writer);
      }
      writer.write(line);
      writer.newLine();
    }
    for (BufferedWriter writer : writerMap.values()) {
      writer.close();
    }
  }
}
