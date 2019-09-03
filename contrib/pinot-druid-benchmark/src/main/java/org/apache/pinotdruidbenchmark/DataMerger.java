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
import java.util.Arrays;


/**
 * Merge multiple chunks of data into one according to <code>l_shipdate</code>.
 */
public class DataMerger {
  private DataMerger() {
  }

  private enum MergeGranularity {
    MONTH, YEAR
  }

  public static void main(String[] args)
      throws Exception {
    if (args.length != 3) {
      System.err.println("3 arguments required: INPUT_DIR, OUTPUT_DIR, MERGE_GRANULARITY.");
      return;
    }

    File inputDir = new File(args[0]);
    File outputDir = new File(args[1]);
    if (!outputDir.exists()) {
      if (!outputDir.mkdirs()) {
        throw new RuntimeException("Failed to create output directory: " + outputDir);
      }
    }

    int subStringLength;
    switch (MergeGranularity.valueOf(args[2])) {
      case MONTH:
        subStringLength = 7;
        break;
      case YEAR:
        subStringLength = 4;
        break;
      default:
        throw new IllegalArgumentException("Unsupported merge granularity: " + args[2]);
    }

    String[] inputFileNames = inputDir.list();
    assert inputFileNames != null;
    Arrays.sort(inputFileNames);

    String currentOutputFileName = "";
    BufferedWriter writer = null;
    for (String inputFileName : inputFileNames) {
      BufferedReader reader = new BufferedReader(new FileReader(new File(inputDir, inputFileName)));
      String expectedOutputFileName = inputFileName.substring(0, subStringLength) + ".csv";
      if (!currentOutputFileName.equals(expectedOutputFileName)) {
        if (writer != null) {
          writer.close();
        }
        currentOutputFileName = expectedOutputFileName;
        writer = new BufferedWriter(new FileWriter(new File(outputDir, currentOutputFileName)));
      }

      assert writer != null;
      String line;
      while ((line = reader.readLine()) != null) {
        writer.write(line);
        writer.newLine();
      }
      reader.close();
    }
    if (writer != null) {
      writer.close();
    }
  }
}
