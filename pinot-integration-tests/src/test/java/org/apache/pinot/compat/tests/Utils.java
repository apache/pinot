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
package org.apache.pinot.compat.tests;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class Utils {

  /**
   * Replace all occurrence of a string in originalDataFile and write the replaced content to replacedDataFile.
   * @param originalDataFile
   * @param replacedDataFile
   * @param original
   * @param replaced
   * @throws IOException
   */
  public static void replaceContent(File originalDataFile, File replacedDataFile, String original, String replaced)
      throws IOException {
    Stream<String> lines = Files.lines(originalDataFile.toPath());
    List<String> replacedContent = lines.map(line -> line.replaceAll(original, replaced)).collect(Collectors.toList());
    Files.write(replacedDataFile.toPath(), replacedContent);
    lines.close();
  }
}
