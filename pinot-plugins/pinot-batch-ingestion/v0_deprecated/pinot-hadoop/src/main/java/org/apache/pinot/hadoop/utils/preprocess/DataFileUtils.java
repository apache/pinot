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
package org.apache.pinot.hadoop.utils.preprocess;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;


public class DataFileUtils {
  private DataFileUtils() {
  }

  public static final String AVRO_FILE_EXTENSION = ".avro";
  public static final String ORC_FILE_EXTENSION = ".orc";

  /**
   * Returns the data files under the input directory with the given file extension.
   */
  public static List<Path> getDataFiles(Path inputDir, String dataFileExtension)
      throws IOException {
    FileStatus fileStatus = HadoopUtils.DEFAULT_FILE_SYSTEM.getFileStatus(inputDir);
    Preconditions.checkState(fileStatus.isDirectory(), "Path: %s is not a directory", inputDir);
    List<Path> dataFiles = new ArrayList<>();
    getDataFilesHelper(HadoopUtils.DEFAULT_FILE_SYSTEM.listStatus(inputDir), dataFileExtension, dataFiles);
    return dataFiles;
  }

  private static void getDataFilesHelper(FileStatus[] fileStatuses, String dataFileExtension, List<Path> dataFiles)
      throws IOException {
    for (FileStatus fileStatus : fileStatuses) {
      Path path = fileStatus.getPath();
      if (fileStatus.isDirectory()) {
        getDataFilesHelper(HadoopUtils.DEFAULT_FILE_SYSTEM.listStatus(path), dataFileExtension, dataFiles);
      } else {
        Preconditions.checkState(fileStatus.isFile(), "Path: %s is neither a directory nor a file", path);
        if (path.getName().endsWith(dataFileExtension)) {
          dataFiles.add(path);
        }
      }
    }
  }
}
