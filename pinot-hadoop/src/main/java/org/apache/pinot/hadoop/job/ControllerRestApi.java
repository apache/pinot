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
package org.apache.pinot.hadoop.job;

import java.io.Closeable;
import java.util.List;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.pinot.common.config.TableConfig;
import org.apache.pinot.common.data.Schema;


public interface ControllerRestApi extends Closeable {

  TableConfig getTableConfig();

  Schema getSchema();

  void pushSegments(FileSystem fileSystem, List<Path> tarFilePaths);

  void sendSegmentUris(List<String> segmentUris);

  /**
   * Delete extra segments after push during REFRESH use cases. Also used in APPEND use cases where
   * a day that has been re-pushed has extra segments.
   * @param segmentUris
   */
  void deleteExtraSegmentUris(List<String> segmentUris);

  List<String> getAllSegments(String tableType);
}
