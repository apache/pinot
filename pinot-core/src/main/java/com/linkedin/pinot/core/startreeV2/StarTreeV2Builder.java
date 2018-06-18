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

package com.linkedin.pinot.core.startreeV2;

import java.io.File;
import java.util.Map;
import java.util.List;
import java.io.Closeable;
import java.io.IOException;
import com.linkedin.pinot.core.segment.creator.ColumnIndexCreationInfo;


public interface StarTreeV2Builder extends Closeable {

  /**
   * Initialize the builder, called before append().
   */
  void init(File indexDir, StarTreeV2Config config) throws Exception;

  /**
   * Build the StarTree
   */
  void build() throws IOException;

  /**
   * Serialize the star tree into a file.
   */
  void serialize(File starTreeFile, Map<String, ColumnIndexCreationInfo> indexCreationInfoMap) throws IOException;

  /**
   * Returns the Meta Data of the Star tree.
   */
  List<String> getMetaData();
}

