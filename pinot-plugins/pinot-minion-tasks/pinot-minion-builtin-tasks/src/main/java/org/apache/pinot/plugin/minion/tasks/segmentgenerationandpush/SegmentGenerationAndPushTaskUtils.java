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
package org.apache.pinot.plugin.minion.tasks.segmentgenerationandpush;

import java.net.URI;
import java.util.Map;
import org.apache.pinot.spi.env.PinotConfiguration;
import org.apache.pinot.spi.filesystem.LocalPinotFS;
import org.apache.pinot.spi.filesystem.PinotFS;
import org.apache.pinot.spi.filesystem.PinotFSFactory;
import org.apache.pinot.spi.ingestion.batch.BatchConfigProperties;
import org.apache.pinot.spi.plugin.PluginManager;
import org.apache.pinot.spi.utils.IngestionConfigUtils;


public class SegmentGenerationAndPushTaskUtils {
  private SegmentGenerationAndPushTaskUtils() {
  }

  private static final PinotFS LOCAL_PINOT_FS = new LocalPinotFS();

  static PinotFS getInputPinotFS(Map<String, String> taskConfigs, URI fileURI)
      throws Exception {
    String fileURIScheme = fileURI.getScheme();
    if (fileURIScheme == null) {
      return LOCAL_PINOT_FS;
    }
    // Try to create PinotFS using given Input FileSystem config always
    String fsClass = taskConfigs.get(BatchConfigProperties.INPUT_FS_CLASS);
    if (fsClass != null) {
      PinotFS pinotFS = PluginManager.get().createInstance(fsClass);
      PinotConfiguration fsProps = IngestionConfigUtils.getInputFsProps(taskConfigs);
      pinotFS.init(fsProps);
      return pinotFS;
    }
    // Fallback to use the PinotFS created by Minion Server configs
    return PinotFSFactory.create(fileURIScheme);
  }

  static PinotFS getOutputPinotFS(Map<String, String> taskConfigs, URI fileURI)
      throws Exception {
    String fileURIScheme = (fileURI == null) ? null : fileURI.getScheme();
    if (fileURIScheme == null) {
      return LOCAL_PINOT_FS;
    }
    // Try to create PinotFS using given Input FileSystem config always
    String fsClass = taskConfigs.get(BatchConfigProperties.OUTPUT_FS_CLASS);
    if (fsClass != null) {
      PinotFS pinotFS = PluginManager.get().createInstance(fsClass);
      PinotConfiguration fsProps = IngestionConfigUtils.getOutputFsProps(taskConfigs);
      pinotFS.init(fsProps);
      return pinotFS;
    }
    // Fallback to use the PinotFS created by Minion Server configs
    return PinotFSFactory.create(fileURIScheme);
  }

  static PinotFS getLocalPinotFs() {
    return LOCAL_PINOT_FS;
  }
}
