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
package com.linkedin.pinot.common.segment.fetcher;

import java.io.File;
import java.util.Set;
import org.apache.commons.configuration.Configuration;


public interface SegmentFetcher {

  /**
   * Initializes configurations
   * @param configs
   */
  void init(Configuration configs);

  /**
   * Fetches segment from a uri location to the local filesystem. Can come from a remote fs or a local fs.
   * @param uri current segment location
   * @param tempFile location segment will be stored locally
   * @throws Exception
   */
  void fetchSegmentToLocal(String uri, File tempFile) throws Exception;

  /**
   * Returns a list of config keys whose value should not be logged.
   *
   * @return List of protected config keys
   */
  Set<String> getProtectedConfigKeys();
}
