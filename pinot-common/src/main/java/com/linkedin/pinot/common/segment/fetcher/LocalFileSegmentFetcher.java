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
package com.linkedin.pinot.common.segment.fetcher;

import java.util.Collections;
import java.util.Set;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class LocalFileSegmentFetcher implements SegmentFetcher {

  private static final Logger LOGGER = LoggerFactory.getLogger(LocalFileSegmentFetcher.class);

  @Override
  public void init(Configuration configs) {
  }

  @Override
  public void fetchSegmentToLocal(String uri, File tempFile) throws Exception {
    FileUtils.copyFile(new File(uri), tempFile);
    LOGGER.info("Copy file from {} to {}; Length of file: {}", uri, tempFile, tempFile.length());
  }

  @Override
  public Set<String> getProtectedConfigKeys() {
    return Collections.<String>emptySet();
  }
}
