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

import java.io.File;

public class TestSegmentFetcher implements SegmentFetcher {
  public int init_called = 0;

  @Override
  public void init(Configuration configs) {
    init_called++;
  }

  @Override
  public void fetchSegmentToLocal(String uri, File tempFile) throws Exception {

  }

  @Override
  public Set<String> getProtectedConfigKeys() {
    return Collections.<String>emptySet();
  }
}
