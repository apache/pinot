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
package org.apache.pinot.common.utils.fetcher;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.helix.HelixManager;
import org.apache.pinot.spi.env.PinotConfiguration;


public interface SegmentFetcher {

  /**
   * Initializes the segment fetcher.
   */
  void init(PinotConfiguration config);

  /**
   * Fetches a segment from URI location to local.
   */
  void fetchSegmentToLocal(URI uri, File dest)
      throws Exception;

  /**
   * Fetches a segment from URI location and untar to local in a streamed manner
   */
  File fetchUntarSegmentToLocalStreamed(URI uri, File dest, long rateLimit, AtomicInteger attempts)
      throws Exception;

  /**
   * Fetches a segment to local from any uri in the given list.
   */
  void fetchSegmentToLocal(List<URI> uri, File dest)
      throws Exception;

  /**
   *
   * @param segmentName The segment file name.
   * @param dest The destination to put the downloaded segment.
   * @param helixManager The Helix cluster manager to get the location of the segment.
   * @param downloadScheme http or https
   * @return true if and only if the segment fetch is successful.
   * @throws Exception
   */
  boolean fetchSegmentToLocal(String segmentName, File dest, HelixManager helixManager, String downloadScheme)
      throws Exception;
}
