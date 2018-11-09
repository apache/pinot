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
package com.linkedin.pinot.core.realtime.stream;

import java.io.Closeable;
import javax.annotation.Nonnull;


/**
 * Interface for provider of stream metadata such as partition count, partition offsets
 */
public interface StreamMetadataProvider extends Closeable {
  /**
   * Fetches the number of partitions for a topic given the stream configs
   * @param timeoutMillis
   * @return
   */
  int fetchPartitionCount(long timeoutMillis);

  /**
   * Fetches the offset for a given partition and offset criteria
   * @param offsetCriteria
   * @param timeoutMillis
   * @return
   * @throws java.util.concurrent.TimeoutException
   */
  long fetchPartitionOffset(@Nonnull OffsetCriteria offsetCriteria, long timeoutMillis)
      throws java.util.concurrent.TimeoutException;
}
