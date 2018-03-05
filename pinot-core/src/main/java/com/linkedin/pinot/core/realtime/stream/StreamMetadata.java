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
package com.linkedin.pinot.core.realtime.stream;

import com.linkedin.pinot.common.utils.CommonConstants;
import java.util.List;


/**
 * Interface for holding metadata of a stream
 */
public interface StreamMetadata {

  String getStreamName();

  String getStreamType();

  int getPartitionCount();

  String getInitialConsumerOffsetCriteria();

  long getPartitionOffset(int partitionId, String offsetCriteria);

  int getFetchTimeoutMillis();

  long getConnectionTimeoutMillis();

  StreamProviderConfig createStreamProviderConfig();

  boolean hasHighLevelConsumerType();

  boolean hasSimpleConsumerType();

  List<CommonConstants.Helix.DataSource.Realtime.Kafka.ConsumerType> getConsumerTypes();

  PinotStreamConsumerFactory getPinotStreamConsumerFactory();
}
