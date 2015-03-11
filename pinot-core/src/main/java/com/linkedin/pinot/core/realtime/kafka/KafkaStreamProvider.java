/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.realtime.kafka;

import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.realtime.StreamProvider;
import com.linkedin.pinot.core.realtime.StreamProviderConfig;

public class KafkaStreamProvider implements StreamProvider{

  @Override
  public void init(StreamProviderConfig streamProviderConfig) {
    // TODO Auto-generated method stub

  }

  @Override
  public void start() {
    // TODO Auto-generated method stub

  }

  @Override
  public void setOffset(long offset) {
    // TODO Auto-generated method stub

  }

  @Override
  public GenericRow next() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public GenericRow next(long offset) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long currentOffset() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public void commit() {
    // TODO Auto-generated method stub

  }

  @Override
  public void commit(long offset) {
    // TODO Auto-generated method stub

  }

  @Override
  public void shutdown() {
    // TODO Auto-generated method stub

  }

}
