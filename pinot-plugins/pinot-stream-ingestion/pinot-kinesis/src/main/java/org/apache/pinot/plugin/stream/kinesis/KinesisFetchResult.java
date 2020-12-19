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
package org.apache.pinot.plugin.stream.kinesis;

import java.util.ArrayList;
import java.util.List;
import org.apache.pinot.spi.stream.v2.Checkpoint;
import org.apache.pinot.spi.stream.v2.FetchResult;
import software.amazon.awssdk.services.kinesis.model.Record;


public class KinesisFetchResult implements FetchResult<Record> {
  private final KinesisCheckpoint _kinesisCheckpoint;
  private final List<Record> _recordList;

  public KinesisFetchResult(KinesisCheckpoint kinesisCheckpoint, List<Record> recordList) {
    _kinesisCheckpoint = kinesisCheckpoint;
    _recordList = recordList;
  }

  @Override
  public Checkpoint getLastCheckpoint() {
    return _kinesisCheckpoint;
  }

  @Override
  public List<Record> getMessages() {
    return _recordList;
  }
}
