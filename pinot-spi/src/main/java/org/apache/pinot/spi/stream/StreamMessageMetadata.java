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
package org.apache.pinot.spi.stream;

import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A class that provides metadata associated with the message of a stream, for e.g.,
 * ingestion-timestamp of the message.
 */
public class StreamMessageMetadata implements RowMetadata {

  private final long _ingestionTimeMs;

  GenericRow _headers;

  /**
   * Construct the stream based message/row message metadata
   *
   * @param ingestionTimeMs  the time that the message was ingested by the stream provider
   *                         use Long.MIN_VALUE if not applicable
   */
  public StreamMessageMetadata(long ingestionTimeMs) {
    _ingestionTimeMs = ingestionTimeMs;
  }

  @Override
  public long getIngestionTimeMs() {
    return _ingestionTimeMs;
  }

  @Override
  public GenericRow getHeaders() {
    return _headers;
  }

  public void setHeaders(GenericRow headers) {
    _headers = headers;
  }
}
