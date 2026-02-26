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
package org.apache.pinot.spi.data.readers;

import java.io.IOException;


/**
 * Exception indicating an I/O or data fetch error that may prevent the RecordReader
 * from advancing its pointer to the next record. This exception should be thrown when
 * the RecordReader encounters an error during data fetching that prevents it from
 * moving forward, potentially causing an infinite loop if the same record is repeatedly
 * attempted.
 * <p>
 * When this exception is thrown and continueOnError is enabled, it will count toward
 * the consecutive failure threshold to prevent infinite loops.
 */
public class RecordFetchException extends IOException {

  public RecordFetchException(String message) {
    super(message);
  }

  public RecordFetchException(String message, Throwable cause) {
    super(message, cause);
  }

  public RecordFetchException(Throwable cause) {
    super(cause);
  }
}
