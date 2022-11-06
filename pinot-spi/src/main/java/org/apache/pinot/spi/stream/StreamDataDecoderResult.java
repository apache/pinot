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

import javax.annotation.Nullable;
import org.apache.pinot.spi.data.readers.GenericRow;


/**
 * A container class for holding the result of a decoder
 * At any point in time, only one of Result or exception is set as null.
 */
public final class StreamDataDecoderResult {
  private final GenericRow _result;
  private final Exception _exception;

  public StreamDataDecoderResult(GenericRow result, Exception exception) {
    _result = result;
    _exception = exception;
  }

  @Nullable
  public GenericRow getResult() {
    return _result;
  }

  @Nullable
  public Exception getException() {
    return _exception;
  }
}
