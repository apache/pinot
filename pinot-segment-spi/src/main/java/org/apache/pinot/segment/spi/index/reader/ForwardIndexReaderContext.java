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
package org.apache.pinot.segment.spi.index.reader;


/// Interface for the context of the forward index reader.
///
/// The forward index reader itself is always stateless because it needs to be accessed by multiple threads. The
/// context can be passed when reading the values from the forward index reader so that reader can store some states
/// inside the context in order to accelerate the following reads.
public interface ForwardIndexReaderContext extends AutoCloseable {

  /// Releases the resources of the context, for example a direct buffer.
  ///
  /// Implementations must be idempotent. The query engine registers the context with
  /// `org.apache.pinot.spi.query.QueryThreadContext#registerResource`, so a context can be closed by its owner and
  /// again when the query task ends.
  @Override
  void close();
}
