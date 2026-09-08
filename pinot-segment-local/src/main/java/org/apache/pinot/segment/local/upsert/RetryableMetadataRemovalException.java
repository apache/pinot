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
package org.apache.pinot.segment.local.upsert;


/// A metadata implementation could not finish removal, but has preserved the work needed to retry it.
/// The caller must keep the affected partition unavailable and the unfinished segment alive until removal succeeds.
/// Throwing this exception does not establish that retry will succeed or that other metadata operations are safe.
public class RetryableMetadataRemovalException extends RuntimeException {
  public RetryableMetadataRemovalException(String message, Throwable cause) {
    super(message, cause);
  }
}
