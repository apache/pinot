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
package org.apache.pinot.common.tier;

/**
 * Tier storage type which uses Pinot servers as storage
 */
public class PinotServerTierStorage implements TierStorage {

  private final String _tag;

  public PinotServerTierStorage(String tag) {
    _tag = tag;
  }

  /**
   * Returns the tag used to identify the servers being used as the tier storage
   */
  public String getTag() {
    return _tag;
  }

  @Override
  public String getType() {
    return TierFactory.PINOT_SERVER_STORAGE_TYPE;
  }
}
