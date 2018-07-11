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
package com.linkedin.pinot.transport.pool;

import com.linkedin.pinot.common.response.ServerInstance;


/**
 *
 * Interface to create/destroy and validate pooled resource
 * The implementation is expected to be thread-safe as concurrent request to create connection for same/different servers
 * is possible.
 *
 * @param <T>
 */
public interface PooledResourceManager<T> {

  /**
   * Create a new resource
   * @param key to identify the pool which will host the resource
   * @return future for the resource creation
   */
  public T create(ServerInstance key);

  /**
   * Destroy the pooled resource
   * @param key Key to identify the pool
   * @param isBad Are we destroying because the connection is bad ?
   * @param resource Resource to be destroyed
   * @return true if successfully destroyed the resource, false otherwise
   */
  public boolean destroy(ServerInstance key, boolean isBad, T resource);

  /**
   * Validate if the resource is good.
   *
   * @param key
   * @param resource
   * @return
   */
  public boolean validate(ServerInstance key, T resource);

}
