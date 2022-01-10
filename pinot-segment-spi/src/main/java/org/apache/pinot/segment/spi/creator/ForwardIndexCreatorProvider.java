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
package org.apache.pinot.segment.spi.creator;

import org.apache.pinot.segment.spi.index.creator.ForwardIndexCreator;


public interface ForwardIndexCreatorProvider {
  /**
   * Creates a {@see ForwardIndexCreator} from information about index creation.
   * This allows a plugin to pattern match index creation information to select
   * an appropriate implementation.
   * @param context context about the index creation.
   * @return a {@see ForwardIndexCreator}
   * @throws Exception whenever something goes wrong matching or constructing the creator
   */
  ForwardIndexCreator newForwardIndexCreator(IndexCreationContext.Forward context)
      throws Exception;
}
