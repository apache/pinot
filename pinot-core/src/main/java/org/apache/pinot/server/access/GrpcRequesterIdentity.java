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
package org.apache.pinot.server.access;

import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import java.util.Map;


/**
 * Identity container for GRPC requests with (optional) authorization metadata
 */
public class GrpcRequesterIdentity extends RequesterIdentity {
  private Multimap<String, String> _metaData;

  public GrpcRequesterIdentity(Map<String, String> metadataMap) {
    _metaData = Multimaps.forMap(metadataMap);
  }

  public Multimap<String, String> getGrpcMetadata() {
    return _metaData;
  }

  public void setGrpcMetadata(Multimap<String, String> metaData) {
    _metaData = metaData;
  }
}
