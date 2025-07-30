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
package org.apache.pinot.query.access;

import io.grpc.Attributes;
import io.grpc.Metadata;


public class AllowAllQueryAccessControlFactory extends QueryAccessControlFactory {
  public static final QueryAccessControl ALLOW_ALL_ACCESS = new QueryAccessControl() {
    @Override
    public boolean hasAccess(Attributes attributes, Metadata metadata) {
      return true;
    }
  };

  @Override
  public QueryAccessControl create() {
    return ALLOW_ALL_ACCESS;
  }
}
