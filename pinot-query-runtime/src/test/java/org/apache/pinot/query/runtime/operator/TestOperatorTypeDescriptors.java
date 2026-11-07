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
package org.apache.pinot.query.runtime.operator;

import org.apache.pinot.common.datatable.StatMap;
import org.apache.pinot.common.response.broker.BrokerResponseNativeV2;


/// Factory for [OperatorTypeDescriptor] stubs, shared by tests that need plugin-style descriptors without a full
/// implementation. The returned descriptor reports the given id, name, and stat key class, and has a no-op
/// [OperatorTypeDescriptor#mergeInto].
public final class TestOperatorTypeDescriptors {
  private TestOperatorTypeDescriptors() {
  }

  @SuppressWarnings("rawtypes")
  public static OperatorTypeDescriptor of(int id, String name, Class statKeyClass) {
    return new OperatorTypeDescriptor() {
      @Override
      public int getId() {
        return id;
      }

      @Override
      public String name() {
        return name;
      }

      @SuppressWarnings("rawtypes")
      @Override
      public Class getStatKeyClass() {
        return statKeyClass;
      }

      @Override
      public void mergeInto(BrokerResponseNativeV2 response, StatMap<?> map) {
      }
    };
  }
}
