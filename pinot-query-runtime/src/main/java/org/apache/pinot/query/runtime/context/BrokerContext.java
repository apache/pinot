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
package org.apache.pinot.query.runtime.context;

import java.util.Objects;
import javax.annotation.Nullable;
import org.apache.pinot.query.runtime.operator.factory.QueryOperatorFactoryProvider;


/**
 * The <code>BrokerContext</code> class is a singleton class which contains all broker related context.
 */
public class BrokerContext {
  private static final BrokerContext INSTANCE = new BrokerContext();

  private BrokerContext() {
  }

  public static BrokerContext getInstance() {
    return INSTANCE;
  }

  // Must be set during broker initialization; null indicates no provider configured yet.
  @Nullable
  private QueryOperatorFactoryProvider _queryOperatorFactoryProvider;

  /**
   * Returns the configured provider or null if none has been set yet.
   */
  @Nullable
  public QueryOperatorFactoryProvider getQueryOperatorFactoryProvider() {
    return _queryOperatorFactoryProvider;
  }

  public void setQueryOperatorFactoryProvider(QueryOperatorFactoryProvider queryOperatorFactoryProvider) {
    _queryOperatorFactoryProvider =
        Objects.requireNonNull(queryOperatorFactoryProvider, "queryOperatorFactoryProvider must be set");
  }
}
