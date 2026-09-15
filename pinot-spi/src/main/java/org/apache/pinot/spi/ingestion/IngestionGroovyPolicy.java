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
package org.apache.pinot.spi.ingestion;


/// Immutable ingestion policy for Groovy-backed transform functions.
///
/// Enum instances are inherently thread-safe and can be shared across ingestion components.
public enum IngestionGroovyPolicy {
  ENABLED(false),
  DISABLED(true);

  private final boolean _ingestionGroovyDisabled;

  IngestionGroovyPolicy(boolean ingestionGroovyDisabled) {
    _ingestionGroovyDisabled = ingestionGroovyDisabled;
  }

  /// Returns the policy corresponding to the given disabled flag.
  public static IngestionGroovyPolicy fromDisabled(boolean ingestionGroovyDisabled) {
    return ingestionGroovyDisabled ? DISABLED : ENABLED;
  }

  /// Returns whether Groovy-backed ingestion transforms are disabled.
  public boolean isIngestionGroovyDisabled() {
    return _ingestionGroovyDisabled;
  }
}
