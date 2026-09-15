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
package org.apache.pinot.spi.recordtransformer.enricher;

import java.util.Objects;
import org.apache.pinot.spi.ingestion.IngestionGroovyPolicy;


/// Immutable, thread-safe context supplied when creating a [RecordEnricher].
///
/// Factories must honor the contained ingestion policy when constructing policy-sensitive enrichers.
public final class RecordEnricherCreationContext {
  private final IngestionGroovyPolicy _ingestionGroovyPolicy;

  public RecordEnricherCreationContext(IngestionGroovyPolicy ingestionGroovyPolicy) {
    _ingestionGroovyPolicy = Objects.requireNonNull(ingestionGroovyPolicy, "ingestionGroovyPolicy must not be null");
  }

  public IngestionGroovyPolicy getIngestionGroovyPolicy() {
    return _ingestionGroovyPolicy;
  }
}
