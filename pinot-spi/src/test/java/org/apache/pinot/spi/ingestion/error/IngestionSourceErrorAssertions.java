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
package org.apache.pinot.spi.ingestion.error;

import java.util.Optional;
import javax.annotation.Nullable;
import org.apache.commons.lang3.exception.ExceptionUtils;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;


/// Assertions for the fixture tests that pin the classifier's rules against each SDK. A failed assertion prints the
/// whole exception tree, so a rule can be corrected from it.
public final class IngestionSourceErrorAssertions {
  private IngestionSourceErrorAssertions() {
  }

  public static IngestionSourceErrorClassification classify(Throwable failure) {
    return IngestionSourceErrorClassifier.classify(failure)
        .orElseThrow(() -> new AssertionError("Not classified: " + describe(failure)));
  }

  public static IngestionSourceErrorClassification assertAuth(Throwable failure,
      IngestionSourceErrorSubReason subReason, @Nullable String target) {
    IngestionSourceErrorClassification classification = classify(failure);
    assertEquals(classification.getCategory(), IngestionSourceErrorCategory.AUTH, describe(failure));
    assertEquals(classification.getSubReason(), subReason, describe(failure));
    assertEquals(classification.getTarget(), target, describe(failure));
    return classification;
  }

  public static IngestionSourceErrorClassification assertSourceRead(Throwable failure, @Nullable String target) {
    IngestionSourceErrorClassification classification = classify(failure);
    assertEquals(classification.getCategory(), IngestionSourceErrorCategory.SOURCE_READ, describe(failure));
    assertEquals(classification.getTarget(), target, describe(failure));
    return classification;
  }

  public static void assertNotClassified(Throwable failure) {
    Optional<IngestionSourceErrorClassification> classification = IngestionSourceErrorClassifier.classify(failure);
    assertTrue(classification.isEmpty(), classification + " for " + describe(failure));
  }

  private static String describe(Throwable failure) {
    return ExceptionUtils.getStackTrace(failure);
  }
}
