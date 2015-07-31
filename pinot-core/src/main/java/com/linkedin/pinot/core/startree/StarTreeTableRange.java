/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.startree;

import com.google.common.base.Objects;

public class StarTreeTableRange {
  private final Integer startDocumentId;
  private final Integer documentCount;

  public StarTreeTableRange(Integer startDocumentId, Integer documentCount) {
    this.startDocumentId = startDocumentId;
    this.documentCount = documentCount;
  }

  public Integer getStartDocumentId() {
    return startDocumentId;
  }

  public Integer getDocumentCount() {
    return documentCount;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
        .add("startDocumentId", startDocumentId)
        .add("documentCount", documentCount)
        .toString();
  }
}
