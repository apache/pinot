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
package org.apache.pinot.common.coordination;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;


/// Deterministic identities shared by native task staging and consensus publication validation.
/// These names identify immutable attempt output; constructing one does not authorize its publication.
public final class NativeTaskOutputIdentity {
  public static final String TASK_TYPE = "NativeSegmentGenerationTask";

  private NativeTaskOutputIdentity() {
  }

  public static String outputSegmentName(UUID taskId, long attempt, int ordinal) {
    validate(taskId, attempt, ordinal);
    return "task_" + taskId + "_" + attempt + "_" + ordinal;
  }

  public static UUID outputObjectId(UUID historyId, UUID taskId, long attempt, int ordinal) {
    validate(taskId, attempt, ordinal);
    if (historyId == null) {
      throw new IllegalArgumentException("Output history is required");
    }
    return UUID.nameUUIDFromBytes((historyId + "\0TASK_OUTPUT\0" + taskId + "\0" + attempt + "\0" + ordinal)
        .getBytes(StandardCharsets.UTF_8));
  }

  public static String outputUri(String rootUri, UUID historyId, UUID taskId, long attempt, UUID incarnation,
      int ordinal, String sha256) {
    validate(taskId, attempt, ordinal);
    if (historyId == null || incarnation == null || sha256 == null || !sha256.matches("[a-f0-9]{64}")) {
      throw new IllegalArgumentException("Invalid output history, incarnation or SHA-256");
    }
    return outputRoot(rootUri).resolve(historyId + "/" + taskId + "/" + attempt + "/" + incarnation + "/"
        + ordinal + "-" + sha256 + ".tar.gz").toASCIIString();
  }

  public static URI outputRoot(String rootUri) {
    try {
      URI root = new URI(rootUri);
      if (!"file".equals(root.getScheme()) || root.getAuthority() != null || root.getQuery() != null
          || root.getFragment() != null || root.getPath() == null || !root.getPath().startsWith("/")
          || root.getRawPath().contains("%") || root.getPath().contains("\\") || !root.normalize().equals(root)) {
        throw new IllegalArgumentException("Output policy requires an absolute canonical file root");
      }
      return new URI("file", "", root.getPath().endsWith("/") ? root.getPath() : root.getPath() + "/", null);
    } catch (URISyntaxException | NullPointerException e) {
      throw new IllegalArgumentException("Invalid output root URI", e);
    }
  }

  private static void validate(UUID taskId, long attempt, int ordinal) {
    if (taskId == null || attempt <= 0 || attempt > 16 || ordinal < 0 || ordinal >= 16) {
      throw new IllegalArgumentException("Invalid task output identity");
    }
  }
}
