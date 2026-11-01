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

import javax.annotation.Nullable;


public final class IngestionSourceErrorVerdict {
  public static final String TARGET_BROKER = "broker";
  public static final String TARGET_SCHEMA_REGISTRY = "schema_registry";
  public static final String TARGET_STS = "sts";
  public static final String TARGET_KMS = "kms";
  public static final String TARGET_IDP = "idp";
  public static final String TARGET_ADMIN = "admin";

  private static final String TOKEN_PUNCTUATION = "_.:/-";
  private static final int MAX_TOKEN_LENGTH = 64;

  @Nullable
  private final IngestionSourceErrorCategory _category;
  @Nullable
  private final String _code;
  @Nullable
  private final String _target;

  private IngestionSourceErrorVerdict(@Nullable IngestionSourceErrorCategory category, @Nullable String code,
      @Nullable String target) {
    _category = category;
    _code = token(code);
    _target = token(target);
  }

  public static IngestionSourceErrorVerdict auth(@Nullable String code, @Nullable String target) {
    return new IngestionSourceErrorVerdict(IngestionSourceErrorCategory.AUTH, code, target);
  }

  public static IngestionSourceErrorVerdict sourceRead(@Nullable String code, @Nullable String target) {
    return new IngestionSourceErrorVerdict(IngestionSourceErrorCategory.SOURCE_READ, code, target);
  }

  public static IngestionSourceErrorVerdict none(@Nullable String code) {
    return new IngestionSourceErrorVerdict(null, code, null);
  }

  @Nullable
  public IngestionSourceErrorCategory getCategory() {
    return _category;
  }

  public boolean isNone() {
    return _category == null;
  }

  @Nullable
  public String getCode() {
    return _code;
  }

  @Nullable
  public String getTarget() {
    return _target;
  }

  @Nullable
  static String token(@Nullable String value) {
    if (value == null) {
      return null;
    }
    StringBuilder token = new StringBuilder(Math.min(value.length(), MAX_TOKEN_LENGTH));
    for (int i = 0; i < value.length() && token.length() < MAX_TOKEN_LENGTH; ) {
      int codePoint = value.codePointAt(i);
      token.append(isTokenSafe(codePoint) ? (char) codePoint : '_');
      i += Character.charCount(codePoint);
    }
    return token.toString();
  }

  private static boolean isTokenSafe(int codePoint) {
    return (codePoint >= 'A' && codePoint <= 'Z') || (codePoint >= 'a' && codePoint <= 'z')
        || (codePoint >= '0' && codePoint <= '9') || TOKEN_PUNCTUATION.indexOf(codePoint) >= 0;
  }

  @Override
  public String toString() {
    String category = _category != null ? _category.name() : "none";
    return category + (_code != null ? " code=" + _code : "")
        + (_target != null ? " target=" + _target : "");
  }
}
