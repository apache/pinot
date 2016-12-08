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
package com.linkedin.pinot.common.response;

import com.linkedin.pinot.common.response.broker.BrokerResponseNative;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;


/**
 * Factory class to build a concrete BrokerResponse object for the given type.
 */
public class BrokerResponseFactory {
  private BrokerResponseFactory() {
  }

  /**
   * Enum for broker response types.
   */
  public enum ResponseType {
    BROKER_RESPONSE_TYPE_NATIVE
  }

  private static final String ILLEGAL_RESPONSE_TYPE = "Unsupported broker response type: ";

  /**
   * Get {@link ResponseType} response type from {@link String} response type.
   * <p>If the input string is null, return the default <code>BROKER_RESPONSE_TYPE_NATIVE</code>.
   */
  @Nonnull
  public static ResponseType getResponseType(@Nullable String responseType) {
    if (responseType == null || responseType.equalsIgnoreCase(ResponseType.BROKER_RESPONSE_TYPE_NATIVE.name())) {
      return ResponseType.BROKER_RESPONSE_TYPE_NATIVE;
    } else {
      throw new IllegalArgumentException(ILLEGAL_RESPONSE_TYPE + responseType);
    }
  }

  /**
   * Get a new broker response.
   */
  @Nonnull
  public static BrokerResponse getBrokerResponse(@Nonnull ResponseType responseType) {
    if (responseType.equals(ResponseType.BROKER_RESPONSE_TYPE_NATIVE)) {
      return new BrokerResponseNative();
    } else {
      throw new IllegalArgumentException(ILLEGAL_RESPONSE_TYPE + responseType);
    }
  }

  /**
   * Get a new broker response with pre-loaded exception.
   */
  @Nonnull
  public static BrokerResponse getBrokerResponseWithException(@Nonnull ResponseType responseType,
      @Nonnull ProcessingException processingException) {
    if (responseType.equals(ResponseType.BROKER_RESPONSE_TYPE_NATIVE)) {
      return new BrokerResponseNative(processingException);
    } else {
      throw new IllegalArgumentException(ILLEGAL_RESPONSE_TYPE + responseType);
    }
  }

  /**
   * Get a new broker response with pre-loaded exceptions.
   */
  @Nonnull
  public static BrokerResponse getBrokerResponseWithExceptions(@Nonnull ResponseType responseType,
      @Nonnull List<ProcessingException> processingExceptions) {
    if (responseType.equals(ResponseType.BROKER_RESPONSE_TYPE_NATIVE)) {
      return new BrokerResponseNative(processingExceptions);
    } else {
      throw new IllegalArgumentException(ILLEGAL_RESPONSE_TYPE + responseType);
    }
  }

  /**
   * Get the static broker response for case when no table is hit. Don't modify the broker response returned.
   */
  @Nonnull
  public static BrokerResponse getStaticNoTableHitBrokerResponse(@Nonnull ResponseType responseType) {
    if (responseType.equals(ResponseType.BROKER_RESPONSE_TYPE_NATIVE)) {
      return BrokerResponseNative.NO_TABLE_RESULT;
    } else {
      throw new IllegalArgumentException(ILLEGAL_RESPONSE_TYPE + responseType);
    }
  }

  /**
   * Get the static empty broker response. Don't modify the broker response returned.
   */
  @Nonnull
  public static BrokerResponse getStaticEmptyBrokerResponse(@Nonnull ResponseType responseType) {
    if (responseType.equals(ResponseType.BROKER_RESPONSE_TYPE_NATIVE)) {
      return BrokerResponseNative.EMPTY_RESULT;
    } else {
      throw new IllegalArgumentException(ILLEGAL_RESPONSE_TYPE + responseType);
    }
  }
}
