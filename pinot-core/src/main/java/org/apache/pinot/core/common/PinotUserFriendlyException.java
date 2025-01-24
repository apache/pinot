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
package org.apache.pinot.core.common;

import java.text.MessageFormat;
import java.util.Locale;
import java.util.ResourceBundle;


/**
 * A Pinot exception whose message is user-friendly and can be localized.
 *
 * These exceptions are localized using the resource bundle {@code pinot.exception} in the classpath. The resource
 * bundle should contain a set of keys and values, where the keys are the exception codes and the values are the
 * localized messages. The localized messages can contain placeholders for parameters, which are filled in using
 * the {@code MessageFormat} class.
 * This means that the messages can be parameterized with {@code {0}}, {@code {1}}, etc.
 *
 * Ideally, only this kind of exception should be thrown to the user, but in practice the vast majority of the codebase
 * still uses {@link RuntimeException} directly. We should gradually migrate to this class.
 */
public class PinotUserFriendlyException extends PinotRuntimeException {

  private static final String I18N = "pinot.exception";
  private final String _i18n;
  private final Object[] _params;

  public PinotUserFriendlyException(String i18n, Object... params) {
    _i18n = i18n;
    _params = params;
  }

  public PinotUserFriendlyException(String message, String i18n, Object... params) {
    super(message);
    _i18n = i18n;
    _params = params;
  }

  public PinotUserFriendlyException(String message, Throwable cause, String i18n, Object... params) {
    super(message, cause);
    _i18n = i18n;
    _params = params;
  }

  public PinotUserFriendlyException(Throwable cause, String i18n, Object... params) {
    super(cause);
    _i18n = i18n;
    _params = params;
  }

  public String getUserMessage(Locale locale) {
    ResourceBundle resourceBundle = ResourceBundle.getBundle(I18N, locale);
    String messageTemplate = resourceBundle.getString(_i18n);
    if (_params == null || _params.length == 0) {
      return messageTemplate;
    }
    return new MessageFormat(messageTemplate, locale).format(_params);
  }
}
