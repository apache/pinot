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
package org.apache.pinot.common.utils;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;


public class ExceptionUtils {
  private static final String CAUSE_SEPARATOR = " -> ";
  private static final String ELLIPSIS = "...";
  private static final int MIN_CAUSE_LENGTH = ELLIPSIS.length() + 2;

  private ExceptionUtils() {
  }

  public static String consolidateExceptionTraces(Throwable e) {
    StringBuilder sb = new StringBuilder();
    // No more than 10 causes
    int maxCauses = 10;
    while (e != null && maxCauses-- > 0) {
      sb.append(getStackTrace(e, 3));
      if (e.getCause() == e) {
        break;
      }
      e = e.getCause();
    }
    return sb.toString();
  }

  public static String consolidateExceptionMessages(Throwable e) {
    StringBuilder sb = new StringBuilder();
    while (e != null) {
      if (StringUtils.isNotBlank(e.getMessage())) {
        if (sb.length() > 0) {
          sb.append(". ");
        }
        sb.append(e.getMessage());
      }
      if (e.getCause() == e) {
        break;
      }
      e = e.getCause();
    }
    return sb.toString();
  }

  /// Appends the messages of `cause` and its causes to `message`, each after `" -> "`. Messages already in the text are
  /// skipped, at most `maxCauses` are kept (the outermost and the innermost, never dropping the root cause), and each
  /// is abbreviated in the middle to `maxCauseLength` characters.
  @Nullable
  public static String appendCauses(@Nullable String message, @Nullable Throwable cause, int maxCauses,
      int maxCauseLength) {
    Preconditions.checkArgument(maxCauses >= 1, "maxCauses must be at least 1, got: %s", maxCauses);
    Preconditions.checkArgument(maxCauseLength >= MIN_CAUSE_LENGTH, "maxCauseLength must be at least %s, got: %s",
        MIN_CAUSE_LENGTH, maxCauseLength);
    if (cause == null) {
      return message;
    }
    String base = message != null ? message : "";
    List<String> causeMessages = new ArrayList<>();
    Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
    for (Throwable t = cause; t != null && visited.add(t); t = t.getCause()) {
      String causeMessage = getOwnMessage(t);
      if (causeMessage != null && !containsMessage(base, causeMessage)) {
        causeMessages.add(causeMessage);
      }
    }
    List<String> distinctMessages = new ArrayList<>(causeMessages.size());
    int numCauseMessages = causeMessages.size();
    for (int i = 0; i < numCauseMessages; i++) {
      String causeMessage = causeMessages.get(i);
      if (!distinctMessages.isEmpty()) {
        String previous = distinctMessages.get(distinctMessages.size() - 1);
        if (causeMessage.equals(previous)) {
          continue;
        }
        // Keep the root cause: the previous message may be elided by the bound
        boolean isRootCause = i == numCauseMessages - 1;
        if (!isRootCause && previous.length() <= maxCauseLength && containsMessage(previous, causeMessage)) {
          continue;
        }
      }
      distinctMessages.add(causeMessage);
    }
    int numCauses = distinctMessages.size();
    if (numCauses == 0) {
      return message;
    }
    StringBuilder sb = new StringBuilder(base);
    int numOutermost = maxCauses > 1 ? 1 : 0;
    int firstInnermost = Math.max(numOutermost, numCauses - (maxCauses - numOutermost));
    for (int i = 0; i < numOutermost; i++) {
      appendCause(sb, distinctMessages.get(i), maxCauseLength);
    }
    if (firstInnermost > numOutermost) {
      if (sb.length() > 0) {
        sb.append(CAUSE_SEPARATOR);
      }
      sb.append(ELLIPSIS);
    }
    for (int i = firstInnermost; i < numCauses; i++) {
      appendCause(sb, distinctMessages.get(i), maxCauseLength);
    }
    return sb.toString();
  }

  @Nullable
  private static String getOwnMessage(Throwable t) {
    Throwable cause = t.getCause();
    boolean hasCause = cause != null && cause != t;
    String message = t.getMessage();
    if (StringUtils.isBlank(message)) {
      if (hasCause) {
        return null;
      }
      String simpleName = t.getClass().getSimpleName();
      return simpleName.isEmpty() ? t.getClass().getName() : simpleName;
    }
    return hasCause && message.equals(cause.toString()) ? null : message;
  }

  private static void appendCause(StringBuilder sb, String causeMessage, int maxCauseLength) {
    if (containsMessage(sb, causeMessage)) {
      return;
    }
    if (sb.length() > 0) {
      sb.append(CAUSE_SEPARATOR);
    }
    sb.append(abbreviateMiddle(causeMessage, maxCauseLength));
  }

  /// Whether `text` contains `message` as a whole word, e.g. `"5"` is not contained in `"table_5"`.
  private static boolean containsMessage(CharSequence text, String message) {
    String string = text.toString();
    int messageLength = message.length();
    boolean startsWithWordChar = isWordChar(message.charAt(0));
    boolean endsWithWordChar = isWordChar(message.charAt(messageLength - 1));
    for (int start = string.indexOf(message); start >= 0; start = string.indexOf(message, start + 1)) {
      int end = start + messageLength;
      boolean boundedBefore = !startsWithWordChar || start == 0 || !isWordChar(string.charAt(start - 1));
      boolean boundedAfter = !endsWithWordChar || end == string.length() || !isWordChar(string.charAt(end));
      if (boundedBefore && boundedAfter) {
        return true;
      }
    }
    return false;
  }

  private static boolean isWordChar(char c) {
    return Character.isLetterOrDigit(c) || c == '_';
  }

  private static String abbreviateMiddle(String message, int maxLength) {
    int length = message.length();
    if (length <= maxLength) {
      return message;
    }
    int numCharsToKeep = maxLength - ELLIPSIS.length();
    int headEnd = (numCharsToKeep + 1) / 2;
    int tailStart = length - numCharsToKeep / 2;
    if (Character.isHighSurrogate(message.charAt(headEnd - 1))) {
      headEnd--;
    }
    if (Character.isLowSurrogate(message.charAt(tailStart))) {
      tailStart++;
    }
    return message.substring(0, headEnd) + ELLIPSIS + message.substring(tailStart);
  }

  public static String getStackTrace(Throwable e) {
    return getStackTrace(e, Integer.MAX_VALUE);
  }

  public static String getStackTrace(Throwable e, int maxLines) {
    return getStackTrace(e, maxLines, Integer.MAX_VALUE);
  }

  public static String getStackTrace(Throwable e, int maxLines, int maxLineLength) {
    StringBuilder sb = new StringBuilder();
    while (e != null) {
      sb.append(e.getMessage()).append("\n");
      for (StackTraceElement ste : e.getStackTrace()) {
        sb.append(ste.toString()).append("\n");
        if (maxLines-- <= 0) {
          return sb.toString();
        }
        if (sb.length() > maxLineLength) {
          return sb.toString();
        }
      }
      e = e.getCause();
    }
    return sb.toString();
  }

  /// Suppress the new throwable by adding it as suppressed to the old throwable if the old throwable is not null.
  /// Otherwise, return the new throwable.
  ///
  /// This is usually used in cases we want to call a function on all elements of a collection even if some of the calls
  /// throw exception, but we don't want to lose the exception information from other calls.
  /// For example, when closing multiple resources, we want to close all resources even if some of them throw exception,
  /// and we want to keep the exception information from all resources instead of just the first one.
  ///
  /// Use this function when the new throwable and old throwable are of the same type, so that we can directly add the
  /// new throwable as suppressed to the old throwable without needing to convert the exception type.
  /// Use [suppress(T2, T1, Function, Class)](ExceptionUtils#supress(T2, T1, Function, Class)) when the new throwable
  /// and old throwable are of different types, and we need to convert the exception type before adding as suppressed.
  ///
  /// Normal usage is something like:
  ///
  /// ```java
  /// MyException throwable = null;
  /// for (Resource resource : resources) {
  ///   try {
  ///     resource.close();
  ///   } catch (MyException e) {
  ///     throwable = ExceptionUtils.suppress(e, throwable);
  ///   }
  /// }
  /// if (throwable != null) {
  ///   throw throwable;
  /// }
  /// ```
  public static <T extends Throwable> T suppress(
      T newThrowable,
      @Nullable T oldThrowable
  ) {
    if (oldThrowable != null) {
      if (oldThrowable != newThrowable) {
        // We only add the new throwable as suppressed to the old throwable when they are different.
        // Otherwise, it will cause infinite loop when printing stack trace.
        // This is pretty common when -XX:+OmitStackTraceInFastThrow is enabled, which will reuse the same throwable
        // instance for the same exception type.
        oldThrowable.addSuppressed(newThrowable);
      }
      return oldThrowable;
    }
    return newThrowable;
  }

  /// Suppress the new throwable by adding it as suppressed to the old throwable if the old throwable is not null.
  /// Otherwise, return the new throwable.
  ///
  /// This is usually used in cases we want to call a function on all elements of a collection even if some of the calls
  /// throw exception, but we don't want to lose the exception information from other calls.
  /// For example, when closing multiple resources, we want to close all resources even if some of them throw exception,
  /// and we want to keep the exception information from all resources instead of just the first one.
  ///
  /// Use this function when the new throwable and old throwable are of different types, and we need to convert the
  /// exception type before adding as suppressed. Use [suppress(T, T)](ExceptionUtils#supress(T, T)) when the new
  /// throwable and old throwable are of the same type, so that we can directly add the new throwable as suppressed to
  /// the old throwable.
  ///
  /// Normal usage is something like:
  ///
  /// ```java
  /// RuntimeException throwable = null;
  /// for (Resource resource : resources) {
  ///   try {
  ///     resource.close();
  ///   } catch (Exception e) {
  ///     throwable = ExceptionUtils.supress(e, throwable, RuntimeException::new);
  ///   }
  /// }
  /// if (throwable != null) {
  ///   throw throwable;
  /// }
  /// ```
  ///
  /// @param newThrowable the new throwable to be suppressed
  /// @param oldThrowable the old throwable to be added with the new throwable as suppressed,
  ///                     or null if there is no old throwable
  /// @param exceptionConverter the function to convert the new throwable to the same type as the old throwable,
  ///                     which will be used when the old throwable is not null
  /// @param clazz the class of the old throwable. If old throwable is null and this parameter is provided,
  ///                     we will check if the new throwable is already of the same type as the old throwable,
  ///                     and if so, we can directly return the new throwable without conversion.
  public static <TO extends Throwable, TI extends Throwable> TO suppress(
      TI newThrowable,
      @Nullable TO oldThrowable,
      Function<TI, TO> exceptionConverter,
      @Nullable Class<TO> clazz
  ) {
    if (oldThrowable != null) {
      if (oldThrowable != newThrowable) {
        // We only add the new throwable as suppressed to the old throwable when they are different.
        // Otherwise, it will cause infinite loop when printing stack trace.
        // This is pretty common when -XX:+OmitStackTraceInFastThrow is enabled, which will reuse the same throwable
        // instance for the same exception type.
        oldThrowable.addSuppressed(newThrowable);
      }
      return oldThrowable;
    }
    if (clazz != null && clazz.isAssignableFrom(newThrowable.getClass())) {
      // If the new throwable is already of the same type as the old throwable, we can directly return it without
      // conversion. This is an optimization to avoid unnecessary exception conversion when the new throwable is already
      // of the same type.
      return (TO) newThrowable;
    }
    return exceptionConverter.apply(newThrowable);
  }
}
