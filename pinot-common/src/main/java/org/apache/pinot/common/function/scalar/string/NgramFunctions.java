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
package org.apache.pinot.common.function.scalar.string;

import it.unimi.dsi.fastutil.objects.ObjectLinkedOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectSet;
import org.apache.pinot.spi.annotations.ScalarFunction;


public class NgramFunctions {

  private final ObjectSet<String> _ngramSet = new ObjectLinkedOpenHashSet<>();

  /**
   * @param input  an input string for ngram generations.
   * @param length the max length of the ngram for the string.
   * @return generate an array of unique ngram of the string that length are exactly matching the specified length.
   */
  @ScalarFunction
  public String[] uniqueNgrams(String input, int length) {
    if (length == 0 || length > input.length()) {
      return new String[0];
    }

    _ngramSet.clear();
    for (int i = 0; i < input.length() - length + 1; i++) {
      _ngramSet.add(input.substring(i, i + length));
    }
    return _ngramSet.toArray(new String[0]);
  }

  /**
   * @param input   an input string for ngram generations.
   * @param minGram the min length of the ngram for the string.
   * @param maxGram the max length of the ngram for the string.
   * @return generate an array of ngram of the string that length are within the specified range [minGram, maxGram].
   */
  @ScalarFunction
  public String[] uniqueNgrams(String input, int minGram, int maxGram) {
    _ngramSet.clear();
    ObjectSet<String> ngramSet = new ObjectLinkedOpenHashSet<>();
    for (int n = minGram; n <= maxGram && n <= input.length(); n++) {
      if (n == 0) {
        continue;
      }
      for (int i = 0; i < input.length() - n + 1; i++) {
        ngramSet.add(input.substring(i, i + n));
      }
    }
    return ngramSet.toArray(new String[0]);
  }
}
