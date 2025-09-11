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
  @ScalarFunction(names = {"uniqueNgrams", "generateUniqueNgrams"})
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
  @ScalarFunction(names = {"uniqueNgrams", "generateUniqueNgrams"})
  public String[] uniqueNgrams(String input, int minGram, int maxGram) {
    _ngramSet.clear();
    for (int n = minGram; n <= maxGram && n <= input.length(); n++) {
      if (n == 0) {
        continue;
      }
      for (int i = 0; i <= input.length() - n; i++) {
        _ngramSet.add(input.substring(i, i + n));
      }
    }
    return _ngramSet.toArray(new String[0]);
  }

  /**
   * Generate unique ngrams (exact length) across all input strings in a multi-value (MV) column.
   *
   * @param inputs array of input strings
   * @param length exact ngram length to generate
   * @return unique ngrams of the specified length across all inputs
   */
  @ScalarFunction(names = {"uniqueNgramsMV", "generateUniqueNgramsMV"})
  public String[] generateUniqueNgramsMV(String[] inputs, int length) {
    if (length <= 0 || inputs == null || inputs.length == 0) {
      return new String[0];
    }

    _ngramSet.clear();
    for (String input : inputs) {
      if (input == null || length > input.length()) {
        continue;
      }
      for (int i = 0; i <= input.length() - length; i++) {
        _ngramSet.add(input.substring(i, i + length));
      }
    }
    return _ngramSet.toArray(new String[0]);
  }

  /**
   * Generate unique ngrams (within [minGram, maxGram]) across all input strings in a MV column.
   *
   * @param inputs array of input strings
   * @param minGram minimum ngram length
   * @param maxGram maximum ngram length
   * @return unique ngrams whose lengths are within the given range across all inputs
   */
  @ScalarFunction(names = {"uniqueNgramsMV", "generateUniqueNgramsMV"})
  public String[] generateUniqueNgramsMV(String[] inputs, int minGram, int maxGram) {
    if (inputs == null || inputs.length == 0) {
      return new String[0];
    }

    _ngramSet.clear();
    for (String input : inputs) {
      if (input == null || minGram > input.length()) {
        continue;
      }
      int inputLength = input.length();
      for (int n = minGram; n <= maxGram && n <= inputLength; n++) {
        if (n == 0) {
          continue;
        }
        for (int i = 0; i <= inputLength - n; i++) {
          _ngramSet.add(input.substring(i, i + n));
        }
      }
    }
    return _ngramSet.toArray(new String[0]);
  }
}
