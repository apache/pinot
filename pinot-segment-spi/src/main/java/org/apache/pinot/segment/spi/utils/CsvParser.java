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
package org.apache.pinot.segment.spi.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class CsvParser {
    private CsvParser() {
        // Hide utility class default constructor
    }

    /**
     * Parse the input csv string with customizable parsing behavior. Sometimes the individual values may contain comma
     * and other white space characters. These characters are sometimes expected to be part of the actual argument.
     *
     * @param input  string to split on comma
     * @param escapeComma whether we should ignore "\," during splitting, replace it with "," after split
     * @param trim   whether we should trim each tokenized terms
     * @return a list of values, empty list if input is empty or null
     */
    public static List<String> parse(@Nullable String input, boolean escapeComma, boolean trim) {
        if (null == input || input.isEmpty()) {
            return Collections.emptyList();
        }

        Stream<String> tokenStream;
        if (escapeComma) {
            // Use regular expression to split on "," unless it is "\,"
            tokenStream = Arrays.stream(input.split("(?<!\\\\),")).map(s -> s.replace("\\,", ","));
            tokenStream = tokenStream.map(s -> s.replace("\\,", ","));
        } else {
            tokenStream = Arrays.stream(input.split(","));
        }

        if (trim) {
            tokenStream = tokenStream.map(String::trim);
        }

        return tokenStream.collect(Collectors.toList());
    }
}
