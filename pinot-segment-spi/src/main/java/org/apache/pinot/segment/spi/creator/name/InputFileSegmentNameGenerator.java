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
package org.apache.pinot.segment.spi.creator.name;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Segment name generator that supports defining the segment name based on the input file name
 * and path, via a pattern (matched against the input file URI) and a template (currently only
 * supports ${filePathPattern:\N}, where N is the group match number from the regex).
 *
 */
@SuppressWarnings("serial")
public class InputFileSegmentNameGenerator implements SegmentNameGenerator {

    private static final Logger LOGGER = LoggerFactory
            .getLogger(InputFileSegmentNameGenerator.class);

    private static final String PARAMETER_TEMPLATE = "${filePathPattern:\\%d}";

    private Pattern filePathPattern;
    private String segmentNameTemplate;

    public InputFileSegmentNameGenerator(String filePathPattern, String segmentNameTemplate) {
        this.filePathPattern = Pattern.compile(filePathPattern);
        this.segmentNameTemplate = segmentNameTemplate;
    }

    @Override
    public String generateSegmentName(int sequenceId, @Nullable Object minTimeValue, @Nullable Object maxTimeValue,
            @Nullable String inputFilePath) {

        if (inputFilePath == null) {
            if (sequenceId >= 0) {
                return String.format("InvalidInputFilePath_%d", sequenceId);
            } else {
                return "InvalidInputFilePath";
            }
        }

        try {
            URI inputFilePathUri = new URI(inputFilePath);
            inputFilePath = inputFilePathUri.getPath();
        } catch (URISyntaxException e) {
            LOGGER.warn(String.format("Can't convert '%s' to URI", inputFilePath), e);
            return safeConvertPathToFilename(inputFilePath);
        }

        Matcher m = filePathPattern.matcher(inputFilePath);
        if (!m.matches()) {
            LOGGER.warn(String.format("No match for pattern '%s' in '%s'", filePathPattern,
                    inputFilePath));
            return safeConvertPathToFilename(inputFilePath);
        }

        String segmentName = segmentNameTemplate;
        for (int i = 1; i <= m.groupCount(); i++) {
            segmentName = segmentName.replace(String.format(PARAMETER_TEMPLATE, i), m.group(i));
        }

        return segmentName;
    }

    private String safeConvertPathToFilename(String inputFilePath) {
        // Strip of leading '/' characters
        inputFilePath = inputFilePath.replaceFirst("^[/]+", "");
        // Try to create a valid filename by removing any '/' or '.' chars with '_'
        return inputFilePath.replaceAll("[/\\.]", "_");
    }

    @Override
    public String toString() {
        return String.format(
                "InputFileSegmentNameGenerator: filePathPattern=%s, segmentNameTemplate=%s",
                filePathPattern, segmentNameTemplate);
    }
}
