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

import com.google.common.base.Preconditions;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Segment name generator that supports defining the segment name based on the input file name and path, via a pattern
 * (matched against the input file URI) and a template (currently only supports ${filePathPattern:\N}, where N is the
 * group match number from the regex).
 *
 */
@SuppressWarnings("serial")
public class InputFileSegmentNameGenerator implements SegmentNameGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(InputFileSegmentNameGenerator.class);

  private static final String PARAMETER_TEMPLATE = "${filePathPattern:\\%d}";

  private final Pattern _filePathPattern;
  private final String _segmentNameTemplate;
  private final URI _inputFileUri;
  private final String _segmentName;
  private final boolean _appendUUIDToSegmentName;

  public InputFileSegmentNameGenerator(String filePathPattern, String segmentNameTemplate, String inputFileUri) {
    this(filePathPattern, segmentNameTemplate, inputFileUri, false);
  }

  public InputFileSegmentNameGenerator(String filePathPattern, String segmentNameTemplate, String inputFileUri,
      boolean appendUUIDToSegmentName) {
    Preconditions.checkArgument(filePathPattern != null, "Missing filePathPattern for InputFileSegmentNameGenerator");
    try {
      _filePathPattern = Pattern.compile(filePathPattern);
    } catch (PatternSyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid filePathPattern: %s for InputFileSegmentNameGenerator", filePathPattern), e);
    }
    Preconditions
        .checkArgument(segmentNameTemplate != null, "Missing segmentNameTemplate for InputFileSegmentNameGenerator");
    _segmentNameTemplate = segmentNameTemplate;
    try {
      _inputFileUri = new URI(inputFileUri);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(
          String.format("Invalid inputFileUri: %s for InputFileSegmentNameGenerator", inputFileUri), e);
    }
    _appendUUIDToSegmentName = appendUUIDToSegmentName;
    _segmentName = makeSegmentName();
  }

  private String makeSegmentName() {
    String inputFilePath = _inputFileUri.getPath();
    Matcher m = _filePathPattern.matcher(inputFilePath);
    String segmentName;

    if (!m.matches()) {
      LOGGER.warn(String.format("No match for pattern '%s' in '%s'", _filePathPattern, inputFilePath));
      segmentName = safeConvertPathToFilename(inputFilePath);
    } else {
      segmentName = _segmentNameTemplate;
      for (int i = 1; i <= m.groupCount(); i++) {
        String value = m.group(i);
        if (value == null) {
          value = "";
        }
        segmentName = segmentName.replace(String.format(PARAMETER_TEMPLATE, i), value);
      }
      segmentName = segmentName.replaceAll("\\s+", "_");
    }
    return _appendUUIDToSegmentName ? JOINER.join(segmentName, UUID.randomUUID()) : segmentName;
  }

  @Override
  public String generateSegmentName(int sequenceId, @Nullable Object minTimeValue, @Nullable Object maxTimeValue) {
    return _segmentName;
  }

  private String safeConvertPathToFilename(String inputFilePath) {
    // Strip of leading '/' characters
    inputFilePath = inputFilePath.replaceFirst("^[/]+", "");
    // Try to create a valid filename by removing any '/' or '.' chars with '_'
    return inputFilePath.replaceAll("[/\\.\\s+]", "_");
  }

  @Override
  public String toString() {
    return String.format(
        "InputFileSegmentNameGenerator: filePathPattern=%s, segmentNameTemplate=%s, inputFileUri=%s, segmentName=%s",
        _filePathPattern, _segmentNameTemplate, _inputFileUri, _segmentName);
  }
}
