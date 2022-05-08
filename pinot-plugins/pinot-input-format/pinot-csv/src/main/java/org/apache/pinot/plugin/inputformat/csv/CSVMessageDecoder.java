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

package org.apache.pinot.plugin.inputformat.csv;

import com.google.common.collect.ImmutableSet;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.pinot.spi.data.readers.GenericRow;
import org.apache.pinot.spi.stream.StreamMessageDecoder;


public class CSVMessageDecoder implements StreamMessageDecoder<byte[]> {

  private static final String CONFIG_FILE_FORMAT = "csv.fileFmt";
  private static final String CONFIG_HEADER = "csv.hdr";
  private static final String CONFIG_DELIMITER = "csv.delim";
  private static final String CONFIG_COMMENT_MARKER = "csv.commentMarker";
  private static final String CONFIG_CSV_ESCAPE_CHARACTER = "csv.EscChar";
  private static final String CONFIG_CSV_MULTI_VALUE_DELIMITER = "csv.multiValDelim";

  private CSVFormat _format;
  private CSVRecordExtractor _recordExtractor;

  @Override
  public void init(Map<String, String> props, Set<String> fieldsToRead, String topicName)
      throws Exception {
    String csvFormat = props.get(CONFIG_FILE_FORMAT);
    CSVFormat format;
    if (csvFormat == null) {
      format = CSVFormat.DEFAULT;
    } else {
      switch (csvFormat.toUpperCase()) {
        case "EXCEL":
          format = CSVFormat.EXCEL;
          break;
        case "MYSQL":
          format = CSVFormat.MYSQL;
          break;
        case "RFC4180":
          format = CSVFormat.RFC4180;
          break;
        case "TDF":
          format = CSVFormat.TDF;
          break;
        default:
          format = CSVFormat.DEFAULT;
          break;
      }
    }

    String csvDelimiter = props.get(CONFIG_DELIMITER);
    if (csvDelimiter != null) {
      format = format.withDelimiter(csvDelimiter.charAt(0));
    }
    String csvHeader = props.get(CONFIG_HEADER);
    if (csvHeader == null) {
      //parse the header automatically from the input
      format = format.withHeader();
    } else {
      format = format.withHeader(StringUtils.split(csvHeader, csvDelimiter));
    }
    if (props.containsKey(CONFIG_COMMENT_MARKER)) {
      Character commentMarker = props.get(CONFIG_COMMENT_MARKER).charAt(0);
      format = format.withCommentMarker(commentMarker);
    }

    if (props.containsKey(CONFIG_CSV_ESCAPE_CHARACTER)) {
      format = format.withEscape(props.get(CONFIG_CSV_ESCAPE_CHARACTER).charAt(0));
    }
    _format = format;
    Character multiValueDelimiter = null;
    if (props.containsKey(CONFIG_CSV_MULTI_VALUE_DELIMITER)) {
      multiValueDelimiter = props.get(CONFIG_CSV_MULTI_VALUE_DELIMITER).charAt(0);
    }

    _recordExtractor = new CSVRecordExtractor();

    CSVRecordExtractorConfig recordExtractorConfig = new CSVRecordExtractorConfig();
    recordExtractorConfig.setMultiValueDelimiter(multiValueDelimiter);
    recordExtractorConfig.setColumnNames(ImmutableSet.copyOf(
        Objects.requireNonNull(_format.getHeader())));
    _recordExtractor.init(fieldsToRead, recordExtractorConfig);
  }

  @Override
  public GenericRow decode(byte[] payload, GenericRow destination) {
    try {
      Iterator<CSVRecord> iterator =
          _format.parse(new InputStreamReader(new ByteArrayInputStream(payload), StandardCharsets.UTF_8)).iterator();
      return _recordExtractor.extract(iterator.next(), destination);
    } catch (IOException e) {
      throw new RuntimeException("Error decoding CSV record from payload", e);
    }
  }

  @Override
  public GenericRow decode(byte[] payload, int offset, int length, GenericRow destination) {
    return decode(Arrays.copyOfRange(payload, offset, offset + length), destination);
  }
}
