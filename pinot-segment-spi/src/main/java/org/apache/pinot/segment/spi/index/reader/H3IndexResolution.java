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
package org.apache.pinot.segment.spi.index.reader;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.type.CollectionLikeType;
import com.fasterxml.jackson.databind.util.StdConverter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;


/**
 * Stores the resolutions for an index. There are in total of H3 resolutions
 * https://h3geo.org/#/documentation/core-library/resolution-table
 * To efficiently serialize the resolutions, we use two bytes for encoding th enabled resolutions. The resolution level
 * maps to the corresponding bit.
 */
@JsonSerialize(converter = H3IndexResolution.ToIntListConverted.class)
@JsonDeserialize(using = H3IndexResolution.Deserializator.class)
public class H3IndexResolution {
  private short _resolutions;

  public H3IndexResolution(List<Integer> resolutions) {
    for (int resolution : resolutions) {
      _resolutions |= 1 << resolution;
    }
  }

  /**
   * Creates the resolutions with the serialized short value
   * @param resolutions
   */
  public H3IndexResolution(short resolutions) {
    _resolutions = resolutions;
  }

  /**
   * @return the encoding of the resolutions into a short value (two bytes)
   */
  public short serialize() {
    return _resolutions;
  }

  public int size() {
    return Integer.bitCount(_resolutions);
  }

  public List<Integer> getResolutions() {
    List<Integer> res = new ArrayList<>();
    for (int i = 0; i < 15; i++) {
      if ((_resolutions & (1 << i)) != 0) {
        res.add(i);
      }
    }
    return res;
  }

  @JsonIgnore
  public int getLowestResolution() {
    return Integer.numberOfTrailingZeros(_resolutions);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    H3IndexResolution that = (H3IndexResolution) o;
    return _resolutions == that._resolutions;
  }

  @Override
  public int hashCode() {
    return Objects.hash(_resolutions);
  }

  public static class ToIntListConverted extends StdConverter<H3IndexResolution, List<Integer>> {
    @Override
    public List<Integer> convert(H3IndexResolution value) {
      return value.getResolutions();
    }
  }

  public static class Deserializator extends StdDeserializer<H3IndexResolution> {
    public Deserializator() {
      super(H3IndexResolution.class);
    }

    @Override
    public H3IndexResolution deserialize(JsonParser p, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      switch (p.currentToken()) {
        case VALUE_NUMBER_INT: {
          long longValue = p.getLongValue();
          if (longValue > Short.MAX_VALUE || longValue < Short.MIN_VALUE) {
            throw new JsonParseException(p, "Resolution value is outside the short int range");
          }
          return new H3IndexResolution((short) longValue);
        }
        case START_ARRAY: {
          CollectionLikeType arrayType = ctxt.getTypeFactory().constructCollectionLikeType(List.class, Integer.class);
          List<Integer> resolutions = ctxt.readValue(p, arrayType);
          return new H3IndexResolution(resolutions);
        }
        default: {
          throw new JsonParseException(p, "Expecting number or array, but found " + p.currentToken());
        }
      }
    }
  }
}
