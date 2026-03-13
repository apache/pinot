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
package org.apache.pinot.spi.data;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import javax.annotation.Nullable;

@JsonIgnoreProperties(ignoreUnknown = true)
public final class DateTimeWithTimeZoneFieldSpec extends FieldSpec {

    private DateTimeFieldSpec _fieldSpec;

    private String _timeZoneId;


    public DateTimeWithTimeZoneFieldSpec() {
        super();
    }

    public DateTimeWithTimeZoneFieldSpec(String name, DataType dataType, String format, String granularity,
                             @Nullable Object sampleValue) {
        _fieldSpec = new DateTimeFieldSpec(name, dataType, format, granularity, sampleValue);
    }

    public DateTimeWithTimeZoneFieldSpec(String name, DataType dataType, String format, String granularity) {
        this(name, dataType, format, granularity, null);
    }

    public DateTimeWithTimeZoneFieldSpec(String name, DataType dataType, String format, String granularity,
                             @Nullable Object defaultNullValue, @Nullable String transformFunction) {
        this(name, dataType, format, granularity, defaultNullValue, transformFunction, null);
    }

    public DateTimeWithTimeZoneFieldSpec(String name, DataType dataType, String format, String granularity,
                             @Nullable Object defaultNullValue, @Nullable String transformFunction,
                             @Nullable String sampleValue) {
        this(name, dataType, format, granularity, sampleValue);
        setDefaultNullValue(defaultNullValue);
        setTransformFunction(transformFunction);
    }

    @Override
    public FieldType getFieldType() {
        return FieldType.DATE_TIME;
    }

    @Override
    public void setSingleValueField(boolean isSingleValueField) {
        Preconditions.checkArgument(isSingleValueField, "Unsupported multi-value for date time field.");
    }

    @Override
    public void setDataType(DataType dataType) {
        _fieldSpec.setDataType(dataType);
    }

    public String getFormat() {
        return _fieldSpec.getFormat();
    }

    public void setFormat(String format) {
        _fieldSpec.setFormat(format);
    }

    @JsonIgnore
    public DateTimeFormatSpec getFormatSpec() {
        return _fieldSpec.getFormatSpec();
    }

    public String getGranularity() {
        return _fieldSpec.getGranularity();
    }

    public void setGranularity(String granularity) {
        _fieldSpec.setGranularity(granularity);
    }

    public Object getSampleValue() {
        return _fieldSpec.getSampleValue();
    }

    public void setSampleValue(String sampleValue) {
        _fieldSpec.setSampleValue(sampleValue);
    }

    @JsonIgnore
    public DateTimeGranularitySpec getGranularitySpec() {
        return _fieldSpec.getGranularitySpec();
    }

    @Override
    public ObjectNode toJsonObject() {
        return _fieldSpec.toJsonObject();
    }

    @Override
    public String toString() {
        return _fieldSpec.toString(); // TODO: adjust for time zone
    }
}
