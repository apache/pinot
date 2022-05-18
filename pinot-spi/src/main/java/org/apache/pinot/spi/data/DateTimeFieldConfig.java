package org.apache.pinot.spi.data;

import org.apache.pinot.spi.config.BaseJsonConfig;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DateTimeFieldConfig extends BaseJsonConfig {
    private String granularity;
    private String granularityTimeUnit;
    private String inputTimeFormat;
    private String pattern;
    private String timeZone;

    public DateTimeFieldConfig(String granularity, String granularityTimeUnit, String inputTimeFormat, String pattern, String timeZone) {
        this.granularity = granularity;
        this.granularityTimeUnit = granularityTimeUnit;
        this.inputTimeFormat = inputTimeFormat;
        this.pattern = pattern;
        this.timeZone = timeZone;
    }

    public String getGranularity() {
        return granularity;
    }

    public void setGranularity(String granularity) {
        this.granularity = granularity;
    }

    public String getGranularityTimeUnit() {
        return granularityTimeUnit;
    }

    public void setGranularityTimeUnit(String granularityTimeUnit) {
        this.granularityTimeUnit = granularityTimeUnit;
    }

    public String getInputTimeFormat() {
        return inputTimeFormat;
    }

    public void setInputTimeFormat(String inputTimeFormat) {
        this.inputTimeFormat = inputTimeFormat;
    }

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public String getFormattedValue() {
        return String.join(":", granularity, granularityTimeUnit, inputTimeFormat, pattern + " " + timeZone);
    }

    public String getGranularityConfig() {
        return String.join(":", granularity, granularityTimeUnit);
    }
}
