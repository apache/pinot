package com.linkedin.thirdeye.client.pinot.slsummary;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.thirdeye.client.ThirdEyeResponse;


// TODO: The cube should be queried from the backend database during runtime.
public class Cube { // the cube (Ca|Cb)
  private double ga; // sum of all ta
  private double gb; // sum of all tb
  private double rg; // global ratio, i.e., gb / ga

  @JsonProperty("records")
  private List<Record> records = new ArrayList<Record>();

  public void setGlobalInfo(ThirdEyeResponse responseGa, ThirdEyeResponse responseGb, int multiplier) {
    this.ga = responseGa.getRow(0).getMetrics().get(0) * multiplier;
    this.gb = responseGb.getRow(0).getMetrics().get(0) * multiplier;
    this.rg = this.gb / this.ga;
  }

  public void addRecordForOneDimension(String dimensionName, ThirdEyeResponse responseTa,
      ThirdEyeResponse responseTb) {
    Map<String, Integer> recordTable = new HashMap<>();

    for (int i = 0; i < responseTa.getNumRows(); ++i) {
      Record record = new Record();
      record.dimensionName = dimensionName;
      record.dimensionValue = responseTa.getRow(i).getDimensions().get(0);
      record.metricA = responseTa.getRow(i).getMetrics().get(0);
      recordTable.put(record.dimensionValue, records.size());
      records.add(record);
    }

    for (int i = 0; i < responseTb.getNumRows(); ++i) {
      String dimensionValue = responseTb.getRow(i).getDimensions().get(0);
      if (recordTable.containsKey(dimensionValue)) {
        int idx = recordTable.get(dimensionValue);
        records.get(idx).metricB = responseTb.getRow(i).getMetrics().get(0);
      } else {
        Record record = new Record();
        record.dimensionName = dimensionName;
        record.dimensionValue = dimensionValue;
        record.metricB = responseTb.getRow(i).getMetrics().get(0);
        records.add(record);
      }
    }
  }

  public int size() {
    return records.size();
  }

  public Record get(int idx) {
    return records.get(idx);
  }

  public void addRecord(Record record) {
    this.records.add(record);
  }

  public void sort(Comparator<Record> comparator) {
    Collections.sort(records, comparator);
  }

  public double getRg() {
    return rg;
  }

  public double getGa() {
    return ga;
  }

  public double getGb() {
    return gb;
  }

  public void toJson(String fileName) throws IOException {
    new ObjectMapper().writeValue(new File(fileName), this);
  }

  public static Cube fromJson(String fileName) throws IOException {
    return new ObjectMapper().readValue(new File(fileName), Cube.class);
  }

  public void removeEmptyRecords() {
    for (int i = records.size() - 1; i >= 0; --i) {
      if (Double.compare(records.get(i).metricA, .0) == 0 || Double.compare(records.get(i).metricB, .0) == 0) {
        records.remove(i);
      }
    }
  }

  @Override
  public String toString() {
    return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
  }
}
