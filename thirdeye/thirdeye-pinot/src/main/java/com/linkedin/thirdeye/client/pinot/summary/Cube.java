package com.linkedin.thirdeye.client.pinot.summary;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.google.gson.Gson;


// TODO: The cube should be queried from the backend database during runtime.
public class Cube { // the cube (Ca|Cb)
  private double ga; // sum of all ta
  private double gb; // sum of all tb
  private double rg; // global ratio, i.e., gb / ga
  private List<Record> records = new ArrayList<>();

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

  public void setRg(double rg) {
    this.rg = rg;
  }

  public double getGa() {
    return ga;
  }

  public void setGa(double ga) {
    this.ga = ga;
  }

  public double getGb() {
    return gb;
  }

  public void setGb(double gb) {
    this.gb = gb;
  }

  public void toJson(String fileName) throws IOException {
    FileWriter fw = new FileWriter(fileName);
    Gson gson = new Gson();
    fw.write(gson.toJson(this));
    fw.flush();
  }

  public static Cube fromJson(String fileName) throws IOException {
    BufferedReader br = new BufferedReader(new FileReader(fileName));
    String stringGa = br.readLine();
    Gson gson = new Gson();
    return gson.fromJson(stringGa, Cube.class);
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
