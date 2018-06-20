package com.linkedin.pinot.core.startreeV2;

import java.util.List;
import java.util.ArrayList;


public class experiments {

  public List<Integer>order = new ArrayList<>();
  public List<List<Object>> data = new ArrayList<List<Object>>();

  public List<Object> d1 = new ArrayList<>();
  public List<Object> d2 = new ArrayList<>();
  public List<Object> d3 = new ArrayList<>();

  public void createData() {
    d1.add(4);
    d1.add(5);
    d1.add(1);
    d1.add(1);
    d1.add(1);
    d1.add(4);

    data.add(d1);

    d2.add(1);
    d2.add(1);
    d2.add(2);
    d2.add(3);
    d2.add(1);
    d2.add(4);

    data.add(d2);

    d3.add(0);
    d3.add(1);
    d3.add(1);
    d3.add(1);
    d3.add(2);
    d3.add(3);

    data.add(d3);

    order.add(1);
    order.add(2);
    order.add(3);
  }

  public void sort() {

  }
}
