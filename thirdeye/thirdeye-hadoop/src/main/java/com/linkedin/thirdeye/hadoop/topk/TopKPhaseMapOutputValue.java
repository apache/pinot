package com.linkedin.thirdeye.hadoop.topk;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import com.linkedin.thirdeye.api.MetricType;

public class TopKPhaseMapOutputValue {

  //String dimensionName;
  //String dimensionValue;
  Number[] metricValues;
  List<MetricType> metricTypes;

  public TopKPhaseMapOutputValue(//String dimensionName, String dimensionValue,
      Number[] metricValues, List<MetricType> metricTypes) {
    //this.dimensionName = dimensionName;
    //this.dimensionValue = dimensionValue;
    this.metricValues = metricValues;
    this.metricTypes = metricTypes;
  }

  public Number[] getMetricValues() {
    return metricValues;
  }

  /*public String getDimensionName() {
    return dimensionName;
  }

  public String getDimensionValue() {
    return dimensionValue;
  }*/

  public byte[] toBytes() throws IOException {

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    byte[] bytes;

    /*// dimension name
    bytes = dimensionName.getBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);

    // dimension value
    bytes = dimensionValue.getBytes();
    dos.writeInt(bytes.length);
    dos.write(bytes);*/

    // metric values
    dos.writeInt(metricValues.length);
    for (int i = 0; i < metricValues.length; i++) {
      Number number = metricValues[i];
      MetricType metricType = metricTypes.get(i);
      switch (metricType) {
        case SHORT:
          dos.writeShort(number.intValue());
          break;
        case LONG:
          dos.writeLong(number.longValue());
          break;
        case INT:
          dos.writeInt(number.intValue());
          break;
        case FLOAT:
          dos.writeFloat(number.floatValue());
          break;
        case DOUBLE:
          dos.writeDouble(number.doubleValue());
          break;
      }
    }

    baos.close();
    dos.close();
    return baos.toByteArray();
  }

  public static TopKPhaseMapOutputValue fromBytes(byte[] buffer, List<MetricType> metricTypes) throws IOException {
    DataInputStream dis = new DataInputStream(new ByteArrayInputStream(buffer));
    int length;

    /*byte[] bytes;
    // dimension name
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    String dimensionName = new String(bytes);

    // dimension value
    length = dis.readInt();
    bytes = new byte[length];
    dis.read(bytes);
    String dimensionValue = new String(bytes);*/

    // metric values
    length = dis.readInt();
    Number[] metricValues = new Number[length];

    for (int i = 0 ; i < length; i++) {
      MetricType metricType = metricTypes.get(i);
      switch (metricType) {
        case SHORT:
          metricValues[i] = dis.readShort();
          break;
        case LONG:
          metricValues[i] = dis.readLong();
          break;
        case INT:
          metricValues[i] = dis.readInt();
          break;
        case FLOAT:
          metricValues[i] = dis.readFloat();
          break;
        case DOUBLE:
          metricValues[i] = dis.readDouble();
          break;
      }
    }

    TopKPhaseMapOutputValue wrapper;
    wrapper = new TopKPhaseMapOutputValue(
        //dimensionName, dimensionValue,
        metricValues, metricTypes);
    return wrapper;
  }

  public static void main(String[] args) throws IOException {
    Number[] metricValues = new Number[4];
    List<MetricType> metricTypes = new ArrayList<>();
    int a = 10;
    double b = 4.5;
    long c = 6721234567543L;
    float d = 22.7F;
    metricValues[0] = a; metricTypes.add(MetricType.INT);
    metricValues[1] = b; metricTypes.add(MetricType.DOUBLE);
    metricValues[2] = c; metricTypes.add(MetricType.LONG);
    metricValues[3] = d; metricTypes.add(MetricType.FLOAT);

    TopKPhaseMapOutputValue valueWrapper = new TopKPhaseMapOutputValue
        (//"d1", "v1",
            metricValues, metricTypes);
    byte[] bytes = valueWrapper.toBytes();
    TopKPhaseMapOutputValue output = TopKPhaseMapOutputValue.fromBytes(bytes, metricTypes);
    for (Number n : metricValues) {
      System.out.println(n);
    }
  }

}
