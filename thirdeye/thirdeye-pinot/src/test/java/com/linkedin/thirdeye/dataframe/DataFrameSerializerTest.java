package com.linkedin.thirdeye.dataframe;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.linkedin.thirdeye.dataframe.util.DataFrameSerializer;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class DataFrameSerializerTest {
  private ObjectMapper mapper;

  @BeforeMethod
  public void beforeMethod() {
    this.mapper = new ObjectMapper();

    SimpleModule module = new SimpleModule();
    module.addSerializer(DataFrame.class, new DataFrameSerializer());

    this.mapper.registerModule(module);
  }

  @Test
  public void testSerializeBooleans() throws Exception {
    DataFrame df = new DataFrame().addSeries("booleans", (byte) 1, (byte) 0, (byte) 1, BooleanSeries.NULL, (byte) 1);
    String serialized = this.mapper.writeValueAsString(df);
    Assert.assertEquals(serialized, "{\"booleans\":[1,0,1,null,1]}");
  }

  @Test
  public void testSerializeDoubles() throws Exception {
    DataFrame df = new DataFrame().addSeries("doubles", 1.1, 2.0, 3.3, DoubleSeries.NULL, 5.5);
    String serialized = this.mapper.writeValueAsString(df);
    Assert.assertEquals(serialized, "{\"doubles\":[1.1,2.0,3.3,null,5.5]}");
  }

  @Test
  public void testSerializeLongs() throws Exception {
    DataFrame df = new DataFrame().addSeries("longs", 1, 2, 3, LongSeries.NULL, 5);
    String serialized = this.mapper.writeValueAsString(df);
    Assert.assertEquals(serialized, "{\"longs\":[1,2,3,null,5]}");
  }

  @Test
  public void testSerializeStrings() throws Exception {
    DataFrame df = new DataFrame().addSeries("strings", "1", "hello", "world", StringSeries.NULL, "!");
    String serialized = this.mapper.writeValueAsString(df);
    Assert.assertEquals(serialized, "{\"strings\":[\"1\",\"hello\",\"world\",null,\"!\"]}");
  }

  @Test
  public void testSerializeObjects() throws Exception {
    DataFrame df = new DataFrame().addSeriesObjects("objects", 1L, "Hi", 0.12, ObjectSeries.NULL, true);
    String serialized = this.mapper.writeValueAsString(df);
    Assert.assertEquals(serialized, "{\"objects\":[\"1\",\"Hi\",\"0.12\",null,\"true\"]}");
  }

  @Test
  public void testSerializeMultiple() throws Exception {
    DataFrame df = new DataFrame()
        .addSeries("booleans", (byte) 1, (byte) 0, (byte) 1, BooleanSeries.NULL, (byte) 1)
        .addSeries("doubles", 1.1, 2.0, 3.3, DoubleSeries.NULL, 5.5)
        .addSeries("strings", "1", "hello", "world", StringSeries.NULL, "!");
    String serialized = this.mapper.writeValueAsString(df);
    Assert.assertEquals(serialized, "{\"booleans\":[1,0,1,null,1],\"doubles\":[1.1,2.0,3.3,null,5.5],\"strings\":[\"1\",\"hello\",\"world\",null,\"!\"]}");
  }

  @Test
  public void testSerializeNested() throws Exception {
    DataFrame df = new DataFrame().addSeries("doubles", 1.1, 2.0, 3.3, DoubleSeries.NULL, 5.5);

    Map<String, Object> map = new LinkedHashMap<>();
    map.put("first", 1L);
    map.put("second", "Hi");
    map.put("third", df);

    String serialized = this.mapper.writeValueAsString(map);
    Assert.assertEquals(serialized, "{\"first\":1,\"second\":\"Hi\",\"third\":{\"doubles\":[1.1,2.0,3.3,null,5.5]}}");
  }

  @Test
  public void testSerializeNestedDeep() throws Exception {
    MyPojo pojo = new MyPojo(Collections.singletonMap("one",
        (Object) Collections.singletonMap("two",
            Collections.singletonList(
                new DataFrame().addSeries("doubles", 1.0, 2.2, DoubleSeries.NULL)
            ))),
        "test");

    String serialized = this.mapper.writeValueAsString(pojo);
    Assert.assertEquals(serialized, "{\"map\":{\"one\":{\"two\":[{\"doubles\":[1.0,2.2,null]}]}},\"value\":\"test\"}");
  }

  private static class MyPojo {
    Map<String, Object> map;
    String value;

    public MyPojo(Map<String, Object> map, String value) {
      this.map = map;
      this.value = value;
    }

    public Map<String, Object> getMap() {
      return map;
    }

    public String getValue() {
      return value;
    }
  }
}
