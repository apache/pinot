package com.linkedin.thirdeye.bootstrap.util;


import com.linkedin.thirdeye.api.StarTreeConfig;
import com.linkedin.thirdeye.api.StarTreeRecord;

/**
 * Interface for record converters
 * @param <T> Uses Avro if default, else text
 */
public interface ThirdeyeConverter<T> {

  StarTreeRecord convert(StarTreeConfig config, T record);

}
