package com.linkedin.pinot.core.operator.transform;

/**
 * Convertor to convert and bucket a datetime value form an sdf format to an epoch format
 */
public class SDFToEpochConvertor extends DateTimeConvertor {

  public SDFToEpochConvertor(String inputFormat, String outputFormat, String outputGranularity) {
    super(inputFormat, outputFormat, outputGranularity);
  }

  @Override
  public Long convert(Object dateTimeValue) {
    Long dateTimeColumnValueMS = convertSDFToMillis(dateTimeValue);
    Long bucketedDateTimevalueMS = bucketDateTimeValueMS(dateTimeColumnValueMS);
    Long dateTimeValueConverted = convertMillisToEpoch(bucketedDateTimevalueMS);
    return dateTimeValueConverted;
  }

}
