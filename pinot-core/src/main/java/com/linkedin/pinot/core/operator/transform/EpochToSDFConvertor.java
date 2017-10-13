package com.linkedin.pinot.core.operator.transform;

/**
 * Convertor to convert and bucket a datetime value form an epoch format to an sdf format
 */
public class EpochToSDFConvertor extends DateTimeConvertor {

  public EpochToSDFConvertor(String inputFormat, String outputFormat, String outputGranularity) {
    super(inputFormat, outputFormat, outputGranularity);
  }

  @Override
  public Long convert(Object dateTimeValue) {
    Long dateTimeColumnValueMS = convertEpochToMillis(dateTimeValue);
    Long bucketedDateTimevalueMS = bucketDateTimeValueMS(dateTimeColumnValueMS);
    Long dateTimeValueConverted = convertMillisToSDF(bucketedDateTimevalueMS);
    return dateTimeValueConverted;
  }



}
