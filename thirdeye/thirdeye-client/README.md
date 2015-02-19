thirdeye-client
===============

Example usage

```
ThirdEyeClient client = ThirdEyeClientFactory.createDefault(new InetSocketAddress(8080));

List<ThirdEyeMetrics> metrics
        = client.getMetrics(ThirdEyeQuery.builder()
                                         .setCollection("abook")
                                         .setStartTime(392815)
                                         .setEndTime(393295)
                                         .setDimensionValue("browserName", "chrome")
                                         .build());

List<ThirdEyeTimeSeries> timeSeries
        = client.getTimeSeries(ThirdEyeQuery.builder()
                                            .setCollection("abook")
                                            .setStartTime(392815)
                                            .setEndTime(393295)
                                            .addMetric("numberOfSuggestedMemberConnections")
                                            .build());
```
