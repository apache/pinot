/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.thirdeye.detection;

import com.linkedin.thirdeye.dataframe.util.MetricSlice;
import java.util.Collection;
import java.util.Collections;


/**
 * The data spec to describe all the input data for a detection stage perform the detection.
 */
public class InputDataSpec {
  final Collection<MetricSlice> timeseriesSlices;
  final Collection<MetricSlice> aggregateSlices;
  final Collection<AnomalySlice> anomalySlices;
  final Collection<EventSlice> eventSlices;

  public InputDataSpec() {
    this.timeseriesSlices = Collections.emptyList();
    this.aggregateSlices = Collections.emptyList();
    this.anomalySlices = Collections.emptyList();
    this.eventSlices = Collections.emptyList();
  }

  public InputDataSpec(Collection<MetricSlice> timeseriesSlices, Collection<MetricSlice> aggregateSlices,
      Collection<AnomalySlice> anomalySlices, Collection<EventSlice> eventSlices) {
    this.timeseriesSlices = timeseriesSlices;
    this.aggregateSlices = aggregateSlices;
    this.anomalySlices = anomalySlices;
    this.eventSlices = eventSlices;
  }

  public Collection<MetricSlice> getTimeseriesSlices() {
    return timeseriesSlices;
  }

  public Collection<MetricSlice> getAggregateSlices() {
    return aggregateSlices;
  }

  public Collection<AnomalySlice> getAnomalySlices() {
    return anomalySlices;
  }

  public Collection<EventSlice> getEventSlices() {
    return eventSlices;
  }

  public InputDataSpec withTimeseriesSlices(Collection<MetricSlice> timeseriesSlices) {
    return new InputDataSpec(timeseriesSlices, this.aggregateSlices, this.anomalySlices, this.eventSlices);
  }

  public InputDataSpec withAggregateSlices(Collection<MetricSlice> aggregateSlices) {
    return new InputDataSpec(this.timeseriesSlices, aggregateSlices, this.anomalySlices, this.eventSlices);
  }

  public InputDataSpec withAnomalySlices(Collection<AnomalySlice> anomalySlices) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, anomalySlices, this.eventSlices);
  }

  public InputDataSpec withEventSlices(Collection<EventSlice> eventSlices) {
    return new InputDataSpec(this.timeseriesSlices, this.aggregateSlices, this.anomalySlices, eventSlices);
  }
}
