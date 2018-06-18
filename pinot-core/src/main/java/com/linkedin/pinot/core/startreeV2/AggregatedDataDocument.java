/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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

package com.linkedin.pinot.core.startreeV2;


public class AggregatedDataDocument {

  private Object _max = -1;
  private Object _min = -1;
  private Object _sum = -1;
  private Object _count = 0;

  public void setMax(Object max) {
    _max = max;
  }

  public Object getMax() {
    return _max;
  }

  public void setMin(Object min) {
    _min = min;
  }

  public Object getMin() {
    return _min;
  }

  public void setSum(Object sum) {
    _sum = sum;
  }

  public Object getSum() {
    return _sum;
  }

  public void setCount(Object count) {
    _count = count;
  }

  public Object getCount(Object count) {
    return _count;
  }

}
