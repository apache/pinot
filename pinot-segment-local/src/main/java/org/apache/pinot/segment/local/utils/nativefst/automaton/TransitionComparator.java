/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.pinot.segment.local.utils.nativefst.automaton;

import java.io.Serializable;
import java.util.Comparator;

class TransitionComparator implements Comparator<Transition>, Serializable {

	static final long serialVersionUID = 10001;

	boolean _toFirst;
	
	TransitionComparator(boolean _toFirst) {
		this._toFirst = _toFirst;
	}
	
	/** 
	 * Compares by (min, reverse max, to) or (to, min, reverse max). 
	 */
	public int compare(Transition t1, Transition t2) {
		if (_toFirst) {
			if (t1._to != t2._to) {
				if (t1._to == null) {
          return -1;
        } else if (t2._to == null) {
          return 1;
        } else if (t1._to._number < t2._to._number) {
          return -1;
        } else if (t1._to._number > t2._to._number) {
          return 1;
        }
			}
		}
		if (t1._min < t2._min) {
      return -1;
    }
		if (t1._min > t2._min) {
      return 1;
    }
		if (t1._max > t2._max) {
      return -1;
    }
		if (t1._max < t2._max) {
      return 1;
    }
		if (!_toFirst) {
			if (t1._to != t2._to) {
				if (t1._to == null) {
          return -1;
        } else if (t2._to == null) {
          return 1;
        } else if (t1._to._number < t2._to._number) {
          return -1;
        } else if (t1._to._number > t2._to._number) {
          return 1;
        }
			}
		}
		return 0;
	}
}
