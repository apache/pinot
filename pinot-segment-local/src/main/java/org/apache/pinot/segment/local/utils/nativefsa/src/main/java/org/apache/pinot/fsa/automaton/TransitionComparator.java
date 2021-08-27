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

package org.apache.pinot.segment.local.utils.nativefsa.src.main.java.org.apache.pinot.fsa.automaton;

import java.io.Serializable;
import java.util.Comparator;

class TransitionComparator implements Comparator<Transition>, Serializable {

	static final long serialVersionUID = 10001;

	boolean to_first;
	
	TransitionComparator(boolean to_first) {
		this.to_first = to_first;
	}
	
	/** 
	 * Compares by (min, reverse max, to) or (to, min, reverse max). 
	 */
	public int compare(Transition t1, Transition t2) {
		if (to_first) {
			if (t1.to != t2.to) {
				if (t1.to == null)
					return -1;
				else if (t2.to == null)
					return 1;
				else if (t1.to.number < t2.to.number)
					return -1;
				else if (t1.to.number > t2.to.number)
					return 1;
			}
		}
		if (t1.min < t2.min)
			return -1;
		if (t1.min > t2.min)
			return 1;
		if (t1.max > t2.max)
			return -1;
		if (t1.max < t2.max)
			return 1;
		if (!to_first) {
			if (t1.to != t2.to) {
				if (t1.to == null)
					return -1;
				else if (t2.to == null)
					return 1;
				else if (t1.to.number < t2.to.number)
					return -1;
				else if (t1.to.number > t2.to.number)
					return 1;
			}
		}
		return 0;
	}
}
