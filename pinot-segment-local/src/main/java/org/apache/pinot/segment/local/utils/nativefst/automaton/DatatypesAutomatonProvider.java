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

/**
 * Automaton provider based on {@link Datatypes}.
 */
public class DatatypesAutomatonProvider implements AutomatonProvider {
	
	private boolean enable_unicodeblocks, enable_unicodecategories, enable_xml;
	
	/**
	 * Constructs a new automaton provider that recognizes all names
	 * from {@link Datatypes#get(String)}.
	 */
	public DatatypesAutomatonProvider() {
		enable_unicodeblocks = enable_unicodecategories = enable_xml = true;
	}
	
	/**
	 * Constructs a new automaton provider that recognizes some of the names
	 * from {@link Datatypes#get(String)}
	 * @param enable_unicodeblocks if true, enable Unicode block names
	 * @param enable_unicodecategories if true, enable Unicode category names
	 * @param enable_xml if true, enable XML related names
	 */
	public DatatypesAutomatonProvider(boolean enable_unicodeblocks, boolean enable_unicodecategories, boolean enable_xml) {
		this.enable_unicodeblocks = enable_unicodeblocks; 
		this.enable_unicodecategories = enable_unicodecategories;
		this.enable_xml = enable_xml;
	}
	
	public Automaton getAutomaton(String name) {
		if ((enable_unicodeblocks && Datatypes.isUnicodeBlockName(name))
				|| (enable_unicodecategories && Datatypes.isUnicodeCategoryName(name))
				|| (enable_xml && Datatypes.isXMLName(name)))
				return Datatypes.get(name);
		return null;
	}
}
