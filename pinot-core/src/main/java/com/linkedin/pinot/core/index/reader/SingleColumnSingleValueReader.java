/**
 * Copyright (C) 2014-2015 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.core.index.reader;

public interface SingleColumnSingleValueReader extends DataFileReader{

	/**
	 * fetch the char at a row
	 * 
	 * @param row
	 * @return
	 */
	char getChar(int row);

	/**
	 * fetch short value at a specific row, col
	 * 
	 * @param row
	 * @return
	 */
	short getShort(int row);

	/**
	 * 
	 * @param row
	 * @return
	 */
	int getInt(int row);

	/**
	 * 
	 * @param row
	 * @return
	 */
	long getLong(int row);

	/**
	 * 
	 * @param row
	 * @return
	 */
	float getFloat(int row);

	/**
	 * 
	 * @param row
	 * @return
	 */
	double getDouble(int row);

	/**
	 * 
	 * @param row
	 * @return
	 */
	String getString(int row);

	/**
	 * 
	 * @param row
	 * @return
	 */
	byte[] getBytes(int row);
}
