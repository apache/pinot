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
package org.apache.pinot.common.utils;

import org.mindrot.jbcrypt.BCrypt;

public class BcryptUtils {

    private static final int DEFALUT_LOG_ROUNDS = 10;
    private static String _bcryptPassword = null;

    private BcryptUtils() {
    }

    public static String encrypt(String password) {
        return encrypt(password, DEFALUT_LOG_ROUNDS);
    }

    public static String encrypt(String password, int saltLogRrounds) {
        _bcryptPassword = BCrypt.hashpw(password, BCrypt.gensalt(saltLogRrounds));
        return _bcryptPassword;
    }

    public static boolean checkpw(String pasword, String encrypedPassword) {
        boolean isMatch = false;
        try {
            isMatch = BCrypt.checkpw(pasword, encrypedPassword);
        } catch (Exception e) {
            System.out.println(e.getMessage());
        } finally {
            return isMatch;
        }
    }
}
