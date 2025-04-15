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
package org.apache.pinot.spi.secretstore;

/**
 * Exception thrown when operations on the {@link SecretStore} fail.
 * This exception encapsulates errors that may occur during secret storage,
 * retrieval, updating, or deletion operations.
 */
public class SecretStoreException extends RuntimeException {

    /**
     * Creates a new SecretStoreException with the specified message.
     *
     * @param message the detail message
     */
    public SecretStoreException(String message) {
        super(message);
    }

    /**
     * Creates a new SecretStoreException with the specified message and cause.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public SecretStoreException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Creates a new SecretStoreException with the specified cause.
     *
     * @param cause the cause of the exception
     */
    public SecretStoreException(Throwable cause) {
        super(cause);
    }

    /**
     * Exception thrown when a requested secret cannot be found.
     */
    public static class SecretNotFoundException extends SecretStoreException {
        public SecretNotFoundException(String secretKey) {
            super("Secret not found: " + secretKey);
        }

        public SecretNotFoundException(String secretKey, Throwable cause) {
            super("Secret not found: " + secretKey, cause);
        }
    }

    /**
     * Exception thrown when permission is denied for a secret store operation.
     */
    public static class SecretPermissionException extends SecretStoreException {
        public SecretPermissionException(String secretKey) {
            super("Permission denied for secret: " + secretKey);
        }

        public SecretPermissionException(String secretKey, Throwable cause) {
            super("Permission denied for secret: " + secretKey, cause);
        }
    }

    /**
     * Exception thrown when the secret store is unavailable or cannot be reached.
     */
    public static class SecretStoreConnectionException extends SecretStoreException {
        public SecretStoreConnectionException(String message) {
            super("Failed to connect to secret store: " + message);
        }

        public SecretStoreConnectionException(String message, Throwable cause) {
            super("Failed to connect to secret store: " + message, cause);
        }
    }
}
