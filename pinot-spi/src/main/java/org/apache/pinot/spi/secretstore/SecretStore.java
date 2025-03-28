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

import java.util.List;

/**
 * Interface for managing secrets in Apache Pinot.
 *
 * This interface abstracts away the details of the underlying secret storage mechanism,
 * allowing Pinot to work with various secret management systems like AWS Secrets Manager,
 * HashiCorp Vault, or other custom implementations.
 *
 * Implementations of this interface should handle all aspects of secret management including
 * secure storage, retrieval, and cleanup of sensitive information such as connection credentials.
 * All implementations must be thread-safe.
 */
public interface SecretStore {

    /**
     * Stores a secret in the secret management system.
     *
     * @param secretKey A unique identifier for the secret, typically following a hierarchical
     *                   naming pattern (e.g., "pinot/tables/myTable/credentials")
     * @param secretValue The actual secret value to be securely stored
     * @return A reference key that can be used later to retrieve the secret
     * @throws SecretStoreException If the secret cannot be stored due to connectivity issues,
     *                             permission problems, or other errors
     */
    String storeSecret(String secretKey, String secretValue) throws SecretStoreException;

    /**
     * Retrieves a secret from the secret management system.
     *
     * @param secretKey The reference key obtained when the secret was stored
     * @return The actual secret value
     * @throws SecretStoreException If the secret cannot be retrieved or doesn't exist
     */
    String getSecret(String secretKey) throws SecretStoreException;

    /**
     * Updates an existing secret with a new value.
     *
     * @param secretKey The reference key for the secret to be updated
     * @param newSecretValue The new value to store
     * @throws SecretStoreException If the secret cannot be updated or doesn't exist
     */
    void updateSecret(String secretKey, String newSecretValue) throws SecretStoreException;

    /**
     * Deletes a secret when it is no longer needed.
     *
     * This method should be called when the associated resource (e.g., a table or connection)
     * is being deleted to ensure proper cleanup of sensitive information.
     *
     * @param secretKey The reference key for the secret to be deleted
     * @throws SecretStoreException If the secret cannot be deleted or doesn't exist
     */
    void deleteSecret(String secretKey) throws SecretStoreException;

    /**
     * Checks if a secret exists in the secret management system.
     *
     * @param secretKey The reference key for the secret
     * @return true if the secret exists, false otherwise
     * @throws SecretStoreException If the check cannot be performed
     */
    boolean exists(String secretKey) throws SecretStoreException;

    /**
     * Lists all secrets with a given prefix.
     *
     * @param prefix The prefix to filter secrets by
     * @return A list of secret keys matching the prefix
     * @throws SecretStoreException If the secret list cannot be retrieved
     */
    List<String> listSecrets(String prefix) throws SecretStoreException;
}
