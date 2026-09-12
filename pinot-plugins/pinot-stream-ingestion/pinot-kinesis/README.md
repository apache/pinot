<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Kinesis request limiting

The `requests_per_second_limit` stream setting accepts positive fractional values
(for example, `0.25`) and defaults to `1.0`. The budget is shared within a server
JVM across consumers with the same stream name, shard, AWS operation, region,
endpoint override, and credential namespace. `GetRecords` and `GetShardIterator`
have separate budgets. Colocated tables sharing a budget do not each receive the
full configured rate.

For each budget, the effective rate is the lowest configured rate among consumers
that have requested that operation and have not closed. Closing the lowest-rate
consumer restores the minimum of the remaining consumers. Closing the last
consumer removes its registrations. The idle limiter retains permit timing for up
to one hour, preserving the previous cache lifetime across short-lived consumers.
Reopening during that period recomputes the rate from the new registrations. Updated table settings take effect when consumers
are recreated; the limiter does not mutate a running consumer's configuration.
Permits already reserved before a rate change retain their existing wait time.

Credential namespaces follow client configuration: assumed-role ARN, explicit
access-key ID (when both access and secret keys are set), or the JVM's default
credential chain. Secret keys and temporary session credentials are not stored in
the limiter key. This avoids extra AWS identity requests and IAM permissions.
Different role ARNs or access-key IDs for the same AWS stream use separate budgets;
default-chain consumers in the same JVM share a namespace. Endpoint overrides are
compared as configured. This is configuration isolation, not AWS account discovery.

The limiter does not coordinate across server JVMs or other applications. Size
the configured rate for all Pinot replicas, separate credential namespaces, and
other AWS readers. A denied permit returns a no-progress batch within the fetch
budget. AWS calls and SDK retries still have their own timeout behavior.
