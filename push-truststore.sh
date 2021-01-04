#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
JAVA_HOME="/Library/Java/JavaVirtualMachines/jdk-13.0.2.jdk/Contents/Home"

CONFIG_DIR="$DIR/truststore"
KEY_TOOL=$JAVA_HOME/bin/keytool
KEYSTORE_PASSWORD="changeit"

TRUST_STORE=$CONFIG_DIR/generated.truststore.jks
KEY_STORE=$CONFIG_DIR/generated.keystore.jks
P12_STORE=$CONFIG_DIR/generated.key.p12

echo "removing any old generated files"
rm -f $TRUST_STORE $KEY_STORE $P12_STORE
echo "writing trust store"

$KEY_TOOL \
  -noprompt \
  -import \
  -storepass $KEYSTORE_PASSWORD \
  -keystore $TRUST_STORE \
  -storetype PKCS12 \
  -file $CONFIG_DIR/ca-cert.pem
echo "converting key/cert into PKCS12"

openssl pkcs12 \
  -export \
  -in $CONFIG_DIR/cert.pem \
  -inkey $CONFIG_DIR/key.pem \
  -out $P12_STORE \
  -password pass:$KEYSTORE_PASSWORD \
  -name localhost
echo "writing key store"

$KEY_TOOL -importkeystore \
  -deststorepass $KEYSTORE_PASSWORD \
  -destkeypass $KEYSTORE_PASSWORD \
  -destkeystore $KEY_STORE \
  -deststoretype PKCS12 \
  -srckeystore $P12_STORE \
  -srcstoretype PKCS12 \
  -srcstorepass $KEYSTORE_PASSWORD \
  -srckeypass $KEYSTORE_PASSWORD \
  -alias localhost
