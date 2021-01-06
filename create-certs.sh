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
DOMAIN="localhost"
IP="192.168.0.126"

KEY_DIR="$DIR/truststore"

mkdir -p $KEY_DIR
rm $KEY_DIR/*.pem

echo "1. Generate CA's private key and self-signed certificate"
openssl req -x509 -newkey rsa:4096 -days 365 -nodes -keyout "$KEY_DIR/ca-key.pem" -out "$KEY_DIR/ca-cert.pem" -subj "/C=US/ST=Someplace/L=Somewhere/O=Apache Pinot/OU=Education/CN=*.example.org/emailAddress=admin@example.org"

#echo "CA's self-signed certificate"
#openssl x509 -in "$KEY_DIR/ca-cert.pem" -noout -text

echo "2. Generate web server's private key and certificate signing request (CSR)"
openssl req -newkey rsa:4096 -nodes -keyout "$KEY_DIR/key.pem" -out "$KEY_DIR/req.pem" -subj "/C=US/ST=Someplace/L=Somewhere/O=Apache Pinot/OU=Education/CN=$DOMAIN/emailAddress=admin@localhost"

echo "3. Use CA's private key to sign web server's CSR and get back the signed certificate"
echo "subjectAltName=DNS:$DOMAIN,IP:$IP" > "$KEY_DIR/ext.cnf"
openssl x509 -req -in "$KEY_DIR/req.pem" -days 60 -CA "$KEY_DIR/ca-cert.pem" -CAkey "$KEY_DIR/ca-key.pem" -CAcreateserial -out "$KEY_DIR/cert.pem" -extfile "$KEY_DIR/ext.cnf"

#echo "Server's signed certificate"
#openssl x509 -in "$KEY_DIR/cert.pem" -noout -text

echo "Verifying certificate"
openssl verify -CAfile "$KEY_DIR/ca-cert.pem" "$KEY_DIR/cert.pem"

