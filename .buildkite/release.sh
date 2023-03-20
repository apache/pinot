#!/bin/bash
set -ex
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

# Setup the JAVA path
setup_env () {
    export JAVA_HOME=$JAVA_HOME_PATH
    export PATH=$JAVA_HOME/bin:$PATH
    echo java --version
}

# Download the JAVA 11
setup_java () {
	rm -rf $JAVA_HOME_PATH || true
    if [[ ! -d $JAVA_HOME_PATH ]]; then
        mkdir -p $JAVA_HOME_PATH
    fi

    BINARY_ARCHIVE_NAME="$(echo $JAVA_JDK_NUMBER | sed 's/-//g').tar.gz"
    ARCHIVE_INSTALL_PATH="$JAVA_HOME_PATH/$BINARY_ARCHIVE_NAME"
    DOWNLOAD_SUCCEEDED=false

    wget "http://artifactory.uber.internal:4587/artifactory/libs-release-local/com/uber/devxp/jdk-linux/${JAVA_JDK_NUMBER}/jdk-linux-${JAVA_JDK_NUMBER}.tar.gz" -P ${ARCHIVE_INSTALL_PATH}
    if [[ "$?" -eq 0 ]]; then
        DOWNLOAD_SUCCEEDED=true
    fi

    if [[ $DOWNLOAD_SUCCEEDED == "false" ]]; then
        echo "Unable to download JDK $JAVA_JDK_NUMBER"
        return
    else
        echo "Download JDK $JAVA_JDK_NUMBER succeeded"
    fi
    tar -xvzf $ARCHIVE_INSTALL_PATH/*.tar.gz -C $JAVA_HOME_PATH --strip 1 1>/dev/null 2>&1
}

JAVA_JDK_NUMBER="11.0.11_9"
JAVA_HOME_PATH="$HOME/java_home/$JAVA_JDK_NUMBER"

if [[ -f "$JAVA_HOME_PATH/bin/java" ]]; then
    echo "JDK 11 was already downloaded"
else
    echo "JDK 11 will be downloaded"
    setup_java
fi
setup_env

TOKEN=$(echo -n "ignored:$UNPM_TOKEN" | base64 -w 0)
echo -e "\n" >> ~/.npmrc
echo "//unpm.uberinternal.com/:_auth=$TOKEN" >> ~/.npmrc

#npm config delete _auth;
#npm config set _auth $TOKEN

export MAVEN_OPTS="-Xmx8G -Xss128M -XX:MetaspaceSize=512M -XX:MaxMetaspaceSize=1024M -XX:+CMSClassUnloadingEnabled"
release_opts=
if [ -n "$RELEASE_VERSION" ]; then
release_opts="$release_opts -DreleaseVersion=$RELEASE_VERSION"
fi
if [ -n "$NEXT_VERSION" ]; then
release_opts="$release_opts -DdevelopmentVersion=$NEXT_VERSION"
fi
# This step also push the merged change to the Uber pinot
mvn -e -B -X release:clean release:prepare release:perform -Darguments="-Dgpg.skip=true -Drat.skip=true -Dlicense.skip=true -DskipTests -Dmaven.javadoc.skip=true -P build-shaded-jar" $release_opts

