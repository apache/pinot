FROM docker.io/library/lazyminion/pinot:v0.11.0.2 AS pinot_base_build

LABEL MAINTAINER=dev@pinot.apache.org

ARG PINOT_BRANCH=access_control
ARG KAFKA_VERSION=2.0
ARG JDK_VERSION=11
ARG PINOT_GIT_URL="https://github.com/Shubham21k/pinot.git"
RUN echo "Trying to build Pinot from [ ${PINOT_GIT_URL} ] on branch [ ${PINOT_BRANCH} ] with Kafka version [ ${KAFKA_VERSION} ]"
ENV PINOT_HOME=/opt/pinot
ENV PINOT_BUILD_DIR=/opt/pinot-build
ENV MAVEN_HOME /usr/share/maven
ENV MAVEN_CONFIG /opt/.m2

RUN cd ${PINOT_HOME} && \
    cd ${PINOT_BUILD_DIR} && \
    git checkout ${PINOT_BRANCH} && \
    mvn -q install package -DskipTests -Drat.skip=true -Dlicense.skip=true -Pbin-dist -Pbuild-shaded-jar -Djdk.version=${JDK_VERSION} -T1C && \
    mkdir -p ${PINOT_HOME}/configs && \
    mkdir -p ${PINOT_HOME}/data && \
    cp -r build/* ${PINOT_HOME}/. && \
    chmod +x ${PINOT_HOME}/bin/*.sh