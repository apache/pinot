FROM lazyminion/pinot:v0.11.0.2

LABEL MAINTAINER=dev@pinot.apache.org

ARG PINOT_GIT_URL="https://github.com/Shubham21k/pinot.git"
ARG PINOT_BRANCH=access_control

# clone the Pinot repository and switch to the specified branch
RUN git clone ${PINOT_GIT_URL} /opt/pinot-src && \
    cd /opt/pinot-src && \
    git checkout ${PINOT_BRANCH}

# copy the updated pinot-server code into the container
COPY pinot-server /opt/pinot-src/pinot-server

# build only the pinot-server code with Maven
RUN cd /opt/pinot-src/pinot-server && \
    mvn -q clean install package -DskipTests -Drat.skip=true -Dlicense.skip=true -Pbin-dist -Pbuild-shaded-jar -Djdk.version=11 -T1C && \
    cp -r target/* /opt/pinot/. && \
    chmod +x /opt/pinot/bin/*.sh
