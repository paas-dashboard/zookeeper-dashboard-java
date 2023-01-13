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

FROM shoothzj/compile:jdk17-mvn AS build
COPY . /opt/compile
WORKDIR /opt/compile
RUN mvn -B clean package -DskipTests

FROM shoothzj/base:jdk17

WORKDIR /opt/zookeeper-dashboard

COPY --from=build /opt/compile/target/zookeeper-dashboard-0.0.1-SNAPSHOT.jar /opt/zookeeper-dashboard/zookeeper-dashboard.jar
COPY --from=build /opt/compile/target/conf /opt/zookeeper-dashboard/conf
COPY --from=build /opt/compile/target/lib /opt/zookeeper-dashboard/lib

RUN wget -q https://github.com/paas-dashboard/zookeeper-dashboard-portal/releases/download/latest/zookeeper-dashboard-portal.tar.gz && \
    tar -xzf zookeeper-dashboard-portal.tar.gz && \
    rm -rf zookeeper-dashboard-portal.tar.gz

ENV STATIC_PATH /opt/zookeeper-dashboard/static/

EXPOSE 10002

CMD ["/usr/bin/dumb-init", "java", "-jar", "/opt/zookeeper-dashboard/zookeeper-dashboard.jar"]
