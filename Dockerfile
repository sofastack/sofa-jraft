# syntax=docker/dockerfile:1

# ---- 构建阶段：编译 jraft-example ----
FROM eclipse-temurin:8-jdk-jammy AS build
WORKDIR /src

# maven wrapper
COPY .mvn/ .mvn/
COPY mvnw .
RUN chmod +x mvnw && sed -i 's/\r$//' mvnw || true

# 复制整个工程（多模块必须）
COPY . .

# 仅构建 jraft-example（并自动构建其依赖模块）
RUN --mount=type=cache,target=/root/.m2 \
    ./mvnw -B -DskipTests -pl jraft-example -am package

# 取出产物（fat-jar 或可运行 jar）
RUN APP=$(./mvnw -q -DforceStdout -pl jraft-example help:evaluate -Dexpression=project.build.finalName) && \
    cp jraft-example/target/${APP}.jar /app.jar

COPY jraft-example/src/main/resources/conf/rheakv/ /conf/


# ---- 运行阶段：最小 JRE ----
FROM eclipse-temurin:8-jre-jammy
WORKDIR /
# 数据目录（日志/快照）
VOLUME ["/data/raft"]
# 统一一些默认环境变量（示例程序若支持，可自行解析）
ENV NODE_ID=1 \
    PEERS="1@node1:8181,2@node2:8181,3@node3:8181" \
    DATA_DIR=/data/raft
COPY --from=build /app.jar /app.jar
COPY --from=build /conf /conf

EXPOSE 8181
ENTRYPOINT ["java", "-jar", "/app.jar"]

