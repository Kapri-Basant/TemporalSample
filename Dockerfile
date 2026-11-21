# syntax=docker/dockerfile:1.7
ARG BUILD_IMAGE=gradle:jdk25
ARG RUN_IMAGE=eclipse-temurin:25-jre
FROM ${BUILD_IMAGE} AS build
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=/home/gradle/.gradle gradle --no-daemon bootJar

FROM ${RUN_IMAGE}
WORKDIR /app
COPY --from=build /app/build/libs/*.jar app.jar
USER 10001:10001
EXPOSE 8080 8081
ENTRYPOINT ["java","-XX:MaxRAMPercentage=70","-jar","/app/app.jar"]
