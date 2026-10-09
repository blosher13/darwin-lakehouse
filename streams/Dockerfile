FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src ./src
RUN mvn -q -B package

# glibc-based JRE: the Zerobus SDK loads a native (JNI) library
FROM eclipse-temurin:21-jre
COPY --from=build /src/target/darwin-zerobus.jar /app/darwin-zerobus.jar
ENTRYPOINT ["java", "-jar", "/app/darwin-zerobus.jar"]
