FROM clojure:lein-2.9.1 as build

COPY . /build/

WORKDIR /build
RUN lein build

FROM openjdk:8u222-jre

COPY --from=build /build/target/blaze-0.7.0-feature.47.9-standalone.jar /app/

WORKDIR /app

CMD ["/bin/bash", "-c", "java $JVM_OPTS -jar blaze-0.7.0-feature.47.9-standalone.jar"]
