# docker build -t mvillalobos/flink-patterns/average-time-series:latest  .
# docker run mvillalobos/flink-patterns/average-time-series:latest

FROM gradle:6.4.1-jdk11

ENV APP_DIR /opt/flink-patterns/average-time-series
ENV GRADLE_OPTS -Dorg.gradle.daemon=false

ADD build.gradle $APP_DIR/
ADD settings.gradle $APP_DIR/

WORKDIR $APP_DIR

RUN mkdir -p $APP_DIR/src/main

ADD src/main $APP_DIR/src/main
ADD timeseries.csv $APP_DIR/

ENTRYPOINT ["gradle", "run"]
