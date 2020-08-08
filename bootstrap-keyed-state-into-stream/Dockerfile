# docker build -t mvillalobos/flink-patterns/bootstrap-keyed-state-into-stream:latest  .
# docker run mvillalobos/flink-patterns/bootstrap-keyed-state-into-stream

FROM gradle:6.4.1-jdk11

ENV APP_DIR /opt/flink-patterns/bootstrap-keyed-state-into-stream
ENV GRADLE_OPTS -Dorg.gradle.daemon=false

ADD build.gradle $APP_DIR/
ADD settings.gradle $APP_DIR/

WORKDIR $APP_DIR

RUN mkdir -p $APP_DIR/src/main

ADD src/main $APP_DIR/src/main

ENTRYPOINT ["gradle", "run"]
