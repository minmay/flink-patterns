# build with docker-compose
FROM flink:1.10.1-scala_2.12-jdk8

ENV APP_DIR /opt/flink-patterns/bootstrap-keyed-state-into-stream
ENV GRADLE_OPTS -Dorg.gradle.daemon=false

ADD gradle/wrapper/gradle-wrapper.jar $APP_DIR/gradle/wrapper/
ADD gradle/wrapper/gradle-wrapper.properties $APP_DIR/gradle/wrapper/
ADD gradlew $APP_DIR/
ADD build.gradle $APP_DIR/
ADD settings.gradle $APP_DIR/

WORKDIR $APP_DIR

RUN ./gradlew

RUN mkdir -p $APP_DIR/src/main
ADD src/main $APP_DIR/src/main

RUN ./gradlew shadowJar

ADD wait-for-it.sh $APP_DIR/
ADD run.sh $APP_DIR/

ENTRYPOINT ["/bin/bash"]
CMD ["java", "-jar", "build/libs/bootstrap-keyed-state-into-stream-all.jar"]
