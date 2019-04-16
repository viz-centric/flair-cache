FROM openjdk:8

RUN addgroup -S flairgroup && adduser -S flairuser -G flairgroup
USER flairuser
WORKDIR /usr/local/bin/flairuser

ENV SPRING_OUTPUT_ANSI_ENABLED=ALWAYS \
    JAVA_OPTS="" \
    JHIPSTER_SLEEP=0

VOLUME /tmp
EXPOSE 12355

COPY *.jar /app.jar

CMD echo "The application will start in ${JHIPSTER_SLEEP}s..." && \
    sleep ${JHIPSTER_SLEEP} && \
    java ${JAVA_OPTS} -Djava.security.egd=file:/dev/./urandom -jar app.jar

