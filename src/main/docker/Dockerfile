FROM openjdk:8

LABEL maintainer="admin@vizcentric.com"
LABEL name="Vizcentric"

RUN groupadd -g 999 flairuser && \
    useradd -r -u 999 -g flairuser flairuser
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

