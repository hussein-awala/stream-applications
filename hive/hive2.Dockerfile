FROM openjdk:8u242-jre

WORKDIR /opt

ENV HADOOP_VERSION=3.1.2
ENV METASTORE_VERSION=2.3.9

ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV HIVE_HOME=/opt/apache-hive-${METASTORE_VERSION}-bin

RUN curl -L https://downloads.apache.org/hive/hive-${METASTORE_VERSION}/apache-hive-${METASTORE_VERSION}-bin.tar.gz | tar zxf - && \
    curl -L https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz | tar zxf -

COPY dependencies.xml /dependencies.xml

COPY ivy-settings.xml /ivy-settings.xml

RUN curl https://repo1.maven.org/maven2/org/apache/ivy/ivy/2.5.0/ivy-2.5.0.jar -o ivy.jar && \
    java -jar ivy.jar -ivy /dependencies.xml -settings /ivy-settings.xml -retrieve "${HIVE_HOME}/lib/[artifact]-[classifier].[ext]"

COPY metastore-site.xml ${HIVE_HOME}/conf/hive-site.xml

COPY hive2/entrypoint.sh /entrypoint.sh

RUN groupadd -r hive --gid=1000 && \
    useradd -r -g hive --uid=1000 -d ${HIVE_HOME} hive && \
    chown hive:hive -R ${HIVE_HOME} && \
    chown hive:hive /entrypoint.sh && chmod +x /entrypoint.sh

USER hive
EXPOSE 9083

WORKDIR $HIVE_HOME/bin

ENTRYPOINT ["sh", "-c", "/entrypoint.sh"]