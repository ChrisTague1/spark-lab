FROM eclipse-temurin:11-jdk

ARG HADOOP_VERSION=3.3.6
ARG SPARK_VERSION=3.5.8

RUN apt-get update && \
    apt-get install -y \
        curl \
        python3 \
        python3-pip \
        procps \
        iputils-ping \
        net-tools \
        vim \
        less \
        && rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------------------
# Hadoop
# -------------------------------------------------------------------

RUN curl -fsSL \
    "https://archive.apache.org/dist/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz" \
    -o /tmp/hadoop.tgz && \
    tar -xzf /tmp/hadoop.tgz -C /opt && \
    mv /opt/hadoop-${HADOOP_VERSION} /opt/hadoop && \
    rm /tmp/hadoop.tgz

# -------------------------------------------------------------------
# Spark
# -------------------------------------------------------------------

RUN curl -fsSL \
    "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz" \
    -o /tmp/spark.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark && \
    rm /tmp/spark.tgz

ENV JAVA_HOME=/opt/java/openjdk
ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark

ENV HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
ENV YARN_CONF_DIR=/opt/hadoop/etc/hadoop

ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin

WORKDIR /workspace

CMD ["bash"]