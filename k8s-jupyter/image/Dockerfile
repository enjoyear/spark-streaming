# docker search jupyter
# jupyter/pyspark-notebook:latest uses Spark 3.5
# almondsh/almond:latest uses Spark 4.0, but has Java 8
FROM jupyter/base-notebook:latest

USER root

# Install Java 17 - that's ALL you need!
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

USER jovyan

# Install Almond (Scala kernel) for Java 17
RUN curl -Lo coursier https://git.io/coursier-cli && \
    chmod +x coursier && \
    ./coursier launch --fork almond:0.14.0-RC14 --scala 2.13.12 -- --install && \
    rm -f coursier

# Optional: Pre-install Python packages if you want Python support
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    matplotlib

WORKDIR /home/jovyan