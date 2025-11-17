# docker search jupyter
# jupyter/pyspark-notebook:latest uses Spark 3.5
# almondsh/almond:latest uses Spark 4.0, but has Java 8
FROM jupyter/base-notebook:latest

# Avoid interactive prompts
ENV DEBIAN_FRONTEND=noninteractive

USER root

# One RUN, fresh lists, retries, no-cache, and cleanup
RUN set -eux; \
    # start clean to avoid stale lists from the base image/layers
    rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*.deb; \
    # update with no-cache and retries
    apt-get update -o Acquire::Retries=5 -o Acquire::http::No-Cache=true;

RUN apt-get install -y --no-install-recommends \
        bash \
        curl

RUN apt-get install -y --no-install-recommends \
        java-common \
        jq \
        gettext

RUN apt-get install -y --no-install-recommends gnupg2

RUN apt-get install -y --no-install-recommends \
        openssl \
        wget

RUN targetCpu="$(arch)" && \
    if [ "$targetCpu" = "x86_64" ]; then \
        correttoUrl="https://corretto.aws/downloads/latest/amazon-corretto-21-x64-linux-jdk.deb"; \
    else \
        correttoUrl="https://corretto.aws/downloads/latest/amazon-corretto-21-aarch64-linux-jdk.deb"; \
    fi && \
    wget -q -O linux-jdk.deb "$correttoUrl"

RUN apt install -y ./linux-jdk.deb && \
    rm -f linux-jdk.deb && \
    update-alternatives --config java

ENV JAVA_HOME=/usr/lib/jvm/java-21-amazon-corretto
ENV PATH=$JAVA_HOME/bin:$PATH

USER jovyan

# Install Almond (Scala kernel)
RUN curl -Lo coursier https://git.io/coursier-cli && \
    chmod +x coursier && \
    ./coursier launch --fork almond:0.14.1 --scala 2.13.16 -- --install && \
    rm -f coursier

# Optional: Pre-install Python packages if you want Python support
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    matplotlib

WORKDIR /home/jovyan

# Set JDK_JAVA_OPTIONS for Arrow memory access
# We need this at the Jupyter Kernel runtim, otherwise it will give the error like
# java.lang.RuntimeException: Failed to initialize MemoryUtil. You must start Java with `--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED` (See https://arrow.apache.org/docs/java/install.html)
ENV JDK_JAVA_OPTIONS="--add-opens=java.base/java.nio=org.apache.arrow.memory.core,ALL-UNNAMED"
