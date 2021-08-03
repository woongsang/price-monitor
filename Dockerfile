FROM ubuntu:18.04

ARG MONGO_DB_PORT
ARG KAFKA_PORT

EXPOSE ${MONGO_DB_PORT}
EXPOSE ${KAFKA_PORT}

# install build dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        build-essential \
        software-properties-common \
        curl \
    && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# install python3.8
RUN add-apt-repository ppa:deadsnakes/ppa
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.8 \
        python3.8-dev \
        python3.8-distutils \
    && \
    curl -fSsL -O https://bootstrap.pypa.io/get-pip.py && \
    python3.8 get-pip.py && \
    rm -f get-pip.py && \
    rm -f /usr/bin/python && \
    ln -s /usr/bin/python3.8 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ADD ./requirements.txt .
RUN pip3.8 install --no-cache-dir -r ./requirements.txt && \
    rm -f ./requirements.txt

ADD .env .
ADD binance_websocket.py .

