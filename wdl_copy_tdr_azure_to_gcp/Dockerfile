FROM gcr.io/google.com/cloudsdktool/google-cloud-cli:alpine


ARG PYTHON_VERSION=3.11.9

USER root
ENV PIP_USER=false

# install build dependencies and needed tools
RUN apk update 
RUN apk add \
    wget \
    curl \
    gcc \
    python3 --repository=http://dl-cdn.alpinelinux.org/alpine/edge/main \ 
    py3-pip \ 
    make \
    dpkg \
    which \
    bash \ 
    ca-certificates \ 
    zlib-dev \
    libffi-dev \
    openssl-dev \
    musl-dev

RUN ln -sf python3 /usr/bin/python
RUN pip3 install --break-system-packages --no-cache --upgrade pip setuptools 

# download and extract python sources
#RUN cd /opt \
#    && wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz \                                              
#    && tar xzf Python-${PYTHON_VERSION}.tgz

# build python and remove left-over sources
#RUN cd /opt/Python-${PYTHON_VERSION} \ 
#    && ./configure --prefix=/usr --enable-optimizations --with-ensurepip=install \
#    && make install \
#    && rm /opt/Python-${PYTHON_VERSION}.tgz /opt/Python-${PYTHON_VERSION} -rf
         


COPY requirements.txt /etc/terra-docker/
COPY stand_alone_terra_and_tdr/ /etc/terra_utils

RUN pip3 install --break-system-packages --no-cache-dir --upgrade pip \
  && pip3 install --break-system-packages --no-cache-dir --upgrade -r /etc/terra-docker/requirements.txt

#install azcopy
RUN curl -s -L https://azcopyvnext.azureedge.net/releases/release-10.24.0-20240326/azcopy_linux_amd64_10.24.0.tar.gz | tar xvzf - --strip-components=1 -C /usr/local/bin/ azcopy_linux_amd64_10.24.0/azcopy
RUN chown root:root /usr/local/bin/azcopy
RUN chmod +x /usr/local/bin/azcopy
RUN ldd /usr/local/bin/azcopy


ENV AZCOPY_BUFFER_GB: 2
ENV AZCOPY_CONCURRENCY_VALUE: 4
 


#RUN curl -sSL https://sdk.cloud.google.com | bash
#RUN curl https://dl.google.com/dl/cloudsdk/release/google-cloud-sdk.tar.gz > /tmp/google-cloud-sdk.tar.gz
#RUN mkdir -p /usr/local/gcloud \
#  && tar -C /usr/local/gcloud -xvf /tmp/google-cloud-sdk.tar.gz \
#  && /usr/local/gcloud/google-cloud-sdk/install.sh --quiet



ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin


#RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && apt-get update -y && apt-get install google-cloud-cli -y

ENV PIP_USER=true
#install gcloud cli
ENV GPG_TTY=$(tty)
