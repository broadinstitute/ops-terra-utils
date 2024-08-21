FROM python:3.11

USER root
# This makes it so pip runs as root, not the user.
ENV PIP_USER=false


RUN apt-get update && apt-get install -yq --no-install-recommends \
  lsb-release \ 
  python3-tk \
  apt-transport-https \
  ca-certificates \ 
  gnupg \ 
  gpg \
  tk-dev \
  libssl-dev \
  xz-utils \
  libhdf5-dev \
  openssl \
  make \
  zlib1g-dev \
  libz-dev \
  libcurl4-openssl-dev \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

ENV HTSLIB_CONFIGURE_OPTIONS="--enable-gcs"

#install gcloud cli
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && apt-get update -y && apt-get install google-cloud-cli -y
    


COPY requirements.txt /etc/terra-docker/
COPY stand_alone_terra_and_tdr/ /etc/terra_utils

RUN pip3 -V \
  && pip3 install --upgrade pip \
  && pip3 install wheel \ 
  && pip3 install --upgrade -r /etc/terra-docker/requirements.txt 

RUN pip3 install --upgrade markupsafe==2.1.2

#install gcsfuse
RUN export GCSFUSE_REPO=gcsfuse-`lsb_release -c -s` \
 && echo "deb [signed-by=/usr/share/keyrings/cloud.google.asc] https://packages.cloud.google.com/apt $GCSFUSE_REPO main" | tee /etc/apt/sources.list.d/gcsfuse.list \ 
 && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | tee /usr/share/keyrings/cloud.google.asc \ 
 && apt-get update \ 
 && apt-get install -yq gcsfuse 

#install azcopy
RUN curl -sSL -O https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb \ 
 && dpkg -i packages-microsoft-prod.deb \ 
 && rm packages-microsoft-prod.deb \ 
 && apt-get update \ 
 && apt-get install azcopy 

# Enable Intel oneDNN optimizatoin by default
ENV TF_ENABLE_ONEDNN_OPTS=1

