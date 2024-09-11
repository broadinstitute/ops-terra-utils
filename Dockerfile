FROM us.gcr.io/broad-dsp-gcr-public/terra-jupyter-python:1.1.5


USER root

ENV AZCOPY_BUFFER_GB: 2
ENV AZCOPY_CONCURRENCY_VALUE: 4

ENV HTSLIB_CONFIGURE_OPTIONS="--enable-gcs"


COPY requirements.txt /etc/terra-docker/
COPY stand_alone_terra_and_tdr/ /etc/terra_utils

RUN pip3 -V \
  && pip3 install --upgrade pip \
  && pip3 install --upgrade -r /etc/terra-docker/requirements.txt

#install azcopy
RUN curl -sSL -O https://packages.microsoft.com/config/ubuntu/20.04/packages-microsoft-prod.deb \ 
 && sudo dpkg -i packages-microsoft-prod.deb \ 
 && rm packages-microsoft-prod.deb \ 
 && sudo apt-get update \ 
 && sudo apt-get install azcopy 

#install gcloud cli
ENV GPG_TTY=$(tty)

RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && apt-get update -y && apt-get install google-cloud-cli -y


ENV USER ops_svc
USER $USER
ENV PIP_USER=true

ENV TF_ENABLE_ONEDNN_OPTS=1

ENTRYPOINT ["/opt/conda/bin/jupyter", "notebook"]