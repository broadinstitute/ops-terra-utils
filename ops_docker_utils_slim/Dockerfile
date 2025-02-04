FROM python:3.11-bookworm


USER root
# This makes it so pip runs as root, not the user.
ENV PIP_USER=false
ENV PIP_ROOT_USER_ACTION=ignore

# install build dependencies and needed tools
RUN apt-get update
RUN apt-get install -yq --no-install-recommends \
    wget \
    curl \
    gcc \
    g++ \
    python3 \
    python3-pip \
    sudo \
    make \
    dpkg \
    apt-transport-https \
    which \
    ca-certificates \
    gnupg

COPY requirements.txt /etc/terra-docker/
COPY python/ /etc/terra_utils/python
COPY wdl/ /etc/terra_utils/wdl
COPY general_markdown/ /etc/terra_utils/general_markdown
COPY .dockstore.yml /etc/terra_utils/


RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg && apt-get update -y && apt-get install google-cloud-cli -y

ENV PATH /usr/local/bin:$PATH

RUN sudo pip3 install --upgrade pip\
  && sudo pip3 install --upgrade -r /etc/terra-docker/requirements.txt

RUN --mount=type=ssh,id=default pip3 install git+ssh://git@github.com/broadinstitute/ops_util_module.git@main

#install azcopy
RUN curl -sSL -O https://packages.microsoft.com/config/debian/12/packages-microsoft-prod.deb  && \
    dpkg -i packages-microsoft-prod.deb && \
    rm packages-microsoft-prod.deb && \
    apt-get update && \
    apt-get install -y azcopy

ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

ENV PIP_USER=true

#install gcloud cli
ENV GPG_TTY=$(tty)

CMD [ "/bin/sh" ]