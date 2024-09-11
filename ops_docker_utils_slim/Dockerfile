FROM python:3.11.9


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
COPY stand_alone_terra_and_tdr/ /etc/terra_utils


RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN sudo apt-get update && sudo apt-get install google-cloud-cli -yq

ENV PATH /usr/local/bin:$PATH

RUN sudo pip3 install --upgrade pip\
  && sudo pip3 install --upgrade -r /etc/terra-docker/requirements.txt

#install azcopy
RUN curl -s -L https://azcopyvnext.azureedge.net/releases/release-10.24.0-20240326/azcopy_linux_amd64_10.24.0.tar.gz | tar xvzf - --strip-components=1 -C /usr/local/bin/ azcopy_linux_amd64_10.24.0/azcopy
RUN chown root:root /usr/local/bin/azcopy
RUN chmod +x /usr/local/bin/azcopy
RUN ldd /usr/local/bin/azcopy


ENV AZCOPY_BUFFER_GB: 2
ENV AZCOPY_CONCURRENCY_VALUE: 4



ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

ENV PIP_USER=true
#RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg add - && apt-get update -y && apt-get install google-cloud-cli -y


#install gcloud cli
ENV GPG_TTY=$(tty)
