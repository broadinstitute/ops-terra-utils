FROM python:3.11.9


USER root
# This makes it so pip runs as root, not the user.
ENV PIP_USER=false
ENV PIP_ROOT_USER_ACTION=ignore

EXPOSE 8000

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
    gnupg \
	openjdk-17-jdk

# Copy the requirements.txt file and the python folder
COPY requirements.txt /etc/terra-docker/
COPY python/ /etc/terra_utils

# Install gcloud sdk
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo gpg --dearmor -o /usr/share/keyrings/cloud.google.gpg
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN sudo apt-get update && sudo apt-get install google-cloud-cli -yq

ENV PATH /usr/local/bin:$PATH

#Install Requirements
RUN sudo pip3 install --upgrade pip\
  && sudo pip3 install --upgrade -r /etc/terra-docker/requirements.txt

#install azcopy
RUN curl -s -L https://azcopyvnext.azureedge.net/releases/release-10.24.0-20240326/azcopy_linux_amd64_10.24.0.tar.gz | tar xvzf - --strip-components=1 -C /usr/local/bin/ azcopy_linux_amd64_10.24.0/azcopy
RUN chown root:root /usr/local/bin/azcopy
RUN chmod +x /usr/local/bin/azcopy
RUN ldd /usr/local/bin/azcopy

# Azcopy environment variables
ENV AZCOPY_BUFFER_GB: 2
ENV AZCOPY_CONCURRENCY_VALUE: 4

#Cromwell Install
RUN curl -L `curl -s https://api.github.com/repos/broadinstitute/cromwell/releases | grep browser_download_url | head -1 | cut -f 4 -d '"'` > cromwell.tmp.jar \
&& sudo mv cromwell.tmp.jar /usr/local/bin/cromwell.jar \
&& curl -L `curl -s https://api.github.com/repos/broadinstitute/wdltool/releases | grep browser_download_url | head -1 | cut -f 4 -d '"'` > wdltool.tmp.jar \
&& sudo mv wdltool.tmp.jar /usr/local/bin/wdltool.jar

ENV CROMWELL_JAR=/usr/local/bin/cromwell.jar
ENV WDLTOOL_JAR=/usr/local/bin/wdltool.jar
# Set the path for gcloud
ENV PATH $PATH:/usr/local/gcloud/google-cloud-sdk/bin

ENV PIP_USER=true
ENV GPG_TTY=$(tty)

CMD java -jar $CROMWELL_JAR server
