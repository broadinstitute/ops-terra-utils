FROM us-central1-docker.pkg.dev/operations-portal-427515/ops-toolbox/ops_terra_utils_slim:latest


USER root
# This makes it so pip runs as root, not the user.
ENV PIP_USER=false
ENV PIP_ROOT_USER_ACTION=ignore

RUN apt-get update 
RUN apt-get install -yq --no-install-recommends \
  openjdk-17-jdk


EXPOSE 8000

#Cromwell Install
RUN curl -L `curl -s https://api.github.com/repos/broadinstitute/cromwell/releases | grep browser_download_url | head -1 | cut -f 4 -d '"'` > cromwell.tmp.jar \
&& sudo mv cromwell.tmp.jar /usr/local/bin/cromwell.jar \
&& curl -L `curl -s https://api.github.com/repos/broadinstitute/wdltool/releases | grep browser_download_url | head -1 | cut -f 4 -d '"'` > wdltool.tmp.jar \
&& sudo mv wdltool.tmp.jar /usr/local/bin/wdltool.jar

ENV CROMWELL_JAR=/usr/local/bin/cromwell.jar
ENV WDLTOOL_JAR=/usr/local/bin/wdltool.jar

ENV PIP_USER=true

CMD java -jar $CROMWELL_JAR server
