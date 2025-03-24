# AMD64 architecture so that it can install Google Chrome
FROM --platform=linux/amd64 apache/airflow:2.10.5 

ADD IS3107_Requirements.txt .

# Ensure were running as root
USER root

# install wget
RUN apt-get update && apt-get install -y wget

# install google chrome - for webscraping
RUN wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -
RUN sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list'
RUN apt-get -y update
RUN apt-get install -y google-chrome-stable

# install chromedriver - for webscraping
RUN apt-get install -yqq unzip
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/local/bin/

# set display port to avoid crash
ENV DISPLAY=:99

# switch bck to airflow
USER airflow

RUN pip install apache-airflow==2.10.5 -r IS3107_Requirements.txt