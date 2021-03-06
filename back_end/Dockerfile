# Created on Oct 05 2020
# @author: gari.ciodaro.guerra
# Docker file to create image of the
# back end application
# arXiv-haystack-app

# Create image with this file:
#     -sudo docker build -t back-end .
#     -sudo docker run -it --network=host back-end
# Enter bash of container
#      -sudo docker exec -it 4fe054a1fe05 /bin/bash

# pass your 
# sudo docker cp keypairspark.pem 4fe054a1fe05:/home/ubuntu/keypairspark.pem


#Download base image ubuntu 16.04
FROM ubuntu:16.04
#Change shell to bash
SHELL [ "/bin/bash", "--login", "-c" ]

#install system requirements
RUN apt-get update && yes|apt-get upgrade
RUN apt-get install -y wget
RUN apt-get install gcc python3-dev -y

# Add sudo uswe
RUN apt-get -y install sudo

# Add user ubuntu with no password, add to sudo group
RUN adduser --disabled-password --gecos '' ubuntu
RUN adduser ubuntu sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER ubuntu
WORKDIR /home/ubuntu/
RUN chmod a+rwx /home/ubuntu/

# Miniconda installing
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh 
RUN bash Miniconda3-latest-Linux-x86_64.sh -b
RUN rm Miniconda3-latest-Linux-x86_64.sh

# Set path to conda
ENV PATH /home/ubuntu/miniconda3/bin:$PATH
RUN echo ". /home/ubuntu/miniconda3/etc/profile.d/conda.sh" >> ~/.profile

# Initialize conda
RUN conda init bash
# Create new environment
RUN conda create -n back_end python=3.6
# Activate new python environment
SHELL ["conda", "run", "-n", "back_end", "/bin/bash", "-c"]
# install required packages
RUN pip install paramiko==2.7.2
RUN pip install sshtunnel==0.1.5
# Install airflow with s3 and postgres support.
RUN pip install 'apache-airflow[s3, postgres]==1.10.12'

# Copy dags and plugins from back end directory
COPY . /home/ubuntu/airflow/

# Change ownership of airflow folder
RUN sudo chown -R ubuntu /home/ubuntu/airflow
RUN sudo chgrp -R ubuntu /home/ubuntu/airflow

# Initialize airflow data base
RUN airflow initdb

#Start airflow web app and scheduler
CMD (airflow scheduler &) && airflow webserver 

# Expose default airflow port.
EXPOSE 8080
