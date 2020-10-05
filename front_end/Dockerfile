# Created on Oct 05 2020
# @author: gari.ciodaro.guerra
# Docker file to create image of the
# front end application
# arXiv-haystack-app
# sudo docker build -t front-end --build-arg CREDENTIALS="$(cat ~/.aws/credentials)" .
# sudo docker run -it --network=host front-end
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

# pass your aws credentials here
COPY . /home/ubuntu/flask/
RUN mkdir .aws
RUN touch .aws/credentials
ARG CREDENTIALS
RUN echo "$CREDENTIALS" > .aws/credentials

#COPY /home/ubuntu/.gari/credentials /home/ubuntu/.aws/credentials

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
RUN conda create -n front_end python=3.6
# Activate new python environment
SHELL ["conda", "run", "-n", "front_end", "/bin/bash", "-c"]
# install required packages
RUN pip install -r /home/ubuntu/flask/requirements.txt 

# Change ownership of flask folder
RUN sudo chown -R ubuntu /home/ubuntu/flask
RUN sudo chgrp -R ubuntu /home/ubuntu/flask
RUN python /home/ubuntu/flask/app.py

# Expose default flask port.
EXPOSE 5000

