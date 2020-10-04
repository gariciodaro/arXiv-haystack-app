#Download base image ubuntu 16.04
FROM ubuntu:16.04
SHELL [ "/bin/bash", "--login", "-c" ]

RUN apt-get update && yes|apt-get upgrade
RUN apt-get install -y wget
RUN apt-get install gcc python3-dev -y

# Add sudo
RUN apt-get -y install sudo

# Add user ubuntu with no password, add to sudo group
RUN adduser --disabled-password --gecos '' ubuntu
RUN adduser ubuntu sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER ubuntu
WORKDIR /home/ubuntu/
RUN chmod a+rwx /home/ubuntu/

# Anaconda installing
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh 
RUN bash Miniconda3-latest-Linux-x86_64.sh -b
RUN rm Miniconda3-latest-Linux-x86_64.sh

# Set path to conda
ENV PATH /home/ubuntu/miniconda3/bin:$PATH


RUN echo ". /home/ubuntu/miniconda3/etc/profile.d/conda.sh" >> ~/.profile


RUN conda init bash
RUN conda create -n back_end python=3.6
SHELL ["conda", "run", "-n", "back_end", "/bin/bash", "-c"]
RUN pip install paramiko==2.7.2
RUN pip install sshtunnel==0.1.5
RUN pip install 'apache-airflow[s3, postgres]'

COPY . /home/ubuntu/airflow/
RUN sudo chown -R ubuntu /home/ubuntu/airflow
RUN sudo chgrp -R ubuntu /home/ubuntu/airflow
RUN airflow initdb
CMD (airflow scheduler &) && airflow webserver 
EXPOSE 8080

#ENTRYPOINT ["conda", "run", "-n", "back_end", "airflow webserver", "run.py"]
#-p 8080:8080

#airflow/airflow.cfg