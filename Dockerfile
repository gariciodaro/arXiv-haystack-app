#Download base image ubuntu 16.04
FROM ubuntu:16.04
SHELL [ "/bin/bash", "--login", "-c" ]
#SHELL [ "/bin/bash", "--login", "-c" ]


#FROM python:3
#FROM docker.elastic.co/elasticsearch/elasticsearch:7.9.1@sha256:0a5308431aee029636858a6efe07e409fa699b02549a78d7904eb931b8c46920

#WORKDIR /app

RUN apt-get update && yes|apt-get upgrade
RUN apt-get install -y emacs

RUN apt-get install -y wget bzip2


# Add sudo
RUN apt-get -y install sudo

# Add user ubuntu with no password, add to sudo group
RUN adduser --disabled-password --gecos '' ubuntu
RUN adduser ubuntu sudo
RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers
USER ubuntu
WORKDIR /home/ubuntu/
RUN chmod a+rwx /home/ubuntu/
#RUN echo `pwd`

# Anaconda installing



#ENV CONDA_DIR $HOME/miniconda3
RUN wget --quiet https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh 
RUN bash Miniconda3-latest-Linux-x86_64.sh -b
RUN rm Miniconda3-latest-Linux-x86_64.sh
RUN sudo apt install -y git

# Set path to conda
#ENV PATH /root/anaconda3/bin:$PATH
ENV PATH /home/ubuntu/miniconda3/bin:$PATH

#RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh

RUN git clone https://github.com/gariciodaro/arXiv-haystack-app.git
#RUN bash Miniconda3-latest-Linux-x86_64.sh

RUN echo ". /home/ubuntu/miniconda3/etc/profile.d/conda.sh" >> ~/.profile
#RUN sudo docker run -d -p 9200:9200 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elasticsearch

RUN conda init bash
#RUN pip install pandas

RUN pip install -r arXiv-haystack-app/requirements.txt
RUN pip install 'apache-airflow[s3, postgres]'
RUN pip install psycopg2
#COPY ./requirements.txt .

#CMD ["apt-get install -y git"]
#CMD [ "source miniconda3/bin/activate"]
#CMD ["pip install -r arXiv-haystack-app/requirements.txt"]
#CMD [ "pip install 'apache-airflow[s3, postgres]' "]


CMD [ "airflow webserver"]
CMD [ "airflow scheduler"]
CMD [ "cd arXiv-haystack-app/front-end","python app.py"]

EXPOSE 8080
EXPOSE 5000