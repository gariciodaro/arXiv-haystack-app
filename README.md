### Still under dev!
# Large scale Q and A on arXiv Kaggle dataset.

This is the final project of my nanodegree on udacity called Data engineering.
For the back-end, Airflow is used for etl orchestration, this includes: 
processing the original .json file of arXiv(stored on S3) on an pypspark cluster (EMR on AWS), saving the star
data model on parquet files again in S3 and then copy those files into redshift. Redshift serves a Flask application
where the user can explore the data sets using queries. 
With [haystack](https://github.com/deepset-ai/haystack) + local elasticsearch server on top to index, read, and answers question of
specific paper of arXiv.

Some of the tables are enriched with papers of the [Neural Information Processing System](https://www.kaggle.com/benhamner/nips-papers). The main
source of information is the [arXiv](https://www.kaggle.com/Cornell-University/arxiv).

<img src="./img/architecture.png">


## Flask + Bootstrap front end.

## Airflow + S3 + EMR + Redshift back-end.


## Usage with docker

+ Run back-end docker container (go to back-end folder) to initialize airflow
webserver on port 8080: 
```
sudo docker build -t back-end .
sudo docker run -it --network=host back-end
```
+ Run elastic search cluster container (go to docker_elastic_search folder) 
on port 9200
```
docker build -t elastic .
sudo docker run -d --network=host -e "discovery.type=single-node" elastic
```
+ Run front-end container (got to front-ent folder) to start flask-app on port 5000. Notice
that we pass our locar aws configuration file to the container:
```
sudo docker build -t front-end --build-arg CREDENTIALS="$(cat ~/.aws/credentials)" .
sudo docker run -it --network=host front-end
```
+ AWS configuration file should look like this:
```
[credentials]
KEY=xxxxxxxxxxxxxx
SECRET=xxxxxxxxxx
REGION=us-west-2
[default]
aws_access_key_id = xxxxxxxxxxxxxxxx
aws_secret_access_key = xxxxxxxxxxxxxxxxx
[DWH] 
DWH_CLUSTER_TYPE=multi-node
DWH_NUM_NODES=2
DWH_NODE_TYPE=dc2.large
DWH_IAM_ROLE_NAME=xxxxxxx
DWH_CLUSTER_IDENTIFIER=xxxxxxxx
DWH_DB=xxxxxxxx
DWH_DB_USER=xxxxxxxxx
DWH_DB_PASSWORD=xxxxxxx
DWH_PORT=5439
DWH_HOST=xxxxxxxxx.xxxxxxxxxxxx.us-west-2.redshift.amazonaws.com
DWH_ROLE=arn:aws:iam::xxxxxxxxxxxxx:role/xxxxxxxxx
[APP]
S3_BUCKED=arxivs3
```


