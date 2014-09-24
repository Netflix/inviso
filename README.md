![Inviso Logo](inviso-lg.png)

# Overview
Inviso is a lightweight tool that provides the ability to search for Hadoop jobs, visualize the performance, and view cluster utilization.  


# Requirements

* JDK 1.7+
* [Apache Tomcat (7+)](http://tomcat.apache.org/download-70.cgi)
* [ElasticSearch (1.0+)](http://www.elasticsearch.org/overview/elkdownloads/)
* Hadoop 2 Cluster (Hadoop

# QuickStart

Inviso is easy to setup given a Hadoop cluster.

1. Pull down required resources and stage them

  ```bash
  > wget http://<mirror>/.../apache-tomcat-7.0.55.tar.gz
  > tar -xzf apache-tomcat-7.0.55.tar.gz
  > rm -r apache-tomcat-7.0.55/webapps/*
  > wget http://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-1.3.2.tar.gz
  > tar -xzf elasticsearch-1.3.2.tar.gz
  ```
2. Clone the Inviso repository and build the java project

  ```bash
  > git clone https://github.com/Netflix/inviso.git
  > cd inviso
  > ./gradlew assemble
  > cd ..
  ```

3. Copy WAR files and link Static Web Pages

  ```bash
  > cp inviso/trace-mr2/build/libs/inviso#mr2#v0.war apache-tomcat-7.0.55/webapps/
  > ln -s `pwd`/inviso/web-ui/public apache-tomcat-7.0.55/webapps/ROOT
  ```

4. Start ElasticSearch and create Indexes

  ```bash
  > ./elasticsearch-1.3.2/bin/elasticsearch -d
  > curl -XPUT http://localhost:9200/inviso -d @inviso/elasticsearch/mappings/config-settings.json
    {"acknowledged":true}
  > curl -XPUT http://localhost:9200/inviso-cluster -d @inviso/elasticsearch/mappings/cluster-settings.json
    {"acknowledged":true}
  ```

5. Start Tomcat

  ```bash
  > ./apache-tomcat-7.0.55/bin/startup.sh
  ```

6. Build virtual environment and index some jobs

  ```bash
  > virtualenv venv
  > source venv/bin/activate
  > pip install -r inviso/jes/requirements.txt
  > cd inviso/jes/
  > cp settings_default.py settings.py
  > python jes.py
  > while true; do sleep 60s; python index_cluster_stats.py; done&
  ```
