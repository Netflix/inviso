![Inviso Logo](inviso-lg.png)

# Overview
Inviso is a lightweight tool that provides the ability to search for Hadoop jobs, visualize the performance, and view cluster utilization.  

# Design and Components

**REST API for Job History:** REST endpoint to load an entire job history file as a json object.

**ElasticSearch:** Search over jobs and correlate Hadoop jobs for Pig and Hive scripts.

**Python Scripts:** Scripts to index job configurations into ElasticSearch for
querying.  These scripts can accomodate a pub/sub model for use with SQS or some
other queuing service to better distribute the load or allow other systems to know
about job events.

**Web UI:** Provides an interface to serach and visualize jobs and cluster data.


# Requirements

* JDK 1.7+
* [Apache Tomcat (7+)](http://tomcat.apache.org/download-70.cgi)
* [ElasticSearch (1.0+)](http://www.elasticsearch.org/overview/elkdownloads/)
* Hadoop 2 Cluster
  * Log aggregation must be enabled for task log linking to work
  * Specific version of Hadoop may need to set in the gradle build file
  * Some functionality is available for Hadoop 1, but requires more configuration

# QuickStart

Inviso is easy to setup given a Hadoop cluster.  To get a quick preview, it is easiest to
configure Inviso on the NameNode/ResourceManager host.

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
  > python index_cluster_stats.py

  #Run in a cron or loop
  > while true; do sleep 60s; python jes.py; done&
  > while true; do sleep 60s; python index_cluster_stats.py; done&
  ```

7. Navigate to http://hostname:8080/

# QuickStart - Docker Version

An alternate way of starting the inviso project would be via docker.  If you already have docker installed, you can run the following command:

```
docker run -d -p 8080:8080 savaki/inviso
```

This will launch inviso in your container running on port 8080

Enjoy!

