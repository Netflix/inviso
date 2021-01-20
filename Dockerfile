FROM dockerfile/java:oracle-java7
MAINTAINER Matt Ho <matt.ho@gmail.com>

# install required dependencies
#
RUN apt-get update && apt-get install -y wget curl git python-pip supervisor
RUN pip install gradle ; \
	pip install virtualenv

# installation will take place in /opt
#
ENV TARGET /opt

# unfortunately, docker doesn't support string interpolation in WORKDIR
#
WORKDIR /opt

# install tomcat
#
ENV TOMCAT_VERSION 7.0.56
RUN wget -qq https://archive.apache.org/dist/tomcat/tomcat-7/v${TOMCAT_VERSION}/bin/apache-tomcat-${TOMCAT_VERSION}.tar.gz ; \
	tar -xzvf apache-tomcat-${TOMCAT_VERSION}.tar.gz ; \
	rm -r apache-tomcat-${TOMCAT_VERSION}/webapps/* ; \
	rm apache-tomcat-${TOMCAT_VERSION}.tar.gz ; \
	ln -s apache-tomcat-${TOMCAT_VERSION} /opt/apache-tomcat 

# install elastic search
#
ENV ELASTICSEARCH_VERSION 1.3.2
RUN wget -qq http://download.elasticsearch.org/elasticsearch/elasticsearch/elasticsearch-${ELASTICSEARCH_VERSION}.tar.gz ; \
	tar -xzvf elasticsearch-${ELASTICSEARCH_VERSION}.tar.gz ; \
	rm elasticsearch-${ELASTICSEARCH_VERSION}.tar.gz

# INVISO_COMMIT_HASH allows us to specify the exact version of inviso this container will build
#
ENV INVISO_COMMIT_HASH a29450ace4a9f467d34f29b6f69094b69edbff1b

# clone the inviso repo and build the project
#
RUN git clone https://github.com/Netflix/inviso.git ; \
	(cd inviso; git reset --hard ${INVISO_COMMIT_HASH} ; ./gradlew assemble) 
RUN cp "${TARGET}/inviso/trace-mr2/build/libs/inviso#mr2#v0.war" ${TARGET}/apache-tomcat-${TOMCAT_VERSION}/webapps/ ; \
	ln -s ${TARGET}/inviso/web-ui/public ${TARGET}/apache-tomcat-${TOMCAT_VERSION}/webapps/ROOT

# configure elasticsearch to point to inviso
#
# NOTE: for some reason the -p flag inside a docker build doesn't write out the pidfile :\
#
RUN ${TARGET}/elasticsearch-${ELASTICSEARCH_VERSION}/bin/elasticsearch -d ; \
	curl -XPUT http://localhost:9200/inviso -d @${TARGET}/inviso/elasticsearch/mappings/config-settings.json ; \
	curl -XPUT http://localhost:9200/inviso-cluster -d @${TARGET}/inviso/elasticsearch/mappings/cluster-settings.json ; \
	kill -9 `ps auxww | grep java | grep -v grep | awk '{print $2}'`

ENV SERVICE_CONF /etc/supervisor/conf.d/services.conf
RUN echo '[program:elasticsearch]' >> ${SERVICE_CONF} ; \
	echo "command=${TARGET}/elasticsearch-${ELASTICSEARCH_VERSION}/bin/elasticsearch" >> ${SERVICE_CONF} ; \
	echo '' >> ${SERVICE_CONF} ; \
	echo '[program:tomcat]' >> ${SERVICE_CONF} ; \
	echo "command=${TARGET}/apache-tomcat-${TOMCAT_VERSION}/bin/catalina.sh run" >> ${SERVICE_CONF} ; \
	echo '' >> ${SERVICE_CONF}

# additional scripting for the boot sequence
EXPOSE 8080

CMD [ "/usr/bin/supervisord", "--nodaemon" ]


