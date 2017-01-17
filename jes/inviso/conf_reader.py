#!/usr/bin/python
import ConfigParser
def getConfElement(section,element):
	config = ConfigParser.ConfigParser()
	fd = config.read("./conf/inviso.conf")
	if(len(fd)==0):
		fd = config.read("../conf/inviso.conf")
	return config.get(section,element)

