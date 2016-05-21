all: thrift javac

thrift:
	for i in `find src/main/thrift/ -type f -regex ".*\.thrift"` ; do thrift --gen java -out src/main/java/ $$i ; done

javac:
	mvn assembly:assembly -DdescriptorId=jar-with-dependencies -Dmaven.test.skip=true
