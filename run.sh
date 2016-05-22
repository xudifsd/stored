mkdir -p /tmp/stored/1/
java -cp target/stored-0.1-jar-with-dependencies.jar org.xudifsd.stored.RaftReactor /tmp/stored/1 0.0.0.0:8001 127.0.0.1:8002 127.0.0.1:8003
