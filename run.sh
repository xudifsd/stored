mkdir -p /tmp/stored/1/
mkdir -p /tmp/stored/2/
mkdir -p /tmp/stored/3/

tmux split-window "java -cp target/stored-0.1-jar-with-dependencies.jar org.xudifsd.stored.RaftReactor /tmp/stored/1 0.0.0.0:8001 127.0.0.1:8002 127.0.0.1:8003 ; read"

tmux split-window "java -cp target/stored-0.1-jar-with-dependencies.jar org.xudifsd.stored.RaftReactor /tmp/stored/1 0.0.0.0:8002 127.0.0.1:8001 127.0.0.1:8003 ; read"

tmux split-window "java -cp target/stored-0.1-jar-with-dependencies.jar org.xudifsd.stored.RaftReactor /tmp/stored/1 0.0.0.0:8003 127.0.0.1:8001 127.0.0.1:8002 ; read"
