mvn clean install  -Dmaven.test.skip=true
scp target/stock_mr-0.0.1-SNAPSHOT-jar-with-dependencies.jar cdh:~/stock_mr.jar