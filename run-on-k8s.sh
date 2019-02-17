spark-submit --class de.haw.tweetspace.Recommender \
             --master k8s://http://127.0.0.1:8001  \
             --deploy-mode cluster             \
             --conf spark.kubernetes.container.image=falcowinkler/spark-example:latest\
             --executor-memory 2G              \
             --num-executors 3             \
             --conf spark.kubernetes.namespace=aca534 \
             /opt/spark/jars/spark-example-0.1.0-SNAPSHOT-standalone.jar
