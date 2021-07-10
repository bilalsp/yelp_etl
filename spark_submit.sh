BUILD_PATH='./build'

# Copy BigQuery Connector to dataproc/lib/
cp .circleci/connectors/spark-bigquery-latest_2.12.jar usr/local/share/google/dataproc/lib/ 

make all

cd ${BUILD_PATH}

${SPARK_HOME}/bin/spark-submit \
    --master yarn \
    --conf spark.logCong=true \
    --driver-java-options '-Dlog4j.configuration=file:log4j.properties -Dvm.logging.level=WARN' \
    --conf "spark.executor.extraJavaOptions=-Dlog4j.configuration=file:log4j.properties -Dvm.logging.level=WARN" \
    --files log4j.properties \
    --py-files src.zip,libs.zip \
    main.py
