#!/bin/bash

# https://github.com/GoogleCloudDataproc/spark-bigquery-connector#downloading-the-connector

#   --jars gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
# !pip install folium
create_cluster() {

    echo "Creating cluster...."

    INSTALL=$1
    
    gcloud beta dataproc clusters create cluster-spark-july \
        --project proj-spark \
        --region us-central1 \
        --zone us-central1-f \
        --num-workers 2 \
        --worker-machine-type custom-2-32768-ext \
        --worker-boot-disk-size 500 \
        --master-machine-type n1-standard-4 \
        --master-boot-disk-size 500 \
        --image-version 2.0-ubuntu18 \
        --enable-component-gateway \
        --optional-components JUPYTER,ZEPPELIN \
        --initialization-actions $INSTALL   
}




main() {

    BUCKET=$1

    # upload install.sh to bucket
    INSTALL=gs://$BUCKET/install.sh
    gsutil cp install.sh $INSTALL
    
    # provision Dataproc
    create_cluster $INSTALL

}



if [ "$#" -ne 1 ]; then
        echo "Usage: ./setup_env.sh bucket-name"
        exit
fi

main $1