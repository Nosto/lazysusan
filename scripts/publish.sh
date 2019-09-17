#! /bin/bash

export NEXUS_USERNAME=$(aws ssm --region us-east-1 get-parameter --name us-east-1_external_jenkins_nexuswrite_username --with-decryption | jq -r ."Parameter"."Value")
export NEXUS_PASSWORD=$(aws ssm --region us-east-1 get-parameter --name us-east-1_external_jenkins_nexuswrite_password --with-decryption | jq -r ."Parameter"."Value")

./gradlew clean build publish -PnexusUsername=$NEXUS_USERNAME -PnexusPassword=$NEXUS_PASSWORD
if $? -eq 0; then
  project_version=`./gradlew properties -q | grep "version:" | awk '{print $2}'`
fi
