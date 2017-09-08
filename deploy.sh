#!/bin/bash
set -e
git pull

mvn clean install
VERSION=`grep -o -a -m 1 -h -r "version>.*</version" ./pom.xml | head -1 | sed "s/version//g" | sed "s/>//" | sed "s/<\///g"`
echo ""
echo "Deploying hops-kafka-authorizer-${VERSION}.jar to snurran.sics.se"
echo ""

scp target/hops-kafka-authorizer-${VERSION}.jar    glassfish@snurran.sics.se:/var/www/hops
