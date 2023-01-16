# kafka-authorizer
Kafka Authorization Engine for HopsWorks

## Deploying/using authorizer

Create package with:

```sh
mvn clean package
```

Upload jar file:

```sh
scp target/hops-kafka-authorizer-3.2.0-SNAPSHOT.jar dev3_vm:/srv/hops/kafka/libs
```

Change default configuartion (if needed) by editing `/etc/default/kafka` or `/srv/hops/kafka/bin/kafka-run-class.sh`

Update log level (if needed) by editing `/srv/hops/kafka/config/log4j.properties`

Afterwards restart service with:

```sh
systemctl restart kafka.service
```
