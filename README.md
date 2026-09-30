# kafka-authorizer
Kafka Authorization Engine for HopsWorks

## Building authorizer

Create package with:

```sh
mvn clean install
```

## Publishing, and what it obliges you to do next

The Jenkins job publishes to `repo.hops.works` under the SNAPSHOT coordinate
`hops-kafka-authorizer/<version>-SNAPSHOT`, overwriting whatever was there. Only `master`
publishes; a run of any other branch builds and tests but does not upload, because that path
is what every broker image resolves.

The broker image is built in
[`docker-images/strimzi-kafka`](https://github.com/logicalclocks/docker-images/tree/master/strimzi-kafka)
— `FROM quay.io/strimzi/kafka` plus this jar. It pins the jar by checksum, so **publishing a
new jar breaks that build until two files there are updated in the same change**:

- `AUTHORIZER_SHA256` — the new jar's `sha256sum`, otherwise the image build fails the
  checksum check.
- `HOPSWORKS_VERSION` — the `-h<n>` component of the image tag, because a different jar is a
  different image and the registry does not enforce tag immutability.

Then bump `cluster.kafka.image.tag` in `charts/kafka/values.yaml` in `hopsworks-helm`
to the new `-h<n>`.

The jar is not reproducible (no `project.build.outputTimestamp`), so the checksum changes on
every publish even when the source does not. The real fix is publishing immutable releases
rather than SNAPSHOTs; until then the pin has to be moved by hand.
