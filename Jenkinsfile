// Builds the authorizer jar and publishes it to repo.hops.works.
//
// The build runs inside a container rather than on the agent, and that is the point: the
// Jenkins agent is launched with /usr/lib/jvm/java-8-openjdk-amd64, and a Java 8 javac
// cannot read the kafka-clients 4.x class files this project compiles against. On the
// agent the build fails with
//
//     bad class file: .../kafka-clients-4.3.0.jar(org/apache/kafka/common/Endpoint.class)
//       class file has wrong version 55.0, should be 52.0
//
// The image supplies its own JDK, so the toolchain no longer depends on what happens to be
// installed on the host. clusterj-onlinefs builds against Kafka 4.3.1 the same way.
//
// Keeping this in the repo rather than in job config means the build, the version it
// publishes and the JDK it uses all move together with the source.
pipeline {
    agent {
        docker {
            // Temurin 21 builds a project that targets 17. Match clusterj-onlinefs so both
            // Kafka 4.x builds share one toolchain.
            image 'maven:3.9.11-eclipse-temurin-21'
            // The .m2 mount keeps downloads across builds. /opt/repository is the directory
            // repo.hops.works/master serves, and the publish stage writes into it, so it has
            // to be visible from inside the container.
            // file-lock sync matches clusterj-onlinefs: $HOME/.m2 is shared between jobs on
            // this agent, and Maven's default sync is not safe against a concurrent writer.
            args '-v $HOME/.m2:/var/maven/.m2 -e MAVEN_CONFIG=/var/maven/.m2 -e MAVEN_OPTS=-Daether.syncContext.named.factory=file-lock -v /opt/repository:/opt/repository'
        }
    }

    stages {
        stage('build') {
            steps {
                sh 'mvn -Duser.home=/var/maven -U clean package'
            }
        }

        // A separate stage, not a post-success step: the pipeline stops here if the build
        // failed, so a compile error can no longer be followed by a "cp: cannot stat" that
        // buries the real cause.
        stage('publish') {
            steps {
                sh '''
                    set -eu
                    # Read the version from the pom rather than an injected variable. The
                    # freestyle job this replaced carried a stale POM_VERSION and published
                    # under 1.4.1-SNAPSHOT while the pom said 5.2.0-SNAPSHOT.
                    # Plugin version pinned: bare `help:evaluate` resolves plugin metadata
                    # from Maven Central on every run, which is subject to its rate limiting.
                    VERSION=$(mvn -Duser.home=/var/maven -q -DforceStdout \
                        org.apache.maven.plugins:maven-help-plugin:3.5.1:evaluate -Dexpression=project.version)
                    JAR="target/hops-kafka-authorizer-${VERSION}.jar"
                    DEST="/opt/repository/master/hops-kafka-authorizer/${VERSION}"

                    test -f "$JAR"
                    mkdir -p "$DEST"
                    cp "$JAR" "$DEST/"

                    # Printed for traceability, not for a pin: docker-images/strimzi-kafka
                    # fetches this jar by version with no checksum. Because the coordinate is
                    # a SNAPSHOT, this cp overwrites whatever was there, so a rebuilt broker
                    # image can pick up a different jar under the same tag. This line is the
                    # record of which bytes a given run published.
                    echo "published ${VERSION} to ${DEST}"
                    sha256sum "$DEST/hops-kafka-authorizer-${VERSION}.jar"
                '''
            }
        }
    }
}
