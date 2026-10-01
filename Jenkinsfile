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
            // Both properties are required - Maven 3.9.11 refuses file-lock on its own with
            // "FileLockNamedLockFactory lock factory requires FS friendly NameMapper" - and
            // they go in MAVEN_ARGS, not MAVEN_OPTS: MAVEN_OPTS is JVM flags, so -D there
            // would not reach the resolver.
            args '-v $HOME/.m2:/var/maven/.m2 -e MAVEN_CONFIG=/var/maven/.m2 -e MAVEN_ARGS="-Daether.syncContext.named.factory=file-lock -Daether.syncContext.named.nameMapper=file-gav" -v /opt/repository:/opt/repository'
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
            // Only master and the release-* branches publish. The coordinate is a single
            // SNAPSHOT file per pom version that this stage overwrites in place, and
            // docker-images pins its checksum - so a manual run of any other branch silently
            // replaces the jar every broker image is built from. That already happened once:
            // build #1 published 5.2.0-SNAPSHOT from a fork branch. Release branches carry
            // their own pom version (master is the Kafka 4.x line; the 3.x line that 5.0 and
            // 5.1 run is a release-* branch), so they publish to their own coordinate.
            //
            // `branch 'master'` alone is not enough: it reads BRANCH_NAME, which only a
            // multibranch pipeline sets. The jenkins.hops.works job is a plain "Pipeline
            // script from SCM", where BRANCH_NAME is null, so that condition is never true
            // and the stage is skipped on every run - a green build that publishes nothing.
            // GIT_BRANCH is what the git plugin sets there (`origin/master`). Both are
            // accepted so the stage keeps working if the job is ever made multibranch.
            when {
                anyOf {
                    branch 'master'
                    branch 'release-*'
                    expression { env.GIT_BRANCH ==~ /origin\/(master|release-.*)/ }
                }
            }
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

                    # docker-images/strimzi-kafka pins AUTHORIZER_SHA256 and fails its build
                    # on a mismatch, so this sum is what the next re-pin uses. The coordinate
                    # is a SNAPSHOT and this cp overwrites it in place. The jar is
                    # reproducible (project.build.outputTimestamp in the pom), so republishing
                    # the same source leaves the sum unchanged; a publish that changes the
                    # source invalidates the pin until someone moves it, and every older
                    # docker-images commit stops building too. A released authorizer version
                    # is the real fix; the branch guard above at least keeps stray branch
                    # builds from doing it.
                    echo "published ${VERSION} to ${DEST}"
                    sha256sum "$DEST/hops-kafka-authorizer-${VERSION}.jar"
                '''
            }
        }
    }
}
