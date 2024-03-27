FROM opensearchstaging/ci-runner:ci-runner-centos7-opensearch-build-v3
USER root
RUN yum install jemalloc lapack-devel blas-devel -y
RUN git clone https://github.com/jmazanec15/k-NN-1.git  --branch micro-benchmarks
WORKDIR ./k-NN-1
ADD setup-lib.sh /
RUN bash /setup-lib.sh
ENV JAVA_HOME="/opt/java/openjdk-21"
RUN ./gradlew --help
ENTRYPOINT [ "./gradlew", "-p", "micro-benchmarks", "run", "--args",  " \"BuildNativeIndexBenchmarks\"" ]