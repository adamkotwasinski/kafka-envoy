#!/bin/bash

./gradlew fatJar && \
	java -jar build/libs/kafka-envoy-0.0.0.jar