#!/bin/bash
export JAVA_HOME="/usr/lib/jvm/java-8-openjdk-$(uname -m | sed 's/x86_64/amd64/;s/aarch64/arm64/')"
exec "$@"
