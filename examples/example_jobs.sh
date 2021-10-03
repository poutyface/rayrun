#!/usr/bin/env bash
ray stop
ray start --head
rayrun examples/jobs.toml

ray stop
