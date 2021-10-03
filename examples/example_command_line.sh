#!/usr/bin/env bash
ray stop
ray start --head
rayrun --num_cpus=1 ./examples/sample.py

ray stop