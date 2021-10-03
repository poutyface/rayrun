#!/usr/bin/env bash
ray stop
ray start --head
rayrun --num-cpus=1 ./examples/sample.py

ray stop