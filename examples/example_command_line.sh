#!/usr/bin/env bash
ray stop
ray start --head
python src/rayrun/rayrun.py --num-cpus=1 ./examples/sample.py