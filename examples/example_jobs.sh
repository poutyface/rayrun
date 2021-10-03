#!/usr/bin/env bash
ray stop
ray start --head
python src/rayrun/rayrun.py examples/jobs.toml
