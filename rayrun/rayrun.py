#!/usr/bin/env python

from __future__ import annotations
import sys
from argparse import REMAINDER, ArgumentParser
import dataclasses
import ray
import ast
import shlex
import runpy



@dataclasses.dataclass
class RayConfig:
    num_cpus: float | None
    num_gpus: float | None
    resources: dict[str, float] | None
    

@ray.remote
def runpy_run(app: str, args: list[str]):
    sys.argv = [app] + [*args]
    runpy.run_path(sys.argv[0], run_name="__main__")


def toml_run(toml_file):
    import tomlkit
    from pathlib import Path

    cfg = tomlkit.loads(Path(toml_file).read_text())
    print(cfg)
    jobs = []
    for job in cfg["job"]:
        print(shlex.split(job["cmd"]))
        scripts = shlex.split(job["cmd"])
        app = scripts[0]
        args = scripts[1:]
        ref = runpy_run.options(
            num_cpus=job["cpus"] if "cpus" in job else None,
            num_gpus=job["gpus"] if "gpus" in job else None,
            resources=job["resources"] if "resources" in job else None
        ).remote(app, args)

        jobs.append(ref)

    ray.get(jobs)
        

"""
@ray.remote
def cmd_run(self, app: str, args: list[str]):
    import subprocess
    cmd = [app] + [*args]
    cmd = " ".join(cmd)
    o = subprocess.check_output(cmd, shell=True, stderr=subprocess.STDOUT, encoding='utf8')
    print(o)
"""
        

def get_args_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Ray resource launcher")

    parser.add_argument(
        "--num-cpus",
        type=int,
        help="Ray option"
    )

    parser.add_argument(
        "--num-gpus",
        type=int,
        help="Ray option"
    )

    parser.add_argument(
        "--resources",
        type=str,
        help="e.g. --resources='{\"res1\":1, \"res2\":2}'"
    )
    
    """
    parser.add_argument(
        "--no_python",
        action='store_true',
        help="when the script is not a Python script"
    )
    """
    
    parser.add_argument(
        "cmd",
        type=str,
        help="Full path or config TOML to your script to be launched on Ray"
    )

    parser.add_argument(
        "args",
        nargs=REMAINDER,
        help="your script args"
    )

    return parser


def run_script(config: RayConfig, app: str, args: list[str]):
    task = runpy_run.options(
        num_cpus=config.num_cpus,
        num_gpus=config.num_gpus,
        resources=config.resources
    ).remote(app, args)

    ray.get(task)
    print("Finish task")


def main():
    parser = get_args_parser()    
    args = parser.parse_args()

    if ".toml" in args.cmd:
        toml_run(args.cmd)
    else:
        resources = ast.literal_eval(args.resources) if args.resources else None

        config = RayConfig(
            num_cpus=args.num_cpus,
            num_gpus=args.num_gpus,
            resources=resources,
        )
        print(f"RayConfig: {config}")
        print(f"cmd: {args.cmd}, args: {args.args}")

        run_script(config, args.cmd, args.args)
    


if __name__ == '__main__':
    main()
