#!/usr/bin/env python

from __future__ import annotations
import sys
from argparse import REMAINDER, ArgumentParser
import dataclasses
import ray
import ast
import shlex
import runpy


"""
@dataclasses.dataclass
class RayParams:
    num_cpus: float | None
    num_gpus: float | None
    resources: dict[str, float] | None
"""
    
@ray.remote
def runpy_run(app: str, args: list[str]):
    sys.argv = [app] + [*args]
    runpy.run_path(sys.argv[0], run_name="__main__")


def run_script(params: dict, app: str, args: list[str]):
    task = runpy_run.options(**params).remote(app, args)
    ray.get(task)
    print("Finish task")

"""
def ray_init(config):
    address = config["address"] if "address" in config else 'auto'
    runtime_env = config["runtime_env"] if "runtime_env" in config else None
    ray.init(address=address, runtime_env=runtime_env)
"""

def check_params(params):
    keys = [
        "num_returns", 
        "memory", 
        "object_store_memory", 
        "accelerator_type",
        "max_retries",
        "placement_group",
        "placement_group_bundle_index",
        "placement_group_capture_child_tasks",
        "runtime_env",
        "override_environment_variables",
        "name"
    ]
    for key in keys:
        if params.get(key, None):
            print("momory not supported yet")


def run_jobs(toml_file):
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

        params = {
            "num_cpus": job.get("num_cpus"),
            "num_gpus": job.get("num_gpus"),
            "resources": job.get("resources"),
        }

        check_params(params)

        task = runpy_run.options(**params).remote(app, args)
        jobs.append(task)

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
        "--num_cpus",
        type=int,
        help="Ray option"
    )

    parser.add_argument(
        "--num_gpus",
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


def main():
    parser = get_args_parser()    
    args = parser.parse_args()

    ray.init(**{
        "address": 'auto'
    })

    if ".toml" in args.cmd:
        run_jobs(args.cmd)
    else:
        resources = ast.literal_eval(args.resources) if args.resources else None

        params = {
            "num_cpus": args.num_cpus,
            "num_gpus": args.num_gpus,
            "resources": resources,
        }
        print(f"RayParams: {params}")
        print(f"cmd: {args.cmd}, args: {args.args}")

        run_script(params, args.cmd, args.args)
    


if __name__ == '__main__':
    main()
