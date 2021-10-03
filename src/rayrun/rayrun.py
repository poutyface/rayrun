#!/usr/bin/env python

from __future__ import annotations
import sys
from argparse import REMAINDER, ArgumentParser
import dataclasses
import ray
from ray.util.placement_group import placement_group
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


def run_script(options: dict, app: str, args: list[str]):
    task = runpy_run.options(**options).remote(app, args)
    ray.get(task)
    print("Finish task")

"""
def ray_init(config):
    address = config["address"] if "address" in config else 'auto'
    runtime_env = config["runtime_env"] if "runtime_env" in config else None
    ray.init(address=address, runtime_env=runtime_env)
"""

def check_options(options):
    keys = [
        "num_returns", 
        "memory", 
        "object_store_memory", 
        "accelerator_type",
        "max_retries",
        "placement_group_capture_child_tasks",
        "runtime_env",
        "override_environment_variables",
        "name"
    ]
    for key in keys:
        if options.get(key, None):
            print("momory not supported yet")



def run_jobs(toml_file):
    import tomlkit
    from pathlib import Path

    cfg = tomlkit.loads(Path(toml_file).read_text())
    print(cfg)
    
    pg = None
    if cfg.get("placement_group", None):
        group = cfg.get("placement_group")
        # because of tomlkit type is Container as dict
        groups = []
        for g in group:
            groups.append(g.value)

        pg = placement_group(groups, strategy="SPREAD")
        ray.get(pg.ready())


    jobs = []
    for job in cfg["job"]:
        print(shlex.split(job["cmd"]))
        scripts = shlex.split(job["cmd"])
        app = scripts[0]
        args = scripts[1:]

        options = {}
        if job.get("options", None):
            options = job.get("options")
            options = {
                "num_cpus": options.get("num_cpus", None),
                "num_gpus": options.get("num_gpus", None),
                "resources": options.get("resources", None),
                "placement_group": pg if options.get("placement_group_bundle_index", None) else 'default',
                "placement_group_bundle_index": options.get("placement_group_bundle_index", -1)
            }

        check_options(options)

        task = runpy_run.options(**options).remote(app, args)
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

        options = {
            "num_cpus": args.num_cpus,
            "num_gpus": args.num_gpus,
            "resources": resources,
        }
        print(f"remote options: {options}")
        print(f"cmd: {args.cmd}, args: {args.args}")

        run_script(options, args.cmd, args.args)
    


if __name__ == '__main__':
    main()
