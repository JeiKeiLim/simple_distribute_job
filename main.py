import argparse

from simple_distribute_job import SimpleDistributeJob, Params, JobWatcher
import os

parser = argparse.ArgumentParser(description="Distribution worker")
parser.add_argument("pc_name", type=str,
                    help='Name of the pc')
parser.add_argument("--verbose", type=int, default=0,
                    help="Level of log detail. -1 : silence, 0: default, 1: detailed, 2: error included")
parser.add_argument('--config', type=str, default=None,
                    help="Config json file path. Default : config.json")
parser.add_argument('--mode', type=str, default="worker",
                    help="worker : process job, watcher : watch job processes")

args = parser.parse_args()

script_path = os.path.dirname(os.path.abspath(__file__))
if args.config is None:
    args.config = script_path + '/config.json'

params = Params(script_path, config=args.config, pc_name=args.pc_name)

sd_job = SimpleDistributeJob(params)
sd_job.verbose = args.verbose

if args.mode == "worker":
    sd_job.run()
else:
    watcher = JobWatcher(sd_job)
    watcher.run()
