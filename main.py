import argparse

from simple_distribute_job import SimpleDistributeJob
from simple_distribute_job import Params


parser = argparse.ArgumentParser(description="Distribution worker")
parser.add_argument("pc_name", type=str,
                    help='Name of the pc')
parser.add_argument("--verbose", type=int, default=0,
                    help="Level of log detail. -1 : silence, 0: default, 1: detailed, 2: error included")

args = parser.parse_args()

params = Params(config='config.json', pc_name=args.pc_name)

sd_job = SimpleDistributeJob(params)
sd_job.verbose = args.verbose

sd_job.run()
