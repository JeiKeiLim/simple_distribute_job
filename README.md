# Simple Distributed Job Scheduler
- Simple distributed job scheduler for multiple servers with only SSH. No additions.

<img src="https://github.com/JeiKeiLim/mygifcontainer/raw/master/simple_distribute_job/demo01.gif" width=480 height=300/>

# How to use
**USAGE**

	main.py [-h] [--verbose VERBOSE] [--config CONFIG] [--mode MODE]
	               [--wt WT] [--max_job MAX_JOB]
	               pc_name
	positional arguments:
	  pc_name            Name of the pc

**Optional arguments**

	  -h, --help         show this help message and exit
	  --verbose VERBOSE  Level of log detail. -1 : silence, 0: default, 1:
	                     detailed, 2: error included
	  --config CONFIG    Config json file path. Default : config.json
      --mode MODE        init : initialize the job states, worker : process job,
                         watcher : watch job processes, clean : clean agent's job
                         status, reset : reset agent's job status
	  --wt WT            Update time(second) for watcher mode. Default : 60
	  --max_job MAX_JOB  Maximum job number to be done in this machine. Default :
	                     -1 (No maximum job limit)

## Example
- **Run initialization**

	python main.py agent_name --config config.json --mode init

- **Run worker agent**

	python main.py agent_name --config config.json

- **Run watcher mode**
	python main.py agent_name --config config.json --mode watcher

<img src="https://github.com/JeiKeiLim/mygifcontainer/raw/master/simple_distribute_job/demo02.gif" width=480 height=246/>



# config.json
You can customize tasks to run and upload the result to the main server via config.json file

	{
	  "server": "serverdomain.com",
	  "user_name": "username",
	  "password": "password",
	  "server_default_path": "/job/scheduling/management/file/will/be/uploaded/to/here",
	  "server_upload_path": "/output/of/your/job/will/be/uploaded/here/",
	  "run_prefix": [
	    "python",
	    "~/script/run_script.py"
	  ],
	  "job_list": ["job_a.txt",
	              "job_b.txt",
	              "job_c.txt",
	              "job_d.txt",
	              "job_e.txt",
	              "job_f.txt"],
	  "run_postfix": [
	    "--type text",
	    "--parse json",
	    "--out ",
	  ],
	  "out_list": ["~/script/out/job_a_done.txt",
	              "~/script/out/job_b_done.txt",
	              "~/script/out/job_c_done.txt",
	              "~/script/out/job_d_done.txt",
	              "~/script/out/job_e_done.txt",]
	}

**Example**

The command of the job will be run_prefix + job_list[i] + run_postfix + out_list[i]

In the example, its running commands will be as below. 

	python ~/script/run_script.py job_a.txt --type text --parse json --out ~/script/out/job_a_done.txt
	python ~/script/run_script.py job_b.txt --type text --parse json --out ~/script/out/job_b_done.txt
	python ~/script/run_script.py job_c.txt --type text --parse json --out ~/script/out/job_c_done.txt
	python ~/script/run_script.py job_d.txt --type text --parse json --out ~/script/out/job_d_done.txt
	python ~/script/run_script.py job_e.txt --type text --parse json --out ~/script/out/job_e_done.txt

And the output files are uploaded to the "server_upload_path"

