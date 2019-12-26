from simple_distribute_job import Params
import time
from datetime import datetime


class JobWatcher:
    def __init__(self, sd_job, update_time=60):
        self.sd_job = sd_job
        self.status = sd_job.get_process_status()
        self.update_time = update_time

    def update_status(self):
        self.status = self.sd_job.get_process_status()

    def get_info(self):
        self.update_status()
        remain_job = self.status[self.status['status'] == Params.STATUS_TODO]
        done_job = self.status[self.status['status'] == Params.STATUS_DONE]
        current_job = self.status[self.status['status'] == Params.STATUS_IN_PROGRESS]

        return current_job, remain_job, done_job

    def run(self):
        while True:
            c_job, r_job, d_job = self.get_info()

            nc_job = c_job.shape[0]
            nr_job = r_job.shape[0]
            nd_job = d_job.shape[0]

            if nr_job == 0:
                print("All work done!")
                break

            server_time = self.sd_job.get_server_time()

            workers = self.status[self.status['assigned'] != 'Unassigned']['assigned'].unique()

            worker_n_jobs = [[(c_job['assigned'] == worker).sum(), (d_job['assigned'] == worker).sum()]
                             for worker in workers]

            worker_s_time = [[c_job['assigned'].values[i], datetime.fromtimestamp(c_job['s_timestamp'].values[i]).strftime("%Y-%m-%d %H:%M:%S")]
                             for i in range(c_job.shape[0])]
            
            print("%s" % server_time)
            print("Currently working %d jobs" % nc_job )
            print("Jobs done : %d" % nd_job )
            print("Remaining jobs : %d" % nr_job)
            print('')
            print(':: Worker list (name : current / done) ::')
            for i in range(len(workers)):
                print("%s : %d / %d" % (workers[i], worker_n_jobs[i][0], worker_n_jobs[i][1]), end='')

                for ws_time in worker_s_time:
                    if workers[i] == ws_time[0]:
                        print(" - Started at %s" % ws_time[1], end='')
                        break
                print('')

            time.sleep(self.update_time)
