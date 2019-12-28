from simple_distribute_job import Params
import time
from datetime import datetime


class JobWatcher:
    def __init__(self, sd_job, update_time=60.0):
        self.sd_job = sd_job
        self.update_time = update_time

        self.status = None
        self.r_job = None
        self.d_job = None
        self.c_job = None
        self.update_status()

    def update_status(self):
        self.status = self.sd_job.get_process_status()
        self.r_job = self.status[self.status['status'] == Params.STATUS_TODO]
        self.d_job = self.status[self.status['status'] == Params.STATUS_DONE]
        self.c_job = self.status[self.status['status'] == Params.STATUS_IN_PROGRESS]

    def get_remain_time(self, update_status=False):
        if update_status:
            self.update_status()

        if self.d_job.shape[0] == 0 or self.c_job.shape[0] == 0:
            return -1, -1, -1, -1

        d_job_c = self.d_job.copy()
        d_job_c = d_job_c.query("s_timestamp != 0 & e_timestamp != 0")

        # Process time for jobs to be done
        d_job_c['p_time'] = (d_job_c['e_timestamp'] - d_job_c['s_timestamp'])
        c_job_e = self.c_job.copy()

        for worker in d_job_c['assigned'].unique():
            w_p_time = d_job_c[d_job_c['assigned'] == worker]['p_time']
            w_p_time = w_p_time.mean()

            # Estimated time for the current machine by mean time of the machine
            c_job_e.loc[c_job_e['assigned'] == worker, 'p_time'] = w_p_time

        # Fill nan with mean time
        c_job_e['p_time'] = c_job_e['p_time'].fillna(c_job_e['p_time'].mean())

        # Estimated time - (current time - start time)
        current_job_estimated_time = (c_job_e['p_time'] - (time.time() - c_job_e['s_timestamp'])).sum() / c_job_e.shape[0]
        # Mean estimated time * number of remain jobs / number of current workers
        remain_job_estimated_time = (c_job_e['p_time'].mean() * self.r_job.shape[0]) / c_job_e['assigned'].unique().shape[0]
        total_estimated_time = current_job_estimated_time + remain_job_estimated_time

        estimated_day = int(total_estimated_time / (60 * 60 * 24))
        estimated_hour = int(total_estimated_time / (60 * 60)) % 24
        estimated_min = int(total_estimated_time / 60) % 60
        estimated_sec = int(total_estimated_time) % 60

        return estimated_day, estimated_hour, estimated_min, estimated_sec

    def print_each_state(self):
        print("▢ : to do, ☑ : in progress, ■ : done, ☒ : error")
        for stat, i in zip(self.status['status'], range(self.status.shape[0])):
            if stat == Params.STATUS_TODO:
                print("▢", end='', sep='')
            elif stat == Params.STATUS_IN_PROGRESS:
                print("☑", end='', sep='')
            elif stat == Params.STATUS_DONE:
                print("■", end='', sep='')
            else:
                print("☒", end='', sep='')

            if (i+1) % 50 == 0:
                print('')

        print('')

    def print_progress_bar(self, max_n=40):
        progress = ((self.status['status'] == Params.STATUS_DONE).sum() / self.status.shape[0])
        pn = max_n * progress

        print('[', sep='', end='')

        for i in range(max_n):
            if i < pn:
                print('#', sep='', end='')
            else:
                print('.', sep='', end='')

        print('] %.2f%%' % (progress*100))

    def print_worker_list(self, detail=False):
        workers = self.status[self.status['assigned'] != 'Unassigned']['assigned'].unique()
        worker_n_jobs = [[(self.c_job['assigned'] == worker).sum(), (self.d_job['assigned'] == worker).sum()]
                         for worker in workers]
        worker_s_time = [[self.c_job['assigned'].values[i],
                          datetime.fromtimestamp(self.c_job['s_timestamp'].values[i]).strftime("%Y-%m-%d %H:%M:%S")]
                         for i in range(self.c_job.shape[0])]

        print(':: Worker list - (current/done) ::')
        for i in range(len(workers)):

            print("(%d) : %s (%d/%d), " % (i, workers[i], worker_n_jobs[i][0], worker_n_jobs[i][1]), end='')

            if not detail and (i+1) % 2 == 0:
                print('')

            if detail:
                for ws_time in worker_s_time:
                    if workers[i] == ws_time[0]:
                        print("Started at %s" % ws_time[1])
                        break

        if not detail and len(workers) > 0 and (i+1) % 2 == 1:
            print('')

    def run(self):
        while True:
            self.update_status()

            nc_job = self.c_job.shape[0]
            nr_job = self.r_job.shape[0]
            nd_job = self.d_job.shape[0]

            if nr_job == 0:
                print("All work done!")
                break

            server_time = self.sd_job.get_server_time()

            print("\n%s" % server_time)
            print("-- Current/Done/Remain/Total Jobs : %d/%d/%d/%d" % (nc_job, nd_job, nr_job, self.status.shape[0]))

            print('')
            self.print_worker_list(detail=False)

            d, h, m, s = self.get_remain_time()
            print('')
            print("Estimated time to be finished : %d Days, %02d:%02d:%02d" % (d, h, m, s))

            self.print_progress_bar()
            self.print_each_state()

            time.sleep(self.update_time)

        self.sd_job.close()
