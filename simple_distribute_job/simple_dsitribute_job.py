import pysftp
import time
import numpy as np
import pandas as pd
import logging
import hashlib
import os
import subprocess
from simple_distribute_job.params import Params


class SimpleDistributeJob:
    DID_I_LOCK_SERVER = False

    def __init__(self, params, max_job=-1):
        self.params = params
        self.sftp = pysftp.Connection(self.params.MAIN_SERVER,
                                      username=self.params.USER_NAME, password=self.params.PASSWORD)
        self.total_n_job = params.N_ROW
        self.remain_n_job = params.N_ROW
        self.done_n_job = 0
        self.verbose = 0
        self.max_job = max_job

    def lock_server(self):

        if not SimpleDistributeJob.DID_I_LOCK_SERVER and self.is_server_locked():
            self.wait_server(keep_lock=False)

        i = 0
        while True:
            i += 1
            try:
                f = self.sftp.open(self.params.SERVER_PROCESS_PATH + 'lock', 'w')
                f.close()
                break
            except:
                self.log("Locking server failed! try again ...(%d)" % i, v_level=1)
                self.wait_server(keep_lock=False)

        SimpleDistributeJob.DID_I_LOCK_SERVER = True

    def unlock_server(self):
        if SimpleDistributeJob.DID_I_LOCK_SERVER:
            self.sftp.execute('rm ' + self.params.SERVER_PROCESS_PATH + 'lock')

        SimpleDistributeJob.DID_I_LOCK_SERVER = False

    def is_server_locked(self):
        return self.sftp.exists(self.params.SERVER_PROCESS_PATH + 'lock')

    def wait_server(self, keep_lock=True, min_wait=1.0, max_wait=5.0):
        i = 0
        while self.is_server_locked():
            i += 1

            self.log("Server is locked waiting ... (%d)" % i, v_level=1)

            time.sleep((np.random.rand() * (max_wait - min_wait)) + min_wait)

        if keep_lock:
            self.lock_server()

        return i

    def get_server_time(self, prefix="[", postfix="]"):
        time = self.sftp.execute('date +"%Y.%m.%d %T.%6N"')
        time = str(time[0])[2:-3]

        return prefix + time + postfix

    def get_process_status(self):
        i = 0
        while True:
            i += 1
            error_msg = "Load status file error try again ... (%d)" % i
            try:
                with self.sftp.open(self.params.SERVER_PROCESS_PATH + 'process_status.csv.gzip') as f:
                    status = pd.read_csv(f, names=Params.STAT_COLUMN_NAME, compression='gzip')

                if status.shape[0] == self.params.N_ROW:
                    break
                self.log(error_msg + " :: Row number not matched", v_level=1)
            except Exception:
                if self.verbose >= 2:
                    logging.exception(self.get_server_time() + " " + error_msg)
                else:
                    self.log(self.get_server_time() + " " + error_msg, v_level=1)

            time.sleep(np.random.rand()*4+1)

        self.remain_n_job = np.sum(status['status'] != Params.STATUS_TODO)
        return status

    # WARNING do not call this method until absolutely sure it is needed.
    def init_progress(self):
        # WARNING do not call this method until absolutely sure it is needed.

        init_status = [[job_cmd, Params.STATUS_TODO, 'Unassigned', 0, 0] for job_cmd in self.params.JOB_LIST]
        init_status = pd.DataFrame(init_status, columns=Params.STAT_COLUMN_NAME)
        self.upload_pandas(init_status, self.params.SERVER_PROCESS_PATH + 'process_status.csv')

    def upload_pandas(self, data, path):
        i = 0
        path += '.gzip'
        status_path = hashlib.md5(str(time.time()).encode()).hexdigest() + '.gzip'
        status_path = self.params.script_local_path + '/intermediate/%s' % status_path
        data.to_csv(status_path, index=False, header=False, compression='gzip')
        while True:
            i += 1
            try:
                self.sftp.put(status_path, path)
                break
            except IOError:
                if self.verbose >= 2:
                    logging.exception(self.get_server_time() + " Writing %s file error! try again ... (%d)" % (path, i))
                else:
                    self.log(self.get_server_time() + " Writing %s file error! try again ... (%d)" % (path, i), v_level=1)

                time.sleep(np.random.rand()*4+1)

        os.remove(status_path)

    def update_status(self, state, idx, status=None, force=False):
        if status is None:
            status = self.get_process_status()

        if status.loc[idx, 'assigned'] == 'Unassigned':
            status.loc[idx, 'assigned'] = self.params.CURRENT_PC
        elif status.loc[idx, 'assigned'] != self.params.CURRENT_PC:
            status.loc[idx, 'assigned'] += "+" + self.params.CURRENT_PC
            if not force:
                status.loc[idx, 'status'] = Params.STATUS_ERROR

        if status.loc[idx, 'status'] >= 0 or force:
            status.loc[idx, 'status'] = state

            if state == Params.STATUS_IN_PROGRESS:
                status.loc[idx, 's_timestamp'] = time.time()
            elif state == Params.STATUS_DONE:
                status.loc[idx, 'e_timestamp'] = time.time()

        self.upload_pandas(status, self.params.SERVER_PROCESS_PATH + 'process_status.csv')

        updated_state = status.loc[idx, 'status']

        return status, updated_state

    def get_rest_video_number(self):
        status = self.get_process_status()
        return np.sum(status['status'] == 0)

    def get_job_idx(self, random_job=True):
        status = self.get_process_status()

        available_jobs = np.argwhere(status['status'].values == Params.STATUS_TODO)[:, 0]
        if available_jobs.shape[0] == 0:
            return -1, None, None
        else:
            if random_job:
                np.random.shuffle(available_jobs)

            job_idx = available_jobs[0]

            status, updated_state = self.update_status(Params.STATUS_IN_PROGRESS, job_idx, status=status)
            return job_idx, status, updated_state

    def log(self, msg, v_level=0, sep=" ", end="\n", print_time=True):
        if self.verbose >= v_level:
            if print_time:
                msg = self.get_server_time() + " " + msg

            print(msg, sep=sep, end=end)

    def log_progress(self, idx, prefix=""):
        self.log(prefix + 'Processing %03dth job (%d/%d) - (%d job done here)' %
                 (idx, self.remain_n_job, self.total_n_job, self.done_n_job), v_level=0)

        self.log(" ....... Job command : %s" % self.params.JOB_LIST[idx], v_level=3)

    def upload_result(self, idx, delete=False):
        local_path, server_path = self.params.get_out_path(idx)

        if self.verbose >= 2:
            upload_callback = SimpleDistributeJob.upload_callback
        else:
            upload_callback = None

        while True:
            try:
                self.sftp.put(local_path, server_path, confirm=True, callback=upload_callback)
                break
            except OSError:
                self.log("Upload failed! try again ...", v_level=2)
                time.sleep(np.random.randint(1, 5))

        if delete:
            os.remove(local_path)

    @staticmethod
    def get_data_size_format(size):
        if size > 1024*1024:
            size /= 1024/1024
            postfix = "MB"
        elif size > 1024:
            size /= 1024
            postfix = "kB"
        else:
            postfix = "B"

        return size, postfix

    @staticmethod
    def upload_callback(sent_bytes, total_bytes):
        sent, sent_postfix = SimpleDistributeJob.get_data_size_format(sent_bytes)
        total, total_postfix = SimpleDistributeJob.get_data_size_format(total_bytes)

        msg = "..... %d%s/%d%s has sent(%.2f%%)" % (sent, sent_postfix,
                                                      total, total_postfix,
                                                      (sent/total)*100)
        print(msg)

    def do_the_job(self, idx):
        self.log_progress(idx, prefix="Start ")

        subprocess.run(self.params.JOB_LIST[idx], shell=True)

        self.wait_server()
        _, updated_state = self.update_status(Params.STATUS_DONE, idx, force=True)
        self.unlock_server()

        if self.params.is_out:
            self.upload_result(idx, delete=True)

        self.done_n_job += 1
        self.log_progress(idx, prefix="done ")

    def run(self):
        self.done_n_job = 0
        while True:
            try:
                time.sleep(np.random.rand() * 5)

                self.wait_server()
                job_idx, current_status, state_written = self.get_job_idx()
                self.unlock_server()

                if job_idx == -1:
                    self.log("All work done and %s did %d jobs!" % (self.params.CURRENT_PC, self.done_n_job), v_level=-1)
                    break

                if state_written != Params.STATUS_IN_PROGRESS:
                    self.log("Something went wrong ... (job_idx/state)(%d/%d)" % (job_idx, state_written), v_level=2)

                self.do_the_job(job_idx)

                if self.max_job > 0:
                    if self.done_n_job >= self.max_job:
                        self.log("My job here is done. (%d jobs done)" % self.done_n_job)
                        break

            except Exception:
                if self.verbose >= 2:
                    logging.exception(self.get_server_time() + " Exception from %s" % self.params.CURRENT_PC)

                if SimpleDistributeJob.DID_I_LOCK_SERVER:
                    self.unlock_server()

        self.close()

    def close(self):
        self.sftp.close()
