import json


class Params:
    STATUS_ERROR = -1
    STATUS_TODO = 0
    STATUS_IN_PROGRESS = 1
    STATUS_DONE = 2

    STAT_COLUMN_NAME = ['job_name', 'status', 'assigned', 's_timestamp', 'e_timestamp']

    def __init__(self, script_path, config='config.json', pc_name='default_pc'):
        with open(config) as f:
            self.cfg = json.load(f)

        self.MAIN_SERVER = self.cfg['server']
        self.USER_NAME = self.cfg['user_name']
        self.PASSWORD = self.cfg['password']

        self.SERVER_PROCESS_PATH = self.cfg['server_default_path']

        self.CURRENT_PC = pc_name

        self.is_out = (len(self.cfg['out_list']) == len(self.cfg['job_list']))

        self.JOB_LIST = []
        for i in range(len(self.cfg['job_list'])):
            prefix = " ".join(self.cfg['run_prefix'])
            postfix = " ".join(self.cfg['run_postfix'])
            if self.is_out:
                postfix += self.cfg['out_list'][i]

            run_cmd = prefix + self.cfg['job_list'][i] + " " + postfix
            self.JOB_LIST.append(run_cmd)

        self.N_ROW = len(self.JOB_LIST)

        self.script_local_path = script_path

    def get_out_path(self, idx):
        local_out_path = self.cfg['out_list'][idx]
        server_out_path = self.cfg['server_upload_path'] + self.cfg['job_list'][idx]

        return local_out_path, server_out_path

