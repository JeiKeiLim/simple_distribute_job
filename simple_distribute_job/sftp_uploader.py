import time


class SftpUploader:

    TIME_LOG = 10
    SHOW_PERCENTAGE_STEP = 25

    def __init__(self, sftp, local_path, server_path):
        self.sftp = sftp
        self.local_path = local_path
        self.server_path = server_path
        self.last_call_back_time = time.time()
        self.last_percentage = 0

    def upload(self, verbose):
        try:
            if verbose >= 1:
                self.sftp.put(self.local_path, self.server_path, confirm=True,
                              callback=lambda a, b: self.upload_callback(a, b))
            else:
                self.sftp.put(self.local_path, self.server_path, confirm=True,
                              callback=None)
        except:
            return False

        return True

    @staticmethod
    def get_data_size_format(size):
        if size > 1024*1024:
            size /= (1024*1024)
            postfix = "MB"
        elif size > 1024:
            size /= 1024
            postfix = "kB"
        else:
            postfix = "B"

        return size, postfix

    def upload_callback(self, sent_bytes, total_bytes):
        t = time.time() - self.last_call_back_time

        print_log = False

        if t > SftpUploader.TIME_LOG:
            print_log = True
            self.last_call_back_time = time.time()

        percentage = (sent_bytes/total_bytes) * 100

        if percentage > (self.last_percentage+SftpUploader.SHOW_PERCENTAGE_STEP):
            print_log = True
            self.last_percentage = int(percentage / SftpUploader.SHOW_PERCENTAGE_STEP) * SftpUploader.SHOW_PERCENTAGE_STEP

        if print_log:
            sent, sent_postfix = SftpUploader.get_data_size_format(sent_bytes)
            total, total_postfix = SftpUploader.get_data_size_format(total_bytes)

            msg = "..... %d%s/%d%s has sent(%.2f%%)" % (sent, sent_postfix,
                                                          total, total_postfix,
                                                          percentage)
            print(msg)
