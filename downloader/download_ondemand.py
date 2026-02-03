from functools import cached_property

import paramiko
from io import BytesIO
from stat import S_ISREG
import re
import json
from datetime import date, timedelta
import fnmatch

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class FTPFileDownloader:
    SECRET_CONF = "sftp-cxm"

    def __init__(self, aws_conn_id='aws_default'):
        self._aws_conn_id = aws_conn_id

    @cached_property
    def aws_hook(self):
        return AwsBaseHook(aws_conn_id=self._aws_conn_id)

    @cached_property
    def s3_client(self):
        return AwsBaseHook(aws_conn_id=self._aws_conn_id, client_type='s3').get_client_type(region_name=self.aws_hook.region_name)

    @cached_property
    def secrets_manager_client(self):
        return AwsBaseHook(aws_conn_id=self._aws_conn_id, client_type='secretsmanager').get_client_type(region_name=self.aws_hook.region_name)

    def format_macro(self, raw_string):
        yesterday = date.today() - timedelta(days=1)

        macro_dict = {
            'day': str(date.today().day).zfill(2),
            'month': str(date.today().month).zfill(2),
            'year': str(date.today().year),
            'day_m_1': str(yesterday.day).zfill(2),
            'month_m_1': str(yesterday.month).zfill(2),
            'year_m_1': str(yesterday.year)
        }

        formatted_string = raw_string.format_map(macro_dict)

        return formatted_string

    def download(self, sftp_path, sftp_file, bucket_date):
        paramiko.util.log_to_file("./paramiko.log")

        # Retrieve Secrets
        secret = self.secrets_manager_client.get_secret_value(SecretId=self.SECRET_CONF)
        ftp_connection = json.loads(secret["SecretString"])

        # Create a Transport object
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        transport = paramiko.Transport((ftp_connection["host"], 22))

        # Connect to a Transport server
        transport.connect(username=ftp_connection["username"], password=ftp_connection["password"])

        # Create an SFTP client
        with paramiko.SFTPClient.from_transport(transport) as sftp:
            sftp.chdir(sftp_path)
            formatted_string = self.format_macro(sftp_file)

            for entry in sftp.listdir_attr(""):
                if fnmatch.fnmatch(entry.filename, formatted_string):

                    mode = entry.st_mode

                    if S_ISREG(mode):
                        f = entry.filename
                        subfolder = re.split(r"[_.]", f)[1]

                        with BytesIO() as data:
                            

                            sftp.getfo(f, data)
                            data.seek(0)
                            if bucket_date == 0:
                                print(f'Downloading file {f} from SFTP to ftp-will-salesforce-prod/{subfolder}/')
                                self.s3_client.upload_fileobj(
                                    data,
                                    'ftp-will-salesforce-prod',
                                    f'{subfolder}/{f}'
                                )
                            else:
                                print(f'Downloading file {f} from SFTP to data-raw-zone-will-prod/salesforce/{subfolder}/{bucket_date}/')
                                self.s3_client.upload_fileobj(
                                    data,
                                    'data-raw-zone-will-prod',
                                    f'salesforce/{subfolder}/{bucket_date}/{f}'
                                )

        transport.close()
