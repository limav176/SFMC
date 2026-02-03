from contextlib import contextmanager
from datetime import datetime, timedelta
from functools import cached_property
import pytz
import yaml
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import paramiko
import fnmatch
import json
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class FTPFileExistsSensor(BaseSensorOperator):
    SECRET_CONF = "sftp-cxm"

    @apply_defaults
    def __init__(self, config_file, aws_conn_id='aws_default', *args, **kwargs):
        super(FTPFileExistsSensor, self).__init__(*args, **kwargs)
        self._config_file = config_file
        self._aws_conn_id = aws_conn_id

    @cached_property
    def config(self):
        with open(self._config_file, 'r') as file:
            config = yaml.safe_load(file)
        return config

    @cached_property
    def aws_hook(self):
        return AwsBaseHook(aws_conn_id=self._aws_conn_id)

    @cached_property
    def secrets_manager_client(self):
        return AwsBaseHook(aws_conn_id=self._aws_conn_id, client_type='secretsmanager').get_client_type(region_name=self.aws_hook.region_name)

    @contextmanager
    def get_transport_client(self):
        paramiko.util.log_to_file("./paramiko.log")

        # Retrieve Secrets
        secret = self.secrets_manager_client.get_secret_value(SecretId=self.SECRET_CONF)
        ftp_connection = json.loads(secret["SecretString"])

        # Create a Transport object
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        transport = paramiko.Transport((ftp_connection["host"], 22))

        # Optimize transport layer
        transport.connect(username=ftp_connection["username"], password=ftp_connection["password"])

        # Connect to a Transport server
        try:
            yield paramiko.SFTPClient.from_transport(transport)
        finally:
            transport.close()

    def format_macro(self, raw_string):
        # Define o fuso horário de São Paulo
        saopaulo_tz = pytz.timezone('America/Sao_Paulo')

        # Obtém a data de hoje no fuso horário de São Paulo
        today = datetime.now(saopaulo_tz).date()
        yesterday = today - timedelta(days=1)

        # Cria o dicionário com as macros usando a data no fuso correto
        macro_dict = {
            'day': str(today.day).zfill(2),
            'month': str(today.month).zfill(2),
            'year': str(today.year),
            'day_m_1': str(yesterday.day).zfill(2),
            'month_m_1': str(yesterday.month).zfill(2),
            'year_m_1': str(yesterday.year)
        }

        formatted_string = raw_string.format_map(macro_dict)
        return formatted_string

    def poke(self, context):
        # Define o fuso horário de São Paulo
        saopaulo_tz = pytz.timezone('America/Sao_Paulo')

        # Obtém a data de hoje no fuso horário de São Paulo
        today = datetime.now(saopaulo_tz).date()

        # Create an SFTP client and check if the file exists
        with self.get_transport_client() as sftp:
            sftp.chdir(self.config["sftp_path"])
            formatted_string = self.format_macro(self.config["sftp_file"])

            # Check if the file exists on the FTP server
            for entry in sftp.listdir_attr(""):
                filename = entry.filename
                file_mod_time = datetime.fromtimestamp(entry.st_mtime, tz=saopaulo_tz).date()

                if fnmatch.fnmatch(filename, formatted_string):
                    if file_mod_time == today:
                        self.log.info(f"File {filename} found and modified today on FTP server.")
                        return True
                    else:
                        self.log.info(
                            f"File {filename} found but not modified today (last modified on {file_mod_time}).")

            self.log.info(f"File matching pattern {formatted_string} not found yet.")
            return False
