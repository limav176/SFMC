import math
import json
import time
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from contextlib import contextmanager
from functools import cached_property
import paramiko
from stat import S_ISREG
from datetime import datetime, timedelta
import pytz
import fnmatch
import yaml
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook


class FTPFileDownloaderOperator(BaseOperator):
    SECRET_CONF = "sftp-cxm"
    CHUNK_SIZE = 32 * 1024 * 1024  # 32 MB

    def __init__(self, config_file, bucket, aws_conn_id='aws_default', *args, **kwargs):
        super().__init__(*args, **kwargs)

        self._config_file = config_file
        self._bucket = bucket
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
    def s3_client(self):
        return AwsBaseHook(aws_conn_id=self._aws_conn_id, client_type='s3').get_client_type(region_name=self.aws_hook.region_name)

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
        transport.packetizer.REKEY_BYTES = pow(2, 40)
        transport.packetizer.REKEY_PACKETS = pow(2, 40)

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

    def upload_part(self, filename, part_number, multipart_upload, chunk):
        part = self.s3_client.upload_part(
            Bucket=self._bucket,
            Key=filename,
            PartNumber=part_number,
            UploadId=multipart_upload["UploadId"],
            Body=BytesIO(chunk)
        )

        return {
            "PartNumber": part_number,
            "ETag": part["ETag"]
        }

    def execute(self, context):
        # Create an SFTP client
        with self.get_transport_client() as sftp:
            sftp.chdir(self.config["sftp_path"])
            formatted_string = self.format_macro(self.config["sftp_file"])

            for entry in sftp.listdir_attr(""):
                filename = entry.filename

                if fnmatch.fnmatch(filename, formatted_string):
                    mode = entry.st_mode

                    if S_ISREG(mode):
                        with sftp.file(filename, mode='rb') as ftp_file:
                            self.log.info(f'Downloading file {filename} from SFTP and uploading to S3 in chunks')

                            # Get file size and calculate chunk count
                            ftp_file.prefetch()
                            ftp_file_size = ftp_file.stat().st_size
                            chunk_count = math.ceil(ftp_file_size / float(self.CHUNK_SIZE))

                            # Start multipart upload in S3
                            multipart_upload = self.s3_client.create_multipart_upload(
                                Bucket=self._bucket,
                                Key=filename
                            )

                            with ThreadPoolExecutor() as executor:
                                futures = []
                                for part_number in range(1, chunk_count + 1):
                                    self.log.info(f'Transferring chunk {part_number} of {chunk_count}')

                                    # Measure time before downloading the chunk
                                    download_start_time = time.time()

                                    # Fetch and read chunk from the FTP file
                                    chunk = ftp_file.read(self.CHUNK_SIZE)

                                    # Measure time after downloading the chunk
                                    download_end_time = time.time()

                                    # Calculate download speed (bytes per second)
                                    download_time = download_end_time - download_start_time
                                    download_speed = len(chunk) / download_time if download_time > 0 else 0

                                    self.log.info(f"Chunk {part_number} download speed: {download_speed / (1024 * 1024):.2f} MB/s")

                                    # Upload the part to S3
                                    futures.append(
                                        executor.submit(
                                            self.upload_part,
                                            filename,
                                            part_number,
                                            multipart_upload,
                                            chunk
                                        )
                                    )

                                parts = [future.result() for future in futures]

                            # Complete multipart upload in S3
                            self.s3_client.complete_multipart_upload(
                                Bucket=self._bucket,
                                Key=filename,
                                UploadId=multipart_upload["UploadId"],
                                MultipartUpload={"Parts": parts}
                            )
                            self.log.info(f'File {filename} uploaded to S3 successfully.')
