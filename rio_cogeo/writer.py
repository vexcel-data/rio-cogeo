import os
import boto3
from rasterio.shutil import copy


SCHEMES = {
    's3': {
        's3:': 2,
        'vsis3': 2,
        's3.amazonaws.com': 3
    }
}

AWS_ACCESS_KEY_ID: 'str' = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_ACCESS_KEY: 'str' = os.environ['AWS_SECRET_ACCESS_KEY']


def write_to_dst(mem, memfile, dst_path, dst_kwargs):
    for k, v in SCHEMES['s3'].items():
        if k in dst_path.split('/')[:v]:
            file_key = '/'.join(dst_path.split('/')[v + 1:])
            bucket_name = dst_path.split('/')[v]

            session = boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY
            )
            s3 = session.client('s3')
            s3.upload_fileobj(memfile, bucket_name, file_key)
            break
    else:
        copy(mem, dst_path, copy_src_overviews=True, **dst_kwargs)
