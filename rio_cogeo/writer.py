import os
import boto3
from rasterio.env import getenv
from rasterio.shutil import copy


SCHEMES = {
    's3': {
        's3:': 2,
        'vsis3': 2,
        's3.amazonaws.com': 3
    }
}


def write_to_dst(mem, memfile, dst_path, dst_kwargs):
    for k, v in SCHEMES['s3'].items():
        if k in dst_path.split('/')[:v]:
            file_key = '/'.join(dst_path.split('/')[v + 1:])
            bucket_name = dst_path.split('/')[v]

            AWS_ACCESS_KEY_ID: 'str' = os.environ.get(
                'AWS_ACCESS_KEY_ID_OUT',
                getenv()['AWS_ACCESS_KEY_ID']
            )
            AWS_SECRET_ACCESS_KEY: 'str' = os.environ.get(
                'AWS_SECRET_ACCESS_KEY_OUT',
                getenv()['AWS_SECRET_ACCESS_KEY']
            )
            AWS_SESSION_TOKEN: 'str' = os.environ.get('AWS_SESSION_TOKEN_OUT')
            AWS_REGION_NAME: 'str' = os.environ.get('AWS_DEFAULT_REGION_OUT')
            AWS_PROFILE_NAME: 'str' = os.environ.get('AWS_DEFAULT_PROFILE_OUT')
            session = boto3.Session(
                aws_access_key_id=AWS_ACCESS_KEY_ID,
                aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
                aws_session_token=AWS_SESSION_TOKEN,
                region_name=AWS_REGION_NAME,
                profile_name=AWS_PROFILE_NAME
            )
            s3 = session.client('s3')
            s3.upload_fileobj(memfile, bucket_name, file_key)
            break
    else:
        copy(mem, dst_path, copy_src_overviews=True, **dst_kwargs)
