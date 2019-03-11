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

            session = boto3.Session(
                aws_access_key_id=getenv()['AWS_ACCESS_KEY_ID'],
                aws_secret_access_key=getenv()['AWS_SECRET_ACCESS_KEY'],
                aws_session_token=getenv().get('AWS_SESSION_TOKEN'),
                region_name=getenv().get('AWS_REGION_NAME'),
                profile_name=getenv().get('AWS_PROFILE_NAME')
            )
            s3 = session.client('s3')
            s3.upload_fileobj(memfile, bucket_name, file_key)
            break
    else:
        copy(mem, dst_path, copy_src_overviews=True, **dst_kwargs)
