#!/usr/bin/env python

import json
import os
import uuid
import boto3


def lambda_handler(event, context):
    try:
        # 이벤트에서 필요한 정보를 추출합니다.
        source_s3 = get_source_s3_info(event)
        destination_s3_bucket, media_convert_role, region = get_environment_variables()

        print(f"Source S3: {source_s3}")
        print(f"MediaConvert Role: {media_convert_role}")
        print(f"Region: {region}")

        # 파일명과 해상도 정보를 추출합니다.
        file_name, width, height, percentage = parse_s3_key(source_s3['key'])
        print(f"File Name: {file_name}")
        print(f"Width: {width}, Height: {height}, Percentage: {percentage}")

        # MediaConvert 클라이언트를 설정합니다.
        mediaconvert_client = create_mediaconvert_client(region)

        # job.json 파일을 읽고 설정을 업데이트합니다.
        job_settings = load_job_settings('job.json', source_s3['uri'])
        update_job_settings(job_settings, width, height,
                            percentage, destination_s3_bucket, file_name)

        # MediaConvert 작업을 생성합니다.
        create_mediaconvert_job(mediaconvert_client,
                                media_convert_role, job_settings)

    except Exception as e:
        print(f'예외 발생: {e}')
        # 필요에 따라 추가적인 예외 처리를 수행할 수 있습니다.


def get_source_s3_info(event):
    """S3 이벤트에서 소스 버킷과 키 정보를 추출합니다."""
    record = event['Records'][0]
    s3_info = record['s3']
    bucket_name = s3_info['bucket']['name']
    object_key = s3_info['object']['key']
    s3_uri = f's3://{bucket_name}/{object_key}'
    return {'bucket': bucket_name, 'key': object_key, 'uri': s3_uri}


def get_environment_variables():
    """환경 변수를 가져옵니다."""
    destination_s3_bucket = os.environ['DestinationS3Bucket']
    media_convert_role = os.environ['MediaConvertRole']
    region = os.environ['AWS_DEFAULT_REGION']
    return destination_s3_bucket, media_convert_role, region


def parse_s3_key(s3_key):
    """S3 키에서 파일명과 해상도 정보를 추출합니다. Ex) test_1280_720_50.mp4"""
    file_name, width, height, percentage_with_ext = s3_key.split('_')[:4]
    percentage = percentage_with_ext.split('.')[0]
    return file_name, width, height, percentage


def create_mediaconvert_client(region):
    """MediaConvert 클라이언트를 생성합니다."""
    mc_client = boto3.client('mediaconvert', region_name=region)
    endpoints = mc_client.describe_endpoints()
    mediaconvert_client = boto3.client(
        'mediaconvert',
        region_name=region,
        endpoint_url=endpoints['Endpoints'][0]['Url'],
        verify=False
    )
    return mediaconvert_client


def load_job_settings(job_settings_path, source_s3_uri):
    """job.json 파일을 로드하고 입력 파일 경로를 설정합니다."""
    with open(job_settings_path) as json_file:
        job_settings = json.load(json_file)["Settings"]
    job_settings['Inputs'][0]['FileInput'] = source_s3_uri
    return job_settings


def update_job_settings(job_settings, width, height, percentage, destination_s3_bucket, file_name):
    """Job 설정을 업데이트합니다."""
    outputs = job_settings['OutputGroups'][0]['Outputs']
    percentages = [None, percentage, None, percentage]

    # 출력마다 비디오 설명을 업데이트합니다.
    for output, pct in zip(outputs, percentages):
        update_video_description(
            output['VideoDescription'], width, height, pct)

    # 출력 파일의 위치를 설정합니다.
    destination_s3 = f's3://{destination_s3_bucket}/{file_name}'
    job_settings['OutputGroups'][0]['OutputGroupSettings']['FileGroupSettings']['Destination'] = destination_s3


def create_mediaconvert_job(client, role, settings):
    """MediaConvert 작업을 생성합니다."""
    asset_id = str(uuid.uuid4())
    job_metadata = {'assetID': asset_id}
    response = client.create_job(
        Role=role,
        UserMetadata=job_metadata,
        Settings=settings
    )

    # 작업 생성에 성공한 경우 로그 출력
    if 'Job' in response:
        print('MediaConvert 작업이 성공적으로 생성되었습니다.')
    else:
        print('MediaConvert 작업 생성에 실패했습니다.')


def update_video_description(video_description, width, height, percentage=None):
    """비디오 설명을 업데이트하여 해상도를 설정합니다."""
    width = int(width)
    height = int(height)

    if percentage is not None:
        percentage = int(percentage)
        width = round(width * percentage / 100)
        height = round(height * percentage / 100)

        # 너비와 높이가 짝수가 되도록 조정합니다.
        width += width % 2
        height += height % 2

    video_description['Width'] = width
    video_description['Height'] = height
