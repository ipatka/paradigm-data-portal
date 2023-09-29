"""functions for generic file operations"""

from __future__ import annotations

import os
import typing
import tempfile


def download_files(
    urls: typing.Sequence[str],
    *,
    output_dir: str,
    skip_existing: bool = True,
) -> None:
    """download a list of files"""

    # get output dir
    if output_dir is None:
        output_dir = '.'
    output_dir = os.path.abspath(os.path.expanduser(output_dir))

    print('downloading', len(urls), 'files')
    print()
    print('using output_dir', output_dir)

    # skip existing files
    if skip_existing:
        url_filenames = [os.path.basename(url) for url in urls]
        skip_urls = set()
        for url, filename in zip(urls, url_filenames):
            if filename in os.listdir(output_dir):
                skip_urls.add(url)
        if len(skip_urls) > 0:
            print()
            print('skipping', len(skip_urls), 'files that already exist')
    else:
        skip_urls = set()

    # download files
    for url in urls:
        if url not in skip_urls:
            download_file(url)

    print()
    print('done')

def download_files_to_s3(
    urls: typing.Sequence[str],
    *,
    s3_client,
    s3_bucket: str,
    prefix: str,
    skip_existing: bool = True,
) -> None:
    """download a list of files and upload them to S3"""

    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmp_dir:
        print('downloading', len(urls), 'files')
        print()
        print('using tmp_dir', tmp_dir)

        # skip existing files
        if skip_existing:
            response = s3_client.list_objects_v2(Bucket=s3_bucket, Prefix=prefix)
            if 'Contents' in response:
                existing_files = {obj['Key'] for obj in response['Contents']}
                skip_urls = {url for url in urls if os.path.join(prefix, os.path.basename(url)) in existing_files}
            else:
                skip_urls = set()
            if len(skip_urls) > 0:
                print()
                print('skipping', len(skip_urls), 'files that already exist')
        else:
            skip_urls = set()

        # download files
        for url in urls:
            if url not in skip_urls:
                local_path = os.path.join(tmp_dir, os.path.basename(url))
                download_file(url, local_path)
                s3_client.upload_file(local_path, s3_bucket, os.path.join(prefix, os.path.basename(url)))
                os.remove(local_path)  # delete the file after upload

        print()
        print('done')


def download_file(url: str, output_path: str | None = None) -> None:
    """download a file"""
    import subprocess

    print()
    print('downloading', url)
    if output_path is None:
        output_path = os.path.basename(url)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    subprocess.call(['curl', url, '--output', output_path])


def get_file_hash(path: str) -> str:
    """get hash of file"""

    import hashlib

    with open(path, 'rb') as f:
        hashed = hashlib.md5(f.read())

    return hashed.hexdigest()


def get_file_hashes(paths: typing.Sequence[str]) -> typing.Sequence[str]:
    """get hashes of multiple files"""

    return [get_file_hash(path) for path in paths]


def upload_file(local_path: str, bucket_path: str) -> None:
    """upload single file to s3 bucket"""

    import subprocess

    command = [
        'rclone',
        'copyto',
        local_path,
        'paradigm-data-portal:' + bucket_path,
        '-v',
    ]

    subprocess.call(command)


def upload_directory(
    local_path: str,
    bucket_path: str,
    *,
    dir_files: typing.Sequence[str] | None,
    remove_deleted_files: bool = False,
) -> None:
    """upload nested directory of files to s3 bucket"""

    import subprocess

    print('uploading directory:', local_path)
    print('to bucket path:', bucket_path)
    print()

    if remove_deleted_files:
        action = 'sync'
    else:
        action = 'copy'

    command = [
        'rclone',
        action,
        local_path,
        'paradigm-data-portal:' + bucket_path,
        '-v',
    ]

    if dir_files is not None:
        # create tempfile with list of files to upload
        import tempfile

        temp_dir = tempfile.mkdtemp()
        temp_path = os.path.join(temp_dir, 'file_list.txt')
        with open(temp_path, 'w') as f:
            f.write('\n'.join(dir_files))
        command.extend(['--files-from', temp_path])

    else:
        command.extend(['--exclude', '".*"'])

    subprocess.call(command)

