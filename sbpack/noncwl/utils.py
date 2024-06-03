import os
import time
import shutil
import logging
import json
import yaml
import re

from typing import Optional
import fnmatch
from sbpack.pack import pack
from sevenbridges.errors import NotFound
from sbpack.noncwl.constants import (
    PACKAGE_SIZE_LIMIT,
    DEFAULT_EXCLUDE_PATTERNS,
)
from wrabbit.parser.constants import (
    EXTENSIONS,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def zip_and_push_to_sb(
        api, workflow_path, project_id, folder_name,
        exclude_patterns: Optional[list] = None
):
    """
    Create .zip package file. Upload .zip file to the designated folder
    for packages on SevenBridges Platform. Delete local .zip file.
    """
    zip_path = zip_directory(workflow_path, exclude_patterns)
    return push_zip(api, zip_path, project_id, folder_name)


def update_timestamp(file_name):
    return re.sub(
        r"(?:\.|)_\d{8}-\d{6}$", "", file_name
    ) + f'_{time.strftime("%Y%m%d-%H%M%S")}'


def zip_directory(workflow_path, exclude_patterns: Optional[list] = None):
    """
    This will create a temporary directory that will store all files from the
    original directory, except for the .git hidden directory. This dir
    sometimes collects a large amount of files that will not be used by the
    tool, and can increase the size of the archive up to 10 times.
    """
    if not exclude_patterns:
        exclude_patterns = []

    intermediary_dir = update_timestamp(os.path.abspath(workflow_path))
    os.mkdir(intermediary_dir)

    for root, dirs, files in os.walk(workflow_path):
        for d in dirs:
            source_file = os.path.join(root, d)
            directory_path = os.path.join(intermediary_dir, os.path.relpath(
                source_file, workflow_path))

            if any([
                fnmatch.fnmatch(
                    directory_path, os.path.join(intermediary_dir, pattern)
                ) for pattern in exclude_patterns + DEFAULT_EXCLUDE_PATTERNS
            ]):
                continue

            try:
                if not os.path.exists(directory_path):
                    os.mkdir(directory_path)
            except FileNotFoundError:
                """Skip folders that cannot be created"""
                pass

        for file in files:
            source_file = os.path.join(root, file)
            dest_file = os.path.join(intermediary_dir, os.path.relpath(
                source_file, workflow_path))

            if any([
                fnmatch.fnmatch(
                    dest_file, os.path.join(intermediary_dir, pattern)
                ) for pattern in exclude_patterns + DEFAULT_EXCLUDE_PATTERNS
            ]):
                continue

            try:
                shutil.copy2(source_file, dest_file)
            except FileNotFoundError:
                pass

    shutil.make_archive(
        intermediary_dir,
        'zip',
        root_dir=intermediary_dir,
        base_dir='./'
    )

    shutil.rmtree(intermediary_dir)
    print(f'Temporary local folder {intermediary_dir} deleted.')

    return intermediary_dir + '.zip'


def push_zip(api, zip_path, project_id, folder_name=None):
    if os.path.getsize(zip_path) > PACKAGE_SIZE_LIMIT:
        logger.error(f"File size too big: {os.path.getsize(zip_path)}")
        raise FileExistsError  # Add the right error

    folder_id = None
    if folder_name:
        # check if the folder already exists
        folder_found = list(api.files.query(
            project=project_id,
            names=[folder_name],
        ).all())

        if folder_found:
            folder_id = folder_found[0].id
        else:
            # if the folder does not exist, make it
            folder_created = api.files.create_folder(
                project=api.projects.get(project_id),
                name=folder_name
            )
            folder_id = folder_created.id

    print(f'Uploading file {zip_path}, '
          f'please wait for the upload to complete.')

    u = api.files.upload(
        zip_path,
        parent=folder_id,
        project=project_id if not folder_id else None,
        overwrite=False
    )

    uploaded_file_id = u.result().id
    print(f'Upload complete!')

    os.remove(zip_path)
    print(f'Temporary local file {zip_path} deleted.')

    return uploaded_file_id


# Deprecated - used only in WDL
def update_schema_code_package(sb_schema, schema_ext, new_code_package):
    """
    Update the package in the sb_schema
    """

    sb_schema_dict = pack(sb_schema)
    sb_schema_dict['app_content']['code_package'] = new_code_package

    if schema_ext.lower() in EXTENSIONS.json_all:
        with open(sb_schema, 'w') as file:
            json.dump(sb_schema_dict, file)

    elif schema_ext.lower() in EXTENSIONS.yaml_all:
        with open(sb_schema, 'w') as file:
            yaml.dump(sb_schema_dict, file)

    return sb_schema_dict


def install_or_upgrade_app(api, app_id, sb_app_raw):
    try:
        app = api.apps.get(app_id)
        revision = app.revision + 1
        print(f"Creating revised app: {app_id}/{revision}")
        api.apps.create_revision(
            id=app_id,
            raw=sb_app_raw,
            revision=revision
        )
        print(f"App revision created successfully!")

    except NotFound:
        print(f"Creating new app: {app_id}")
        api.apps.install_app(
            id=app_id,
            raw=sb_app_raw
        )
        print(f"App created successfully!")
