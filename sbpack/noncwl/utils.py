import os
import time
import shutil
import logging
import json
import yaml
import re

from sbpack.pack import pack
from sevenbridges.errors import NotFound
from sbpack.noncwl.constants import (
    PACKAGE_SIZE_LIMIT,
    EXTENSIONS,
    NF_TO_CWL_PORT_MAP,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def nf_schema_type_mapper(input_type_string):
    """
    Convert nextflow schema input type to CWL
    """
    type_ = input_type_string.get('type', 'string')
    format_ = input_type_string.get('format', '')

    return type_mapper(type_, format_)


def nf_to_sb_input_mapper(port_id, port_data, category=None, required=False):
    """
    Convert a single input from Nextflow schema to SB schema
    """
    sb_input = dict()
    sb_input['id'] = port_id
    sb_input['type'] = nf_schema_type_mapper(port_data)
    sb_input['inputBinding'] = {
        'prefix': f'--{port_id}',
    }

    if not required:
        sb_input['type'].append('null')

    if category:
        sb_input['sbg:category'] = category

    for nf_field, sb_field in NF_TO_CWL_PORT_MAP.items():
        if nf_field in port_data:
            value = port_data[nf_field]
            if value == ":" and nf_field == 'default':
                # Bug prevents running a task if an input's
                #  default value is exactly ":". This bug will likely be
                #  fixed at the time of release of this version.
                value = " :"
            sb_input[sb_field] = value

    return sb_input


def type_mapper(type_, format_):
    if isinstance(type_, str):
        if type_ == 'string' and 'path' in format_:
            if format_ == 'file-path':
                return ['File']
            if format_ == 'directory-path':
                return ['Directory']
            if format_ == 'path':
                return ['File']
        if type_ == 'string':
            return ['string']
        if type_ == 'integer':
            return ['int']
        if type_ == 'number':
            return ['float']
        if type_ == 'boolean':
            return ['boolean']
        if type_ == 'object':
            # this should be a record type (dictionary)
            # it is provided as '{"key1": "value1", "key2": "value2"}'
            return ['string']
        return [type_]
    elif isinstance(type_, list):
        temp_type_list = []
        for m in type_:
            temp_type_list.extend(type_mapper(m, format_))
        return temp_type_list


def create_profile_enum(profiles: list):
    """
    If profiles are defined in the config file, this input stores the profiles
    They are added to the commandline as -profile foo,bar,foobar
    :param profiles: list of profiles
    :return: Profiles enum array input
    """
    return {
        "id": "profile",
        "type": [
            "null",
            {
                "type": "array",
                "items": {
                    "type": "enum",
                    "name": "profile",
                    "symbols": profiles
                }
            }
        ],
        "label": "Profiles",
        "doc": "Select which profile(s) you want to use for task execution.",
        "inputBinding": {
            "prefix": "-profile",
            "itemSeparator": ",",
            "shellQuote": False,
        }
    }


def get_dict_depth(dict_, level=0):
    """
    Find the depth of the dictionary. Example:
    {'a': 1} - returns 0;
    {'a': {'b': 2}} - returns 1...

    :param dict_: input dictionary
    :param level: depth of the outer dict
    :return: int
    """
    n = level
    for k, v in dict_.items():
        if type(v) is dict:
            lv = get_dict_depth(v, level + 1)
            if lv > n:
                n = lv
    return n


def zip_and_push_to_sb(api, workflow_path, project_id, folder_name):
    """
    Create .zip package file. Upload .zip file to the designated folder
    for packages on SevenBridges Platform. Delete local .zip file.
    """
    zip_path = zip_directory(workflow_path)
    return push_zip(api, zip_path, project_id, folder_name)


def update_timestamp(file_name):
    return re.sub(
        r"(?:\.|)_\d{8}-\d{6}$", "", file_name
    ) + f'_{time.strftime("%Y%m%d-%H%M%S")}'


def zip_directory(workflow_path):
    """
    This will create a temporary directory that will store all files from the
    original directory, except for the .git hidden directory. This dir
    sometimes collects a large amount of files that will not be used by the
    tool, and can increase the size of the archive up to 10 times.
    """

    intermediary_dir = update_timestamp(os.path.abspath(workflow_path))
    os.mkdir(intermediary_dir)

    for root, dirs, files in os.walk(workflow_path):
        pattern = re.compile(r'(?:^|.*/)\.git(?:$|/.*)')
        if re.match(pattern, root):
            continue

        dirs = [d for d in dirs if not re.match(pattern, d)]
        for d in dirs:
            source_file = os.path.join(root, d)
            directory_path = os.path.join(intermediary_dir, os.path.relpath(
                source_file, workflow_path))
            if not os.path.exists(directory_path):
                os.mkdir(directory_path)

        for file in files:
            source_file = os.path.join(root, file)
            dest_file = os.path.join(intermediary_dir, os.path.relpath(
                source_file, workflow_path))
            shutil.copy2(source_file, dest_file)

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


def get_readme(path):
    """
    Find readme file is there is one in the path folder
    """
    for file in os.listdir(path):
        if file.lower() == 'readme.md':
            return os.path.join(path, file)
    return None


def get_tower_yml(path):
    """
    Find tower.yml file is there is one in the path folder
    """
    for file in os.listdir(path):
        if file.lower() == 'tower.yml':
            return os.path.join(path, file)
    return None


def get_entrypoint(path):
    """
    Auto find main.nf or similar file is there is one in the path folder.
    """
    possible_paths = []
    for file in os.listdir(path):
        if file.lower() == 'main.nf':
            return file

        if file.lower().endswith('.nf'):
            possible_paths.append(file)

    if len(possible_paths) > 1:
        raise Exception(
            'Detected more than 1 nextflow file in the root of the '
            'workflow-path. Please use `--entrypoint` to specify which script '
            'you want to use as the workflow entrypoint')
    elif len(possible_paths) == 1:
        return possible_paths.pop()
    else:
        return None


def get_config_files(path):
    """
    Auto find config files.
    """
    paths = []
    for file in os.listdir(path):
        if file.lower().endswith('.config'):
            paths.append(os.path.join(path, file))
    return paths or None


def find_config_section(file_path: str, section: str) -> str:
    section_text = ""
    found_section = False
    brackets = 0

    with open(file_path, 'r') as file:
        for line in file.readlines():
            if found_section:
                section_text += line
                brackets += line.count("{") - line.count("}")

            if brackets < 0:
                break

            if re.findall(section + r'\s+\{', line):
                section_text += "{\n"
                found_section = True

    return section_text


def parse_config_file(file_path: str) -> dict:
    profiles_text = find_config_section(file_path, 'profiles')

    # Extract profiles using regex
    profiles = {}
    block_pattern = re.compile(
        r'\s*(\w+)\s*{([^}]+)}', re.MULTILINE | re.DOTALL
    )
    key_val_pattern = re.compile(
        r'([a-zA-Z._]+)(?:\s+|)=(?:\s+|)([^\s]+)'
    )
    include_pattern = re.compile(
        r'includeConfig\s+[\'\"]([a-zA-Z_.\\/]+)[\'\"]'
    )

    blocks = re.findall(block_pattern, profiles_text)
    for name, content in blocks:
        settings = dict(re.findall(key_val_pattern, content))
        profiles[name] = settings
        include_path = re.findall(include_pattern, content)
        if include_path:
            profiles[name]['includeConfig'] = include_path
            include_path = include_path.pop()
            additional_path = os.path.join(
                os.path.dirname(file_path), include_path)
            params_text = find_config_section(additional_path, 'params')
            params = dict(re.findall(key_val_pattern, params_text))
            for param, val in params.items():
                profiles[name][f"params.{param}"] = val

    # return currently returns includeConfig and settings, which are not used
    # but could be used in the future versions of sbpack
    return profiles


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
