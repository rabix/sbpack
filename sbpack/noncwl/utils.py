import os
import time
import shutil
import logging
import json
import yaml
import re
from sevenbridges.errors import NotFound

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PACKAGE_SIZE_LIMIT = 100 * 1024 * 1024

# A generic SB input array of files that should be available on the
# instance but are not explicitly provided to the execution as wdl params.
GENERIC_FILE_ARRAY_INPUT = {
    "id": "auxiliary_files",
    "type": "File[]?",
    "label": "Auxiliary files",
    "doc": "List of files not added as explicit workflow inputs but "
           "required for workflow execution."
}

GENERIC_OUTPUT_DIRECTORY = {
    "id": "nf_workdir",
    "type": "Directory?",
    "label": "Work Directory",
    "doc": "This is a template output. "
           "Please change glob to directories specified in "
           "publishDir in the workflow.",
    "outputBinding": {
        "glob": "work"
    }
}

# Requirements to be added to sb wrapper
WRAPPER_REQUIREMENTS = [
    {
        "class": "InlineJavascriptRequirement"
    },
    {
        "class": "InitialWorkDirRequirement",
        "listing": [
            "$(inputs.auxiliary_files)"
        ]
    }
]


# Keys that should be skipped when parsing nextflow tower yaml file
SKIP_NEXTFLOW_TOWER_KEYS = [
    'tower',
    'mail',
]


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


def validate_inputs(inputs):
    types = {
        'str': 'string?',
        'file': 'File?',
        'dir': 'Directory?',
        'files': 'File[]?',
        'dirs': 'Directory[]?'
    }
    exit_codes = ['e', 'exit', 'quit', 'q']

    for input_ in inputs:
        if 'string' in input_['type']:
            new_type = input(f'What input type is "{input_["id"]}"?\n')
            while new_type.lower() not in \
                    list(types.keys()) + exit_codes:
                print(
                    f'{new_type} is not a valid input. Please use the '
                    f'following notation:')
                for key, val in types.items():
                    print(f"\t{key}: {val}")
                new_type = input()
            if new_type in exit_codes:
                break

            nt = types[new_type]
            input_['type'] = nt
    return inputs


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

    basename = os.path.basename(os.path.abspath(workflow_path)) + '_' + \
        time.strftime("%Y%m%d-%H%M%S")

    zip_path = os.path.join(os.path.dirname(workflow_path), basename + '.zip')
    shutil.make_archive(zip_path[:-4], 'zip', root_dir=workflow_path,
                        base_dir='./')

    if os.path.getsize(zip_path) > PACKAGE_SIZE_LIMIT:
        logger.error(f"File size too big: {os.path.getsize(zip_path)}")
        raise FileExistsError  # Add the right error

    folder_found = list(api.files.query(
        project=project_id,
        names=[folder_name],
        limit=100,
    ).all())

    if not folder_found:
        folder_created = api.files.create_folder(
            project=api.projects.get(project_id),
            name=folder_name
        )
        folder_id = folder_created.id
    else:
        folder_id = folder_found[0].id

    print(f'Uploading file {zip_path}, '
          f'please wait for the upload to complete.')
    u = api.files.upload(zip_path, parent=folder_id, overwrite=False)

    uploaded_file_id = u.result().id
    print(f'Upload complete!')

    os.remove(zip_path)
    print(f'Local file {zip_path} deleted.')

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

    if possible_paths:
        return possible_paths.pop()
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


def parse_config_file(file_path):
    profiles_text = ""

    with open(file_path, 'r') as file:
        found_profiles = False
        brackets = 0

        for line in file.readlines():
            if found_profiles:
                profiles_text += line
                brackets += line.count("{") - line.count("}")

            if brackets < 0:
                break

            if re.findall(r'profiles\s+\{', line):
                profiles_text += "{\n"
                found_profiles = True

    # Extract profiles using regex
    profiles = {}
    pattern = re.compile(r'^\s*(\w+)\s*{([^}]+)}', re.MULTILINE | re.DOTALL)
    blocks = re.findall(pattern, profiles_text)
    for name, content in blocks:
        settings = dict(re.findall(r'\s*([a-zA-Z.]+)\s*=\s*(.*)', content))
        profiles[name] = settings
        include_path = re.findall(
            r'includeConfig\s+[\'\"]([a-zA-Z_.\\/]+)[\'\"]', content)
        if include_path:
            profiles[name]['includeConfig'] = include_path

    # return currently returns includeConfig and settings, which are not used
    # but could be used in the future versions of sbpack
    return profiles


def update_schema_code_package(sb_schema, schema_ext, new_code_package):
    """
    Update the package in the sb_schema
    """
    if schema_ext.lower() in ['json', 'cwl']:
        with open(sb_schema, 'r') as file:
            sb_schema_json = json.load(file)
        sb_schema_json['app_content']['code_package'] = new_code_package
        with open(sb_schema, 'w') as file:
            json.dump(sb_schema_json, file)

        return sb_schema_json

    elif schema_ext.lower() in ['yaml', 'yml']:
        with open(sb_schema, 'r') as file:
            sb_schema_yaml = yaml.safe_load(file)
        sb_schema_yaml['app_content']['code_package'] = new_code_package
        with open(sb_schema, 'w') as file:
            yaml.dump(sb_schema_yaml, file)

        return sb_schema_yaml


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
