import re
import json
import argparse
import logging
import os
import yaml

import sbpack.lib as lib

from nf_core.schema import PipelineSchema
from sbpack.version import __version__
from sbpack.noncwl.utils import (
    get_dict_depth,
    zip_and_push_to_sb,
    get_readme,
    get_tower_yml,
    get_entrypoint,
    get_config_files,
    parse_config_file,
    create_profile_enum,
    update_schema_code_package,
    install_or_upgrade_app,
    nf_to_sb_input_mapper,
)
from sbpack.noncwl.constants import (
    ExecMode,
    GENERIC_FILE_ARRAY_INPUT,
    GENERIC_NF_OUTPUT_DIRECTORY,
    WRAPPER_REQUIREMENTS,
    SKIP_NEXTFLOW_TOWER_KEYS,
    EXTENSIONS,
    NF_TO_CWL_CATEGORY_MAP,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

NF_SCHEMA_DEFAULT_NAME = 'nextflow_schema.json'


class SBNextflowWrapper:
    def __init__(self, workflow_path, dump_schema=False, sb_doc=None):
        self.sb_wrapper = dict()
        self.nf_ps = PipelineSchema()
        self.sb_package_id = None
        self.workflow_path = workflow_path
        self.dump_schema = dump_schema
        self.nf_schema_path = None
        self.nf_config_files = None
        self.sb_doc = sb_doc
        self.executor_version = None
        self.execution_mode = None

    def nf_schema_build(self):
        """
        Build nextflow schema using nf_core schema build feature and save to
        a file
        """
        nf_schema_path = os.path.join(
            self.workflow_path,
            NF_SCHEMA_DEFAULT_NAME,
        )

        # if the file doesn't exist, nf-core raises exception and logs
        logging.getLogger("nf_core.schema").setLevel("CRITICAL")

        self.nf_ps.schema_filename = nf_schema_path
        self.nf_ps.build_schema(
            pipeline_dir=self.workflow_path,
            no_prompts=True,
            web_only=False,
            url='',
        )
        return nf_schema_path

    @staticmethod
    def file_is_nf_schema(path: str) -> bool:
        """
        Validation if the provided file is an NF schema file
        """
        try:
            schema = yaml.safe_load(path)
            if 'definitions' not in schema:
                return False
            if type(schema['definitions']) is not dict:
                return False
            for value in schema['definitions'].values():
                if 'properties' not in value:
                    return False
            else:
                return True
        except Exception as e:
            logger.info(f"File {path} is not an nf schema file (due to {e})")
            return False

    def generate_sb_inputs(self):
        """
        Generate SB inputs schema
        """
        cwl_inputs = list()

        # ## Add profiles to the input ## #
        self.nf_config_files = get_config_files(self.workflow_path)

        profiles = dict()

        for path in self.nf_config_files:
            profiles.update(parse_config_file(path))

        profiles_choices = sorted(list(set(profiles.keys())))

        if profiles:
            cwl_inputs.append(create_profile_enum(profiles_choices))

        # Optional inputs due to profiles
        # optional_inputs = []
        # for profile_id, profile_contents in profiles.items():
        #     for key in profile_contents.keys():
        #         if 'params.' in key:
        #             input_ = key.rsplit('params.', 0)
        #             optional_inputs.extend(input_)
        # optional_inputs = set(optional_inputs)

        # ## Add inputs ## #
        if self.nf_schema_path:
            with open(self.nf_schema_path, 'r') as f:
                nf_schema = yaml.safe_load(f)

            for p_key, p_value in nf_schema.get('properties', {}).items():
                cwl_inputs.append(
                    nf_to_sb_input_mapper(p_key, p_value))
            for def_name, definition in nf_schema.get(
                    'definitions', {}).items():
                # Nextflow inputs schema contains multiple definitions where
                # each definition contains multiple properties
                category = dict()

                for nf_field, sb_field in NF_TO_CWL_CATEGORY_MAP.items():
                    if nf_field in definition:
                        category[sb_field] = definition[nf_field]

                input_category = 'Inputs'
                if 'title' in definition:
                    input_category = category['sbg:title']

                for port_id, port_data in definition['properties'].items():
                    req = False
                    # if port_id in definition.get('required', []) and \
                    #         port_id not in optional_inputs:
                    #     req = True

                    cwl_inputs.append(nf_to_sb_input_mapper(
                        port_id,
                        port_data,
                        category=input_category,
                        required=req,
                    ))

        # Add the generic file array input - auxiliary files
        cwl_inputs.append(GENERIC_FILE_ARRAY_INPUT)

        input_ids = set()
        for inp in cwl_inputs:
            base_id = inp['id']
            id_ = base_id
            i = 1
            while id_ in input_ids:
                id_ = f'{base_id}_{i}'
                i += 1

            input_ids.add(id_)
            inp['id'] = id_

        return cwl_inputs

    def generate_sb_outputs(self):
        """
        Generate SB output schema
        """
        output_ids = set()
        cwl_outputs = list()

        if get_tower_yml(self.workflow_path):
            cwl_outputs.extend(
                self.parse_output_yml(
                    open(get_tower_yml(self.workflow_path)))
            )

        # if the only output is reports, or there are no outputs, add generic
        if len(cwl_outputs) == 0 or \
                (len(cwl_outputs) == 1 and cwl_outputs[0]['id'] == 'reports'):
            cwl_outputs.append(GENERIC_NF_OUTPUT_DIRECTORY)

        for output in cwl_outputs:
            base_id = output['id']
            id_ = base_id
            i = 1
            while id_ in output_ids:
                id_ = f'{base_id}_{i}'
                i += 1

            output_ids.add(id_)
            output['id'] = id_

        return cwl_outputs

    def make_output_type(self, key, output_dict, is_record=False):
        """
        This creates an output of specific type based on information provided
        through output_dict.

        :param key:
        :param output_dict:
        :param is_record:
        :return:
        """

        converted_cwl_output = dict()

        file_pattern = re.compile(r'.*\.(\w+)$')
        folder_pattern = re.compile(r'[^.]+$')
        id_key = 'id'

        if is_record:
            id_key = 'name'

        name = key
        if 'display' in output_dict:
            name = output_dict['display']

        clean_id = re.sub(r'[^a-zA-Z0-9_]', "", name.replace(
            " ", "_")).lower()

        # Case 1: Output is a Record-type
        if get_dict_depth(output_dict) > 0:
            # this is a record, go through the dict_ recursively
            fields = [self.make_output_type(key, val, is_record=True)
                      for key, val in output_dict.items()]

            used_field_ids = set()

            for field in fields:
                base_field_id = field.get('name', 'Output')

                # Since name fields can be the same for multiple inputs,
                # correct the name if it has already been used.
                chk_id = base_field_id
                i = 1
                if chk_id in used_field_ids:
                    chk_id = f"{base_field_id}_{i}"
                    i += 1
                used_field_ids.add(chk_id)

                field['name'] = chk_id

            converted_cwl_output = {
                id_key: clean_id,
                "label": name,
                "type": [
                    "null",
                    {
                        "type": "record",
                        "fields": fields,
                        "name": clean_id
                    }
                ]
            }

        # Case 2: Output is a File type
        elif re.fullmatch(file_pattern, key):
            # create a list of files output
            converted_cwl_output = {
                id_key: clean_id,
                "label": name,
                "type": "File[]?",
                "outputBinding": {
                    "glob": key
                }
            }

        # Case 3: Output is a folder type
        elif re.fullmatch(folder_pattern, key):
            # create a list of directories output
            converted_cwl_output = {
                id_key: clean_id,
                "label": name,
                "type": "Directory[]?",
                "outputBinding": {
                    "glob": key,
                    "loadListing": "deep_listing"
                }
            }
        return converted_cwl_output

    def parse_output_yml(self, yml_file):
        """
        Extracts output information from a YAML file, usually in tower.yml
        format.

        :param yml_file: path to YAML file.
        :return: list of outputs in CWL format.
        """
        outputs = list()
        yml_schema = yaml.safe_load(yml_file)

        for key, value in yml_schema.items():
            # Tower yml file can use "tower" key in the yml file to designate
            # some configurations tower uses. Since these are not output
            # definitions, we skip these.
            if key in SKIP_NEXTFLOW_TOWER_KEYS:
                continue
            if key == "reports" and type(value) is dict:
                temp = value.copy()
                for k, v in temp.items():
                    value[f"work/**/**/{k}"] = v
                    del value[k]

            outputs.append(
                self.make_output_type(key, value)
            )

        return outputs

    @staticmethod
    def parse_cwl(file, return_key):
        """
        Subset CWL based on return_key provided.

        :param file: Path to the CWL file.
        :param return_key: Key to subset the CWL with.
        :return:
        """
        return_list = list()
        schema = dict()

        try:
            schema = yaml.safe_load(file)
        except yaml.YAMLError:
            logger.error("CWL schema not in JSON or YAML format")

        if schema and return_key in schema:
            return_list.extend(schema[return_key])

        return return_list

    def dump_sb_wrapper(self, out_format=EXTENSIONS.yaml):
        """
        Dump SB wrapper for nextflow workflow to a file
        """
        sb_wrapper_path = os.path.join(
            self.workflow_path, f'sb_nextflow_schema.{out_format}')
        if out_format in EXTENSIONS.yaml_all:
            with open(sb_wrapper_path, 'w') as f:
                yaml.dump(self.sb_wrapper, f, indent=4, sort_keys=True)
        elif out_format in EXTENSIONS.json_all:
            with open(sb_wrapper_path, 'w') as f:
                json.dump(self.sb_wrapper, f, indent=4, sort_keys=True)

    def generate_sb_app(
            self, sb_schema=None, sb_entrypoint='main.nf',
            executor_version=None, execution_mode=None
    ):  # default nextflow entrypoint
        """
        Generate an SB app for a nextflow workflow, OR edit the one created and
        defined by the user
        """

        if sb_schema:
            new_code_package = self.sb_package_id if \
                self.sb_package_id else None
            schema_ext = sb_schema.split('/')[-1].split('.')[-1]

            return update_schema_code_package(sb_schema, schema_ext,
                                              new_code_package)

        else:
            self.sb_wrapper['cwlVersion'] = 'None'
            self.sb_wrapper['class'] = 'nextflow'

            self.sb_wrapper['inputs'] = self.generate_sb_inputs()
            self.sb_wrapper['outputs'] = self.generate_sb_outputs()
            self.sb_wrapper['requirements'] = WRAPPER_REQUIREMENTS

            app_content = dict()
            if self.sb_package_id:
                app_content['code_package'] = self.sb_package_id
            app_content['entrypoint'] = sb_entrypoint

            if executor_version or self.executor_version:
                app_content['executor_version'] = executor_version or \
                                                  self.executor_version

            self.sb_wrapper['app_content'] = app_content

            if execution_mode or self.execution_mode:
                if 'hints' not in self.sb_wrapper:
                    self.sb_wrapper['hints'] = []

                self.sb_wrapper['hints'].append(
                    {
                        'class': 'sbg:NextflowExecutionMode',
                        'value': execution_mode.value
                    }
                )

            if self.sb_doc:
                self.sb_wrapper['doc'] = self.sb_doc
            elif get_readme(self.workflow_path):
                with open(get_readme(self.workflow_path), 'r') as f:
                    self.sb_wrapper['doc'] = f.read()
            return self.sb_wrapper


def main():
    # CLI parameters
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile", default="default",
        help="SB platform profile as set in the SB API credentials file.",
    )
    parser.add_argument(
        "--appid", required=True,
        help="Takes the form {user or division}/{project}/{app_id}.",
    )
    parser.add_argument(
        "--workflow-path", required=True,
        help="Path to the main workflow directory",
    )
    parser.add_argument(
        "--entrypoint", required=False,
        help="Relative path to the workflow from the main workflow directory. "
             "If not provided, 'main.nf' will be used if available. "
             "If not available, but a single '*.nf' is located in the "
             "workflow-path will be used. If more than one '*.nf' script is "
             "detected, an error is raised.",
    )
    parser.add_argument(
        "--sb-package-id", required=False,
        help="Id of an already uploaded package",
    )
    parser.add_argument(
        "--sb-doc", required=False,
        help="Path to a doc file for sb app. If not provided, README.md "
             "will be used if available",
    )
    parser.add_argument(
        "--dump-sb-app", action="store_true", required=False,
        help="Dump created sb app to file if true and exit",
    )
    parser.add_argument(
        "--no-package", action="store_true", required=False,
        help="Only provide a sb app schema and a git URL for entrypoint",
    )
    parser.add_argument(
        "--executor-version", required=False,
        help="Version of the Nextflow executor to be used with the app.",
    )
    parser.add_argument(
        "--execution-mode", type=ExecMode, choices=list(ExecMode),
        required=False, default=None,
        help="Execution mode for your application.",
    )
    parser.add_argument(
        "--json", action="store_true", required=False,
        help="Dump sb app schema in JSON format (YAML by default)",
    )
    parser.add_argument(
        "--sb-schema", required=False,
        help="Do not create new schema, use this schema file. "
             "It is sb_nextflow_schema in JSON or YAML format.",
    )
    parser.add_argument(
        "--revision-note", required=False,
        default=None, type=str,
        help="Revision note to be placed in the CWL schema if the app is "
             "uploaded to the sbg platform.",
    )

    args = parser.parse_args()

    # Preprocess CLI parameter values
    entrypoint = args.entrypoint or \
        get_entrypoint(args.workflow_path) or 'main.nf'

    sb_doc = None
    if args.sb_doc:
        with open(args.sb_doc, 'r') as f:
            sb_doc = f.read()

    # Init api and nf_wrapper
    api = lib.get_profile(args.profile)

    nf_wrapper = SBNextflowWrapper(
        workflow_path=args.workflow_path,
        sb_doc=sb_doc
    )

    if args.sb_schema:
        # take the input schema, create new zip, upload zip,
        # add that zip to the schema, create app
        project_id = '/'.join(args.appid.split('/')[:2])
        nf_wrapper.sb_package_id = zip_and_push_to_sb(
            api=api,
            workflow_path=args.workflow_path,
            project_id=project_id,
            folder_name='nextflow_workflows'
        )
        sb_app = nf_wrapper.generate_sb_app(
            sb_entrypoint=entrypoint,
            sb_schema=args.sb_schema,
            executor_version=args.executor_version,
            execution_mode=args.execution_mode.value,
        )

    else:
        # Zip and upload
        if args.sb_package_id:
            nf_wrapper.sb_package_id = args.sb_package_id
        elif not args.no_package:
            project_id = '/'.join(args.appid.split('/')[:2])
            nf_wrapper.sb_package_id = zip_and_push_to_sb(
                api=api,
                workflow_path=args.workflow_path,
                project_id=project_id,
                folder_name='nextflow_workflows'
            )

        nf_schema_path = nf_wrapper.nf_schema_build()
        nf_wrapper.nf_schema_path = nf_schema_path

        # Create app
        sb_app = nf_wrapper.generate_sb_app(
            sb_entrypoint=entrypoint,
            executor_version=args.executor_version,
            execution_mode=args.execution_mode,
        )
        # Dump app to local file
        out_format = EXTENSIONS.json if args.json else EXTENSIONS.yaml
        nf_wrapper.dump_sb_wrapper(out_format=out_format)

    # Install app
    if not args.dump_sb_app:
        revision_note = f"Uploaded using sbpack v{__version__}"

        if args.revision_note:
            revision_note = str(args.revision_note)

        sb_app["sbg:revisionNotes"] = revision_note

        install_or_upgrade_app(api, args.appid, sb_app)


if __name__ == "__main__":
    main()
