import re
import ruamel.yaml
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
    update_schema_code_package,
    install_or_upgrade_app,
    validate_inputs,
    GENERIC_FILE_ARRAY_INPUT,
    GENERIC_OUTPUT_DIRECTORY,
    WRAPPER_REQUIREMENTS,
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PACKAGE_SIZE_LIMIT = 100 * 1024 * 1024  # MB
NF_SCHEMA_DEFAULT_NAME = 'nextflow_schema.json'


class SBNextflowWrapper:
    def __init__(self, workflow_path, dump_schema=False, sb_doc=None):
        self.sb_wrapper = dict()
        self.nf_ps = PipelineSchema()
        self.sb_package_id = None
        self.workflow_path = workflow_path
        self.dump_schema = dump_schema
        self.nf_schema_path = None
        self.sb_doc = sb_doc
        self.executor_version = None
        self.output_schemas = None
        self.input_schemas = None

    @staticmethod
    def nf_schema_type_mapper(input_type_string):
        """
        Convert nextflow schema input type to CWL
        """
        type_ = input_type_string.get('type', 'string')
        format_ = input_type_string.get('format', '')
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
        return [type_]

    @staticmethod
    def nf_cwl_port_map():
        """
        Mappings of nextflow input fields to SB input fields
        nextflow_key: cwl_key mapping
        """
        return {
            'default': 'sbg:toolDefaultValue',
            'description': 'label',
            'help_text': 'doc',
        }

    def nf_to_sb_input_mapper(self, port_id, port_data, required=False):
        """
        Convert a single input from Nextflow schema to SB schema
        """
        sb_input = dict()
        sb_input['id'] = port_id
        sb_input['type'] = self.nf_schema_type_mapper(port_data)
        if not required:
            sb_input['type'].append('null')
        for nf_field, sb_field in self.nf_cwl_port_map().items():
            if nf_field in port_data:
                sb_input[sb_field] = port_data[nf_field]
        sb_input['inputBinding'] = {
            'prefix': f'--{port_id}',
        }
        return sb_input

    def collect_nf_definition_properties(self, definition):
        """
        Nextflow inputs schema contains multiple definitions where each
        definition contains multiple properties
        """
        cwl_inputs = list()
        for port_id, port_data in definition['properties'].items():
            cwl_inputs.append(self.nf_to_sb_input_mapper(
                port_id,
                port_data,
                required=port_id in definition.get('required', [])),
            )
            # Nextflow schema field "required" lists input_ids
            # for required inputs
        return cwl_inputs

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
    def file_is_nf_schema(path):
        try:
            schema = ruamel.yaml.safe_load(path)
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

    def generate_sb_inputs(self, manual_validation=False):
        """
        Generate SB inputs schema
        """
        cwl_inputs = list()
        if self.input_schemas:
            nf_schemas = [
                f for f in self.input_schemas if self.file_is_nf_schema(f)
            ]

            if nf_schemas:
                self.nf_schema_path = nf_schemas.pop().name

        if self.nf_schema_path:
            with open(self.nf_schema_path, 'r') as f:
                nf_schema = ruamel.yaml.safe_load(f)

            for p_key, p_value in nf_schema.get('properties', {}).items():
                cwl_inputs.append(
                    self.nf_to_sb_input_mapper(p_key, p_value))
            for def_name, definition in nf_schema.get(
                    'definitions', {}).items():
                cwl_inputs.extend(
                    self.collect_nf_definition_properties(definition))

        if self.input_schemas:
            for file in self.input_schemas:
                if file.name == self.nf_schema_path:
                    continue
                if file.name.split('.').pop().lower() in \
                        ['yaml', 'yml', 'json', 'cwl']:
                    cwl_inputs.extend(self.parse_cwl(file, 'inputs'))

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

        if manual_validation:
            print('Input validation')
            cwl_inputs = validate_inputs(cwl_inputs)
            print('Input validation completed')
        return cwl_inputs

    def generate_sb_outputs(self):
        """
        Generate SB output schema
        """
        output_ids = set()
        cwl_outputs = list()

        if self.output_schemas:
            for file in self.output_schemas:
                if file.name.split('.').pop().lower() in ['yml', 'yaml']:
                    cwl_outputs.extend(self.parse_output_yml(file))
                if file.name.split('.').pop().lower() in ['json', 'cwl']:
                    cwl_outputs.extend(self.parse_cwl(file, 'outputs'))

        cwl_outputs.append(GENERIC_OUTPUT_DIRECTORY)

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
            # create a list of files outptu
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
        yml_schema = ruamel.yaml.safe_load(yml_file)

        for key, value in yml_schema.items():
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

    def dump_sb_wrapper(self, out_format='yaml'):
        """
        Dump SB wrapper for nextflow workflow to a file
        """
        sb_wrapper_path = os.path.join(
            self.workflow_path, f'sb_nextflow_schema.{out_format}')
        if out_format == 'yaml':
            with open(sb_wrapper_path, 'w') as f:
                yaml.dump(self.sb_wrapper, f, indent=4, sort_keys=True)
        elif out_format == 'json' or out_format == 'cwl':
            with open(sb_wrapper_path, 'w') as f:
                json.dump(self.sb_wrapper, f, indent=4, sort_keys=True)

    def generate_sb_app(
            self, sb_schema=None, sb_entrypoint='main.nf',
            executor_version=None, output_schemas=None, input_schemas=None,
            manual_validation=False
    ):  # default nextflow entrypoint
        """
        Generate an SB app for a nextflow workflow, OR edit the one created and
        defined by the user
        """
        if output_schemas:
            self.output_schemas = output_schemas
        if input_schemas:
            self.input_schemas = input_schemas

        if sb_schema:
            new_code_package = self.sb_package_id if \
                self.sb_package_id else None
            schema_ext = sb_schema.split('/')[-1].split('.')[-1]

            return update_schema_code_package(sb_schema, schema_ext,
                                              new_code_package)

        else:
            self.sb_wrapper['cwlVersion'] = 'None'
            self.sb_wrapper['class'] = 'nextflow'

            self.sb_wrapper['inputs'] = self.generate_sb_inputs(
                manual_validation
            )
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
        "--entrypoint", required=True,
        help="Relative path to the workflow from the main workflow directory",
    )
    parser.add_argument(
        "--workflow-path", required=True,
        help="Path to the main workflow directory",
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
        "--json", action="store_true", required=False,
        help="Dump sb app schema in JSON format (YAML by default)",
    )
    parser.add_argument(
        "--sb-schema", required=False,
        help="Do not create new schema, use this schema file. "
             "It is sb_nextflow_schema in JSON or YAML format.",
    )
    parser.add_argument(
        "--output-schema-files", required=False,
        default=None, type=argparse.FileType('r'), nargs='+',
        help="Additional output schema files in CWL or tower.yml format.",
    )
    parser.add_argument(
        "--input-schema-files", required=False,
        default=None, type=argparse.FileType('r'), nargs='+',
        help="Additional input schema files in CWL format.",
    )
    parser.add_argument(
        "--revision-note", required=False,
        default=None, type=str, nargs="+",
        help="Revision note to be placed in the CWL schema if the app is "
             "uploaded to the sbg platform.",
    )
    parser.add_argument(
        "--manual-validation", required=False, action="store_true",
        default=False,
        help="You will have to provide validation for all 'string' type inputs"
             " if are string (s), file (f), directory (d), list of file (lf),"
             " or list of directory (ld) type inputs.",
    )

    args = parser.parse_args()

    # Preprocess CLI parameter values

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
            sb_entrypoint=args.entrypoint,
            sb_schema=args.sb_schema,
            executor_version=args.executor_version,
            manual_validation=args.manual_validation
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

        # Build or update nextflow inputs schema
        if not args.input_schema_files:
            nf_schema_path = nf_wrapper.nf_schema_build()
            nf_wrapper.nf_schema_path = nf_schema_path

        # Create app
        sb_app = nf_wrapper.generate_sb_app(
            sb_entrypoint=args.entrypoint,
            executor_version=args.executor_version,
            output_schemas=args.output_schema_files,
            input_schemas=args.input_schema_files,
            manual_validation=args.manual_validation
        )
        # Dump app to local file
        out_format = 'json' if args.json else 'yaml'
        nf_wrapper.dump_sb_wrapper(out_format=out_format)

    # Install app
    if not args.dump_sb_app:
        revision_note = f"Uploaded using sbpack v{__version__}"

        if args.revision_note:
            revision_note = str(" ".join(args.revision_note))

        if not args.sb_schema:
            sb_app["sbg:revisionNotes"] = revision_note

        install_or_upgrade_app(api, args.appid, sb_app)


if __name__ == "__main__":
    main()
