import ruamel.yaml
import json
import sbpack.lib as lib
import argparse
from nf_core.schema import PipelineSchema
import logging
from sevenbridges.errors import NotFound
from sbpack.version import __version__
import os
import yaml
from sbpack.noncwl.utils import (zip_and_push_to_sb, get_readme, update_schema_code_package, install_or_upgrade_app,
                                 GENERIC_FILE_ARRAY_INPUT, WRAPPER_REQUIREMENTS)

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
        self.nf_schema_path = os.path.join(workflow_path,
                                           NF_SCHEMA_DEFAULT_NAME)
        self.sb_doc = sb_doc

    @staticmethod
    def nf_schema_type_mapper(t):
        """
        Convert nextflow schema input type to CWL
        """
        if t == 'string':
            return ['string']
        if t == 'integer':
            return ['int']
        if t == 'number':
            return ['float']
        if t == 'boolean':
            return ['boolean']
        return [t]

    @staticmethod
    def nf_cwl_port_map():
        """
        Mappings of nextflow input fields to SB input fields
        nextflow_key: cwl_key mapping
        """
        return {
            'default': 'sbg:toolDefaultValue',
            'description': 'label',
            'help_text': 'doc'
        }

    @staticmethod
    def default_nf_sb_outputs():
        """
        Default output for a Nextflow execution
        """
        return [
            {
                "id": "nf_workdir",
                "type": "Directory",
                "doc": "This is a template output. "
                       "Please change glob to directories specified in publishDir in the workflow.",
                "outputBinding": {
                    "glob": "work"
                }
            }
        ]

    def nf_to_sb_input_mapper(self, port_id, port_data, required=False):
        """
        Convert a single input from Nextflow schema to SB schema
        """
        sb_input = dict()
        sb_input['id'] = port_id
        sb_input['type'] = self.nf_schema_type_mapper(port_data.get('type', 'string'))
        if not required:
            sb_input['type'].append('null')
        for nf_field, sb_field in self.nf_cwl_port_map().items():
            if nf_field in port_data:
                sb_input[sb_field] = port_data[nf_field]
        sb_input['inputBinding'] = {
            'prefix': f'--{port_id}'
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
                required=port_id in definition.get('required', []))
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
            NF_SCHEMA_DEFAULT_NAME
        )

        # if the file doesn't exist, nf-core raises exception and logs
        logging.getLogger("nf_core.schema").setLevel("CRITICAL")

        self.nf_ps.schema_filename = nf_schema_path
        self.nf_ps.build_schema(
            pipeline_dir=self.workflow_path,
            no_prompts=True,
            web_only=False,
            url=''
        )

    def generate_sb_inputs(self):
        """
        Generate SB inputs schema
        """
        with open(self.nf_schema_path, 'r') as f:
            nf_schema = ruamel.yaml.safe_load(f)

        cwl_inputs = list()
        for p_key, p_value in nf_schema.get('properties', {}).items():
            cwl_inputs.append(self.nf_to_sb_input_mapper(p_key, p_value))
        for def_name, definition in nf_schema.get('definitions', {}).items():
            cwl_inputs.extend(self.collect_nf_definition_properties(definition))
        cwl_inputs.append(GENERIC_FILE_ARRAY_INPUT)
        return cwl_inputs

    def dump_sb_wrapper(self, out_format='yaml'):
        """
        Dump SB wrapper for nextflow workflow to a file
        """
        sb_wrapper_path = os.path.join(self.workflow_path, f'sb_nextflow_schema.{out_format}')
        if out_format == 'yaml':
            with open(sb_wrapper_path, 'w') as f:
                yaml.dump(self.sb_wrapper, f, indent=4, sort_keys=True)
        elif out_format == 'json':
            with open(sb_wrapper_path, 'w') as f:
                json.dump(self.sb_wrapper, f, indent=4, sort_keys=True)

    def generate_sb_app(self, sb_schema=None,
                        sb_entrypoint='main.nf'):  # default nextflow entrypoint
        """
        Generate a SB app for a nextflow workflow, OR edit the one created and
        defined by the user
        """
        if sb_schema:
            new_code_package = self.sb_package_id if self.sb_package_id else None
            schema_ext = sb_schema.split('/')[-1].split('.')[-1]

            return update_schema_code_package(sb_schema, schema_ext,
                                              new_code_package)

        else:
            self.sb_wrapper['cwlVersion'] = 'None'
            self.sb_wrapper['class'] = 'nextflow'

            self.sb_wrapper['inputs'] = self.generate_sb_inputs()

            self.sb_wrapper['outputs'] = self.default_nf_sb_outputs()
            self.sb_wrapper['requirements'] = WRAPPER_REQUIREMENTS
            app_content = dict()
            if self.sb_package_id:
                app_content['code_package'] = self.sb_package_id
            app_content['entrypoint'] = sb_entrypoint
            self.sb_wrapper['app_content'] = app_content
            if self.sb_doc:
                self.sb_wrapper['doc'] = self.sb_doc
            elif get_readme(self.workflow_path):
                with open(get_readme(self.workflow_path), 'r') as f:
                    self.sb_wrapper['doc'] = f.read()
            return self.sb_wrapper


def main():
    pass
    # CLI parameters
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="default",
                        help="SB platform profile as set in the SB API "
                             "credentials file.")
    parser.add_argument("--appid", required=True,
                        help="Takes the form "
                             "{user or division}/{project}/{app_id}.")
    parser.add_argument("--entrypoint", required=True,
                        help="Relative path to the workflow from the main "
                             "workflow directory")
    parser.add_argument("--workflow-path", required=True,
                        help="Path to the main workflow directory")
    parser.add_argument("--sb-package-id", required=False,
                        help="Id of an already uploaded package")
    parser.add_argument("--sb-doc", required=False,
                        help="Path to a doc file for sb app. If not provided, "
                             "README.md will be used if available")
    parser.add_argument("--dump-sb-app",
                        action="store_true", required=False,
                        help="Dump created sb app to file if true and exit")
    parser.add_argument("--no-package",
                        action="store_true", required=False,
                        help="Only provide a sb app schema and a git URL for "
                             "entrypoint")
    parser.add_argument("--json",
                        action="store_true", required=False,
                        help="Dump sb app schema in JSON format "
                             "(YAML by default)")
    parser.add_argument("--sb-schema", required=False,
                        help="Do not create new schema, use this schema file. "
                             "It is sb_nextflow_schema in JSON or YAML format.")

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
        sb_app = nf_wrapper.generate_sb_app(sb_entrypoint=args.entrypoint,
                                            sb_schema=args.sb_schema)

    else:
        # Zip and upload
        if args.sb_package_id:
            nf_wrapper.sb_package_id = args.sb_package_id
        elif not args.no_package:
            projectid = '/'.join(args.appid.split('/')[:2])
            nf_wrapper.sb_package_id = zip_and_push_to_sb(
                api=api,
                workflow_path=args.workflow_path,
                project_id=projectid,
                folder_name='nextflow_workflows'
            )

        # Build or update nextflow inputs schema
        nf_wrapper.nf_schema_build()
        # Create app
        sb_app = nf_wrapper.generate_sb_app(sb_entrypoint=args.entrypoint)
        # Dump app to local file
        out_format = 'json' if args.json else 'yaml'
        nf_wrapper.dump_sb_wrapper(out_format=out_format)

    # Install app
    if not args.dump_sb_app:

        if not args.sb_schema:
            sb_app[
                "sbg:revisionNotes"
            ] = f"Uploaded using sbpack v{__version__}"

        install_or_upgrade_app(api,args.appid,sb_app)


if __name__ == "__main__":
    main()
