import re
import json
import argparse
import logging
import os
import yaml

import sbpack.lib as lib
from packaging import version

from nf_core.schema import PipelineSchema
from sbpack.version import __version__
from sbpack.pack import pack
from sbpack.noncwl.utils import (
    get_dict_depth,
    zip_and_push_to_sb,
    get_readme,
    get_tower_yml,
    get_entrypoint,
    get_executor_version,
    get_latest_sb_schema,
    get_sample_sheet_schema,
    get_config_files,
    parse_config_file,
    create_profile_enum,
    install_or_upgrade_app,
    nf_to_sb_input_mapper,
)
from sbpack.noncwl.constants import (
    sample_sheet,
    ExecMode,
    GENERIC_FILE_ARRAY_INPUT,
    GENERIC_NF_OUTPUT_DIRECTORY,
    INLINE_JS_REQUIREMENT,
    LOAD_LISTING_REQUIREMENT,
    AUX_FILES_REQUIREMENT,
    SKIP_NEXTFLOW_TOWER_KEYS,
    EXTENSIONS,
    NF_TO_CWL_CATEGORY_MAP,
    SAMPLE_SHEET_FILE_ARRAY_INPUT,
    SAMPLE_SHEET_SWITCH,
    NF_SCHEMA_DEFAULT_NAME,
    SB_SCHEMA_DEFAULT_NAME,
    REMOVE_INPUT_KEY,
)
from sbpack.noncwl.wrapper import Wrapper

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SBNextflowWrapper:
    def __init__(self, workflow_path, sb_doc=None):
        self.sb_wrapper = Wrapper()
        self.nf_ps = PipelineSchema()
        self.workflow_path = workflow_path
        self.nf_schema_path = None
        self.nf_config_files = None
        self.sb_doc = sb_doc

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
        self.nf_schema_path = nf_schema_path

    def generate_sb_inputs(self):
        """
        Generate SB inputs schema
        """

        # ## Add profiles to the input ## #
        self.nf_config_files = get_config_files(self.workflow_path)

        profiles = dict()

        for path in self.nf_config_files:
            profiles.update(parse_config_file(path))

        profiles_choices = sorted(list(set(profiles.keys())))

        if profiles:
            self.sb_wrapper.safe_add_input(
                create_profile_enum(profiles_choices)
            )

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
                self.sb_wrapper.safe_add_input(
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

                    self.sb_wrapper.safe_add_input(nf_to_sb_input_mapper(
                        port_id,
                        port_data,
                        category=input_category,
                        required=req,
                    ))

        # Add the generic file array input - auxiliary files
        self.sb_wrapper.safe_add_input(GENERIC_FILE_ARRAY_INPUT)
        self.sb_wrapper.add_requirement(AUX_FILES_REQUIREMENT)
        self.sb_wrapper.add_requirement(INLINE_JS_REQUIREMENT)

    def generate_sb_outputs(self):
        """
        Generate SB output schema
        """
        if get_tower_yml(self.workflow_path):
            for output in self.parse_output_yml(
                    open(get_tower_yml(self.workflow_path))
            ):
                self.sb_wrapper.safe_add_output(output)

        # if the only output is reports, or there are no outputs, add generic
        if len(self.sb_wrapper.outputs) == 0 or \
                (len(self.sb_wrapper.outputs) == 1 and
                 self.sb_wrapper.outputs[0]['id'] == 'reports'):
            self.sb_wrapper.safe_add_output(GENERIC_NF_OUTPUT_DIRECTORY)

    def parse_sample_sheet_schema(self, path):
        """
        Example sample sheet:
        sample_sheet_input: input_sample_sheet  # taken from app wrapper
        sample_sheet_name: samplesheet.csv
        header:
          - SampleID
          - Fastq1
          - Fastq2
        rows:
          - sample_id
          - path
          - path
        defaults:
          - NA
          - NA
          - NA
        group_by: sample_id
        format_: csv

        """
        schema = yaml.safe_load(path)

        sample_sheet_input = schema.get('sample_sheet_input')
        sample_sheet_name = schema.get('sample_sheet_name', 'samplesheet')
        header = schema.get('header', 'null')

        # fix rows
        rows = schema.get('rows')
        for i, r in enumerate(rows):
            if "." not in r:
                if r == 'path':
                    n = 0
                    new_r = f'files[{n}].path'
                    while new_r in rows:
                        n += 1
                        new_r = f'files[{n}].path'
                    rows[i] = new_r
                else:
                    rows[i] = f'files[0].metadata.{r}'

        defaults = schema.get('defaults', 'null')

        # fix group by
        group_by = schema.get('group_by')
        if type(group_by) is str:
            group_by = [group_by]
        for i, gb in enumerate(group_by):

            if "." not in gb:
                if gb in ['file', 'none']:
                    group_by[i] = 'file.path'
                else:
                    group_by[i] = f'file.metadata.{gb}'

        format_ = schema.get('format_', None)

        if format_ and not sample_sheet_name.endswith(format_):
            sample_sheet_name += f".{format_}".lower()

        if not format_ and not sample_sheet_name.endswith(['.tsv', '.csv']):
            raise Exception('Sample sheet format could not be identified. '
                            'Please specify one of "tsv" or "csv" in the '
                            'sample sheet schema file.')

        if not format_ and sample_sheet_name.endswith(['.tsv', '.csv']):
            format_ = sample_sheet_name.split('.').pop().lower()

        if format_.lower() not in ['tsv', 'csv']:
            raise Exception(f'Unrecognized sample sheet format "{format_}".')

        # Step 1:
        # add a new input to the pipeline
        #    - new input must not clash with other inputs by ID
        # Ensure that the new input is unique

        # Create the sample sheet file array input
        file_input = self.sb_wrapper.safe_add_input(
            SAMPLE_SHEET_FILE_ARRAY_INPUT
        )
        file_input_id = file_input.get('id')

        # Step 2:
        # add argument for sample sheet
        #    - requires: sample sheet input (sample_sheet_input),
        #                file input (ss_file_input)
        #    - if the sample sheet is provided on input,
        #      do not generate a new ss
        input_changes = {
            'id': sample_sheet_input,
            'loadContents': True,
            'inputBinding': REMOVE_INPUT_KEY
        }

        prefix = self.sb_wrapper.get_input(
            sample_sheet_input
        )['inputBinding']['prefix']

        self.sb_wrapper.update_input(input_changes)
        self.sb_wrapper.add_argument(
            {
                "prefix": prefix,
                "shellQuote": False,
                "valueFrom": SAMPLE_SHEET_SWITCH.format(
                    file_input=f"inputs.{file_input_id}",
                    sample_sheet=f"inputs.{sample_sheet_input}",
                    sample_sheet_name=sample_sheet_name,
                )
            }
        )

        # Step 3:
        # add file requirement
        #    - requires: sample sheet schema
        #    - add InitialWorkDirRequirement if there are none
        #    - if there are, append the entry to listing
        ss = sample_sheet(
            file_name=sample_sheet_name,
            sample_sheet_input=f"inputs.{sample_sheet_input}",
            format_=format_,
            input_source=f"inputs.{file_input_id}",
            header=header,
            rows=rows,
            defaults=defaults,
            group_by=group_by,
        )

        self.sb_wrapper.add_requirement(ss)
        self.sb_wrapper.add_requirement(INLINE_JS_REQUIREMENT)
        self.sb_wrapper.add_requirement(LOAD_LISTING_REQUIREMENT)

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
                    changed_key = f"work/**/{k}"
                    while "/**/**/" in changed_key:
                        changed_key = changed_key.replace("/**/**/", "/**/")
                    value[changed_key] = v
                    del value[k]

            outputs.append(
                self.make_output_type(key, value)
            )

        return outputs

    def dump_sb_wrapper(self, out_format=EXTENSIONS.yaml):
        """
        Dump SB wrapper for nextflow workflow to a file
        """
        print('Writing sb nextflow schema file...')
        basename = SB_SCHEMA_DEFAULT_NAME
        counter = 0
        sb_wrapper_path = os.path.join(
            self.workflow_path,
            f'{basename}.{out_format}'
        )

        while os.path.exists(sb_wrapper_path):
            counter += 1
            sb_wrapper_path = os.path.join(
                self.workflow_path,
                f'{basename}.{counter}.{out_format}'
            )

        print(f"Schema written to file <{sb_wrapper_path}>")

        if out_format in EXTENSIONS.yaml_all:
            with open(sb_wrapper_path, 'w') as f:
                yaml.dump(self.sb_wrapper.dump(), f, indent=4, sort_keys=True)
        elif out_format in EXTENSIONS.json_all:
            with open(sb_wrapper_path, 'w') as f:
                json.dump(self.sb_wrapper.dump(), f, indent=4, sort_keys=True)

    def generate_sb_app(
            self, sb_schema=None, sb_entrypoint='main.nf',
            executor_version=None, sb_package_id=None, execution_mode=None,
            sample_sheet_schema=None,
    ):
        """
        Generate an SB app for a nextflow workflow, OR edit the one created and
        defined by the user
        """

        if sb_schema:
            sb_schema_dict = pack(sb_schema)
            self.sb_wrapper.load(sb_schema_dict)

        else:
            self.sb_wrapper.cwl_version = 'None'
            self.sb_wrapper.class_ = 'nextflow'

            self.generate_sb_inputs()
            self.generate_sb_outputs()

            if sample_sheet_schema:
                self.parse_sample_sheet_schema(open(sample_sheet_schema))

            self.sb_wrapper.set_app_content(
                code_package=sb_package_id,
                entrypoint=sb_entrypoint,
                executor_version=executor_version,
            )

            if execution_mode:
                self.sb_wrapper.add_hint({
                    'class': 'sbg:NextflowExecutionMode',
                    'value': execution_mode.value
                })

            if self.sb_doc:
                self.sb_wrapper.add_docs(self.sb_doc)


def main():
    # CLI parameters
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile", default="default", required=False,
        help="SB platform profile as set in the SB API credentials file.",
    )
    parser.add_argument(
        "--appid", required=False,
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
        help=f"Do not create new schema, use this schema file. "
             f"It is {SB_SCHEMA_DEFAULT_NAME} in JSON or YAML format.",
    )
    parser.add_argument(
        "--revision-note", required=False,
        default=None, type=str,
        help="Revision note to be placed in the CWL schema if the app is "
             "uploaded to the sbg platform.",
    )
    parser.add_argument(
        "--sample-sheet-schema", required=False,
        default=None, type=str,
        help="Path to the sample sheet schema yaml. The sample sheet schema "
             "should contain the following keys: 'sample_sheet_input', "
             "'sample_sheet_name', 'header', 'rows', 'defaults', 'group_by', "
             "'format_'"
    )
    parser.add_argument(
        "--auto", action="store_true", required=False,
        help="Automatically detect all possible inputs directly from the "
             "--workflow-path location",
    )

    args = parser.parse_args()

    # Preprocess CLI parameter values
    # This stores them into variables that can be updated if --auto is used
    entrypoint = args.entrypoint or \
        get_entrypoint(args.workflow_path) or 'main.nf'
    sb_schema = args.sb_schema or None
    executor_version = args.executor_version or None
    execution_mode = args.execution_mode or None
    revision_note = args.revision_note or \
        f"Uploaded using sbpack v{__version__}"
    sample_sheet_schema = args.sample_sheet_schema or None
    dump_sb_app = args.dump_sb_app or False

    sb_doc = None
    if args.sb_doc:
        with open(args.sb_doc, 'r') as f:
            sb_doc = f.read()
    elif get_readme(args.workflow_path):
        with open(get_readme(args.workflow_path), 'r') as f:
            sb_doc = f.read()

    test_sign, test_executor_version = get_executor_version(sb_doc or "")
    if test_sign and executor_version and "edge" not in executor_version:
        if test_sign == "=" and version.parse(executor_version) != \
                version.parse(test_executor_version):
            logger.warning(
                f"Provided executor version {executor_version} does not"
                f" match detected version {test_sign}{test_executor_version}"
            )
        if test_sign == ">" and version.parse(executor_version) <= \
                version.parse(test_executor_version):
            logger.warning(
                f"Provided executor version {executor_version} does not"
                f" match detected version {test_sign}{test_executor_version}"
            )
        if test_sign == "<" and version.parse(executor_version) >= \
                version.parse(test_executor_version):
            logger.warning(
                f"Provided executor version {executor_version} does not"
                f" match detected version {test_sign}{test_executor_version}"
            )
        if test_sign == ">=" and version.parse(executor_version) < \
                version.parse(test_executor_version):
            logger.warning(
                f"Provided executor version {executor_version} does not"
                f" match detected version {test_sign}{test_executor_version}"
            )
        if test_sign == "<=" and version.parse(executor_version) > \
                version.parse(test_executor_version):
            logger.warning(
                f"Provided executor version {executor_version} does not"
                f" match detected version {test_sign}{test_executor_version}"
            )

    if args.auto:
        # This is where the magic happens
        if not sb_schema:
            sb_schema = get_latest_sb_schema(args.workflow_path)
        # detect nextflow executor version from description
        executor_version = test_executor_version

        # Set execution mode to multi-instance
        if not execution_mode:
            execution_mode = ExecMode.multi

        # locate sample sheet
        if not sample_sheet_schema:
            sample_sheet_schema = get_sample_sheet_schema(args.workflow_path)

        # if appid is not provided, dump the app
        if not args.appid:
            dump_sb_app = True

    # Input validation
    if not dump_sb_app:
        # appid is required
        if not args.appid:
            raise Exception(
                "The --appid argument is required if "
                "--dump-sb-app is not used"
            )

    nf_wrapper = SBNextflowWrapper(
        workflow_path=args.workflow_path,
        sb_doc=sb_doc
    )

    if sb_schema:
        # parse input schema
        nf_wrapper.generate_sb_app(
            sb_schema=sb_schema
        )
    else:
        # build schema
        nf_wrapper.nf_schema_build()

        # Create app
        nf_wrapper.generate_sb_app(
            sb_entrypoint=entrypoint,
            executor_version=executor_version,
            execution_mode=execution_mode,
            sample_sheet_schema=sample_sheet_schema,
        )

    # Install app
    if dump_sb_app:
        # Dump app to local file
        out_format = EXTENSIONS.json if args.json else EXTENSIONS.yaml
        nf_wrapper.dump_sb_wrapper(out_format=out_format)
    else:
        api = lib.get_profile(args.profile)

        sb_package_id = None
        if args.sb_package_id:
            sb_package_id = args.sb_package_id
        elif not args.no_package:
            project_id = '/'.join(args.appid.split('/')[:2])
            sb_package_id = zip_and_push_to_sb(
                api=api,
                workflow_path=args.workflow_path,
                project_id=project_id,
                folder_name='nextflow_workflows'
            )

        nf_wrapper.sb_wrapper.set_app_content(
            code_package=sb_package_id
        )

        nf_wrapper.sb_wrapper.add_revision_note(revision_note)

        # Dump app to local file
        out_format = EXTENSIONS.json if args.json else EXTENSIONS.yaml
        if not sb_schema:
            nf_wrapper.dump_sb_wrapper(out_format=out_format)
        install_or_upgrade_app(api, args.appid, nf_wrapper.sb_wrapper.dump())


if __name__ == "__main__":
    main()
