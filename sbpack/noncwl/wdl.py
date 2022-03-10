import json
import yaml
import os
import sbpack.lib as lib
import argparse
import logging
from sevenbridges.errors import NotFound
from sbpack.version import __version__
import re
from subprocess import check_call
from sbpack.noncwl.utils import (zip_and_push_to_sb, get_readme, install_or_upgrade_app,
                                 update_schema_code_package, GENERIC_FILE_ARRAY_INPUT, WRAPPER_REQUIREMENTS)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PACKAGE_SIZE_LIMIT = 100 * 1024 * 1024  # MB
JAVA_EXE = os.getenv('SBPACK_WDL_JAVA_EXE', 'java')


class SBWDLWrapper:
    def __init__(self, workflow_path, entrypoint, dump_schema=False,
                 sb_doc=None, wdl_input=None, womtool_path=None):
        self.sb_wrapper = dict()
        self.sb_package_id = None
        self.workflow_path = workflow_path
        self.entrypoint = entrypoint
        self.wdl_input = wdl_input
        self.womtool_path = womtool_path
        self.dump_schema = dump_schema
        self.sb_doc = sb_doc

    @staticmethod
    def default_wdl_sb_outputs():
        """
        Default output for a WDL execution
        """
        return [
            {
                "id": "output_txt",
                "doc": "This is a template output."
                       " Please modify to collect final outputs using glob inside the working directory.",
                "type": "File[]",
                "outputBinding": {
                    "glob": "*.txt"
                }
            }
        ]

    @staticmethod
    def parse_type(type_string):
        t, _, attribute_string = re.search('([^\(]*)(\((.*)\))?',
                                           type_string).groups()
        attribute_string = attribute_string \
            if attribute_string is not None else ''

        attributes = attribute_string.split(',')
        # {optional , default : 9000}
        if 'optional' in attributes[0] and '?' not in t:
            t = t.strip()
            t = t + '?'
        else:
            t = t.strip()

        attribute_dict = {}
        for attr_string in attributes:
            split = attr_string.split('=')
            attr = split[0].strip()
            val = None if len(split) == 1 else split[1].strip()
            attribute_dict[attr] = val
        return t, attribute_dict

    @staticmethod
    def womtool_type_mapper(t):
        # this allows for Files and other types to stay uppercase
        primitive_types = ['String', 'Int', 'Float', 'Boolean']
        if t in primitive_types or t in [elem+'?' for elem in primitive_types]:
            t = t.lower()
        return t

    def generate_sb_inputs(self):
        """
        Generate SB inputs schema
        """
        womtool_inputs = self.wdl_input

        if not womtool_inputs:

            wdl_path = f'{self.workflow_path}/{self.entrypoint}'
            java_cmd = f"{JAVA_EXE} -jar {self.womtool_path} inputs {wdl_path} " \
                       f"> {self.workflow_path}/womtool_inputs.json"
            call_java_womtool = check_call(java_cmd, shell=True)
            with open(f"{self.workflow_path}/womtool_inputs.json", 'r') as f:
                womtool_inputs = json.load(f)

        cwl_inputs = list()
        for key, value in womtool_inputs.items():
            typ, descr = self.parse_type(value)
            if bool(re.search('\[(.*?)\]', typ)):
                typ_temp = self.womtool_type_mapper(re.search('\[(.*?)\]', typ).groups()[0]) + '[]'
                if '?' in typ:
                    typ = typ_temp + '?'
            else:
                typ = self.womtool_type_mapper(typ)

            new_item = {
                'id': key.replace('.', '_'),
                'type': typ,
                'inputBinding': {'prefix': '{}'.format(key)}
            }

            if 'default' in descr.keys():
                new_item['sbg:toolDefaultValue'] = descr['default']
            cwl_inputs.append(new_item)

        cwl_inputs.append(GENERIC_FILE_ARRAY_INPUT)

        return cwl_inputs

    def dump_sb_wrapper(self, out_format='yaml'):
        """
        Dump SB wrapper for WDL workflow to a file
        """
        sb_wrapper_path = os.path.join(self.workflow_path, f'sb_wdl_schema.{out_format}')
        if out_format == 'yaml':
            with open(sb_wrapper_path, 'w') as f:
                yaml.dump(self.sb_wrapper, f, indent=4, sort_keys=True)
        elif out_format == 'json':
            with open(sb_wrapper_path, 'w') as f:
                json.dump(self.sb_wrapper, f, indent=4, sort_keys=True)

    def generate_sb_app(self, sb_entrypoint, sb_schema=None):
        """
        Generate a raw SB app for a WDL workflow, or use provided sb_schema file
        """

        if sb_schema:
            new_code_package = self.sb_package_id if self.sb_package_id else None
            schema_ext = sb_schema.split('/')[-1].split('.')[-1]

            return update_schema_code_package(sb_schema, schema_ext,
                                              new_code_package)

        else:
            self.sb_wrapper['cwlVersion'] = 'None'
            self.sb_wrapper['class'] = 'wdl'
            self.sb_wrapper['inputs'] = self.generate_sb_inputs()
            self.sb_wrapper['outputs'] = self.default_wdl_sb_outputs()

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
    parser.add_argument("--profile", help="SB platform profile as set in "
                                          "the SB API credentials file.",
                        default="default")
    parser.add_argument("--appid", help="Takes the form {user or division}/"
                                        "{project}/{app_id}.", required=True)
    parser.add_argument("--entrypoint",
                        help="Relative path to the workflow from the main "
                             "workflow directory",
                        required=True)
    parser.add_argument("--workflow-path",
                        help="Path to the main workflow directory",
                        required=True)
    parser.add_argument("--sb-package-id",
                        help="Id of an already uploaded package",
                        required=False)
    parser.add_argument("--womtool-input",
                        help="Path to inputs .JSON generated by womtool",
                        required=False)
    parser.add_argument("--sb-doc",
                        help="""Path to a doc file for sb app. 
                        If not provided, README.md will be used if available""",
                        required=False)
    parser.add_argument("--dump-sb-app",
                        action="store_true", required=False,
                        help="Dump created sb app to file if true and exit")
    parser.add_argument("--no-package",
                        action="store_true", required=False,
                        help="Only provide a sb app schema and a git URL "
                             "for entrypoint")
    parser.add_argument("--womtool-path", required=False,
                        help="Path to womtool-X.jar")
    parser.add_argument("--json", action="store_true", required=False,
                        help="Dump sb app schema in JSON format "
                             "(YAML by default)")
    parser.add_argument("--sb-schema", required=False,
                        help="Do not create new schema, use this schema file. "
                             "It is sb_wdl_schema in JSON or YAML format.")
    args = parser.parse_args()

    # Preprocess CLI parameter values

    sb_doc = None
    if args.sb_doc:
        with open(args.sb_doc, 'r') as f:
            sb_doc = f.read()

    if args.womtool_input:
        with open(args.womtool_input, 'r') as f:
            wom_input = json.load(f)
    else:
        wom_input = None

    if args.womtool_path:
        wom_path = args.womtool_path
    else:
        wom_path = None
    if not args.womtool_input and not args.womtool_path and not args.sb_schema:
        raise SystemExit(f"Please specify either --womtool-path, --sb-schema or "
                         f"--womtools-input")

    # Init api and wdl_wrapper
    api = lib.get_profile(args.profile)
    wdl_wrapper = SBWDLWrapper(
        workflow_path=args.workflow_path,
        entrypoint=args.entrypoint,
        wdl_input=wom_input,
        womtool_path=wom_path,
        sb_doc=sb_doc
    )

    if args.sb_schema:
        # take the input schema, create new zip, upload zip,
        # add that zip to the schema, create app
        project_id = '/'.join(args.appid.split('/')[:2])
        wdl_wrapper.sb_package_id = zip_and_push_to_sb(
            api=api,
            workflow_path=args.workflow_path,
            project_id=project_id,
            folder_name='wdl_workflows'
        )
        sb_app = wdl_wrapper.generate_sb_app(sb_entrypoint=args.entrypoint,
                                             sb_schema=args.sb_schema)

    else:
        # Zip and upload
        if args.sb_package_id:
            wdl_wrapper.sb_package_id = args.sb_package_id
        elif not args.no_package:
            projectid = '/'.join(args.appid.split('/')[:2])
            wdl_wrapper.sb_package_id = zip_and_push_to_sb(
                api=api,
                workflow_path=args.workflow_path,
                project_id=projectid,
                folder_name='wdl_workflows'
            )

        # Create app
        sb_app = wdl_wrapper.generate_sb_app(sb_entrypoint=args.entrypoint)

        # Dump app to local file
        out_format = 'json' if args.json else 'yaml'
        wdl_wrapper.dump_sb_wrapper(out_format=out_format)

    # Install app
    if not args.dump_sb_app:

        if not args.sb_schema:
            sb_app[
                "sbg:revisionNotes"
            ] = f"Uploaded using sbpack v{__version__}"

        install_or_upgrade_app(api, args.appid, sb_app)

if __name__ == "__main__":
    main()
