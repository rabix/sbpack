import argparse
import logging
import yaml
import os

import sbpack.lib as lib

from nf_core.schema import PipelineSchema
from sbpack.version import __version__

from sbpack.noncwl.constants import (
    DEFAULT_EXCLUDE_PATTERNS,
)

from sbpack.noncwl.utils import (
    zip_and_push_to_sb,
    install_or_upgrade_app,
    remove_local_file,
    get_git_repo,
)

from wrabbit.parser.utils import (
    get_latest_sb_schema,
    get_sample_sheet_schema,
)

from wrabbit.parser.constants import (
    ExecMode,
    EXTENSIONS,
    NF_SCHEMA_DEFAULT_NAME,
    SB_SCHEMA_DEFAULT_NAME,
)

from wrabbit.parser.nextflow import (
    NextflowParser
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class SBNextflowWrapper(NextflowParser):
    def __init__(self, workflow_path, *args, **kwargs):
        super().__init__(workflow_path, *args, **kwargs)
        self.nf_ps = PipelineSchema()

    def nf_schema_build(self):
        """
        Build nextflow schema using nf_core schema build feature and save to
        a file
        """
        if self.nf_schema_path:
            return

        base_dir = os.path.join(
            self.workflow_path, os.path.dirname(self.entrypoint)
        )
        nf_schema_path = os.path.join(
            base_dir,
            NF_SCHEMA_DEFAULT_NAME,
        )

        # if the file doesn't exist, nf-core raises exception and logs
        logging.getLogger("nf_core.schema").setLevel("CRITICAL")

        self.nf_ps.schema_filename = nf_schema_path
        # if not os.path.exists(nf_schema_path):
        self.nf_ps.build_schema(
            pipeline_dir=base_dir,
            no_prompts=True,
            web_only=False,
            url='',
        )
        self.nf_schema_path = nf_schema_path

        self.init_config_files()


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
        "--workflow-path", required=False,
        help="Path to the main workflow directory.",
    )
    parser.add_argument(
        "--git-url", required=False,
        help="URL to the git repository.",
    )
    parser.add_argument(
        "--branch", required=False,
        help="Used with --git-url. If git url is provided, branch to clone.",
    )
    parser.add_argument(
        "--entrypoint", required=False,
        help="Relative path to the workflow from the main workflow directory. "
             "If not provided, 'main.nf' will be used if available. "
             "If not available, but a single '*.nf' is located in the "
             "workflow-path (or git-url) will be used. If more than one '*.nf'"
             " script is detected, an error is raised.",
    )
    parser.add_argument(
        "--sb-package-id", required=False,
        help="Id of an already uploaded package.",
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
    # parser.add_argument(
    #     "--no-package", action="store_true", required=False,
    #     help="Only provide an sb app schema and a git URL for entrypoint",
    # )
    parser.add_argument(
        "--executor-version", required=False,
        help="Version of the Nextflow executor to be used with the app.",
    )
    parser.add_argument(
        "--execution-mode", type=ExecMode, choices=list(ExecMode),
        required=False, default=ExecMode.multi,
        help="Execution mode for your application. Can be multi-instance or "
             "single-instance",
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
        "--app-name", required=False,
        default=None, type=str,
        help="Name of the app to be shown on the platform.",
    )
    parser.add_argument(
        "--exclude", required=False,
        default=None, type=str, nargs="+",
        help=f"Glob patterns you want to exclude from the code package. "
             f"By default the following patterns are excluded: "
             f"{DEFAULT_EXCLUDE_PATTERNS}"
    )
    parser.add_argument(
        "--sample-sheet-schema", required=False,
        default=None, type=str,
        help="This options is deprecated. Please use sbmanifest to generate "
             "valid sample sheets for the SevenBridges powered platforms.\n"
             "Path to the sample sheet schema yaml. The sample sheet schema "
             "should contain the following keys: 'sample_sheet_input', "
             "'sample_sheet_name', 'header', 'rows', 'defaults', 'group_by', "
             "'format_'"
    )
    parser.add_argument(
        "--auto", action="store_true", required=False,
        help="Automatically detect all possible inputs directly from the "
             "--workflow-path or --git-url location",
    )

    args = parser.parse_args()

    # Preprocess CLI parameter values
    # This stores them into variables that can be updated if --auto is used
    entrypoint = args.entrypoint or None
    sb_schema = args.sb_schema or None
    executor_version = args.executor_version or None
    execution_mode = args.execution_mode or None
    revision_note = args.revision_note or \
        f"Uploaded using sbpack v{__version__}"
    sample_sheet_schema = args.sample_sheet_schema or None
    label = args.app_name or None
    readme_path = args.sb_doc or None
    dump_sb_app = args.dump_sb_app or False
    sb_package_id = args.sb_package_id or None
    workflow_path = args.workflow_path or None
    git_url = args.git_url or None
    branch = args.branch or None
    cleanup_workflow_path = False  # changes to True if temp git dir is created

    # Input validation
    if (not workflow_path and not git_url) or \
            (workflow_path and git_url):
        raise Exception(
            "Either --workflow_path OR --git_url must be provided."
        )

    if not dump_sb_app and not args.appid and not args.auto:
        raise Exception(
            "The --appid argument is required if "
            "--dump-sb-app and/or --auto are not used"
        )

    if git_url and not label:
        label = os.path.basename(git_url)
        if branch:
            label += f" {branch}"

    if sb_schema:
        if execution_mode:
            logger.warning(
                "Using --sb-schema option overwrites --execution-mode."
            )

        if label:
            logger.warning(
                "Using --sb-schema option overwrites --app-name."
            )

        if executor_version:
            logger.warning(
                "Using --sb-schema option overwrites --executor-version."
            )

        if entrypoint:
            logger.warning(
                "Using --sb-schema option overwrites --entrypoint."
            )

        if readme_path:
            logger.warning(
                "Using --sb-schema option overwrites --sb-doc."
            )

        if revision_note:
            logger.warning(
                "Using --sb-schema option overwrites --revision-note."
            )

    if git_url:
        cleanup_workflow_path = True
        workflow_path = get_git_repo(git_url, branch)

    if args.auto:
        # This is where the magic happens
        if not sb_schema:
            sb_schema = get_latest_sb_schema(workflow_path)
            if sb_schema:
                logger.info(f'Using sb schema <{sb_schema}>')

        # Set execution mode to multi-instance
        if not execution_mode:
            execution_mode = ExecMode.multi
            logger.info(f'Using execution mode <{execution_mode}>')

        # locate sample sheet
        if not sample_sheet_schema:
            sample_sheet_schema = get_sample_sheet_schema(workflow_path)
            if sample_sheet_schema:
                logger.info(
                    f'Using sample sheet schema <{sample_sheet_schema}>'
                )

        # if appid is not provided, dump the app
        if not args.appid:
            dump_sb_app = True
            logger.info(
                f'Appid not provided. App is not going to be uploaded.'
            )

    nf_wrapper = SBNextflowWrapper(
        workflow_path=workflow_path,
        readme_path=readme_path,
        label=label,
        entrypoint=entrypoint,
        executor_version=executor_version,
        sb_package_id=sb_package_id,
        search_subfolders=True,
    )

    if sb_schema:
        # parse input schema
        with open(sb_schema, 'r') as s:
            schema = yaml.safe_load(s)
            nf_wrapper.sb_wrapper.load(schema)
    else:
        # Create app
        nf_wrapper.generate_sb_app(
            execution_mode=execution_mode,
            sample_sheet_schema=sample_sheet_schema,
        )

    # Install app
    if dump_sb_app:
        # Dump app to local file
        out_format = EXTENSIONS.json if args.json else EXTENSIONS.yaml
        nf_wrapper.dump_sb_wrapper(out_format=out_format)

    else:
        # App should be installed on the platform
        api = lib.get_profile(args.profile)

        # 1. if the code package is not provided on input,
        # create and upload it
        if not sb_package_id:
            project_id = '/'.join(args.appid.split('/')[:2])
            sb_package_id = zip_and_push_to_sb(
                api=api,
                workflow_path=workflow_path,
                project_id=project_id,
                folder_name='nextflow_workflows',
                exclude_patterns=args.exclude,
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

    if cleanup_workflow_path:
        remove_local_file(workflow_path)


if __name__ == "__main__":
    main()
