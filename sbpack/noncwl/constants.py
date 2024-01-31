from enum import Enum
from sbpack.noncwl import read_js_template


# ############################## Generic Bits ############################### #
PACKAGE_SIZE_LIMIT = 100 * 1024 * 1024  # 100 MB
REMOVE_INPUT_KEY = "REMOVE_THIS_KEY"


# keep track of what extensions are applicable for processing
class EXTENSIONS:
    yaml = 'yaml'
    yml = 'yml'
    json = 'json'
    cwl = 'cwl'

    yaml_all = [yaml, yml, cwl]
    json_all = [json]
    all_ = [yaml, yml, json, cwl]


# ############################ CWL Standard Bits ############################ #
# A generic SB input array of files that should be available on the
# instance but are not explicitly provided to the execution as wdl params.
SAMPLE_SHEET_FUNCTION = read_js_template("sample_sheet_generator.js")
SAMPLE_SHEET_SWITCH = read_js_template("sample_sheet_switch.js")

GENERIC_FILE_ARRAY_INPUT = {
    "id": "auxiliary_files",
    "type": "File[]?",
    "label": "Auxiliary files",
    "doc": "List of files not added as explicit workflow inputs but "
           "required for workflow execution."
}

SAMPLE_SHEET_FILE_ARRAY_INPUT = {
    "id": "file_input",
    "type": "File[]?",
    "label": "Input files",
    "doc": "List of files that will be used to autogenerate the sample sheet "
           "that is required for workflow execution."
}

GENERIC_NF_OUTPUT_DIRECTORY = {
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

GENERIC_WDL_OUTPUT_DIRECTORY = {
    "id": "output_txt",
    "doc": "This is a template output. "
           "Please modify to collect final outputs using "
           "glob inside the working directory.",
    "type": "File[]",
    "outputBinding": {
        "glob": "*.txt"
    }
}

# Requirements for sb wrapper
INLINE_JS_REQUIREMENT = {
    'class': "InlineJavascriptRequirement"
}
LOAD_LISTING_REQUIREMENT = {
    'class': "LoadListingRequirement"
}
AUX_FILES_REQUIREMENT = {
    "class": "InitialWorkDirRequirement",
    "listing": [
        "$(inputs.auxiliary_files)"
    ]
}

# Legacy - Delete after updating wdl.py
WRAPPER_REQUIREMENTS = [
    INLINE_JS_REQUIREMENT,
    AUX_FILES_REQUIREMENT
]


def sample_sheet(
        file_name, sample_sheet_input, format_, input_source, header, rows,
        defaults, group_by):
    basename = ".".join(file_name.split(".")[:-1])
    ext = file_name.split(".")[-1]
    new_name = f"{basename}.new.{ext}"

    return {
        "class": "InitialWorkDirRequirement",
        "listing": [
            {
                "entryname": f"${{ return {sample_sheet_input} ? {sample_sheet_input}.nameroot + '.new' + {sample_sheet_input}.nameext : '{file_name}' }}",
                "entry": SAMPLE_SHEET_FUNCTION.format_map(locals()),
                "writable": False
            }
        ]
    }


# ############################## Nextflow Bits ############################## #
# Keys that should be skipped when parsing nextflow tower yaml file

NF_SCHEMA_DEFAULT_NAME = 'nextflow_schema.json'
SB_SCHEMA_DEFAULT_NAME = 'sb_nextflow_schema'

# Mappings of nextflow input fields to SB input fields
#  nextflow_key: cwl_key mapping
NF_TO_CWL_PORT_MAP = {
    'default': 'sbg:toolDefaultValue',
    'description': 'label',
    'help_text': 'doc',
    'mimetype': 'format',
    'fa_icon': 'sbg:icon',
    'pattern': 'sbg:pattern',
    'hidden': 'sbg:hidden',
}

# Mappings of nextflow definition fields to SB category fields
#  nextflow_key: cwl_key mapping
NF_TO_CWL_CATEGORY_MAP = {
    'title': 'sbg:title',
    'description': 'sbg:doc',
    'fa_icon': 'sbg:icon',
}

# What keys to skip from the tower.yml file
SKIP_NEXTFLOW_TOWER_KEYS = [
    'tower',
    'mail',
]


class ExecMode(Enum):
    single = 'single-instance'
    multi = 'multi-instance'

    def __str__(self):
        return self.value
