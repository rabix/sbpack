from enum import Enum

# ############################## Generic Bits ############################### #
PACKAGE_SIZE_LIMIT = 100 * 1024 * 1024  # 100 MB


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
GENERIC_FILE_ARRAY_INPUT = {
    "id": "auxiliary_files",
    "type": "File[]?",
    "label": "Auxiliary files",
    "doc": "List of files not added as explicit workflow inputs but "
           "required for workflow execution."
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

# ############################## Nextflow Bits ############################## #
# Keys that should be skipped when parsing nextflow tower yaml file

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
