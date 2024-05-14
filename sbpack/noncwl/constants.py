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

# Nextflow
DEFAULT_EXCLUDE_PATTERNS = [
    "*.git",
    "*.git*",
    ".git",
    ".git*",
    # ".github",
    # ".gitignore",
    # ".gitpod.yml",
    "work",
    ".nextflow.log",
    ".DS_Store",
    ".devcontainer",
    ".editorconfig",
    ".gitattributes",
    ".nextflow",
    # ".nf-core.yml",
    ".pre-commit-config.yaml",
    ".prettierignore",
    ".prettierrc.yml",
    ".idea",
    ".pytest_cache",
    "*.egg-info",
]
