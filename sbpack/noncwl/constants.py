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
SAMPLE_SHEET_FUNCTION = """
${{
    if (!inputs.input_files){{
        return \"\";
    }};
    
    var input_source = [].concat(inputs.input_files);
    var sample_sheet_input = inputs.sample_sheet_input;
    var sample_sheet = [];
    
    if (sample_sheet_input){{
        var contents = sample_sheet_input.contents.split(\"\\n\");
        var format_ = sample_sheet_input.nameext.slice(1);
        
        var split_char = \"\";
        
        switch (format_) {{
            case 'csv':
                split_char = \",\";
            case 'tsv':
                split_char = \"\\t\";
        }};
        
        for (var i=0; i < input_source.length; i++){{
            var file = input_source[i];
            for (var row=0; row < contents.length; row++){{
                var row_data = contents[row].split(split_char);
                for (var column=0; column < row_data.length; column++){{
                    var cell = row_data[column];
                    if (cell == file.basename){{
                        cell = file.path;
                    }}
                    row_data[column] = cell;
                }}
                contents[row] = row_data.join(split_char);
            }}
        }}
        sample_sheet = contents;
    }} else {{
        var format_ = inputs.format;
        var header = inputs.header;
        var row = inputs.rows;
        var defaults = inputs.defaults;
        var group_by = inputs.group_by;
        
        var split_char = \"\";
        
        switch (format_) {{
            case 'csv':
                split_char = \",\";
            case 'tsv':
                split_char = \"\\t\";
        }}
        var sample_sheet = [];
        
        if (header){{
            sample_sheet.push(header.join(split_char));
        }};
        var groups = {{}};
        
        for (var i = 0; i < input_source.length; i ++){{
            var file = input_source[i];
            var group_criteria = [];
            for (var j = 0; j < group_by.length; j ++){{
                group_criteria.push(eval(group_by[j]));
            }}
            try {{
                groups[group_criteria.join(\".\")].push(file)
            }} catch(ex) {{
                groups[group_criteria.join(\".\")] = [file]
            }}
        }}
        
        if (defaults.length < row.length){{
            for (var i = 0; i < row.length - defaults.length + 1; i++) defaults.push(\"\");
        }};
        
        for (k in groups){{
            var row_data = [];
            var files = groups[k];
            
            files.sort(function(a, b) {{
                var name_a = a.basename.toUpperCase();
                var name_b = b.basename.toUpperCase();
                if (name_a < name_b){{
                    return -1;
                }} else if (name_a > name_b){{
                    return 1;
                }} else {{
                    return 0;
                }}
            }});
            
            for (var j = 0; j < row.length; j ++){{
                var d = \"\";
                try {{
                    var d = eval(row[j]);
                    if (d == undefined){{
                        d = defaults[j];
                    }}
                }} catch(ex) {{
                    var d = defaults[j];
                }}
                row_data.push(d);
            }}
            
            sample_sheet.push(row_data.join(split_char));
        }}
    }}
    return sample_sheet.join(\"\\n\");
}}
"""

SAMPLE_SHEET_SWITCH = """
${{
    if ({file_input}) {{
        return '{sample_sheet_name}';
    }} else if (!{file_input} && {sample_sheet}){{
        return {sample_sheet};
    }} else {{
        return "";
    }}
}}
"""

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
