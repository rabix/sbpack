${{
    function compareFiles(file_a, file_b) {{
        if (file_a.path < file_b.path) {{
            return -1;
        }} else if (file_a.path > file_b.path) {{
            return 1;
        }}
        return 0;
    }}

    var input_source = [].concat({input_source}).sort(compareFiles);
    if (input_source.length == 0){{
        // Return empty file if no input files are given.
        // Ensures that sample sheet is generated only if there are files to
        // either:
        //   - map onto an existing sample sheet, or
        //   - generate a sample sheet from input file info/metadata
        return "";
    }};

    var sample_sheet_input = {sample_sheet_input};

    var sample_sheet = [];

    if (sample_sheet_input){{
        // If the sample sheet file is given, map inputs to the contents
        var contents = sample_sheet_input.contents.split("\n");
        var format_ = sample_sheet_input.nameext.slice(1);
        
        var split_char = "";

        if (format_ == 'csv'){{
            split_char = ',';
        }}

        if (format_ == 'tsv'){{
            split_char = '\t';
        }}

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
        // If the samples are given, create the sample sheet from input data
        var format_ = "{format_}";
        var header = {header};
        var row = {rows};
        var defaults = {defaults};
        var group_by = {group_by};

        var split_char = "";

        if (format_ == 'csv'){{
            split_char = ',';
        }}

        if (format_ == 'tsv'){{
            split_char = '\t';
        }}

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
                groups[group_criteria.join(".")].push(file)
            }} catch(ex) {{
                groups[group_criteria.join(".")] = [file]
            }}
        }}

        if (defaults.length < row.length){{
            for (var i = 0; i < row.length - defaults.length + 1; i++){{
                defaults.push("");
            }}
        }};

        for (var k in groups){{
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
                var d = "";
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
    return sample_sheet.join("\n");
}}