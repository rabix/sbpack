${{
    if ({file_input}) {{
        return '{sample_sheet_name}';
    }} else if (!{file_input} && {sample_sheet}){{
        return {sample_sheet};
    }} else {{
        return "";
    }}
}}