from typing import Union, Optional
from sevenbridges.models.project import Project

import sbpack.lib as lib
import argparse
import os


def validate_file(api, file_name, project, parent=None, is_folder=False):
    if ":" in file_name:
        return file_name

    if ";" in file_name:
        for f_n in file_name.split(";"):
            validate_file(api, f_n, project)

    file_name = file_name.strip('/')
    paths_to_check = [os.path.basename(file_name)]
    cur_path = file_name
    while os.path.dirname(cur_path):
        cur_path = os.path.dirname(cur_path)
        paths_to_check.append(os.path.basename(cur_path))

    checked = {}
    keys = []
    parent = None
    for path in paths_to_check[::-1]:
        keys.append(path)
        if "/".join(keys) in checked:
            parent = checked['/'.join(keys)]
        else:
            file = api.files.query(names=[path], parent=parent)
            parent = path
            checked["/".join(keys)] = file


def remap_cell(project_root, path):
    # prefix it with the project root
    if ";" in path:
        return ";".join([remap_cell(project_root, f) for f in path.split(";")])

    if ":" not in path:
        while path.startswith('/'):
            path = path[1:]
        return f"vs:///Projects/{project_root}/{path}"
    else:
        return path


def validate_sheet(
        api,
        project: Project,
        path_to_file: str,
        remap_columns: list,
):
    ext = path_to_file.split('.')[-1]
    if ext.lower() == 'csv':
        split_char = ','
    elif ext.lower() == 'tsv':
        split_char = '\t'
    else:
        raise ValueError(
            f"Invalid file type '{ext}'. Expected a .tsv or .csv file."
        )

    validated = dict()
    with open(path_to_file, 'r') as input_file:
        header = input_file.readline().strip('\n').split(split_char)

        indices = []
        for column in remap_columns:
            try:
                indices.append(header.index(column))
            except ValueError:
                raise ValueError(
                    f"Header column '{column}' not found in the "
                    f"sample sheet header."
                )

        for line in input_file.readlines():
            if line:
                line = line.strip('\n').split(split_char)
                for i in indices:
                    if line[i] in validated:
                        continue

                    validate_file(api, line[i], project)


def remap(
        project_root: str,
        path_to_file: str,
        remap_columns: list,
):
    ext = path_to_file.split('.')[-1]
    if ext.lower() == 'csv':
        split_char = ','
    elif ext.lower() == 'tsv':
        split_char = '\t'
    else:
        raise ValueError(
            f"Invalid file type '{ext}'. Expected a .tsv or .csv file."
        )

    sheet = []

    with open(path_to_file, 'r') as input_file:
        header = input_file.readline().strip('\n').split(split_char)
        sheet.append(split_char.join(header))

        indices = []
        for column in remap_columns:
            try:
                indices.append(header.index(column))
            except ValueError:
                raise ValueError(
                    f"Header column '{column}' not found in the "
                    f"sample sheet header."
                )

        for line in input_file.readlines():
            if line:
                line = line.strip('\n').split(split_char)
                for i in indices:
                    line[i] = remap_cell(project_root, line[i])
                line = split_char.join(line)
                sheet.append(line)

    return "\n".join(sheet)


def main():
    # CLI parameters
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile", required=False,
        default="default", type=str,
        help="SB platform profile as set in the SB API credentials file.",
    )
    parser.add_argument(
        "--projectid", required=True,
        type=str,
        help="Takes the form {user or division}/{project}.",
    )
    parser.add_argument(
        "--sample-sheet", required=True,
        type=str,
        help="Path to the sample sheet."
    )
    parser.add_argument(
        "--columns", required=True,
        metavar='string', nargs='+', type=str,
        help="Specify columns that contain paths to files on the platform"
             "as a list of strings separated by spaces.",
    )
    parser.add_argument(
        "--output", '-o', required=False,
        type=str,
        help="Name of the output file.",
    )
    parser.add_argument(
        "--upload", '-u', action='store_true', required=False,
        help="Upload the file to the project after making it.",
    )
    parser.add_argument(
        "--tags", required=False,
        metavar='string', nargs='+', type=str,
        help="Specify tags that you want the sample sheet to have on the "
             "platform, after it is uploaded.",
    )
    parser.add_argument(
        "--validate", action='store_true', required=False,
        help="Validate if each file exists on target project location.",
    )

    args = parser.parse_args()

    project = args.projectid
    api = lib.get_profile(args.profile)

    project_root = api.files.get(
        api.projects.get(project).root_folder
    ).name

    sheet = remap(
        project_root,
        args.sample_sheet,
        args.columns
    )

    validate_sheet(
        api,
        project,
        args.sample_sheet,
        args.columns
    )

    if not args.output:
        name = os.path.basename(args.sample_sheet)

        save_path = os.path.join(
            os.path.dirname(args.sample_sheet),
            name
        )
        i = 0
        while os.path.exists(save_path):
            i += 1
            save_path = os.path.join(
                os.path.dirname(args.sample_sheet),
                f"_{i}_{name}"
            )
    else:
        save_path = args.output

    with open(save_path, 'w') as output:
        output.write(sheet)

    if args.upload:
        name = os.path.basename(args.sample_sheet)
        if args.output:
            name = args.output

        temp_name = name
        i = 0

        while api.files.query(project=project, names=[temp_name]):
            i += 1
            temp_name = f"_{i}_{name}"

        file = api.files.upload(
            save_path, project, file_name=temp_name
        ).result()
        if args.tags:
            file.tags = args.tags
            file.save()


if __name__ == "__main__":
    main()
