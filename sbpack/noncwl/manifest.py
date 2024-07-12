from sevenbridges.models.project import Project

import logging
import sbpack.lib as lib
import argparse
import os


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def paths_to_check(file_name: str) -> list:
    """
    :param file_name: Contents of a single manifest file cell that contains
    path(s) to files.
    """
    chk = []
    rtrn = []

    if ";" in file_name:
        # This should handle the case when there are multiple files in the
        #  same cell, but they are separated by ";"
        # For example:
        #  file.ext;folder/file.ext
        chk.extend(file_name.split(";"))
    else:
        chk.append(file_name)

    for file_name in chk:
        if ":" in file_name:
            # If a file is in cloud storage, skip it
            continue

        file_name = file_name.strip('/')
        rtrn.append(file_name)
        cur_path = file_name
        while os.path.dirname(cur_path):
            cur_path = os.path.dirname(cur_path)
            rtrn.append(cur_path)

    return rtrn


def remap_cell(project_root: str, path: str) -> str:
    """
    Remaps a file path to the 'vs:' file system.

    Supports multiple files separated with ';'.

    :param project_root: Name of the project root directory.
    :param path: File path.
    :return: File path(s) prefixed with 'vs:///Projects/' and project_root.
    """
    # prefix it with the project root
    if ";" in path:
        return ";".join([remap_cell(project_root, f) for f in path.split(";")])

    if path and ":" not in path:
        while path.startswith('/'):
            path = path[1:]
        if path:
            return f"vs:///Projects/{project_root}/{path}"
    else:
        return path


def validate_sheet(
        api,
        project: Project,
        path_to_file: str,
        remap_columns: list,
) -> None:
    """
    Go through the sample sheet and validate if files contained within are
    located in the project.

    :param api: SevenBridges API
    :param project: Project on a SevenBridges powered platform where the files
    are located.
    :param path_to_file: Path to the sample sheet (manifest) file.
    :param remap_columns: Names of the columns to remap. These columns must
    contain paths to files.
    """
    # Collect the extension of the file to determine the split character
    # If the file is a CSV, use ","; or TSV, use "\t"
    ext = path_to_file.split('.')[-1]
    if ext.lower() == 'csv':
        split_char = ','
    elif ext.lower() == 'tsv':
        split_char = '\t'
    else:
        raise ValueError(
            f"Invalid file type '{ext}'. Expected a .tsv or .csv file."
        )

    # Create a list of unique paths to files and directories that the files are
    #  contained in.
    to_validate = list()
    with open(path_to_file, 'r') as input_file:
        # Assume first row is the header
        header = input_file.readline().strip('\n').split(split_char)

        # Create a list of indices based on the column names.
        indices = []
        for column in remap_columns:
            try:
                indices.append(header.index(column))
            except ValueError:
                raise ValueError(
                    f"Header column '{column}' not found in the "
                    f"sample sheet header."
                )

        # Assume all lines below the first are the table contents.
        for line in input_file.readlines():
            line = line.strip('\n')

            # Skip empty lines
            if not line:
                continue

            line = line.split(split_char)
            for i in indices:
                to_validate.extend(paths_to_check(line[i]))

    # ### Check collected paths ### #
    # Memoize checked paths
    checked = {}
    errors = []

    for path in sorted(list(to_validate)):
        if path in checked:
            continue
        else:
            basename = os.path.basename(path)
            if not basename:
                continue
            parent = None
            if os.path.dirname(path):
                parent = checked[os.path.dirname(path)]

            file = api.files.query(
                names=[basename],
                project=project if not parent else None,
                parent=parent)
            if file:
                checked[path] = file[0]
            else:
                raise FileExistsError(
                    f"File <{path}> does not exist within "
                    f"project <{project}>")


def remap(
        project_root: str,
        path_to_file: str,
        remap_columns: list,
) -> str:
    """
    Remap paths from a manifest file to vs:// paths.

    Remapping is performed only on file paths that are not already in cloud
    storage. Paths to project files in the manifest should point to their
    relative location in the project root. For example, if a file ("file.ext")
    is located in a directory named "directory", which resides in the project
    root, then the correct path to that file would be "directory/file.ext".

    The function assumes that the first row is always the header.

    :param project_root: Name of the project root directory.
    :param path_to_file: Path to the manifest file.
    :param remap_columns: Names of manifest file columns that contain paths to
    input files.
    :return: Manifest file with remapped columns in string format.
    """
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
        "--upload", action='store_true', required=False,
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

    project = api.projects.get(project)
    project_root = api.files.get(project.root_folder).name

    logger.info('Remapping manifest files.')
    sheet = remap(
        project_root,
        args.sample_sheet,
        args.columns
    )
    logger.info('Remapping complete.')

    if args.validate:
        logger.info('Validating manifest.')
        validate_sheet(
            api,
            project,
            args.sample_sheet,
            args.columns
        )
        logger.info('Validation complete.')

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
        logger.info(f'Saving remapped manifest file to <{save_path}>.')
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

        logger.info(
            f'Uploading remapped manifest file to project {project} '
            f'under filename <{temp_name}>.')
        file = api.files.upload(
            save_path, project, file_name=temp_name
        ).result()
        if args.tags:
            file.tags = args.tags
            file.save()


if __name__ == "__main__":
    main()
