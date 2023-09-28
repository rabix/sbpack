import argparse
import logging
import sbpack.lib as lib
from sbpack.noncwl.utils import install_or_upgrade_app, push_zip

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    # CLI parameters
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--profile", default="default", nargs="+",
        help="SB platform profile as set in the SB API credentials file. If "
             "you are using sbcopy to copy an app from one division to "
             "another, please provide two profiles - first profile for the "
             "source app (appid), and second for the destination project "
             "(projectid)."
    )
    parser.add_argument(
        "--appid", required=True,
        help="What to copy? Takes the form "
             "{user or division}/{project}/{app_id} or "
             "{user or division}/{project}/{app_id}/{revision_no}."
    )
    parser.add_argument(
        "--projectid", required=True,
        help="Where to copy? Takes the form {user or division}/{project}"
    )
    args = parser.parse_args()

    # Preprocess CLI parameter values

    # Init api
    if len(args.profile) > 1:
        api_source = lib.get_profile(args.profile[0])
        api_dest = lib.get_profile(args.profile[1])
    else:
        api_source = lib.get_profile(args.profile[0])
        api_dest = api_source

    # Source and destination apps
    source_app = api_source.apps.get(args.appid)
    sb_app_raw = source_app.raw
    destination_app_id = args.projectid + '/' + args.appid.split('/')[2]

    # Copy the code package
    source_package = source_app.raw.get(
        'app_content', {}
    ).get('code_package', '')

    if source_package:
        # The app_content.code_package field exists and contains the id of the
        # code package file

        source_package_file = api_source.files.get(source_package)
        if api_source == api_dest:
            # Copy has been performed in the same division/env
            # Copy the file to the destination through the API
            new_file_id = api_source.files.get(
                source_package_file.copy(project=args.projectid)
            ).id
        else:
            # Copy has been performed between two different divisions/envs
            # Download the file
            name = source_package_file.name
            source_package_file.download(
                path=name,
                overwrite=True
            )

            # Find out if the parent folder is the root of the project
            parent = source_package_file.parent
            project = api_source.projects.get(source_package_file.project)
            if parent == project.root_folder:
                # If the parent is the root, then set folder name to None
                # This means that the code package will go into the root of the
                # destination project
                folder_name = None
            else:
                # Parent is not the root, so use it when pushing the zip file
                # to preserve the folder structure
                folder_name = api_source.files.get(parent).name

            # Push the zip to the destination project
            new_file_id = push_zip(
                api=api_dest,
                zip_path=name,
                project_id=args.projectid,
                folder_name=folder_name
            )
            # With this complete the code package is now at the destination

        # Change the id of the code package to the new file
        sb_app_raw['app_content']['code_package'] = new_file_id

    # Use the install_or_upgrade_app function to copy the app
    install_or_upgrade_app(api_dest, destination_app_id, sb_app_raw)


if __name__ == "__main__":
    main()
