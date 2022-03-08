import argparse
import logging
import sbpack.lib as lib
from sevenbridges.errors import NotFound
from sbpack.noncwl.utils import install_or_upgrade_app

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main():
    # CLI parameters
    parser = argparse.ArgumentParser()
    parser.add_argument("--profile", default="default",
                        help="SB platform profile as set in the SB API "
                             "credentials file.")
    parser.add_argument("--appid", required=True,
                        help="What to copy? Takes the form {user or division}/{project}/{app_id} "
                             "or {user or division}/{project}/{app_id}/{revision_no}.")
    parser.add_argument("--projectid", required=True,
                        help="Where to copy? Takes the form "
                             "{user or division}/{project}")
    args = parser.parse_args()

    # Preprocess CLI parameter values

    # Init api
    api = lib.get_profile(args.profile)

    # Source and destination apps
    source_app = api.apps.get(args.appid)
    sb_app_raw = source_app.raw
    destination_app_id = args.projectid + '/' + args.appid.split('/')[2]

    # Copy the code package
    source_package = source_app.raw.get('app_content', {}).get('code_package', '')
    if source_package:
        new_file = api.files.get(api.files.get(source_package).copy(project=args.projectid))
        sb_app_raw['app_content']['code_package'] = new_file.id

    install_or_upgrade_app(api,destination_app_id, sb_app_raw)


if __name__ == "__main__":
    main()
