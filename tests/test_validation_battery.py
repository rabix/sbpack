import subprocess
import sys
import pathlib
import urllib.parse

import pytest

from ruamel.yaml import YAML

from sbpack.pack import pack

fast_yaml = YAML(typ="safe")


def cwl_is_valid(fname):
    try:
        subprocess.run(["cwltool", "--validate", str(fname)], check=True)
        return True
    except subprocess.CalledProcessError as e:
        sys.stderr.write("Could not validate %s.", fname)
        return False


@pytest.mark.parametrize(
    ('f',),
    [("tools/clt1.cwl",), ("tools/clt2.cwl",), ("tools/clt3.cwl",),
     ("workflows/wf1.cwl",), ("workflows/wf2.cwl",), ("workflows/wf4.cwl",),
     ("workflows/wf-with-git.cwl",),
     ("https://raw.githubusercontent.com/rabix/sbpack/master/tests/workflows/wf1.cwl",),
     ("https://raw.githubusercontent.com/rabix/sbpack/master/tests/workflows/wf2.cwl",),
     ("https://raw.githubusercontent.com/rabix/sbpack/master/tests/workflows/wf4.cwl",),
     ("remote-cwl/tool1.cwl",), ("remote-cwl/tool2.cwl",), ("remote-cwl/wf1.cwl",)
     ]
)
@pytest.mark.skipif(sys.platform == 'win32',
                    reason='Skip on windows due to errors in cwltool with import pwd')
def test_local_packing_with_validation(f):
    url = urllib.parse.urlparse(f)
    packed_name = pathlib.Path(url.path).stem + "-packed.cwl"

    cwl = pack(f)
    fpacked = pathlib.Path(packed_name)
    with fpacked.open("w") as fout:
        fast_yaml.dump(cwl, fout)
    assert cwl_is_valid(fpacked)
    fpacked.unlink()
