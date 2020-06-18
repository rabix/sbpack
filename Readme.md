# sbpack

![](https://github.com/rabix/sbpack/workflows/Tests/badge.svg)
[![PyPI version](https://badge.fury.io/py/sbpack.svg)](https://pypi.org/project/sbpack/)

Upload (`sbpack`) and download (`sbpull`) CWL apps to/from any Seven Bridges powered platform. 
Resolves linked processes, schemadefs and `$include`s and `$import`s.

## Installation

(It is good practice to install Python programs in a virtual environment. 
[pipx] is a very effective tool for installing command line Python tools in isolated environments)

[pipx]: https://github.com/pipxproject/pipx

`sbpack` needs Python 3.6 or later

```
pip3 install pipx  # in case you don't have pipx
pipx ensurepath # ensures CLI application directory is on your $PATH
```

### Install latest release on pypi
```bash
pipx install sbpack
# or pipx upgrade
```

### Install latest (unreleased) code
```
pipx install git+https://github.com/rabix/sbpack.git
# use pipx upgrade ... if upgrading an existing install
```

## Usage
```
Usage
   sbpack <profile> <id> <cwl>
 
where:
  <profile> refers to a SB platform profile as set in the SB API credentials file.
  <id> takes the form {user}/{project}/{app_id} which installs (or updates) 
       "app_id" located in "project" of "user".
  <cwl> is the path to the main CWL file to be uploaded. This can be a remote file.
```
 
## Uploading workflows defined remotely

`sbpack` handles local paths and remote URLs in a principled manner. This means that
`sbpack` will handle packing and uploading a local workflow that links to a remote workflow
which itself has linked workflows. It will therefore also handle packing a fully 
remote workflow.

For example, to pack and upload the workflow located at `https://github.com/Duke-GCB/GGR-cwl/blob/master/v1.0/ATAC-seq_pipeline/pipeline-se.cwl`
go to the `raw` button and use that URL, like:

```bash
sbpack sbg kghosesbg/sbpla-31744/ATAC-seq-pipeline-se https://raw.githubusercontent.com/Duke-GCB/GGR-cwl/master/v1.0/ATAC-seq_pipeline/pipeline-se.cwl
``` 

## Local packing
```
Usage
    cwlpack <cwl> > packed.cwl
```

The `cwlpack` utility allows you to pack a workflow and print it out on `stdout` instead of 
uploading it to a SB platform.


## Side-note
As an interesting side note, packing a workflow can get around at least two `cwltool` bugs 
[[1]][cwltoolbug1], [[2]][cwltoolbug2].

[cwltoolbug1]: https://github.com/common-workflow-language/cwltool/issues/1304
[cwltoolbug2]: https://github.com/common-workflow-language/cwltool/issues/1306


## Pulling (and unpacking)
`sbpull` will retrieve CWL from any SB powered platform and save it to local disk. 

```bash
sbpull sbg admin/sbg-public-data/salmon-workflow-1-2-0/ salmon.cwl
```

With the `--unpack` option set, it will also explode the workflow recursively, extracting out each
sub-process into its own file. 

```bash
sbpull sbg admin/sbg-public-data/salmon-workflow-1-2-0/ salmon.cwl --unpack
```

> This is useful if you want to use SB platform CWL with your own workflows. You can pull the relevant
CWL into your code repository and use it with the rest of your code. If you use the `--unpack` option 
you can access the individual components of the SB CWL workflow separately.

### Pulling a particular revision

While
```bash
sbpull sbg admin/sbg-public-data/bismark-0-21-0/ bismark.cwl
```
will pull the latest version of Bismark on the platform,

```bash
sbpull sbg admin/sbg-public-data/bismark-0-21-0/2 bismark.cwl
```
will pull revision 2 of this tool


## Note on reversibility
**`sbpack` and `sbpull --unpack` are not textually reversible. The packed and unpacked CWL 
representations are functionally identical, however if you `sbpack` a workflow, and 
then `sbpull --unpack` it, they will look different.**


## Credentials file and profiles

If you use the SBG API you already have an API configuration file. If
not, you should create one. It is located in 
`~/.sevenbridges/credentials`. ([Documentation][cred-doc])

[cred-doc]: https://docs.sevenbridges.com/docs/store-credentials-to-access-seven-bridges-client-applications-and-libraries

Briefly, each section in the SBG configuration file (e.g. `[cgc]`) is a 
profile name and has two entries. The end-point and an authentication
token, which you get from your developer tab on the platform.

```
[sbg-us]
api_endpoint = https://api.sbgenomics.com/v2
auth_token   = <dev token here>

[sbg-eu]
api_endpoint = https://eu-api.sbgenomics.com/v2
auth_token   = <dev token here>

[sbg-china]
api_endpoint = https://api.sevenbridges.cn/v2
auth_token   = <dev token here>

[cgc]
api_endpoint = https://cgc-api.sbgenomics.com/v2
auth_token   = <dev token here>

[cavatica]
api_endpoint = https://cavatica-api.sbgenomics.com/v2
auth_token   = <dev token here>

[nhlbi]
api_endpoint = https://api.sb.biodatacatalyst.nhlbi.nih.gov/v2
auth_token   = <dev token here>
```

You can have several profiles on the same platform if, for example, you 
are an enterprise user and you belong to several divisions. Please refer
to the API documentation for more detail.
