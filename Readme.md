# sbpack

Packs a DocWithUrl workflow, resolving linked processes, includes and imports
and uploads it to a project on the SB platform.

```
sbpack <profile> <id> <cwl>
```

Here `<profle>` refers to a SB platform profile

`<id>` takes the form `{user}/{project}/{app_id}` which installs (or updates)
the app `id` located in `project` of `user`. 
 

## Credentials file and profiles

If you use the SBG API you already have an API configuration file. If
not, you should create one. It is located in 
`~/.sevenbridges/credentials`.

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
```

You can have several profiles on the same platform if, for example, you 
are an enterprise user and you belong to several divisions. Please refer
to the API documentation for more detail.
