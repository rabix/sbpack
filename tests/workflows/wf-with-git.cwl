class: Workflow
cwlVersion: v1.2
inputs:
  in: File
steps:
  git:
    in:
      - id: file1
        source: in
    out:
      - id: output_file
    run: https://raw.githubusercontent.com/common-workflow-language/cwl-v1.2/v1.2.0/tests/cat3-tool-docker.cwl

