#!/usr/bin/env cwl-runner

# Checks symbolic links on github

class: Workflow
cwlVersion: v1.0

requirements:
  SchemaDefRequirement:
    types:
    - $import: ../types/recursive.yml

inputs:
  in1:
    type: ../types/recursive.yml#file_with_sample_meta

outputs:
- id: out1
  type: File
  outputSource: '#s1/out1'

steps:
  s1:
    in:
      in1: '#in1'
    run:
      class: CommandLineTool
      cwlVersion: v1.0

      requirements: []

      inputs:
        in1: ../types/recursive.yml#file_with_sample_meta

      outputs:
        out1:
          type: File
          outputBinding:
            glob: '*.txt'

      baseCommand:
      - echo
      arguments:
      - hello world
    out:
    - out1
