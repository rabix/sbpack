#!/usr/bin/env cwl-runner

# Inline schemadef

class: CommandLineTool
cwlVersion: v1.0

requirements:
# This does not validate
#   SchemaDefRequirement:
#     - name: user_type1
#       type: record
#       fields:
#         - name: prop
#           type: string

# Neither does this
#   SchemaDefRequirement:
#     types:
#         user_type1
#             type: record
#             fields:
#                 - name: prop
#                   type: string

  SchemaDefRequirement:
    types:
      - name: user_type1
        type: record
        fields:
            - name: prop
              type: string

inputs:
  in1: user_type1

outputs:
  out1:
    type: user_type1
    outputBinding:
      outputEval: $(inputs.in1)

baseCommand: []
arguments: []
