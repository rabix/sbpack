#!/usr/bin/env cwl-runner

# Inline schemadef with crossreference

class: CommandLineTool
cwlVersion: v1.0

requirements:
  SchemaDefRequirement:
    types:
      - name: user_type1
        type: record
        fields:
            - name: prop
              type: string
      - name: user_type2
        type: record
        fields:
            - name: prop
              type: user_type1

inputs:
  in1: user_type2
  in2: user_type1?
  in3: user_type1[]
  in4: user_type1[]?

outputs:
  out1:
    type: user_type2
    outputBinding:
      outputEval: $(inputs.in1)
  out2:
    type: user_type1[]
    outputBinding:
      outputEval: $(inputs.in3)

baseCommand: []
arguments: []
