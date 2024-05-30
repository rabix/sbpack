class: Workflow
cwlVersion: v1.0
inputs:
  in1: ../types/singletype.yml#simple_record

steps:
  s1:
    run: ../tools/clt2.cwl
    in:
      in1: in1
    out: [out1]
    
outputs:
    out1: 
      type: user_type1
      outputSource: s1/out1

requirements:
  SchemaDefRequirement:
    types:
    - $import: ../types/singletype.yml
    - name: user_type1  # For tools/clt2.cwl
      type: record
      fields:
          - name: prop
            type: string
