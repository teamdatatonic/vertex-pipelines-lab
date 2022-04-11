from kfp.v2 import dsl

# import your pipeline components
from pipeline_components import hello

@dsl.pipeline(name="my-pipeline")
def pipeline(input_name: str):
    
    hello(input_name)
