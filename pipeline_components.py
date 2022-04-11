from kfp.v2 import dsl

@dsl.component
def hello(name: str):
    print(f"Hello, {name}!")
