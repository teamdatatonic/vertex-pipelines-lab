from typing import Optional

# Import KFP + Vertex SDK
from kfp.v2 import compiler
from google.cloud import aiplatform

# Import variables from variables.py
import variables

# Import pipeline definition
from pipeline import pipeline as pipeline_func

def compile():
    """
    Uses the kfp compiler package to compile the pipeline function into a workflow yaml
    Args:
        None
    Returns:
        None
    """
    compiler.Compiler().compile(
        pipeline_func=pipeline_func,
        package_path="pipeline.json",
        type_check=False,
    )

def trigger_pipeline(
    project_id: str,
    location: str,
    template_path: str,
    pipeline_root: str,
    service_account: str,
    parameter_values: dict = {},
    encryption_spec_key_name: Optional[str] = None,
    network: Optional[str] = None,
    enable_caching: Optional[bool] = None,
) -> aiplatform.PipelineJob:
    """Trigger the Vertex Pipeline run.
    Args:
        project_id (str): GCP Project ID in which to run the Vertex Pipeline
        location (str): GCP region in which to run the Vertex Pipeline
        template_path (str): local or GCS path containing the (JSON) KFP
        pipeline definition
        pipeline_root (str): GCS path to use as the pipeline root (for passing
         metadata/artifacts within the pipeline)
        parameter_values (dict): dictionary containing the input parameters
        for the KFP pipeline
        service_account (str): email address of the service account that
        should be used to execute the ML pipeline in Vertex
        encryption_spec_key_name (Optional[str]): Cloud KMS resource ID
        of the customer managed encryption key (CMEK) that will protect the job
        network (Optional[str]): name of Compute Engine network to
        which the job should be visible
        enable_caching (Optional[bool]): Whether to enable caching of pipeline
        component results if component+inputs are the same. Defaults to None
        (enable caching, except where disabled at a component level)
    """

    # Initialise API client
    aiplatform.init(project=project_id, location=location)

    # Instantiate PipelineJob object
    pl = aiplatform.pipeline_jobs.PipelineJob(
        display_name=template_path,
        enable_caching=enable_caching,
        template_path=template_path,
        parameter_values=parameter_values,
        pipeline_root=pipeline_root,
        encryption_spec_key_name=encryption_spec_key_name,
    )

    # Execute pipeline in Vertex
    pl.submit(
        service_account=service_account,
        network=network,
    )

    return pl

if __name__ == "__main__":
    # First compile the pipeline to pipeline.json
    compile()
    
    # Trigger the compiled pipeline on Vertex Pipelines
    trigger_pipeline(
        project_id=variables.GCP_PROJECT,
        location=variables.GCP_LOCATION,
        template_path="pipeline.json",
        pipeline_root=f"{variables.GCS_BUCKET}/vertex_root",
        service_account=variables.VERTEX_SA_EMAIL,
        parameter_values=variables.PIPELINE_PARAMETERS,
        # encryption_spec_key_name=None,
        # network=None,
        enable_caching=False,
    )
