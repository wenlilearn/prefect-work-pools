# Lab 103 Instructions:
# In the UI, make an email automation that fires when flow runs complete
# Then toggle it off
# See the event feed in the UI
# Create a Markdown artifact that prints a weather forecast in a nicely formatted table
# Stretch 1: Create a flow that contains a task that uses caching
# Stretch 2: Change the cache policy, does the task run or not run as you expect?

import httpx
from prefect import flow, task, runtime
from prefect.variables import Variable
from prefect.logging import get_run_logger
from prefect.cache_policies import INPUTS
from prefect.artifacts import create_markdown_artifact


@task(log_prints=True, persist_result=True, cache_policy=INPUTS)
def fetch_weather(lat: float = 38.9, lon: float = -77.0):
    """
    Fetches the weather forecast for a given latitude and longitude.
    """
    logger = get_run_logger()
    logger.info(f"Fetching weather data from {runtime.task_run.parameters}")

    # raise ValueError("Forced failure for testing purposes.")
    url = f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}&hourly=temperature_2m"
    response = httpx.get(url)
    response.raise_for_status()
    data = response.json()
    print(data)
    forecasted_temp = data["hourly"]["temperature_2m"][0]
    print("Forecasted temperature:", forecasted_temp)
    Variable.set("forecasted_temp", forecasted_temp, overwrite=True)
    return forecasted_temp


@task
def save_weather(temp):
    logger = get_run_logger()
    logger.info(f'The current flow_run_id is: {runtime.flow_run.name}')
    logger.info(f'The current task_run_id is: {runtime.task_run.name}')
    logger.info(f'The current temp is: {temp}')

    return temp


@flow
def pipeline(lat=38.9, lon=-77.0):
    """
    Main pipeline function to fetch and save weather data.
    """
    logger = get_run_logger()
    logger.info("Starting the weather pipeline...")
    logger.info("The current flow_run_id is: %s", runtime.flow_run.name)

    temp = fetch_weather(lat, lon)
    result = save_weather(temp)

    return result


if __name__ == "__main__":
    pipeline.from_source(
        source="https://github.com/wenlilearn/prefect-work-pools.git",
        entrypoint="104.py:pipeline"
    ).deploy(
        name="test-pipeline",
        work_pool_name="Test"
    )
