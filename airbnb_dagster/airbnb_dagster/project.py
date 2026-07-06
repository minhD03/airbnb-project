from pathlib import Path

from dagster_dbt import DbtProject

airbnb_dbt_project = DbtProject(
    # # Local path to the dbt project
    # project_dir=Path(__file__).joinpath("..", "..", "..", "airbnb_dbt").resolve(),
    # packaged_project_dir=Path(__file__).joinpath("..", "..", "dbt-project").resolve(),
    # Docker path to the dbt project
    project_dir=Path(__file__).joinpath("..", "..", "airbnb_dbt").resolve(),
    packaged_project_dir=Path(__file__).joinpath("..", "dbt-project").resolve(),
)
airbnb_dbt_project.prepare_if_dev()
