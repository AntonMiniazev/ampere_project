from datetime import date
from importlib.resources import files

import yaml
from pydantic import BaseModel, ConfigDict, Field, NonNegativeInt


class BootstrapPolicy(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    n_of_init_clients: NonNegativeInt
    n_delivery_resource: NonNegativeInt
    project_start_date: date = Field(default_factory=date.today)


def load_policy(
    *,
    n_of_init_clients: int | None = None,
    n_delivery_resource: int | None = None,
    project_start_date: date | None = None,
) -> BootstrapPolicy:
    resource = files("init_source_preparation") / "bootstrap_defaults.yaml"
    defaults = yaml.safe_load(resource.read_text(encoding="utf-8"))
    overrides = {
        "n_of_init_clients": n_of_init_clients,
        "n_delivery_resource": n_delivery_resource,
        "project_start_date": project_start_date,
    }
    return BootstrapPolicy.model_validate(
        {
            **defaults,
            **{key: value for key, value in overrides.items() if value is not None},
        }
    )
