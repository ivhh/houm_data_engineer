from marshmallow_dataclass import dataclasses


@dataclasses.dataclass()
class RestOperator:
    url: str
    method: str
    path_vars: set = dataclasses.field(default_factory=set)
    required_params: set = dataclasses.field(default_factory=set)
    allowed_params: set = dataclasses.field(default_factory=set)
    required_headers: set = dataclasses.field(default_factory=set)
    allowed_headers: set = dataclasses.field(default_factory=set)
