import yaml
from cmd_slack.format.schema import SetupConfig

def load_setup(path: str | Path) -> SetupConfig:
    with open(path) as f:
        raw = yaml.safe_load(f)
    return SetupConfig(**raw)

def load_all_setups(directory: str | Path) -> list[SetupConfig]:
    directory = Path(directory)
    return [
        load_setup(f)
        for f in directory.glob("*.yaml")
    ]