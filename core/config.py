from pathlib import Path
import yaml
import structlog

log = structlog.get_logger()
DEFAULT_CONFIG_PATH = Path(__file__).parent.parent / "config" / "sources.yaml"


def load_sources(config_path=None, active_only=True):
    path = config_path or DEFAULT_CONFIG_PATH
    with open(path) as f:
        data = yaml.safe_load(f)
    sources = data.get("sources", [])
    if active_only:
        sources = [s for s in sources if s.get("active", False)]
    return sources


def get_source(source_id, config_path=None):
    sources = load_sources(config_path, active_only=False)
    for s in sources:
        if s["id"] == source_id:
            return s
    return None
