"""Input directories name real host paths, mounted identically in the container.

Compose mounts each input filesystem at the same path on both sides (the
``x-input-volumes`` anchor), so ``sources.<name>.input.dir`` can be the path the
data actually has. These tests pin the three properties that makes possible:
the value is taken verbatim, an env var can override it per source, and a path
that could not resolve the same on both sides is refused.
"""

import json
import os
import re
import sys
from pathlib import Path
from unittest import mock

import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from config import Config  # noqa: E402

REPO_ROOT = Path(__file__).parent.parent
COMPOSE_FILES = (
    "docker-compose.yaml",
    "docker-compose-dev.yaml",
    "docker-compose-beta-1.yaml",
)
INPUT_SERVICES = (
    "producer",
    "worker1",
    "worker2",
    "worker-light1",
    "worker-light2",
    "worker-light3",
)


def _service_blocks(text: str) -> dict[str, str]:
    """Map service name -> its own body, bounded by the next service.

    Bounding matters: an unbounded search for a service's `volumes:` line will
    happily match a *later* service's, so a mis-wired service would pass. That
    is exactly the defect this module's anchor test exists to catch, so it must
    not be able to slip through the test itself.
    """
    services = re.search(r"^services:\n(.*)", text, re.S | re.M)
    if not services:
        return {}
    body = services.group(1)
    found: dict[str, str] = {}
    for match in re.finditer(
        r"^  ([a-z0-9-]+):\n(.*?)(?=^  [a-z0-9-]+:|^volumes:|^networks:|\Z)",
        body,
        re.S | re.M,
    ):
        found[match.group(1)] = match.group(2)
    return found


@pytest.fixture
def env_vars():
    """The minimum environment Config needs to boot."""
    return {
        "LOG_LEVEL": "INFO",
        "DATA_DIR": "/app/data",
        "JOB_TTL_MINUTES": "60",
        "S3_TILES_DATA_ENDPOINT": "seaweedfs:8333",
        "S3_TILES_DATA_BUCKET_NAME": "tiles-data",
        "S3_TILES_DATA_TILES_PROCESSOR_USER": "u",
        "S3_TILES_DATA_TILES_PROCESSOR_PASSWORD": "p",
        "RABBITMQ_HOST": "rabbitmq",
        "RABBITMQ_PORT": "5672",
        "RABBITMQ_USER": "u",
        "RABBITMQ_PASSWORD": "p",
        "RABBITMQ_QUEUE": "q",
        "RABBITMQ_DLX": "dlx",
        "RABBITMQ_DLQ": "dlq",
        "RABBITMQ_RADAR_LIGHT_QUEUE": "rq",
        "RABBITMQ_WRF_LIGHT_QUEUE": "wq",
    }


def _config(tmp_path, env, input_cfg, extra_env=None):
    settings = {
        "timezone": "UTC",
        "bounds": {"minx": -90, "miny": -60, "maxx": -30, "maxy": -15},
        "sources": {"radar-sinarame": {"input": input_cfg}},
    }
    path = tmp_path / "settings.json"
    path.write_text(json.dumps(settings))
    with mock.patch.dict(os.environ, {**env, **(extra_env or {})}, clear=True):
        return Config(settings_path=path)


def test_dir_is_taken_verbatim(tmp_path, env_vars):
    """A real host path survives into input_dir unchanged.

    This is the whole point of the identity mount: no prefixing, no rewriting
    into /app/data, no translation of any kind.
    """
    config = _config(
        tmp_path, env_vars, {"mode": "local", "dir": "/mnt/nfs/smn/radar-realtime"}
    )
    assert config.RADAR_INPUT.input_dir == "/mnt/nfs/smn/radar-realtime"


def test_dir_defaults_to_data_dir_per_source(tmp_path, env_vars):
    """Omitting dir keeps the previous behaviour, so nothing breaks on upgrade."""
    config = _config(tmp_path, env_vars, {"mode": "local"})
    assert config.RADAR_INPUT.input_dir == "/app/data/radar-sinarame"


def test_env_var_overrides_settings_per_source(tmp_path, env_vars):
    """<PREFIX>_INPUT_DIR wins, per the project's env-over-settings convention."""
    config = _config(
        tmp_path,
        env_vars,
        {"mode": "local", "dir": "/mnt/nfs/smn/radar"},
        {"RADAR_SINARAME_INPUT_DIR": "/srv/override/radar"},
    )
    assert config.RADAR_INPUT.input_dir == "/srv/override/radar"


@pytest.mark.parametrize("empty", ["", "   "])
def test_empty_env_var_is_discarded(tmp_path, env_vars, empty):
    """An empty variable means "not set", matching compose's `:-` fallback.

    .env ships all six keys present but blank, so a blank one must fall through
    to settings.json rather than being taken literally — an empty input_dir
    would make the source read the container's working directory.
    """
    config = _config(
        tmp_path,
        env_vars,
        {"mode": "local", "dir": "/mnt/nfs/smn/radar"},
        {"RADAR_SINARAME_INPUT_DIR": empty},
    )
    assert config.RADAR_INPUT.input_dir == "/mnt/nfs/smn/radar"


def test_empty_env_var_and_no_setting_falls_back_to_the_default(tmp_path, env_vars):
    """Blank everywhere still lands on ${DATA_DIR}/<source-name>."""
    config = _config(
        tmp_path, env_vars, {"mode": "local"}, {"RADAR_SINARAME_INPUT_DIR": ""}
    )
    assert config.RADAR_INPUT.input_dir == "/app/data/radar-sinarame"


def test_conflicting_env_and_settings_is_reported(tmp_path, env_vars, caplog):
    """settings.json says the mode, .env says where — when both name a path, say so.

    Env silently winning is how "I changed settings.json and nothing happened"
    becomes a debugging session.
    """
    with caplog.at_level("WARNING"):
        config = _config(
            tmp_path,
            env_vars,
            {"mode": "local", "dir": "/mnt/from-settings"},
            {"RADAR_SINARAME_INPUT_DIR": "/mnt/from-env"},
        )
    assert config.RADAR_INPUT.input_dir == "/mnt/from-env"
    assert config.RADAR_INPUT.input_dir_origin == "env"
    message = caplog.text
    assert "/mnt/from-settings" in message and "/mnt/from-env" in message
    assert "environment wins" in message


def test_matching_env_and_settings_is_quiet(tmp_path, env_vars, caplog):
    """Agreeing values are not a conflict; warning on them would train people to ignore it."""
    with caplog.at_level("WARNING"):
        _config(
            tmp_path,
            env_vars,
            {"mode": "local", "dir": "/mnt/same"},
            {"RADAR_SINARAME_INPUT_DIR": "/mnt/same"},
        )
    assert "environment wins" not in caplog.text


def test_dir_configured_for_a_non_local_mode_is_reported(tmp_path, env_vars, caplog):
    """A path on an s3/provider source is ignored, which should never be silent."""
    with caplog.at_level("WARNING"):
        _config(
            tmp_path,
            env_vars,
            {"mode": "s3", "s3_bucket": "radar-input", "dir": "/mnt/ignored"},
        )
    assert "reads no folder" in caplog.text
    assert "/mnt/ignored" in caplog.text


@pytest.mark.parametrize("state", ["missing", "empty"])
def test_unusable_local_dir_warns_at_startup(tmp_path, env_vars, caplog, state):
    """A local source pointed at nothing must say so at boot, not just tick quietly."""
    target = tmp_path / "feed"
    if state == "empty":
        target.mkdir()
    config = _config(tmp_path, env_vars, {"mode": "local", "dir": str(target)})
    with caplog.at_level("WARNING"):
        config.log_config()
    # Scoped to this source: the other five default to paths that do not exist
    # on a test host and warn for the same (correct) reason.
    line = next(r for r in caplog.records if str(target) in r.getMessage())
    assert "find nothing" in line.getMessage()
    assert state.upper() in line.getMessage() or state in line.getMessage()


def test_usable_local_dir_does_not_warn(tmp_path, env_vars, caplog):
    """A folder with files in it is the normal case and must stay quiet."""
    target = tmp_path / "feed"
    target.mkdir()
    (target / "a.H5").write_text("x")
    config = _config(tmp_path, env_vars, {"mode": "local", "dir": str(target)})
    with caplog.at_level("WARNING"):
        config.log_config()
    assert not [r for r in caplog.records if str(target) in r.getMessage()]


@pytest.mark.parametrize("bad", ["data/radar", "./data/radar", "../radar"])
def test_relative_dir_is_refused(tmp_path, env_vars, bad):
    """A relative path resolves against the container's cwd and silently misses.

    A source reading the wrong place looks exactly like one with no new data, so
    this has to fail at startup rather than at the first empty tick.
    """
    with pytest.raises(ValueError, match="must be an absolute path"):
        _config(tmp_path, env_vars, {"mode": "local", "dir": bad})


def test_trailing_slash_is_normalized(tmp_path, env_vars):
    """`/mnt/x/` and `/mnt/x` must not produce two different repositories."""
    config = _config(tmp_path, env_vars, {"mode": "local", "dir": "/mnt/nfs/radar/"})
    assert config.RADAR_INPUT.input_dir == "/mnt/nfs/radar"


@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_every_input_service_shares_the_input_volumes_anchor(compose_file):
    """The anchor is what makes "add one line per filesystem" true.

    If a service stopped referencing it, adding an identity mount would leave
    that worker reading an empty directory — and only that worker, which is the
    hardest version of this failure to spot.
    """
    text = (REPO_ROOT / compose_file).read_text()
    assert (
        "x-input-volumes: &input-volumes" in text
    ), f"{compose_file} has no input-volumes anchor to add mounts to"
    blocks = _service_blocks(text)
    present = [svc for svc in INPUT_SERVICES if svc in blocks]
    assert present, f"{compose_file} defines none of {INPUT_SERVICES}"
    missing = [
        svc
        for svc in present
        if not re.search(r"^    volumes: \*input-volumes$", blocks[svc], re.M)
    ]
    assert not missing, (
        f"{compose_file}: these services read source data but do not use "
        f"*input-volumes, so identity mounts would not reach them: {missing}"
    )


def _is_identity_mount(line: str) -> bool:
    """True when a volume line mounts a path onto itself, read-only.

    Checked by splitting rather than by regex: the fallback carries its own
    ${...}, and a pattern that tries to match balanced braces twice is easier to
    get subtly wrong than the thing it is guarding.
    """
    spec = line.removeprefix("- ").removesuffix(":ro")
    if not line.endswith(":ro"):
        return False
    middle, remainder = divmod(len(spec) - 1, 2)
    if remainder or spec[middle] != ":":
        return False
    return spec[:middle] == spec[middle + 1 :]


def _env_prefix(source_name: str) -> str:
    """The env prefix for a source, matching what config.py passes."""
    return source_name.upper().replace("-", "_")


def _sources_by_mode() -> tuple[list[str], list[str]]:
    """(local sources, everything else) from the shipped settings."""
    sources = json.loads((REPO_ROOT / "settings.json").read_text())["sources"]
    local, other = [], []
    for name, block in sources.items():
        (local if block["input"]["mode"] == "local" else other).append(name)
    return local, other


@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_local_sources_have_an_identity_mount(compose_file):
    """Every source that reads a folder gets one, in identity form.

    Derived from settings.json, so flipping a source to "local" fails here until
    its mount line is added — rather than at its first silently empty tick.
    """
    text = (REPO_ROOT / compose_file).read_text()
    local, _ = _sources_by_mode()
    assert local, "settings.json has no local sources to check"
    missing = []
    for name in local:
        prefix = _env_prefix(name)
        line = next(
            (
                ln.strip()
                for ln in text.splitlines()
                if ln.strip().startswith(f"- ${{{prefix}_INPUT_DIR")
            ),
            None,
        )
        if line is None or not _is_identity_mount(line):
            missing.append(f"{prefix}_INPUT_DIR")
    assert not missing, f"{compose_file} has no identity mount for: {missing}"


@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_non_local_sources_have_no_mount(compose_file):
    """A source that reads S3 or a provider must not require a folder variable.

    Mounting one anyway forces an operator to invent a path for a directory
    nothing will ever open, purely so compose does not choke on `::ro`. That is
    overhead with no payoff, and it is how the variable list stops meaning
    anything.
    """
    text = (REPO_ROOT / compose_file).read_text()
    _, other = _sources_by_mode()
    assert other, "settings.json has no non-local sources to check"
    stray = [
        f"{_env_prefix(name)}_INPUT_DIR"
        for name in other
        if f"${{{_env_prefix(name)}_INPUT_DIR}}" in text
    ]
    assert not stray, (
        f"{compose_file} references input dirs for sources that read no folder: "
        f"{stray}"
    )


@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_input_mounts_carry_no_default(compose_file):
    """.env is the single place a path is decided.

    A `${VAR:-...}` fallback would put a second, invisible answer in the compose
    file, so an operator who cleared the variable would silently get the old
    location instead of an error.
    """
    text = (REPO_ROOT / compose_file).read_text()
    defaulted = re.findall(r"\$\{([A-Z0-9_]+_INPUT_DIR):-", text)
    assert not defaulted, (
        f"{compose_file}: these mounts carry a default, so .env is no longer "
        f"the only place the path is set: {sorted(set(defaulted))}"
    )


@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_input_mounts_are_enabled(compose_file):
    """The mounts ship live, so .env is the only switch."""
    text = (REPO_ROOT / compose_file).read_text()
    commented = re.findall(r"^  # - \$\{([A-Z0-9_]+_INPUT_DIR)", text, re.M)
    assert not commented, (
        f"{compose_file}: these mounts are commented out, so setting their "
        f"variable in .env would silently do nothing: {commented}"
    )


@pytest.mark.parametrize("compose_file", COMPOSE_FILES)
def test_input_vars_reach_the_containers(compose_file):
    """Mounting the path is only half of it — the app has to be told as well.

    A container only sees the variables its `environment:` block lists. Mount it
    but omit the variable and the bind is there while the app keeps reading
    ${DATA_DIR}/<source-name>: the data is present and invisible, which reads
    exactly like a source with no new files.
    """
    text = (REPO_ROOT / compose_file).read_text()
    local, _ = _sources_by_mode()
    blocks = _service_blocks(text)
    problems = []
    for svc in INPUT_SERVICES:
        body = blocks.get(svc)
        if body is None or "volumes: *input-volumes" not in body:
            continue
        for name in local:
            var = f"{_env_prefix(name)}_INPUT_DIR"
            if f"- {var}=${{{var}}}" not in body:
                problems.append(f"{svc}:{var}")
    assert not problems, (
        f"{compose_file}: mounted but not passed to the app, so the files are "
        f"present and unread: {problems}"
    )


def test_env_example_uses_absolute_paths():
    """The shipped example must not make the path depend on where you stood.

    `${PWD}` is interpolated from the shell's working directory, not the compose
    file's, so `docker compose -f tiles-processor/... ` run from the parent would
    silently bind a different tree and every local source would read nothing.
    """
    text = (REPO_ROOT / ".env.example").read_text()
    assigned = re.findall(r"^([A-Z0-9_]+_INPUT_DIR)=(.*)$", text, re.M)
    assert assigned, ".env.example declares no input directories"
    bad = [f"{k}={v}" for k, v in assigned if not v.startswith("/")]
    assert not bad, (
        f".env.example must use absolute paths; these are relative or "
        f"interpolated: {bad}"
    )


@pytest.mark.parametrize("settings_name", ["settings.json", "settings-beta-1.json"])
def test_shipped_settings_use_absolute_dirs(settings_name):
    """Whatever ships must already satisfy the absolute-path rule."""
    settings = json.loads((REPO_ROOT / settings_name).read_text())
    relative = {
        name: block["input"]["dir"]
        for name, block in settings["sources"].items()
        if "dir" in (block.get("input") or {})
        and not block["input"]["dir"].startswith("/")
    }
    assert not relative, f"{settings_name} has relative input dirs: {relative}"
