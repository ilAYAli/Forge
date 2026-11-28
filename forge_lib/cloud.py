"""Ephemeral cloud workers: ``--workers aws:64`` adds 64 cloud cores to a run.

A cloud pool is an entry in the coordinator's ``~/.config/forge/workers.json``::

    {
      "host": "aws",
      "cloud": {"profile": "forge", "region": "eu-north-1", "family": "c7a", "market": "on-demand"},
      "cores": 64,
      "enabled": false
    }

``--workers aws`` adds ``cores`` cloud cores to a run, ``aws:16`` sixteen and
``aws:max`` what the quota allows.  With ``"enabled": true`` every run that
uses the default workers gets ``cores`` cloud cores without asking.

Instances are reached over Tailscale (no inbound ports), join a run through
``forge worker set NAME=linux:max RUN`` once provisioned, and are terminated
when the run ends.  Each instance also powers itself off (and is thereby
terminated) after ``idle_minutes`` without Forge processes, and after
``max_hours`` regardless, so a lost coordinator never leaves one running.

Engines are built with ``-march=native``, so the coordinator's binaries may not
run on the cloud CPU.  Every ``enyo_<commit>`` an engine link points at is
built once on a cloud worker with the engine repo's ``scripts/deploy.sh``,
cached per instance family, and copied to later workers of that family.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import fcntl
import json
import os
import re
import shlex
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable

from .config import cache_dir, config_workers_path
from .core import expand_path, read_json, run_dir_for_name, script_path, write_json

CLOUD_TAG = "forge-cloud"
RUN_TAG = "forge-run"
POOL_TAG = "forge-pool"
SG_NAME = "forge-cloud"
READY_FILE = "/var/lib/forge-cloud-ready"
CANONICAL_OWNER = "099720109477"
# 24.04 LTS: the 26.04 EC2 images never reached instance metadata (2026-11-27).
# Enyo needs glibc >= 2.38; 24.04 ships 2.39.
UBUNTU_IMAGE = "ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"
QUOTA_CODES = {"on-demand": "L-1216C47A", "spot": "L-34B43A08"}
LIVE_STATES = "pending,running"
SSH_OPTS = [
    "-o", "BatchMode=yes",
    "-o", "ConnectTimeout=10",
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "LogLevel=ERROR",
]
ENTRY_RE = re.compile(r"^(?P<pool>[A-Za-z][\w-]*)(?::(?P<cores>\d+|max))?$")
CLOUD_KEYS = ("profile", "region", "family", "market", "worker_profile", "user", "tailscale_key",
              "engine_repo", "engine_net")
ENGINE_COMMANDS = ("candidate", "reference")
CLOUD_INT_KEYS = ("max_hours", "idle_minutes", "boot_timeout_seconds", "llvm_version")


@dataclass
class Pool:
    name: str
    profile: str = "forge"
    region: str = "eu-north-1"
    family: str = "c7a"
    market: str = "on-demand"
    worker_profile: str = "linux"
    max_hours: int = 12
    idle_minutes: int = 15
    boot_timeout_seconds: int = 600
    user: str = field(default_factory=lambda: os.environ.get("USER") or "forge")
    tailscale_key: str = "~/.config/forge/tailscale.key"
    sync: list[str] = field(default_factory=lambda: ["~/.cache/forge/tools"])
    engines: list[str] = field(default_factory=lambda: ["~/assets/engines/candidate", "~/assets/engines/reference"])
    engine_repo: str = "~/code/chess/enyo"
    engine_net: str = "~/assets/nets/reference.net"
    llvm_version: int = 21
    cores: int | str | None = None  # an int, "max" (the free quota) or unset
    enabled: bool = False

    @property
    def state_dir(self) -> Path:
        return cache_dir() / "cloud"


def _words(value: Any) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(item) for item in value]
    return str(value).split()


def is_cloud_record(item: Any) -> bool:
    return isinstance(item, dict) and isinstance(item.get("cloud"), dict)


def pool_from_record(item: dict[str, Any]) -> Pool:
    name = str(item.get("host") or "").strip()
    if not ENTRY_RE.match(name) or ":" in name:
        raise SystemExit(f"cloud: invalid pool name {name!r}")
    raw = item["cloud"]
    provider = str(raw.get("provider") or "aws")
    if provider != "aws":
        raise SystemExit(f"cloud: {name}: unsupported provider {provider!r} (only aws)")
    pool = Pool(name=name)
    for key in CLOUD_KEYS:
        if raw.get(key) not in (None, ""):
            setattr(pool, key, str(raw[key]))
    for key in CLOUD_INT_KEYS:
        if raw.get(key) not in (None, ""):
            setattr(pool, key, int(raw[key]))
    for key in ("sync", "engines"):
        words = _words(raw.get(key))
        if words is not None:
            setattr(pool, key, words)
    if pool.market not in QUOTA_CODES:
        raise SystemExit(f"cloud: {name}: market must be on-demand or spot, got {pool.market!r}")
    if item.get("cores") is not None:
        raw_cores = str(item["cores"]).strip().lower()
        if raw_cores == "max":
            pool.cores = "max"
        elif raw_cores.isdigit():
            pool.cores = int(raw_cores)
        else:
            raise SystemExit(f"cloud: {name}: cores must be a number or \"max\", got {item['cores']!r}")
    pool.enabled = bool(item.get("enabled", True))
    return pool


def cloud_pools(path: Path | None = None) -> dict[str, Pool]:
    """Cloud pools listed in a workers file (default: the coordinator's)."""
    path = config_workers_path() if path is None else path
    try:
        payload = read_json(path)
    except (OSError, ValueError):
        return {}
    entries = payload.get("workers") if isinstance(payload, dict) else None
    pools: dict[str, Pool] = {}
    for item in entries if isinstance(entries, list) else []:
        if is_cloud_record(item):
            pool = pool_from_record(item)
            pools[pool.name] = pool
    return pools


def pool_request(pool: Pool) -> int | None:
    """The cores a bare pool name asks for; None means as many as the quota allows."""
    return None if pool.cores == "max" else int(pool.cores or 0)


def parse_cloud_entry(entry: str, pools: dict[str, Pool]) -> tuple[str, int | None] | None:
    """``aws:64`` -> ("aws", 64); ``aws:max`` -> ("aws", None); ``aws`` -> the pool's
    configured cores; other specs -> None."""
    match = ENTRY_RE.match(entry.strip())
    if not match or match["pool"] not in pools:
        return None
    cores = match["cores"]
    if cores is None:
        pool = pools[match["pool"]]
        if pool.cores is None:
            raise SystemExit(f"cloud: {pool.name}: set \"cores\" in workers.json or ask for {pool.name}:N")
        return pool.name, pool_request(pool)
    return match["pool"], None if cores == "max" else int(cores)


def split_cloud_entries(
    value: str | None,
    pools: dict[str, Pool] | None = None,
) -> tuple[str | None, list[tuple[str, int | None]]]:
    """Separate cloud pool entries from an inline ``--workers`` list.

    Returns the remaining inline list (None when only cloud entries were given,
    meaning the default workers) and the cloud requests.
    """
    if value is None:
        return value, []
    pools = cloud_pools() if pools is None else pools
    if not pools:
        return value, []
    rest: list[str] = []
    requests: list[tuple[str, int | None]] = []
    for entry in value.split(","):
        parsed = parse_cloud_entry(entry, pools)
        if parsed is None:
            rest.append(entry)
        else:
            requests.append(parsed)
    if not requests:
        return value, []
    return (",".join(rest) if rest else None), requests


# --- AWS ---------------------------------------------------------------------

class CloudError(RuntimeError):
    pass


def aws(pool: Pool, *args: str, parse: bool = True, timeout: float = 180) -> Any:
    env = {**os.environ, "AWS_PROFILE": pool.profile, "AWS_REGION": pool.region, "AWS_PAGER": ""}
    command = ["aws", *args] + (["--output", "json"] if parse else [])
    try:
        result = subprocess.run(command, env=env, capture_output=True, text=True, timeout=timeout)
    except FileNotFoundError:
        raise CloudError("the aws CLI is not installed on this host") from None
    except subprocess.TimeoutExpired:
        raise CloudError(f"aws {' '.join(args[:2])}: timed out") from None
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip().splitlines()
        raise CloudError(f"aws {' '.join(args[:2])}: {detail[-1] if detail else result.returncode}")
    if not parse:
        return result.stdout
    return json.loads(result.stdout) if result.stdout.strip() else None


def instance_types(pool: Pool) -> dict[str, dict[str, int]]:
    """{type: {"vcpus", "cores"}} for the pool's family, cached for a week."""
    path = pool.state_dir / f"types-{pool.region}-{pool.family}.json"
    try:
        if time.time() - path.stat().st_mtime < 7 * 86400:
            return read_json(path)["types"]
    except (OSError, KeyError, ValueError):
        pass
    data = aws(pool, "ec2", "describe-instance-types", "--filters",
               f"Name=instance-type,Values={pool.family}.*", "Name=bare-metal,Values=false")
    types = {
        item["InstanceType"]: {
            "vcpus": int(item["VCpuInfo"]["DefaultVCpus"]),
            "cores": int(item["VCpuInfo"].get("DefaultCores") or item["VCpuInfo"]["DefaultVCpus"]),
        }
        for item in data.get("InstanceTypes", [])
    }
    if not types:
        raise CloudError(f"no {pool.family} instance types in {pool.region}")
    path.parent.mkdir(parents=True, exist_ok=True)
    write_json(path, {"types": types})
    return types


def plan_instances(cores: int, types: dict[str, dict[str, int]]) -> list[str]:
    """Fewest instances covering ``cores``: largest sizes first, rounded up."""
    sizes = sorted(types.items(), key=lambda item: item[1]["cores"], reverse=True)
    plan: list[str] = []
    remaining = cores
    while remaining > 0:
        fitting = [name for name, info in sizes if info["cores"] <= remaining]
        if fitting:
            name = fitting[0]
        else:
            name = min(sizes, key=lambda item: item[1]["cores"])[0]
        plan.append(name)
        remaining -= types[name]["cores"]
    return plan


def free_vcpus(pool: Pool) -> int:
    """vCPUs the account may still start in this market."""
    quota = aws(pool, "service-quotas", "get-service-quota", "--service-code", "ec2",
                "--quota-code", QUOTA_CODES[pool.market])
    limit = int(float(quota["Quota"]["Value"]))
    market_filter = ["Name=instance-lifecycle,Values=spot"] if pool.market == "spot" else []
    data = aws(pool, "ec2", "describe-instances", "--filters",
               f"Name=instance-state-name,Values={LIVE_STATES}", *market_filter,
               "--query", "Reservations[].Instances[].[CpuOptions.CoreCount,CpuOptions.ThreadsPerCore,InstanceLifecycle]")
    used = 0
    for core_count, threads, lifecycle in data or []:
        if pool.market == "on-demand" and lifecycle == "spot":
            continue
        used += int(core_count or 0) * int(threads or 1)
    return max(0, limit - used)


@dataclass
class Instance:
    id: str
    name: str
    type: str
    state: str
    run: str
    lifecycle: str
    launched: str


def list_instances(pool: Pool, run: str | None = None, *, states: str = LIVE_STATES) -> list[Instance]:
    filters = [f"Name=tag-key,Values={CLOUD_TAG}", f"Name=tag:{POOL_TAG},Values={pool.name}",
               f"Name=instance-state-name,Values={states}"]
    if run is not None:
        filters.append(f"Name=tag:{RUN_TAG},Values={run}")
    data = aws(pool, "ec2", "describe-instances", "--filters", *filters,
               "--query", "Reservations[].Instances[]")
    instances = []
    for item in data or []:
        tags = {tag["Key"]: tag["Value"] for tag in item.get("Tags") or []}
        instances.append(Instance(
            id=item["InstanceId"],
            name=tags.get("Name") or instance_name(item["InstanceId"]),
            type=item["InstanceType"],
            state=item["State"]["Name"],
            run=tags.get(RUN_TAG, ""),
            lifecycle=item.get("InstanceLifecycle") or "on-demand",
            launched=item.get("LaunchTime", ""),
        ))
    return sorted(instances, key=lambda item: item.launched)


def instance_name(instance_id: str) -> str:
    # Matches the hostname the instance gives itself in user_data().
    return f"aws-{instance_id[-8:]}"


def terminate(pool: Pool, ids: list[str]) -> None:
    if ids:
        aws(pool, "ec2", "terminate-instances", "--instance-ids", *ids)


# --- Coordinator-side setup --------------------------------------------------

def ssh_conf_path() -> Path:
    return expand_path("~/.ssh/forge-cloud.conf")


def ensure_ssh_include() -> None:
    ssh_dir = expand_path("~/.ssh")
    ssh_dir.mkdir(mode=0o700, exist_ok=True)
    conf = ssh_conf_path()
    conf.touch(mode=0o600, exist_ok=True)
    main = ssh_dir / "config"
    line = f"Include {conf}"
    text = main.read_text(encoding="utf-8") if main.exists() else ""
    if line not in text.splitlines():
        # Include must precede the first Host block to apply to every host.
        tmp = main.with_suffix(".forge-tmp")
        tmp.write_text(f"{line}\n{text}", encoding="utf-8")
        tmp.chmod(0o600)
        tmp.replace(main)


_ssh_conf_lock = threading.Lock()


def set_ssh_host(name: str, ip: str | None, user: str = "") -> None:
    """Add (or with ip=None remove) a cloud host in ~/.ssh/forge-cloud.conf."""
    conf = ssh_conf_path()
    with _ssh_conf_lock:
        text = conf.read_text(encoding="utf-8") if conf.exists() else ""
        text = re.sub(rf"(?ms)^# {re.escape(name)}\n.*?^# end {re.escape(name)}\n", "", text)
        if ip is not None:
            text += (
                f"# {name}\nHost {name}\n    HostName {ip}\n    User {user}\n"
                "    StrictHostKeyChecking no\n    UserKnownHostsFile /dev/null\n    LogLevel ERROR\n"
                f"# end {name}\n"
            )
        if conf.exists() or ip is not None:
            conf.write_text(text, encoding="utf-8")


def worker_key() -> Path:
    """Key cloud workers use to reach the coordinator, accepted only from Tailscale."""
    key = expand_path("~/.ssh/forge_cloud")
    if not key.exists():
        subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", "forge-cloud-worker", "-f", str(key)],
                       check=True)
    public = key.with_suffix(".pub").read_text(encoding="utf-8").strip()
    authorized = expand_path("~/.ssh/authorized_keys")
    current = authorized.read_text(encoding="utf-8") if authorized.exists() else ""
    if public not in current:
        with authorized.open("a", encoding="utf-8") as handle:
            if current and not current.endswith("\n"):
                handle.write("\n")
            handle.write(f'from="100.64.0.0/10" {public}\n')
        authorized.chmod(0o600)
    return key


def coordinator_public_key() -> str:
    for name in ("id_ed25519.pub", "id_rsa.pub", "id_ecdsa.pub"):
        path = expand_path(f"~/.ssh/{name}")
        if path.exists():
            return path.read_text(encoding="utf-8").strip()
    raise CloudError("no SSH public key in ~/.ssh for the coordinator")


def tailscale_status() -> dict[str, Any]:
    try:
        result = subprocess.run(["tailscale", "status", "--json"], capture_output=True, text=True, timeout=20)
    except FileNotFoundError:
        raise CloudError("tailscale is not installed on the coordinator") from None
    if result.returncode != 0:
        raise CloudError(f"tailscale status: {result.stderr.strip()}")
    return json.loads(result.stdout)


def tailscale_peer(name: str) -> tuple[str, bool] | None:
    for peer in (tailscale_status().get("Peer") or {}).values():
        if peer.get("HostName") == name and peer.get("TailscaleIPs"):
            return str(peer["TailscaleIPs"][0]), bool(peer.get("Online"))
    return None


def security_group(pool: Pool) -> str:
    data = aws(pool, "ec2", "describe-security-groups", "--filters", f"Name=group-name,Values={SG_NAME}")
    groups = data.get("SecurityGroups") or []
    if groups:
        return str(groups[0]["GroupId"])
    # No inbound rules: Tailscale only needs outbound traffic.
    created = aws(pool, "ec2", "create-security-group", "--group-name", SG_NAME,
                  "--description", "Forge cloud workers (no inbound; Tailscale)",
                  "--tag-specifications", f"ResourceType=security-group,Tags=[{{Key={CLOUD_TAG},Value=1}}]")
    return str(created["GroupId"])


def image_id(pool: Pool) -> str:
    path = pool.state_dir / f"ami-{pool.region}"
    try:
        if time.time() - path.stat().st_mtime < 7 * 86400:
            return path.read_text(encoding="utf-8").strip()
    except OSError:
        pass
    ami = aws(pool, "ec2", "describe-images", "--owners", CANONICAL_OWNER,
              "--filters", f"Name=name,Values={UBUNTU_IMAGE}", "Name=state,Values=available",
              "--query", "sort_by(Images,&CreationDate)[-1].ImageId", timeout=300)
    if not ami:
        raise CloudError(f"no Ubuntu 24.04 image in {pool.region}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(ami), encoding="utf-8")
    return str(ami)


def user_data(pool: Pool, *, authkey: str, public_key: str) -> str:
    user = shlex.quote(pool.user)
    idle = max(1, pool.idle_minutes)
    return f"""#!/bin/bash
set -eux
shutdown -h +{pool.max_hours * 60} "forge-cloud lifetime reached"
TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token -H "X-aws-ec2-metadata-token-ttl-seconds: 300")
IID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/meta-data/instance-id)
NAME=aws-${{IID: -8}}
hostnamectl set-hostname "$NAME"
id {user} >/dev/null 2>&1 || useradd -m -s /bin/bash {user}
echo "{pool.user} ALL=(ALL) NOPASSWD:ALL" > /etc/sudoers.d/90-forge-cloud
chmod 440 /etc/sudoers.d/90-forge-cloud
install -d -m 700 -o {user} -g {user} /home/{user}/.ssh
echo {shlex.quote(public_key)} > /home/{user}/.ssh/authorized_keys
chown {user}:{user} /home/{user}/.ssh/authorized_keys
chmod 600 /home/{user}/.ssh/authorized_keys
# Power off (and so terminate) once the Forge user has had no processes for
# {idle} minutes; provisioning and worker processes all run as that user.
cat > /usr/local/sbin/forge-cloud-idle <<'IDLE'
#!/bin/bash
stamp=/run/forge-cloud-busy
[ -f "$stamp" ] || touch "$stamp"
if ps -u {user} -o comm= | grep -qvE '^(systemd|\\(sd-pam\\))$'; then touch "$stamp"; fi
if [ $(( $(date +%s) - $(stat -c %Y "$stamp") )) -ge {idle * 60} ]; then
    shutdown -h now "forge-cloud idle for {idle} minutes"
fi
IDLE
chmod 755 /usr/local/sbin/forge-cloud-idle
cat > /etc/systemd/system/forge-cloud-idle.service <<'UNIT'
[Service]
Type=oneshot
ExecStart=/usr/local/sbin/forge-cloud-idle
UNIT
cat > /etc/systemd/system/forge-cloud-idle.timer <<'UNIT'
[Timer]
OnBootSec=1min
OnUnitActiveSec=1min
[Install]
WantedBy=timers.target
UNIT
systemctl daemon-reload
systemctl enable --now forge-cloud-idle.timer
export DEBIAN_FRONTEND=noninteractive
apt-get update -q
apt-get install -yq python3 git rsync make g++
# Public repositories declared with git@github.com: URLs fetch anonymously.
git config --system url."https://github.com/".insteadOf git@github.com:
curl -fsSL https://tailscale.com/install.sh | sh
tailscale up --authkey={shlex.quote(authkey)} --hostname="$NAME" --accept-dns=true
touch {READY_FILE}
"""


def launch(pool: Pool, types: list[str], run: str) -> list[Instance]:
    key_path = expand_path(pool.tailscale_key)
    if not key_path.exists() or not key_path.read_text(encoding="utf-8").strip():
        raise CloudError(f"missing Tailscale auth key: {key_path}")
    ensure_ssh_include()
    worker_key()
    sg = security_group(pool)
    ami = image_id(pool)
    script = user_data(pool, authkey=key_path.read_text(encoding="utf-8").strip(), public_key=coordinator_public_key())
    market = []
    if pool.market == "spot":
        market = ["--instance-market-options",
                  '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"one-time",'
                  '"InstanceInterruptionBehavior":"terminate"}}']
    tags = f"{{Key={CLOUD_TAG},Value=1}},{{Key={POOL_TAG},Value={pool.name}}},{{Key={RUN_TAG},Value={run}}}"
    launched: list[Instance] = []
    with tempfile.NamedTemporaryFile("w", prefix="forge-cloud-", suffix=".sh") as handle:
        handle.write(script)
        handle.flush()
        for instance_type in sorted(set(types)):
            count = types.count(instance_type)
            data = aws(
                pool, "ec2", "run-instances", "--image-id", ami, "--instance-type", instance_type,
                "--count", str(count), "--security-group-ids", sg, "--user-data", f"file://{handle.name}",
                "--instance-initiated-shutdown-behavior", "terminate",
                "--metadata-options", "HttpTokens=required",
                "--block-device-mappings",
                '[{"DeviceName":"/dev/sda1","Ebs":{"VolumeSize":16,"VolumeType":"gp3","DeleteOnTermination":true}}]',
                "--tag-specifications", f"ResourceType=instance,Tags=[{tags}]", f"ResourceType=volume,Tags=[{tags}]",
                *market,
            )
            for item in data.get("Instances") or []:
                instance = Instance(
                    id=item["InstanceId"], name=instance_name(item["InstanceId"]), type=instance_type,
                    state="pending", run=run, lifecycle=pool.market, launched=item.get("LaunchTime", ""),
                )
                aws(pool, "ec2", "create-tags", "--resources", instance.id, "--tags", f"Key=Name,Value={instance.name}",
                    parse=False)
                launched.append(instance)
    return launched


# --- Worker-side provisioning ------------------------------------------------

def ssh(host: str, command: str, *, input_text: str | None = None, timeout: float = 1800,
        check: bool = True) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(["ssh", *SSH_OPTS, host, command], input=input_text, capture_output=True,
                            text=True, timeout=timeout)
    if check and result.returncode != 0:
        detail = (result.stderr or result.stdout).strip().splitlines()
        raise CloudError(f"{host}: {detail[-1] if detail else f'ssh exit {result.returncode}'}")
    return result


def rsync(sources: list[str], destination: str, *extra: str) -> None:
    result = subprocess.run(["rsync", "-a", *extra, "-e", shlex.join(["ssh", *SSH_OPTS]), *sources, destination],
                            capture_output=True, text=True, timeout=3600)
    if result.returncode != 0:
        detail = result.stderr.strip().splitlines()
        raise CloudError(f"rsync to {destination}: {detail[-1] if detail else result.returncode}")


def wait_ready(pool: Pool, instance: Instance, *, log: Callable[[str], None]) -> str:
    deadline = time.monotonic() + pool.boot_timeout_seconds
    while time.monotonic() < deadline:
        peer = tailscale_peer(instance.name)
        if peer and peer[1]:
            probe = subprocess.run(["ssh", *SSH_OPTS, f"{pool.user}@{peer[0]}", f"test -f {READY_FILE}"],
                                   capture_output=True, timeout=30)
            if probe.returncode == 0:
                return peer[0]
        time.sleep(5)
    raise CloudError(f"{instance.name}: not ready after {pool.boot_timeout_seconds}s")


_engine_locks: dict[str, threading.Lock] = {}
_engine_locks_guard = threading.Lock()


def _engine_lock(key: str) -> threading.Lock:
    with _engine_locks_guard:
        return _engine_locks.setdefault(key, threading.Lock())


def build_engine(pool: Pool, name: str, family: str, base: str, cached: Path, *, log: Callable[[str], None]) -> None:
    commit = base.removeprefix("enyo_")
    repo = expand_path(pool.engine_repo)
    check = subprocess.run(["git", "-C", str(repo), "rev-parse", "-q", "--verify", f"{commit}^{{commit}}"],
                           capture_output=True, text=True)
    if check.returncode != 0:
        raise CloudError(f"{base}: commit {commit} not found in {repo}")
    log(f"{name}: building {base} for {family} (once; cached afterwards)")
    llvm = pool.llvm_version
    ssh(name, (
        f"command -v clang++-{llvm} >/dev/null && command -v cmake >/dev/null && test -d /usr/include/c++/14 || {{ "
        "set -e; export DEBIAN_FRONTEND=noninteractive; "
        "curl -fsSL https://apt.llvm.org/llvm.sh -o /tmp/llvm.sh; "
        f"sudo bash /tmp/llvm.sh {llvm} >/tmp/llvm.log 2>&1; "
        # <print> needs GCC 14's libstdc++; 24.04 defaults to 13.
        "sudo apt-get install -yq cmake libstdc++-14-dev >/dev/null; }"
    ))
    src, home = f"build/{base}", f"build/home-{base}"
    ssh(name, f"rm -rf ~/{src} ~/{home} && mkdir -p ~/{src} ~/{home}/assets/nets")
    rsync([str(expand_path(pool.engine_net))], f"{name}:{home}/assets/nets/reference.net", "-L")
    archive = subprocess.Popen(["git", "-C", str(repo), "archive", "--format=tar", commit], stdout=subprocess.PIPE)
    untar = subprocess.run(["ssh", *SSH_OPTS, name, f"tar -x -C ~/{src}"], stdin=archive.stdout,
                           capture_output=True, text=True)
    archive.wait()
    if archive.returncode or untar.returncode:
        raise CloudError(f"{name}: could not send {base} sources")
    # deploy.sh wants a clean git checkout and installs into $HOME/assets/engines.
    result = ssh(name, (
        f"set -e; cd ~/{src}; "
        f"git init -q && git add -A && git -c user.name=forge -c user.email=forge@localhost commit -qm {commit}; "
        f"HOME=~/{home} CC=clang-{llvm} CXX=clang++-{llvm} LLVM_PROFDATA=llvm-profdata-{llvm} "
        f"scripts/deploy.sh --candidate > ~/{src}.log 2>&1 || {{ tail -20 ~/{src}.log >&2; exit 1; }}; "
        f"grep 'bench nodes' ~/{src}.log"
    ), check=False)
    if result.returncode != 0:
        raise CloudError(f"{name}: building {base} failed:\n{result.stderr.strip()}")
    log(f"{name}: {base} {result.stdout.strip().removeprefix('==> ')}")
    cached.parent.mkdir(parents=True, exist_ok=True)
    tmp = cached.with_name(f".{base}.tmp")
    rsync([f"{name}:{src}/build/enyo"], str(tmp))
    tmp.replace(cached)


def install_engine(pool: Pool, name: str, family: str, path: Path, *, log: Callable[[str], None]) -> None:
    """Put an engine (a link or a binary) at the same path on a cloud worker.

    ``enyo_<commit>`` binaries are rebuilt for the cloud CPU (see
    build_engine); anything else, Stockfish included, is copied as is.
    """
    target = path.resolve()
    base = target.name
    if base.startswith("enyo_"):
        cached = pool.state_dir / "engines" / family / base
        with _engine_lock(f"{family}/{base}"):
            if not cached.exists():
                build_engine(pool, name, family, base, cached, log=log)
        ssh(name, f"mkdir -p {shlex.quote(str(target.parent))}")
        rsync([str(cached)], f"{name}:{target}")
    else:
        rsync([str(target)], f"{name}:/", "--relative", "--omit-dir-times")
    if path.is_symlink():
        ssh(name, f"mkdir -p {shlex.quote(str(path.parent))} && "
                  f"ln -sfn {shlex.quote(str(target))} {shlex.quote(str(path))}")
    ssh(name, f"printf 'uci\\nquit\\n' | {shlex.quote(str(path))} | grep -q uciok")


def install_engines(pool: Pool, name: str, family: str, *, log: Callable[[str], None]) -> None:
    for raw in pool.engines:
        link = expand_path(raw)
        if not link.is_symlink():
            log(f"{name}: skip {raw} (not an engine link)")
            continue
        install_engine(pool, name, family, link, log=log)


def run_engines(run: str) -> list[Path]:
    """Engines a run's manifest requires (sprt's candidate and reference).

    A run may name engines directly (~/assets/engines/enyo_fc5052b,
    ~/assets/engines/stockfish) instead of through the pool's engine links;
    a cloud worker that lacks one fails setup and never joins.
    """
    try:
        manifest = read_json(run_dir_for_name(run) / "manifest.json")
    except (OSError, ValueError):
        return []
    requirements = manifest.get("requirements") if isinstance(manifest, dict) else None
    commands = requirements.get("commands") if isinstance(requirements, dict) else None
    paths: list[Path] = []
    for item in commands if isinstance(commands, list) else []:
        if not isinstance(item, dict) or str(item.get("name")) not in ENGINE_COMMANDS or not item.get("path"):
            continue
        path = expand_path(str(item["path"]))
        if path.exists() and path not in paths:
            paths.append(path)
    return paths


def provision(pool: Pool, instance: Instance, ip: str, *, log: Callable[[str], None]) -> None:
    coordinator = (tailscale_status().get("Self") or {}).get("DNSName", "").rstrip(".")
    if not coordinator:
        raise CloudError("the coordinator has no Tailscale DNS name")
    from .config import default_coordinator_host

    coord_alias = default_coordinator_host() or os.uname().nodename.split(".")[0]
    ensure_ssh_include()
    set_ssh_host(instance.name, ip, pool.user)
    key = worker_key()
    rsync([str(key)], f"{instance.name}:.ssh/id_ed25519")
    ssh(instance.name, "cat > ~/.ssh/config && chmod 600 ~/.ssh/config ~/.ssh/id_ed25519", input_text=(
        f"Host {coord_alias}\n    HostName {coordinator}\n    User {os.environ.get('USER', pool.user)}\n"
        "    IdentityFile ~/.ssh/id_ed25519\n    StrictHostKeyChecking no\n"
        "    UserKnownHostsFile /dev/null\n    LogLevel ERROR\n"
    ))
    ssh(instance.name, f"ssh -o BatchMode=yes {coord_alias} true")
    paths = [str(expand_path(item)) for item in pool.sync if expand_path(item).exists()]
    if paths:
        rsync(paths, f"{instance.name}:/", "--relative", "--omit-dir-times")
    install_engines(pool, instance.name, instance.type.split(".", 1)[0], log=log)


# --- Scaling -----------------------------------------------------------------

def run_lock(run: str):
    path = cache_dir() / "cloud" / "locks" / f"{run}.lock"
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("w")
    fcntl.flock(handle, fcntl.LOCK_EX)
    return handle


def forge_cli(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run([sys.executable, str(script_path()), *args], capture_output=True, text=True,
                          stdin=subprocess.DEVNULL)


def run_is_live(run: str) -> bool:
    try:
        from .status import load_live_run_summaries, run_current

        return any(Path(item["manifest"]).parent.name == run and run_current(item)
                   for item in load_live_run_summaries(lease_seconds=600))
    except Exception:
        return run_dir_for_name(run).exists()


def symlinked_inputs(run: str) -> list[Path]:
    """Targets of a run's symlinked inputs.

    Forge copies a shared-home input link as a link: home workers already
    hold its target, a new cloud worker does not.
    """
    try:
        manifest = read_json(run_dir_for_name(run) / "manifest.json")
    except (OSError, ValueError):
        return []
    requirements = manifest.get("requirements") if isinstance(manifest, dict) else None
    inputs = requirements.get("inputs") if isinstance(requirements, dict) else None
    paths: list[Path] = []
    for item in inputs if isinstance(inputs, list) else []:
        raw = item.get("path") if isinstance(item, dict) else None
        if not raw:
            continue
        link = expand_path(str(raw))
        if link.is_symlink() and link.resolve().exists():
            paths.extend([link, link.resolve()])
    return paths


def sync_run_inputs(instance: Instance, run: str) -> None:
    paths = symlinked_inputs(run)
    if paths:
        rsync([str(path) for path in paths], f"{instance.name}:/", "--relative", "--omit-dir-times")


def join_run(pool: Pool, instance: Instance, run: str, *, log: Callable[[str], None]) -> bool:
    sync_run_inputs(instance, run)
    family = instance.type.split(".", 1)[0]
    for engine in run_engines(run):
        install_engine(pool, instance.name, family, engine, log=log)
    for attempt in range(6):
        result = forge_cli("worker", "set", f"{instance.name}={pool.worker_profile}:max", run, "--no-notify")
        if result.returncode == 0:
            log(f"{instance.name}: joined {run}")
            return True
        detail = (result.stderr or result.stdout).strip().splitlines()
        log(f"{instance.name}: worker set failed ({detail[-1] if detail else result.returncode}); retrying")
        time.sleep(20)
    return False


def bring_up(pool: Pool, instance: Instance, run: str, *, log: Callable[[str], None]) -> bool:
    try:
        ip = wait_ready(pool, instance, log=log)
        provision(pool, instance, ip, log=log)
        if not run_is_live(run):
            log(f"{instance.name}: {run} is no longer active; terminating")
            terminate(pool, [instance.id])
            return False
        if join_run(pool, instance, run, log=log):
            return True
    except (CloudError, OSError, subprocess.SubprocessError) as exc:
        log(f"{instance.name}: {exc}")
    log(f"{instance.name}: terminating {instance.id}")
    try:
        terminate(pool, [instance.id])
    except CloudError as exc:
        log(f"{instance.name}: terminate failed: {exc}")
    set_ssh_host(instance.name, None)
    return False


def drain(instance: Instance, run: str) -> None:
    forge_cli("worker", "set", f"{instance.name}:0", run, "--force", "--no-notify")


def scale(pool: Pool, run: str, cores: int | None, *, log: Callable[[str], None] = print) -> int:
    """Bring a run's cloud capacity in this pool to ``cores`` (None: as much as the quota allows)."""
    types = instance_types(pool)
    with run_lock(run):
        current = list_instances(pool, run)
        have = sum(types.get(item.type, {"cores": 0})["cores"] for item in current)
        if cores is None:
            vcpus_per_core = max(info["vcpus"] // max(1, info["cores"]) for info in types.values())
            cores = have + free_vcpus(pool) // vcpus_per_core
        if cores < have:
            remaining = have
            removed = []
            for instance in reversed(current):  # newest first
                if remaining <= cores:
                    break
                drain(instance, run)
                removed.append(instance)
                remaining -= types.get(instance.type, {"cores": 0})["cores"]
            terminate(pool, [item.id for item in removed])
            for instance in removed:
                set_ssh_host(instance.name, None)
            forget_hosts(run, {item.name for item in removed})
            log(f"cloud: {run} {pool.name} cores={have}->{remaining}; terminated "
                + " ".join(item.name for item in removed))
            return 0
        if cores == have:
            log(f"cloud: {run} {pool.name} cores={have} unchanged")
            return 0
        vcpus_per_core = max(info["vcpus"] // max(1, info["cores"]) for info in types.values())
        available = free_vcpus(pool) // vcpus_per_core
        wanted = cores - have
        if available < wanted:
            log(f"cloud: quota allows {available} more {pool.market} cores (asked for {wanted})")
            wanted = available
        if wanted <= 0:
            return 1
        plan = plan_instances(wanted, types)
        if sum(types[name]["cores"] for name in plan) > available:
            # Rounding up to the smallest size overshot the quota.
            plan.sort(key=lambda name: types[name]["cores"])
            plan.pop(0)
        if not plan:
            return 1
        launched = launch(pool, plan, run)
        log(f"cloud: {run} {pool.name} cores={have}->{have + sum(types[i.type]['cores'] for i in launched)}; "
            "starting " + " ".join(f"{i.name}({i.type})" for i in launched))
    with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(launched))) as executor:
        results = list(executor.map(lambda item: bring_up(pool, item, run, log=log), launched))
    return 0 if all(results) else 1


def forget_hosts(run: str, names: set[str]) -> None:
    """Drop terminated cloud hosts from a run's workers snapshot (resume would try them)."""
    path = run_dir_for_name(run) / "workers.json"
    if not names or not path.exists():
        return
    try:
        payload = read_json(path)
        entries = payload.get("workers")
        if not isinstance(entries, list):
            return
        kept = [item for item in entries
                if (item.get("host") if isinstance(item, dict) else str(item).split("=", 1)[0].split(":", 1)[0])
                not in names]
        if len(kept) != len(entries):
            write_json(path, {**payload, "workers": kept})
    except (OSError, ValueError):
        pass


def release_run(run: str, *, log: Callable[[str], None] = print) -> int:
    """Terminate every cloud worker of a run; never raises."""
    try:
        pools = cloud_pools()
    except SystemExit:
        return 0
    terminated = 0
    for pool in pools.values():
        try:
            instances = list_instances(pool, run, states="pending,running,stopping,stopped")
            terminate(pool, [item.id for item in instances])
        except CloudError as exc:
            log(f"cloud: {run}: could not release {pool.name} workers: {exc}")
            continue
        for item in instances:
            set_ssh_host(item.name, None)
        forget_hosts(run, {item.name for item in instances})
        terminated += len(instances)
        if instances:
            log(f"cloud: {run}: terminated " + " ".join(item.name for item in instances))
    return terminated


def cloud_requests(workers_arg: str | None, pools: dict[str, Pool]) -> list[tuple[str, int | None]]:
    """Cloud capacity a run's ``--workers`` asks for.

    Inline lists name pools explicitly (``aws``, ``aws:16``); the default
    workers and workers files bring their enabled pools with their cores.
    """
    if workers_arg is not None and not expand_path(workers_arg).exists():
        return split_cloud_entries(workers_arg, pools)[1]
    if workers_arg is not None:
        # A resolved workers file (template runs, run snapshots) carries the
        # requests it was made from.
        try:
            carried = read_json(expand_path(workers_arg)).get("cloud")
        except (OSError, ValueError, AttributeError):
            carried = None
        if isinstance(carried, list):
            return [(str(item["pool"]), None if item.get("cores") is None else int(item["cores"]))
                    for item in carried if isinstance(item, dict) and str(item.get("pool")) in pools]
    file_pools = pools if workers_arg is None else cloud_pools(expand_path(workers_arg))
    return [(pool.name, pool_request(pool)) for pool in file_pools.values()
            if pool.enabled and pool.cores and pool.name in pools]


def carry_cloud_requests(payload: dict[str, Any], workers_arg: str | None) -> dict[str, Any]:
    """Keep a run's cloud requests in a resolved workers payload written to a file."""
    try:
        requests = cloud_requests(workers_arg, cloud_pools())
    except SystemExit:
        return payload
    if not requests:
        return payload
    return {**payload, "cloud": [{"pool": pool, "cores": cores} for pool, cores in requests]}


def start_cloud_requests(workers_arg: str | None, run: str, *, log: Callable[[str], None]) -> None:
    """Start background scaling for the cloud pools a run's workers ask for."""
    try:
        pools = cloud_pools()
        requests = cloud_requests(workers_arg, pools)
    except SystemExit as exc:
        log(f"cloud: {exc}")
        return
    for pool_name, cores in requests:
        spawn_scale(pool_name, cores, run)
        log(f"cloud: {run} starting {pool_name}:{'max' if cores is None else cores} in the background "
            f"(log: {cloud_log_path(run)})")


def cloud_log_path(run: str) -> Path:
    return run_dir_for_name(run) / "cloud.log"


def spawn_scale(pool_name: str, cores: int | None, run: str) -> None:
    path = cloud_log_path(run)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        subprocess.Popen(
            [sys.executable, str(script_path()), "cloud", "scale",
             f"{pool_name}:{'max' if cores is None else cores}", run],
            stdin=subprocess.DEVNULL, stdout=handle, stderr=subprocess.STDOUT, start_new_session=True,
        )


# --- CLI ---------------------------------------------------------------------

def _log(message: str) -> None:
    print(f"{time.strftime('%H:%M:%S')} {message}", flush=True)


def _pool(name: str | None = None) -> Pool:
    pools = cloud_pools()
    if not pools:
        raise SystemExit(f"cloud: no cloud pool in {config_workers_path()} (add an entry with a \"cloud\" object)")
    if name is None:
        return next(iter(pools.values()))
    if name not in pools:
        raise SystemExit(f"cloud: unknown pool {name!r} (configured: {', '.join(pools)})")
    return pools[name]


def cmd_cloud(args: argparse.Namespace) -> int:
    try:
        return _cmd_cloud(args)
    except CloudError as exc:
        raise SystemExit(f"cloud: {exc}") from None


def _cmd_cloud(args: argparse.Namespace) -> int:
    command = args.cloud_command
    if command == "scale":
        pools = cloud_pools()
        parsed = parse_cloud_entry(args.spec, pools)
        if parsed is None:
            raise SystemExit(f"cloud: expected POOL:CORES or POOL:max, got {args.spec!r}")
        return scale(pools[parsed[0]], args.run, parsed[1], log=_log)
    if command == "ls":
        pool = _pool()
        types = instance_types(pool)
        instances = list_instances(pool, states="pending,running,stopping,stopped,shutting-down")
        if not instances:
            print("no cloud workers")
            return 0
        for item in instances:
            cores = types.get(item.type, {}).get("cores", "?")
            print(f"{item.name:14} {item.id:20} {item.type:14} cores={cores:<4} {item.lifecycle:9} "
                  f"{item.state:13} run={item.run or '-'} launched={item.launched}")
        return 0
    if command == "down":
        pool = _pool()
        instances = list_instances(pool, states="pending,running,stopping,stopped")
        if args.names:
            instances = [item for item in instances if item.name in args.names or item.id in args.names]
        elif args.run:
            instances = [item for item in instances if item.run == args.run]
        if not instances:
            print("no matching cloud workers")
            return 0
        for item in instances:
            if item.run:
                drain(item, item.run)
        terminate(pool, [item.id for item in instances])
        for item in instances:
            set_ssh_host(item.name, None)
            if item.run:
                forget_hosts(item.run, {item.name})
        print("terminated " + " ".join(item.name for item in instances))
        return 0
    if command == "setup":
        pool = _pool()
        instances = {item.name: item for item in list_instances(pool, states="running")}
        for name in args.names:
            if name not in instances:
                raise SystemExit(f"cloud: {name}: no running instance")
            peer = tailscale_peer(name)
            if not peer:
                raise SystemExit(f"cloud: {name}: not on the tailnet")
            provision(pool, instances[name], peer[0], log=_log)
            _log(f"{name}: ready at {peer[0]}")
        return 0
    raise SystemExit(f"cloud: unknown command {command!r}")


def add_cloud_parser(sub: Any) -> None:
    cloud = sub.add_parser(
        "cloud",
        help="Ephemeral cloud workers (--workers aws:N, forge worker set aws:N RUN).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    cloud_sub = cloud.add_subparsers(dest="cloud_command", metavar="command", required=True)
    scale_parser = cloud_sub.add_parser("scale", help="Set a run's cloud cores: POOL:CORES or POOL:max (0 removes).")
    scale_parser.add_argument("spec")
    scale_parser.add_argument("run")
    cloud_sub.add_parser("ls", help="List cloud workers.")
    down = cloud_sub.add_parser("down", help="Terminate cloud workers (default: all).")
    down.add_argument("names", nargs="*", help="Worker names or instance ids.")
    down.add_argument("--run", help="Only this run's workers.")
    setup = cloud_sub.add_parser("setup", help="Re-run provisioning on running workers.")
    setup.add_argument("names", nargs="+")
    cloud.set_defaults(func=cmd_cloud)
