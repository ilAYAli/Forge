from __future__ import annotations

from .core import *


def item_profiles(item: dict[str, Any], *, field: str) -> set[str]:
    values: list[Any] = []
    if "profile" in item:
        values.append(item.get("profile"))
    if "profiles" in item:
        raw = item.get("profiles")
        if isinstance(raw, str):
            values.append(raw)
        elif isinstance(raw, list):
            values.extend(raw)
        else:
            raise SystemExit(f"{field}.profiles must be a string or list of strings")
    profiles = {str(value).strip() for value in values if str(value).strip()}
    if "" in profiles:
        profiles.remove("")
    return profiles

def item_matches_profile(item: dict[str, Any], active_profile: str | None, *, field: str) -> bool:
    profiles = item_profiles(item, field=field)
    if not profiles:
        return True
    return bool(active_profile and active_profile in profiles)

def manifest_profile_names(manifest: dict[str, Any]) -> set[str]:
    profiles = set()
    path_maps = manifest.get("path_maps", {})
    if isinstance(path_maps, dict):
        profiles.update(str(profile) for profile in path_maps)
    req = manifest.get("requirements", {})
    if isinstance(req, dict):
        for field in ("inputs", "commands", "repos"):
            raw = req.get(field, [])
            if isinstance(raw, list):
                for item in raw:
                    if isinstance(item, dict):
                        profiles.update(item_profiles(item, field=f"requirements.{field}"))
        git_req = req.get("git")
        if isinstance(git_req, dict):
            profiles.update(item_profiles(git_req, field="requirements.git"))
    return profiles

def apply_profile(manifest: dict[str, Any], profile: str | None) -> dict[str, Any]:
    if not profile:
        return manifest
    path_maps = manifest.get("path_maps", {})
    if not isinstance(path_maps, dict):
        raise SystemExit("path_maps must be an object")
    selected = path_maps.get(profile)
    if selected is None:
        if profile not in manifest_profile_names(manifest):
            raise SystemExit(f"unknown profile {profile!r}")
        selected = {}
    if not isinstance(selected, dict):
        raise SystemExit(f"path_maps.{profile} must be an object")
    translated = translate_obj(copy.deepcopy(manifest), {str(k): str(v) for k, v in selected.items()})
    translated["_active_profile"] = profile
    return translated

def validate_manifest_payload(manifest: dict[str, Any], *, label: str) -> None:
    if not schema_matches(manifest.get("schema"), "task"):
        raise SystemExit(f"{label}: unsupported schema {manifest.get('schema')!r}")
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list):
        raise SystemExit(f"{label}: tasks must be a list")
    seen: set[str] = set()
    seen_outputs: dict[str, str] = {}
    for task in tasks:
        if not isinstance(task, dict):
            raise SystemExit(f"{label}: each task must be an object")
        task_id = str(task.get("id", ""))
        if not task_id:
            raise SystemExit(f"{label}: task is missing id")
        if task_id in seen:
            raise SystemExit(f"{label}: duplicate task id {task_id}")
        seen.add(task_id)
        outputs = task.get("outputs", [])
        if not isinstance(outputs, list):
            raise SystemExit(f"{label}: {task_id}: outputs must be a list")
        for output in outputs:
            output_key = str(output)
            other_task = seen_outputs.get(output_key)
            if other_task is not None:
                raise SystemExit(
                    f"{label}: duplicate output path {output_key!r} in "
                    f"{other_task} and {task_id}")
            seen_outputs[output_key] = task_id

def load_manifest(path: Path) -> dict[str, Any]:
    import pickle
    cache_path = path.with_suffix(".cache.pkl")
    try:
        manifest_mtime = path.stat().st_mtime_ns
        if cache_path.stat().st_mtime_ns >= manifest_mtime:
            with open(cache_path, "rb") as _f:
                return pickle.load(_f)
    except (OSError, pickle.UnpicklingError, EOFError, AttributeError):
        pass
    manifest = read_json(path)
    validate_manifest_payload(manifest, label=str(path))
    try:
        _tmp = cache_path.with_name(f".{cache_path.name}.{os.getpid()}.tmp")
        with open(_tmp, "wb") as _f:
            pickle.dump(manifest, _f, protocol=pickle.HIGHEST_PROTOCOL)
        os.replace(_tmp, cache_path)
    except OSError:
        pass
    return manifest

def task_template_values(manifest: dict[str, Any], task: dict[str, Any]) -> dict[str, Any]:
    vars_obj = manifest.get("vars", {})
    if not isinstance(vars_obj, dict):
        raise SystemExit("vars must be an object")
    values: dict[str, Any] = {
        "task_id": task["id"],
        "index": task.get("index", ""),
        "shards": len(manifest["tasks"]),
    }
    values.update({str(k): v for k, v in vars_obj.items()})
    values.update({str(k): v for k, v in task.items() if k not in {"command", "outputs"}})
    return values

def materialize_manifest(template: dict[str, Any], *, run_name: str, run_dir: Path) -> dict[str, Any]:
    manifest = copy.deepcopy(template)
    manifest["name"] = run_name
    manifest["state_dir"] = str(run_dir / "state")
    manifest["log_dir"] = str(run_dir / "logs")
    manifest["claim_dir"] = str(run_dir / "claims")
    vars_obj = manifest.get("vars", {})
    if vars_obj in (None, ""):
        vars_obj = {}
    if not isinstance(vars_obj, dict):
        raise SystemExit("vars must be an object")
    vars_obj = dict(vars_obj)
    vars_obj["run_dir"] = str(run_dir)
    vars_obj.setdefault("output_dir", str(run_dir / "outputs"))
    manifest["vars"] = vars_obj
    for task in manifest["tasks"]:
        outputs = task.get("outputs", [])
        if not isinstance(outputs, list):
            raise SystemExit(f"{task['id']}: outputs must be a list")
        values = task_template_values(manifest, task)
        task["outputs"] = [
            str(expand_path(str(output).format(**values)))
            for output in outputs
        ]
    validate_manifest_payload(manifest, label=f"run {run_name}")
    return manifest

def manifest_base(manifest: dict[str, Any], key: str, fallback: str) -> Path:
    return expand_path(str(manifest.get(key) or fallback))

def state_dir(manifest: dict[str, Any]) -> Path:
    return manifest_base(manifest, "state_dir", "state")

def log_dir(manifest: dict[str, Any]) -> Path:
    return manifest_base(manifest, "log_dir", "logs")

def claim_dir(manifest: dict[str, Any]) -> Path:
    return expand_path(str(manifest.get("claim_dir") or (state_dir(manifest).parent / "claims")))

def work_dir(manifest: dict[str, Any]) -> Path:
    return manifest_base(manifest, "work_dir", ".")

def find_task(manifest: dict[str, Any], *, task_id: str | None, index: int | None) -> dict[str, Any]:
    tasks = manifest["tasks"]
    matches = []
    for task in tasks:
        if task_id is not None and task.get("id") == task_id:
            matches.append(task)
        elif index is not None and int(task.get("index", -1)) == index:
            matches.append(task)
    if not matches:
        target = task_id if task_id is not None else index
        raise SystemExit(f"no task matches {target!r}")
    if len(matches) > 1:
        raise SystemExit("task selector matched multiple tasks")
    return matches[0]

def iter_git_requirements(manifest: dict[str, Any], req: dict[str, Any]) -> list[dict[str, Any]]:
    repos = []
    git_req = req.get("git")
    if isinstance(git_req, dict):
        repos.append({
            "name": git_req.get("name") or "git",
            "path": git_req.get("repo") or manifest.get("work_dir") or ".",
            "commit": git_req.get("commit") or "",
            "require_clean": bool(git_req.get("require_clean", False)),
            "remote": git_req.get("remote") or "",
            "branch": git_req.get("branch") or "",
            "url": git_req.get("url") or "",
        })
    raw_repos = req.get("repos", [])
    if raw_repos in (None, ""):
        raw_repos = []
    if not isinstance(raw_repos, list):
        raise SystemExit("requirements.repos must be a list")
    for item in raw_repos:
        if not isinstance(item, dict):
            raise SystemExit("requirements.repos entries must be objects")
        repos.append(item)
    return repos

def check_git_requirement(item: dict[str, Any]) -> None:
    repo = expand_path(str(item.get("path") or item.get("repo") or "."))
    label = str(item.get("name") or repo)
    if not (repo / ".git").exists():
        raise SystemExit(f"{label}: not a git checkout: {repo}")
    expected_commit = str(item.get("commit") or "").strip()
    if expected_commit:
        actual_commit = git_output(repo, "rev-parse", "HEAD")
        resolved_commit = resolve_git_commit(repo, expected_commit, label=label)
        if actual_commit != resolved_commit:
            raise SystemExit(
                f"{label}: git commit {actual_commit} != required {resolved_commit}"
            )
    branch = str(item.get("branch") or "").strip()
    if branch and not expected_commit:
        actual_branch = git_output(repo, "rev-parse", "--abbrev-ref", "HEAD")
        if actual_branch != branch:
            raise SystemExit(f"{label}: git branch {actual_branch} != required {branch}")
    if bool(item.get("require_clean", False)):
        status = git_output(repo, "status", "--porcelain")
        if status:
            raise SystemExit(f"{label}: git worktree is not clean")

def check_command_requirement(item: dict[str, Any]) -> None:
    if not isinstance(item, dict):
        raise SystemExit("requirements.commands entries must be objects")
    label = str(item.get("name") or item.get("path") or item.get("command") or "command")
    path = str(item.get("path") or "").strip()

    def requirement_context() -> str:
        details = []
        if path:
            details.append(f"path={path}")
        command_text = item.get("command")
        if command_text is not None:
            details.append(f"command={command_text}")
        return f" ({'; '.join(details)})" if details else ""

    def command_output() -> str:
        output = (result.stdout or "").strip()
        return output if output else "<no output>"

    def check_path() -> None:
        if not path:
            return
        resolved = shutil.which(path) if os.path.sep not in path else str(expand_path(path))
        if not resolved or not Path(resolved).exists():
            raise SystemExit(f"{label}: required command not found: {path}")
        if not os.access(resolved, os.X_OK):
            raise SystemExit(f"{label}: required command is not executable: {resolved}")

    command = item.get("command")
    if command is None:
        check_path()
    if command is None and path and (item.get("contains") or item.get("regex")):
        command = [path, "--version"]
    if command is None:
        return

    timeout = bounded_timeout(
        item.get("timeout_s"), DEFAULT_REQUIREMENT_TIMEOUT_SECONDS)
    result = command_result(command, timeout=timeout)
    output = result.stdout
    if result.returncode != int(item.get("rc", 0)):
        raise SystemExit(
            f"{label}: command failed rc={result.returncode}{requirement_context()}: "
            f"{command_output()}"
        )
    check_path()
    contains = str(item.get("contains") or "")
    if contains and contains not in output:
        raise SystemExit(f"{label}: output does not contain {contains!r}: {output.strip()}")
    regex = str(item.get("regex") or "")
    if regex and not re.search(regex, output):
        raise SystemExit(f"{label}: output does not match {regex!r}: {output.strip()}")

def requirement_commands(value: Any, field: str) -> list[str]:
    if value in (None, "", []):
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return list(value)
    raise SystemExit(f"{field} must be a string or list of strings")

def run_repo_hook(item: dict[str, Any], field: str, *, repo: Path) -> None:
    label = str(item.get("name") or repo)
    for index, command in enumerate(requirement_commands(item.get(field), f"{label}.{field}"), start=1):
        result = command_result(
            command, cwd=repo,
            timeout=bounded_timeout(item.get(f"{field}_timeout_s")))
        if result.returncode != 0:
            raise SystemExit(
                f"{label}: {field}[{index}] failed rc={result.returncode}: {result.stdout.strip()}"
            )

def input_requirement_items(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    req = manifest.get("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    inputs = req.get("inputs", [])
    if not isinstance(inputs, list):
        raise SystemExit("requirements.inputs must be a list")
    active_profile = str(manifest.get("_active_profile") or "").strip() or None
    items = []
    for item in inputs:
        if not isinstance(item, dict):
            raise SystemExit("requirements.inputs entries must be objects")
        if item_matches_profile(item, active_profile, field="requirements.inputs"):
            items.append(item)
    return items


def source_requirement_items(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    req = manifest.get("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    sources = req.get("sources", [])
    if sources in (None, ""):
        return []
    if not isinstance(sources, list):
        raise SystemExit("requirements.sources must be a list")
    active_profile = str(manifest.get("_active_profile") or "").strip() or None
    items = []
    for item in sources:
        if not isinstance(item, dict):
            raise SystemExit("requirements.sources entries must be objects")
        if item_matches_profile(item, active_profile, field="requirements.sources"):
            items.append(item)
    return items


def check_source_requirement(item: dict[str, Any], *, run_hooks: bool = True) -> None:
    from .source_artifacts import source_artifact_matches

    root = expand_path(str(item.get("path") or ""))
    digest = str(item.get("digest") or "")
    label = str(item.get("name") or root)
    if not digest or not source_artifact_matches(root, digest=digest):
        raise SystemExit(f"{label}: source artifact does not match {digest or '?'}")
    if run_hooks:
        run_repo_hook(item, "probe", repo=root)

def check_requirements(manifest: dict[str, Any]) -> None:
    req = manifest.get("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    active_profile = str(manifest.get("_active_profile") or "").strip() or None

    python_min = str(req.get("python_min", "")).strip()
    if python_min:
        actual = ".".join(map(str, sys.version_info[:3]))
        try:
            require_min_version("python", actual, python_min)
        except SystemExit as exc:
            raise SystemExit(
                f"python executable {sys.executable!r}: {exc}"
            ) from None

    git_min = str(req.get("git_min", "")).strip()
    if git_min:
        actual = command_output(["git", "--version"])
        require_min_version("git", actual, git_min)

    for item in source_requirement_items(manifest):
        check_source_requirement(item)

    for item in iter_git_requirements(manifest, req):
        if not item_matches_profile(item, active_profile, field="requirements.repos"):
            continue
        check_git_requirement(item)
        run_repo_hook(item, "probe", repo=expand_path(str(item.get("path") or item.get("repo") or ".")))

    commands = req.get("commands", [])
    if commands in (None, ""):
        commands = []
    if not isinstance(commands, list):
        raise SystemExit("requirements.commands must be a list")
    for item in commands:
        if not isinstance(item, dict):
            raise SystemExit("requirements.commands entries must be objects")
        if not item_matches_profile(item, active_profile, field="requirements.commands"):
            continue
        check_command_requirement(item)

    for item in input_requirement_items(manifest):
        source = expand_path(str(item.get("source") or ""))
        path = source if item.get("source") and source.exists() else expand_path(
            str(item.get("path", ""))
        )
        if not path.exists():
            raise SystemExit(f"required input missing: {path}")
        expected_sha = str(item.get("sha256") or "").strip()
        if expected_sha and sha256_file(path) != expected_sha:
            raise SystemExit(f"required input checksum mismatch: {path}")

def cmd_plan(args: argparse.Namespace) -> int:
    out = expand_path(args.out)
    state = expand_path(args.state_dir) if args.state_dir else out.parent / "state"
    logs = expand_path(args.log_dir) if args.log_dir else out.parent / "logs"
    kind = clean_manifest_text(args.kind, label="--kind")
    description = clean_manifest_text(args.description, label="--description")
    comment = clean_manifest_text(getattr(args, "comment", None), label="--comment")
    task_label = clean_manifest_text(args.task_label, label="--task-label")
    vars_obj: dict[str, str] = {}
    for item in args.var:
        key, sep, value = item.partition("=")
        if not sep or not key:
            raise SystemExit(f"invalid --var {item!r}; expected KEY=VALUE")
        vars_obj[key] = value
    if args.output_dir:
        vars_obj["output_dir"] = str(expand_path(args.output_dir))

    path_maps: dict[str, dict[str, str]] = {}
    for item in args.path_map:
        profile, sep, mapping = item.partition(":")
        src, sep2, dst = mapping.partition("=")
        if not sep or not sep2 or not profile or not src:
            raise SystemExit(
                f"invalid --path-map {item!r}; expected PROFILE:/source=/dest"
            )
        path_maps.setdefault(profile, {})[src] = dst

    tasks = []
    generated_outputs: dict[str, str] = {}
    width = max(4, len(str(args.shards - 1)))
    progress_enabled = any([
        args.progress_unit,
        args.progress_total,
        args.progress_total_lines,
        args.progress_log_regex,
        args.progress_output_unit,
    ])
    if args.progress_total and args.progress_total_lines:
        raise SystemExit("choose only one of --progress-total or --progress-total-lines")
    if progress_enabled and not args.progress_log_regex:
        raise SystemExit("--progress-log-regex is required when progress metadata is set")
    progress_line_totals: list[int] | None = None
    if args.progress_total_lines:
        total_lines = count_file_lines(expand_path(str(args.progress_total_lines).format(**vars_obj)))
        base = total_lines // args.shards
        extra = total_lines % args.shards
        progress_line_totals = [
            base + (1 if index < extra else 0)
            for index in range(args.shards)
        ]
    for index in range(args.shards):
        task_id = f"{args.task_prefix}_{index:0{width}d}"
        format_vars = {
            "index": index,
            "shard": index,
            "task_id": task_id,
            "shards": args.shards,
            "run_dir": "{run_dir}",
            "output_dir": vars_obj.get("output_dir", "{output_dir}"),
            **vars_obj,
        }
        outputs = [
            template.format(**format_vars)
            for template in args.output_template
        ]
        for output in outputs:
            other_task = generated_outputs.get(output)
            if other_task is not None:
                raise SystemExit(
                    f"{out}: duplicate output path {output!r} in "
                    f"{other_task} and {task_id}")
            generated_outputs[output] = task_id
        task = {
            "id": task_id,
            "index": index,
            "command": args.command_template,
            "outputs": outputs,
        }
        if task_label:
            task["label"] = clean_manifest_text(
                task_label.format(**format_vars),
                label=f"{task_id}.label",
            )
        if progress_enabled:
            progress: dict[str, Any] = {
                "log_regex": str(args.progress_log_regex),
            }
            if args.progress_unit:
                progress["unit"] = str(args.progress_unit)
            if args.progress_output_unit:
                progress["output_unit"] = str(args.progress_output_unit)
            if args.progress_total:
                total = str(args.progress_total).format(**format_vars)
                try:
                    progress["total"] = int(total)
                except ValueError:
                    progress["total"] = total
            elif progress_line_totals is not None:
                progress["total"] = progress_line_totals[index]
            task["progress"] = progress
        tasks.append(task)

    git_requirement = None
    if not args.no_git_requirement:
        git_repo = expand_path(args.git_repo or args.work_dir)
        git_commit = args.git_commit or git_output(git_repo, "rev-parse", "HEAD")
        git_requirement = git_requirement_record(
            git_repo,
            git_commit,
            require_clean=args.require_clean_git,
        )
    manifest = {
        "schema": SCHEMA,
        "name": args.name,
        "created_at": now(),
        "work_dir": str(expand_path(args.work_dir)),
        "state_dir": str(state),
        "log_dir": str(logs),
        "claim_dir": str(out.parent / "claims"),
        "vars": vars_obj,
        "path_maps": path_maps,
        "requirements": {
            "python_min": args.python_min,
            "git_min": args.git_min,
            "inputs": [],
        },
        "tasks": tasks,
    }
    if git_requirement is not None:
        manifest["requirements"]["git"] = git_requirement
    if kind:
        manifest["kind"] = kind
    if description:
        manifest["description"] = description
    if comment:
        manifest["comment"] = comment
    write_json(out, manifest)
    print(out)
    return 0

def cmd_add_input(args: argparse.Namespace) -> int:
    path = expand_path(args.manifest)
    manifest = load_manifest(path)
    req = manifest.setdefault("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    inputs = req.setdefault("inputs", [])
    if not isinstance(inputs, list):
        raise SystemExit("requirements.inputs must be a list")
    for raw in args.path:
        input_path = expand_path(raw)
        if not input_path.exists():
            raise SystemExit(f"input does not exist: {input_path}")
        record = file_record(input_path)
        if args.name:
            record["name"] = str(args.name)
        inputs[:] = [
            item for item in inputs
            if not isinstance(item, dict) or item.get("path") != str(input_path)
        ]
        inputs.append(record)
    write_json(path, manifest)
    print(path)
    return 0

def cmd_add_repo(args: argparse.Namespace) -> int:
    path = expand_path(args.manifest)
    manifest = load_manifest(path)
    req = manifest.setdefault("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    repos = req.setdefault("repos", [])
    if not isinstance(repos, list):
        raise SystemExit("requirements.repos must be a list")
    repo = expand_path(args.path)
    commit = args.commit or git_output(repo, "rev-parse", "HEAD")
    record = {
        "name": args.name or repo.name,
        "path": str(repo),
        "commit": commit,
    }
    if args.url:
        record["url"] = args.url
    if args.branch:
        record["branch"] = args.branch
    if args.remote:
        record["remote"] = args.remote
    if args.require_clean:
        record["require_clean"] = True
    if args.allow_untracked:
        record["allow_untracked"] = True
    if args.allow_dirty:
        record["allow_dirty"] = True
    for field in ("build", "install", "probe"):
        values = getattr(args, field)
        if values:
            record[field] = values
        timeout = getattr(args, f"{field}_timeout_s")
        if timeout is not None:
            record[f"{field}_timeout_s"] = timeout
    repos[:] = [
        item for item in repos
        if not isinstance(item, dict) or item.get("path") != str(repo)
    ]
    repos.append(record)
    write_json(path, manifest)
    print(path)
    return 0

def cmd_add_command(args: argparse.Namespace) -> int:
    if not args.path and not args.command:
        raise SystemExit("add-command requires --path, --command, or both")
    path = expand_path(args.manifest)
    manifest = load_manifest(path)
    req = manifest.setdefault("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    commands = req.setdefault("commands", [])
    if not isinstance(commands, list):
        raise SystemExit("requirements.commands must be a list")
    record: dict[str, Any] = {
        "name": args.name or args.path or args.command,
    }
    if args.path:
        record["path"] = args.path
    if args.command:
        record["command"] = args.command
    if args.contains:
        record["contains"] = args.contains
    if args.regex:
        record["regex"] = args.regex
    if args.rc != 0:
        record["rc"] = args.rc
    if args.timeout_s is not None:
        record["timeout_s"] = args.timeout_s
    if args.profile:
        record["profiles"] = args.profile
    key = str(record["name"])
    commands[:] = [
        item for item in commands
        if not isinstance(item, dict) or str(item.get("name") or item.get("path") or item.get("command")) != key
    ]
    commands.append(record)
    write_json(path, manifest)
    print(path)
    return 0

def git_requirement_allows_dirty(item: dict[str, Any]) -> bool:
    if bool(item.get("allow_dirty", False)):
        return True
    if "require_clean" in item:
        return not bool(item.get("require_clean", False))
    return False

def git_requirement_status(repo: Path, item: dict[str, Any]) -> str:
    command = ["git", "-C", str(repo), "status", "--porcelain"]
    if bool(item.get("allow_untracked", False)):
        command.append("--untracked-files=no")
    return command_output(command)

def sync_git_requirement(item: dict[str, Any]) -> None:
    repo = expand_path(str(item.get("path") or item.get("repo") or "."))
    label = str(item.get("name") or repo)
    url = str(item.get("url") or "").strip()
    if not repo.exists() and url:
        repo.parent.mkdir(parents=True, exist_ok=True)
        run_checked(["git", "clone", url, str(repo)])
    if not (repo / ".git").exists():
        raise SystemExit(f"{label}: not a git checkout: {repo}")
    status = git_requirement_status(repo, item)
    if status and not git_requirement_allows_dirty(item):
        raise SystemExit(f"{label}: git worktree is not clean")
    remote = str(item.get("remote") or "").strip()
    ensure_git_remote(repo, remote, url)
    if remote:
        fetch_git_remote(repo, remote)
    branch = str(item.get("branch") or "").strip()
    commit = str(item.get("commit") or "").strip()
    if branch:
        run_checked(["git", "-C", str(repo), "checkout", branch])
        if remote:
            run_checked(["git", "-C", str(repo), "pull", "--ff-only", remote, branch])
    if commit:
        resolved_commit = resolve_git_commit(repo, commit, label=label)
        run_checked(["git", "-C", str(repo), "checkout", resolved_commit])
    run_repo_hook(item, "build", repo=repo)
    run_repo_hook(item, "install", repo=repo)
    run_repo_hook(item, "probe", repo=repo)
    print(f"{label}: {git_output(repo, 'rev-parse', 'HEAD')}")


def sync_source_requirement(item: dict[str, Any]) -> None:
    root = expand_path(str(item.get("path") or ""))
    check_source_requirement(item, run_hooks=False)
    run_repo_hook(item, "build", repo=root)
    run_repo_hook(item, "install", repo=root)
    run_repo_hook(item, "probe", repo=root)

def cmd_sync_repos(args: argparse.Namespace) -> int:
    manifest = apply_profile(load_manifest(expand_path(args.manifest)), args.profile)
    req = manifest.get("requirements", {})
    if not isinstance(req, dict):
        raise SystemExit("requirements must be an object")
    repos = iter_git_requirements(manifest, req)
    sources = source_requirement_items(manifest)
    if not repos and not sources:
        raise SystemExit("no repositories or sources declared")
    for item in sources:
        sync_source_requirement(item)
    for item in repos:
        sync_git_requirement(item)
    check_requirements(manifest)
    return 0
