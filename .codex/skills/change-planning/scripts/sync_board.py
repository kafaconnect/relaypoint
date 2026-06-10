#!/usr/bin/env python3
"""Idempotently sync OpenSpec per-task files to a GitHub Projects v2 board."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote


STATUS_TO_BOARD = {
    "todo": "Ready",
    "in_progress": "In Progress",
    "blocked": "Blocked",
    "done": "Done",
}


@dataclass
class Task:
    path: Path
    id: str
    slice: str
    title: str
    status: str
    issue: int | None
    specs: str


class SyncError(RuntimeError):
    pass


def run(args: list[str], *, cwd: Path, capture: bool = True, check: bool = True) -> str:
    proc = subprocess.run(
        args,
        cwd=cwd,
        check=False,
        text=True,
        stdout=subprocess.PIPE if capture else None,
        stderr=subprocess.PIPE if capture else None,
    )
    if check and proc.returncode != 0:
        detail = (proc.stderr or proc.stdout or "").strip()
        raise SyncError(f"command failed: {' '.join(args)}\n{detail}")
    return (proc.stdout or "").strip()


def gh_json(args: list[str], *, cwd: Path) -> Any:
    out = run(["gh", *args], cwd=cwd)
    return json.loads(out) if out else None


def find_root(start: Path) -> Path:
    cur = start.resolve()
    for path in [cur, *cur.parents]:
        if (path / "openspec").is_dir():
            return path
    raise SyncError("cannot find repo root containing openspec/")


def infer_change(root: Path, explicit: str | None) -> str:
    if explicit:
        return explicit
    branch = run(["git", "branch", "--show-current"], cwd=root, check=False)
    if branch.startswith("change/") and len(branch) > len("change/"):
        return branch.split("/", 1)[1]
    changes_dir = root / "openspec" / "changes"
    changes = sorted(p.name for p in changes_dir.iterdir() if p.is_dir()) if changes_dir.exists() else []
    if len(changes) == 1:
        return changes[0]
    raise SyncError("change is ambiguous; pass --change")


def infer_repo(root: Path, explicit: str | None) -> str | None:
    if explicit:
        return explicit
    remote = run(["git", "config", "--get", "remote.origin.url"], cwd=root, check=False)
    patterns = [
        r"github\.com[:/](?P<owner>[^/]+)/(?P<repo>[^/.]+)(?:\.git)?$",
        r"^git@github\.com:(?P<owner>[^/]+)/(?P<repo>[^/.]+)(?:\.git)?$",
    ]
    for pat in patterns:
        match = re.search(pat, remote)
        if match:
            return f"{match.group('owner')}/{match.group('repo')}"
    return None


def parse_project_yml(root: Path) -> dict[str, Any]:
    path = root / ".github" / "project.yml"
    cfg: dict[str, Any] = {"github": {}, "project": {}}
    if not path.exists():
        return cfg
    section = None
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.rstrip()
        if not line or line.lstrip().startswith("#"):
            continue
        if not line.startswith(" ") and line.endswith(":"):
            section = line[:-1]
            continue
        if section in {"github", "project"} and line.startswith("  ") and ":" in line:
            key, value = line.strip().split(":", 1)
            cfg[section][key] = value.strip().strip('"')
    return cfg


def parse_frontmatter(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    if not text.startswith("---\n"):
        raise SyncError(f"{path} has no YAML frontmatter")
    end = text.find("\n---", 4)
    if end < 0:
        raise SyncError(f"{path} has unterminated YAML frontmatter")
    data: dict[str, str] = {}
    for raw in text[4:end].splitlines():
        if ":" not in raw:
            continue
        key, value = raw.split(":", 1)
        data[key.strip()] = value.strip().strip('"')
    return data


def load_tasks(root: Path, change: str) -> list[Task]:
    change_root = root / "openspec" / "changes" / change
    tasks_dir = change_root / "tasks"
    if not tasks_dir.is_dir():
        if (change_root / "tasks.md").exists():
            raise SyncError(
                f"{change} uses legacy tasks.md without tasks/*.md; convert to per-task files first"
            )
        raise SyncError(f"missing task directory: {tasks_dir}")

    tasks: list[Task] = []
    for path in sorted(tasks_dir.glob("*.md")):
        fm = parse_frontmatter(path)
        task_id = fm.get("id") or path.stem.split("-", 2)[0]
        task_slice = fm.get("slice") or task_id.split("-", 1)[0]
        title = fm.get("title") or path.stem
        status = fm.get("status", "todo")
        if status not in STATUS_TO_BOARD:
            raise SyncError(f"{path} has unsupported status: {status}")
        issue_raw = fm.get("issue", "")
        issue = int(issue_raw) if issue_raw.isdigit() else None
        tasks.append(
            Task(
                path=path,
                id=task_id,
                slice=task_slice,
                title=title,
                status=status,
                issue=issue,
                specs=fm.get("specs", ""),
            )
        )
    if not tasks:
        raise SyncError(f"no task files found under {tasks_dir}")
    return tasks


def change_title(root: Path, change: str) -> str:
    proposal = root / "openspec" / "changes" / change / "proposal.md"
    if proposal.exists():
        for raw in proposal.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if line.startswith("# "):
                return line[2:].strip()
            if line.startswith("## Title"):
                continue
    return change


def ensure_label(root: Path, repo: str, name: str, color: str, description: str, dry: bool) -> None:
    url_name = quote(name, safe="")
    exists = run(
        ["gh", "api", f"repos/{repo}/labels/{url_name}", "--jq", ".name"],
        cwd=root,
        check=False,
    )
    if exists == name:
        return
    cmd = [
        "gh",
        "label",
        "create",
        name,
        "-R",
        repo,
        "--color",
        color,
        "--description",
        description,
    ]
    if dry:
        print("DRY:", " ".join(cmd))
    else:
        run(cmd, cwd=root, capture=False)


def ensure_labels(root: Path, repo: str, change: str, dry: bool) -> None:
    ensure_label(root, repo, "type:epic", "7057ff", "OpenSpec Epic issue", dry)
    ensure_label(root, repo, "type:story", "1d76db", "OpenSpec Story issue", dry)
    ensure_label(root, repo, "type:task", "0e8a16", "OpenSpec Task issue", dry)
    ensure_label(root, repo, f"openspec-change:{change}", "ededed", f"OpenSpec change {change}", dry)


def ensure_milestone(root: Path, repo: str, title: str, dry: bool) -> None:
    existing = run(
        ["gh", "api", f"repos/{repo}/milestones?state=all", "--jq", f'.[] | select(.title=="{title}") | .number'],
        cwd=root,
        check=False,
    )
    if existing.strip():
        return
    cmd = ["gh", "api", f"repos/{repo}/milestones", "-f", f"title={title}"]
    if dry:
        print("DRY:", " ".join(cmd))
    else:
        run(cmd, cwd=root)


def list_change_issues(root: Path, repo: str, change: str) -> list[dict[str, Any]]:
    return gh_json(
        [
            "issue",
            "list",
            "-R",
            repo,
            "--state",
            "all",
            "--label",
            f"openspec-change:{change}",
            "--limit",
            "1000",
            "--json",
            "number,title,url,labels,state",
        ],
        cwd=root,
    ) or []


def has_label(issue: dict[str, Any], name: str) -> bool:
    return any(label.get("name") == name for label in issue.get("labels", []))


def issue_url(root: Path, repo: str, number: int) -> str:
    return run(["gh", "issue", "view", str(number), "-R", repo, "--json", "url", "--jq", ".url"], cwd=root)


def create_issue(
    root: Path,
    repo: str,
    title: str,
    body: str,
    labels: list[str],
    milestone: str,
    assignee: str,
    dry: bool,
) -> int:
    cmd = [
        "gh",
        "issue",
        "create",
        "-R",
        repo,
        "--title",
        title,
        "--body",
        body,
        "--milestone",
        milestone,
    ]
    for label in labels:
        cmd.extend(["--label", label])
    if assignee:
        cmd.extend(["--assignee", assignee])
    if dry:
        print("DRY:", " ".join(cmd))
        return -1
    url = run(cmd, cwd=root)
    return int(url.rstrip("/").split("/")[-1])


def ensure_issue(
    root: Path,
    repo: str,
    issues: list[dict[str, Any]],
    label: str,
    title_prefix: str,
    title: str,
    body: str,
    labels: list[str],
    milestone: str,
    assignee: str,
    dry: bool,
) -> int:
    labelled = [issue for issue in issues if has_label(issue, label)]
    if len(labelled) == 1:
        return int(labelled[0]["number"])
    for issue in issues:
        if has_label(issue, label) and issue["title"].startswith(title_prefix):
            return int(issue["number"])
    return create_issue(root, repo, title, body, labels, milestone, assignee, dry)


def write_task_issue(task: Task, issue: int, dry: bool) -> None:
    if dry or task.issue == issue or issue < 0:
        return
    text = task.path.read_text(encoding="utf-8")
    end = text.find("\n---", 4)
    fm = text[4:end]
    rest = text[end:]
    if re.search(r"^issue:\s*.*$", fm, flags=re.M):
        fm = re.sub(r"^issue:\s*.*$", f"issue: {issue}", fm, count=1, flags=re.M)
    else:
        fm = fm.rstrip() + f"\nissue: {issue}\n"
    task.path.write_text("---\n" + fm + rest, encoding="utf-8")


def project_fields(root: Path, org: str, project_number: str) -> list[dict[str, Any]]:
    data = gh_json(["project", "field-list", project_number, "--owner", org, "--format", "json"], cwd=root)
    return data.get("fields", []) if isinstance(data, dict) else []


def find_field(fields: list[dict[str, Any]], name: str) -> dict[str, Any] | None:
    return next((field for field in fields if field.get("name") == name), None)


def option_id(field: dict[str, Any], name: str) -> str | None:
    for option in field.get("options", []) or []:
        if option.get("name") == name:
            return option.get("id")
    return None


def iteration_id(field: dict[str, Any], title: str) -> str | None:
    sources = [field.get("iterations", []), field.get("configuration", {}).get("iterations", [])]
    for source in sources:
        for iteration in source or []:
            if iteration.get("title") == title or iteration.get("name") == title:
                return iteration.get("id")
    return None


def project_items(root: Path, org: str, project_number: str) -> list[dict[str, Any]]:
    data = gh_json(
        ["project", "item-list", project_number, "--owner", org, "--limit", "1000", "--format", "json"],
        cwd=root,
    )
    return data.get("items", []) if isinstance(data, dict) else []


def project_item_id(items: list[dict[str, Any]], issue_number: int) -> str | None:
    for item in items:
        content = item.get("content") or {}
        if content.get("number") == issue_number:
            return item.get("id")
    return None


def ensure_project_item(
    root: Path,
    org: str,
    project_number: str,
    repo: str,
    issue_number: int,
    items: list[dict[str, Any]],
    dry: bool,
) -> str | None:
    if dry and issue_number < 0:
        print(f"DRY: add issue #{issue_number} to project {project_number}")
        return None
    existing = project_item_id(items, issue_number)
    if existing:
        return existing
    url = issue_url(root, repo, issue_number)
    cmd = ["gh", "project", "item-add", project_number, "--owner", org, "--url", url, "--format", "json", "--jq", ".id"]
    if dry:
        print("DRY:", " ".join(cmd))
        return None
    item_id = run(cmd, cwd=root)
    items.append({"id": item_id, "content": {"number": issue_number}})
    return item_id


def edit_project_field(root: Path, project_id: str, item_id: str, field: dict[str, Any], value: str, kind: str, dry: bool) -> None:
    if dry:
        print(f"DRY: set project field {field.get('name')}={value}")
        return
    cmd = ["gh", "project", "item-edit", "--id", item_id, "--project-id", project_id, "--field-id", field["id"]]
    if kind == "single":
        opt = option_id(field, value)
        if not opt:
            print(f"WARN: field {field.get('name')} has no option {value}", file=sys.stderr)
            return
        cmd.extend(["--single-select-option-id", opt])
    elif kind == "iteration":
        iter_id = iteration_id(field, value)
        if not iter_id:
            print(f"WARN: iteration field has no iteration {value}", file=sys.stderr)
            return
        cmd.extend(["--iteration-id", iter_id])
    elif kind == "text":
        cmd.extend(["--text", value])
    run(cmd, cwd=root)


def sync_item_fields(
    root: Path,
    project_id: str,
    item_id: str | None,
    fields: list[dict[str, Any]],
    status: str,
    release_train: str,
    iteration: str,
    change: str,
    capability: str | None,
    risk: str | None,
    dry: bool,
) -> None:
    if not item_id:
        return
    specs = [
        ("Status", STATUS_TO_BOARD[status], "single"),
        ("Iteration", iteration, "iteration"),
        ("Release Train", release_train, "single"),
        ("Mstone", release_train, "single"),
        ("OpenSpec Change", change, "text"),
    ]
    if capability:
        specs.append(("Capability", capability, "single"))
    if risk:
        specs.append(("Risk", risk, "single"))
    for name, value, kind in specs:
        field = find_field(fields, name)
        if field:
            edit_project_field(root, project_id, item_id, field, value, kind, dry)


def edit_issue_state(root: Path, repo: str, number: int, status: str, milestone: str, assignee: str, dry: bool) -> None:
    if dry or number < 0:
        return
    run(["gh", "issue", "edit", str(number), "-R", repo, "--milestone", milestone, "--add-assignee", assignee], cwd=root)
    if status == "done":
        run(["gh", "issue", "close", str(number), "-R", repo, "--reason", "completed"], cwd=root, check=False)
    else:
        run(["gh", "issue", "reopen", str(number), "-R", repo], cwd=root, check=False)


def subissue_numbers(root: Path, repo: str, parent: int) -> set[int]:
    out = run(
        ["gh", "api", f"repos/{repo}/issues/{parent}/sub_issues", "--paginate", "--jq", ".[].number"],
        cwd=root,
        check=False,
    )
    return {int(line) for line in out.splitlines() if line.strip().isdigit()}


def issue_database_id(root: Path, repo: str, number: int) -> int:
    out = run(["gh", "api", f"repos/{repo}/issues/{number}", "--jq", ".id"], cwd=root)
    return int(out)


def add_subissue(root: Path, repo: str, parent: int, child: int, dry: bool) -> None:
    if parent < 0 or child < 0:
        return
    if dry:
        print(f"DRY: link #{parent} -> #{child}")
        return
    if child in subissue_numbers(root, repo, parent):
        return
    child_id = issue_database_id(root, repo, child)
    cmd = ["gh", "api", "-X", "POST", f"repos/{repo}/issues/{parent}/sub_issues", "-F", f"sub_issue_id={child_id}"]
    run(cmd, cwd=root)


def regenerate_index(root: Path, change: str, dry: bool) -> None:
    script = root / "scripts" / "tasks-index.sh"
    if not script.exists():
        print("WARN: scripts/tasks-index.sh not found; skipped tasks.md regeneration", file=sys.stderr)
        return
    if dry:
        print(f"DRY: bash {script} {change}")
    else:
        run(["bash", str(script), change], cwd=root, capture=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--change")
    parser.add_argument("--org")
    parser.add_argument("--repo")
    parser.add_argument("--project-number")
    parser.add_argument("--project-id")
    parser.add_argument("--release-train", required=True)
    parser.add_argument("--iteration", required=True)
    parser.add_argument("--assignee", required=True)
    parser.add_argument("--capability")
    parser.add_argument("--risk")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    root = find_root(Path.cwd())
    change = infer_change(root, args.change)
    cfg = parse_project_yml(root)
    repo = args.repo or cfg["github"].get("repo") or infer_repo(root, None)
    if not repo:
        raise SyncError("repo is unknown; pass --repo owner/name or add .github/project.yml")
    org = args.org or cfg["github"].get("org") or repo.split("/", 1)[0]
    project_number = args.project_number or cfg["project"].get("number") or os.getenv("PROJECT_NUMBER")
    project_id = args.project_id or cfg["project"].get("id")
    if not project_number:
        raise SyncError("PROJECT_NUMBER missing; run board-bootstrap or pass --project-number")
    if not project_id:
        project_id = run(["gh", "project", "view", project_number, "--owner", org, "--format", "json", "--jq", ".id"], cwd=root)

    tasks = load_tasks(root, change)
    slices = sorted({task.slice for task in tasks})
    by_slice = {name: [task for task in tasks if task.slice == name] for name in slices}

    ensure_labels(root, repo, change, args.dry_run)
    ensure_milestone(root, repo, args.release_train, args.dry_run)
    fields = project_fields(root, org, project_number)
    items = project_items(root, org, project_number)
    issues = list_change_issues(root, repo, change)

    epic_title = f"[Epic] {change}: {change_title(root, change)}"
    epic_body = f"OpenSpec change: `{change}`.\n\nSource of truth: `openspec/changes/{change}/`.\nBoard owns delivery state only."
    epic = ensure_issue(
        root,
        repo,
        issues,
        "type:epic",
        f"[Epic] {change}",
        epic_title,
        epic_body,
        ["type:epic", f"openspec-change:{change}"],
        args.release_train,
        args.assignee,
        args.dry_run,
    )
    epic_item = ensure_project_item(root, org, project_number, repo, epic, items, args.dry_run)
    sync_item_fields(root, project_id, epic_item, fields, "blocked", args.release_train, args.iteration, change, args.capability, args.risk, args.dry_run)
    edit_issue_state(root, repo, epic, "todo", args.release_train, args.assignee, args.dry_run)

    story_numbers: dict[str, int] = {}
    task_numbers: dict[Path, int] = {}
    for slice_name, slice_tasks in by_slice.items():
        story_title = f"[Story] {change} {slice_name}"
        story_body = f"OpenSpec change: `{change}`.\n\nStory slice: `{slice_name}`.\nTasks are native sub-issues and source files live under `openspec/changes/{change}/tasks/`."
        story = ensure_issue(
            root,
            repo,
            issues,
            "type:story",
            story_title,
            story_title,
            story_body,
            ["type:story", f"openspec-change:{change}"],
            args.release_train,
            args.assignee,
            args.dry_run,
        )
        story_numbers[slice_name] = story
        story_status = "done" if all(task.status == "done" for task in slice_tasks) else "todo"
        story_item = ensure_project_item(root, org, project_number, repo, story, items, args.dry_run)
        sync_item_fields(root, project_id, story_item, fields, story_status, args.release_train, args.iteration, change, args.capability, args.risk, args.dry_run)
        edit_issue_state(root, repo, story, story_status, args.release_train, args.assignee, args.dry_run)
        add_subissue(root, repo, epic, story, args.dry_run)

        for task in slice_tasks:
            if task.issue:
                number = task.issue
            else:
                task_title = f"[Task] {change} {task.id} - {task.title}"
                task_prefix = f"[Task] {change} {task.id}"
                task_body = (
                    f"OpenSpec task: `openspec/changes/{change}/tasks/{task.path.name}`.\n"
                    f"Scenario ids: `{task.specs or 'see task file'}`.\n"
                )
                number = ensure_issue(
                    root,
                    repo,
                    issues,
                    "type:task",
                    task_prefix,
                    task_title,
                    task_body,
                    ["type:task", f"openspec-change:{change}"],
                    args.release_train,
                    args.assignee,
                    args.dry_run,
                )
                write_task_issue(task, number, args.dry_run)
            task_numbers[task.path] = number
            task_item = ensure_project_item(root, org, project_number, repo, number, items, args.dry_run)
            sync_item_fields(root, project_id, task_item, fields, task.status, args.release_train, args.iteration, change, args.capability, args.risk, args.dry_run)
            edit_issue_state(root, repo, number, task.status, args.release_train, args.assignee, args.dry_run)
            add_subissue(root, repo, story, number, args.dry_run)

    regenerate_index(root, change, args.dry_run)
    missing_issue = sum(1 for task in tasks if not task.issue and task_numbers.get(task.path, -1) < 0)
    print(
        json.dumps(
            {
                "change": change,
                "repo": repo,
                "epic": epic,
                "stories": len(story_numbers),
                "tasks": len(task_numbers),
                "task_files": len(tasks),
                "missing_issue": missing_issue,
                "project_number": project_number,
                "iteration": args.iteration,
                "release_train": args.release_train,
                "assignee": args.assignee,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SyncError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(2)
