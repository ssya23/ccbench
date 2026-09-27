#!/usr/bin/env python3
"""results/<実験名>/<プロトコル>/<条件>/run*.log を集計し、実験ディレクトリ直下に
runs.csv (1行=1回の実行)、summary.csv (1行=プロトコル x 条件、tps が真ん中の回の値)、
wound_matrix.csv (1行=wound 行列の1マス、tps が真ん中の回の値、ログに行列がある場合のみ) を書く。

使い方: python3 experiments/parse.py results/tpcc_thread [results/...]
"""
import csv
import sys
from pathlib import Path

# 行頭の文字列 -> 列名。Details per transaction type より前にある全体の値。
GLOBAL_KEYS = {
    "throughput[tps]:": "tps",
    "abort_rate:": "abort_rate",
    "commit_counts_:": "commits",
    "abort_counts_:": "aborts",
}
# Transaction type: X の下にある値。列名は X_<名前> になる。
PER_TX_KEYS = {
    "commits:": "commits",
    "aborts:": "aborts",
    "by wound:": "aborts_by_wound",
    "by status:": "aborts_by_status",
    "wounds issued:": "wounds_issued",
    "abort rate:": "abort_rate",
}
# 条件の列の並び。ここにないものは名前順で後ろに付く。
COND_ORDER = ["thd", "wh"]


def to_num(s):
    try:
        return int(s)
    except ValueError:
        return float(s)


def parse_log(path):
    """1つのログから (項目 -> 値, wound 行列のマスのリスト) を返す。"""
    metrics = {}
    cells = []  # (wounder, victim, storage, count)
    tx = None
    matrix = None  # 行列を読んでいる途中の状態
    for raw in path.read_text(errors="replace").splitlines():
        line = raw.strip()
        if not line:
            continue

        if line.startswith("Wound matrix (row = wounder, column = victim):"):
            matrix = {"wounder": None, "cols": None}
            continue
        if line.startswith("Wound matrix by storage (wounder = "):
            wounder = line[len("Wound matrix by storage (wounder = "):].split(",")[0]
            matrix = {"wounder": wounder, "cols": None}
            continue
        if matrix is not None:
            tokens = line.split()
            if matrix["cols"] is None:
                matrix["cols"] = tokens[:-1]  # 最後の列は total
                continue
            if tokens[0] == "total":
                matrix = None
                continue
            name, values = tokens[0], tokens[1:-1]
            for col, v in zip(matrix["cols"], values):
                if matrix["wounder"] is None:  # 行=加害者, 列=被害者
                    cells.append((name, col, "all", int(v)))
                else:  # 行=被害者, 列=Storage
                    cells.append((matrix["wounder"], name, col, int(v)))
            continue

        if line.startswith("Transaction type:"):
            tx = line.split(":", 1)[1].strip()
            continue
        keys = GLOBAL_KEYS if tx is None else PER_TX_KEYS
        for prefix, name in keys.items():
            if line.startswith(prefix):
                value = to_num(line[len(prefix):].split()[0])
                metrics[name if tx is None else f"{tx}_{name}"] = value
                break
    return metrics, cells


def parse_cond(name):
    return {k: to_num(v) for k, v in (kv.split("=", 1) for kv in name.split("_"))}


def fmt(v):
    return f"{v:.4f}".rstrip("0").rstrip(".") if isinstance(v, float) else str(v)


def collect(exp_dir):
    runs = []  # dict: cc, cond, run, ok, metrics, cells
    for cc_dir in sorted(p for p in exp_dir.iterdir() if p.is_dir()):
        for cond_dir in sorted(p for p in cc_dir.iterdir() if p.is_dir()):
            cond = parse_cond(cond_dir.name)
            for log in sorted(cond_dir.glob("run*.log")):
                metrics, cells = parse_log(log)
                ok = "tps" in metrics
                if not ok:
                    print(f"warning: {log} に結果がありません (失敗またはタイムアウト)。集計から外します。")
                runs.append({"cc": cc_dir.name, "cond": cond, "run": int(log.stem[3:]),
                             "ok": ok, "metrics": metrics, "cells": cells})
    return runs


def cond_keys(runs):
    keys = {k for r in runs for k in r["cond"]}
    return [k for k in COND_ORDER if k in keys] + sorted(keys - set(COND_ORDER))


def metric_order(runs):
    order = []
    for r in runs:
        for k in r["metrics"]:
            if k not in order:
                order.append(k)
    first = [k for k in ["tps", "abort_rate", "commits", "aborts"] if k in order]
    return first + [k for k in order if k not in first]


def group(runs, ckeys):
    groups = {}
    for r in runs:
        if r["ok"]:
            key = (r["cc"],) + tuple(r["cond"].get(k) for k in ckeys)
            groups.setdefault(key, []).append(r)
    return dict(sorted(groups.items(), key=lambda kv: tuple(
        (x is None, x) for x in kv[0])))


def write_runs(exp_dir, runs, ckeys, mkeys):
    with open(exp_dir / "runs.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["cc", *ckeys, "run", "ok", *mkeys])
        for r in sorted(runs, key=lambda r: (r["cc"], *((r["cond"].get(k) is None, r["cond"].get(k))
                                                        for k in ckeys), r["run"])):
            w.writerow([r["cc"], *(r["cond"].get(k) for k in ckeys), r["run"], int(r["ok"]),
                        *(fmt(r["metrics"][k]) if k in r["metrics"] else "" for k in mkeys)])


def median_run(rs):
    """tps で並べて真ん中の回を返す (偶数回なら真ん中2つの小さい方)。"""
    return sorted(rs, key=lambda r: r["metrics"]["tps"])[(len(rs) - 1) // 2]


def write_summary(exp_dir, groups, ckeys):
    gkeys = list(GLOBAL_KEYS.values())
    header = ["cc", *ckeys, "runs", "median_run", *gkeys]
    with open(exp_dir / "summary.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for key, rs in groups.items():
            med = median_run(rs)
            w.writerow([*key, len(rs), med["run"],
                        *(fmt(med["metrics"][k]) if k in med["metrics"] else "" for k in gkeys)])


def write_wound_matrix(exp_dir, groups, ckeys):
    if not any(r["cells"] for rs in groups.values() for r in rs):
        return False
    with open(exp_dir / "wound_matrix.csv", "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["cc", *ckeys, "median_run", "wounder", "victim", "storage", "count"])
        for key, rs in groups.items():
            med = median_run(rs)
            for wounder, victim, storage, count in med["cells"]:
                w.writerow([*key, med["run"], wounder, victim, storage, count])
    return True


def main():
    if len(sys.argv) < 2:
        sys.exit(f"使い方: {sys.argv[0]} results/<実験名> [results/<実験名> ...]")
    for arg in sys.argv[1:]:
        exp_dir = Path(arg)
        runs = collect(exp_dir)
        if not runs:
            print(f"warning: {exp_dir} にログがありません。")
            continue
        ckeys = cond_keys(runs)
        mkeys = metric_order([r for r in runs if r["ok"]])
        groups = group(runs, ckeys)
        write_runs(exp_dir, runs, ckeys, mkeys)
        write_summary(exp_dir, groups, ckeys)
        outputs = ["runs.csv", "summary.csv"]
        if write_wound_matrix(exp_dir, groups, ckeys):
            outputs.append("wound_matrix.csv")
        print(f"{exp_dir}: {', '.join(outputs)} を書きました ({len(groups)} 条件, {len(runs)} 回分)")


if __name__ == "__main__":
    main()
