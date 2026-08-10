# 本地 Airflow 学习环境

这个分支提供一套本地学习用 Airflow 3.2.1。它不会修改生产 Airflow，也不会把 Airflow 加进项目 `pyproject.toml`。

## 启动

```bash
scripts/local_airflow_setup.sh
scripts/local_airflow_webserver.sh
```

打开：

```text
http://localhost:8080
```

登录：

```text
admin / admin
```

## 推荐学习路径

1. 进入 DAG 首页，看这几个 DAG：
   - `hko_realtime_current`
   - `hko_realtime_archive_backfill`
   - `hko_official_d1`
   - `hko_realtime_raw_cleanup`
   - `hko_initial_backfill`
2. 点进 `hko_realtime_current`，看 `Grid`、`Graph`、`Code`。
3. 本地稳定跑单个任务：

```bash
scripts/local_airflow_test_current.sh
```

这条命令会通过 Airflow 的 `tasks test` 执行：

```bash
uv run python update_hko_realtime_postgres.py --mode current --include-rainfall
```

然后查询本地 Docker Postgres，确认 raw 表和 provisional daily 表有数据。

## 为什么本地不默认启动 scheduler

生产环境应该用真正的 Airflow scheduler。但在 macOS 临时 venv 里，Airflow 的 scheduled task runner 偶尔会卡在本机子进程管理上，容易把学习重点带偏。

所以本地 demo 默认用于：

- 看 Airflow UI
- 看 DAG schedule / task / code
- 用 `tasks test` 稳定执行单个任务
- 验证业务脚本可以落库

生产集成仍然按 `AIRFLOW_RUNBOOK.md`，把 DAG 放到 `/opt/llm/airflow/dags`，由现有 Airflow 统一调度。
