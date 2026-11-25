"""
main.py
CLI entrypoint for the motivated seller pipeline.

Usage:
  python main.py run
  python main.py run --source pa_philadelphia_tax_delinquent
  python main.py push-sheet
  python main.py migrate-sheet-tab <tab>
  python main.py init-db
"""
import importlib
import json
import os
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import click
import structlog
from dotenv import load_dotenv

load_dotenv()
structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(
        logging.getLevelName(os.environ.get("LOG_LEVEL", "INFO"))
    )
)
log = structlog.get_logger()


@click.group()
def cli():
    pass


@cli.command()
def init_db():
    from core.database import init_db as _init
    engine = _init()
    click.echo("Database initialized.")


@cli.command()
@click.option("--source", default=None)
@click.option("--config", default=None)
@click.option("--dry-run", is_flag=True)
def run(source, config, dry_run):
    from core.config import load_sources, get_source
    from core.database import init_db as _init
    from core.dedupe import upsert_lead

    engine = _init()

    if source:
        sources = [get_source(source)]
        if not sources[0]:
            click.echo(f"Source '{source}' not found.", err=True)
            sys.exit(1)
    else:
        sources = load_sources(config_path=config)

    started_at = datetime.now(timezone.utc)
    summary = {
        "started_at": started_at.isoformat(),
        "completed_at": None,
        "dry_run": dry_run,
        "sources": [],
    }
    successful_sources = 0

    for cfg in sources:
        module_name = f"adapters.{cfg['parser_module']}"
        try:
            mod = importlib.import_module(module_name)
        except ImportError as e:
            log.warning("main.adapter_not_found", module=module_name, error=str(e))
            summary["sources"].append({
                "source_id": cfg["id"], "status": "failed", "error": str(e)[:500],
                "total": 0, "new": 0,
            })
            continue

        class_name = _to_class_name(cfg["parser_module"])
        adapter_cls = getattr(mod, class_name, None)
        if not adapter_cls:
            log.warning("main.class_not_found", class_name=class_name)
            summary["sources"].append({
                "source_id": cfg["id"], "status": "failed",
                "error": f"Adapter class {class_name} not found", "total": 0, "new": 0,
            })
            continue

        adapter = adapter_cls(cfg, engine=engine)
        log.info("main.adapter_start", source_id=cfg["id"])

        new_count = 0
        total_count = 0
        try:
            for record in adapter.run():
                total_count += 1
                if not dry_run:
                    _, is_new = upsert_lead(record, engine=engine)
                    if is_new:
                        new_count += 1
            successful_sources += 1
            summary["sources"].append({
                "source_id": cfg["id"], "status": "success",
                "total": total_count, "new": new_count,
            })
            log.info("main.adapter_done", source_id=cfg["id"],
                     total=total_count, new=new_count, dry_run=dry_run)
        except Exception as e:
            log.exception("main.adapter_failed", source_id=cfg["id"], error=str(e))
            summary["sources"].append({
                "source_id": cfg["id"], "status": "failed", "error": str(e)[:500],
                "total": total_count, "new": new_count,
            })

    summary["completed_at"] = datetime.now(timezone.utc).isoformat()
    summary["successful_sources"] = successful_sources
    summary["failed_sources"] = len(sources) - successful_sources
    summary_path = Path("data/run_summary.json")
    summary_path.parent.mkdir(parents=True, exist_ok=True)
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")

    if sources and successful_sources == 0:
        raise click.ClickException("All configured sources failed. See data/run_summary.json.")


@cli.command()
@click.option("--sheet-id", envvar="GOOGLE_SHEET_ID", required=True)
@click.option("--tab", default="master_leads")
@click.option("--limit", default=500, type=int)
def push_sheet(sheet_id, tab, limit):
    from core.database import init_db as _init
    from core.sheets_export import push_leads_to_sheet
    engine = _init()
    pushed = push_leads_to_sheet(
        spreadsheet_id=sheet_id,
        tab_name=tab,
        engine=engine,
        limit=limit,
    )
    click.echo(f"Pushed {pushed} leads to '{tab}'.")


@cli.command()
@click.argument("tab_name")
@click.option("--sheet-id", envvar="GOOGLE_SHEET_ID", required=True)
def migrate_sheet_tab(tab_name, sheet_id):
    from core.sheets_export import add_date_uploaded_to_existing_tab
    add_date_uploaded_to_existing_tab(
        spreadsheet_id=sheet_id,
        tab_name=tab_name,
    )
    click.echo(f"Migrated tab '{tab_name}' — 'Date Uploaded' column added.")


def _to_class_name(module_name: str) -> str:
    return "".join(w.capitalize() for w in module_name.split("_")) + "Adapter"


if __name__ == "__main__":
    cli()
