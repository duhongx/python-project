#!/usr/bin/env python3
"""
PostgreSQL -> KingBase 表结构同步工具
比对并补全 KingBase 中缺失的字段
"""

import sys
from rich.console import Console
from rich.panel import Panel

from config import load_config
from database import DatabaseManager
from comparator import SchemaComparator
from report import ReportGenerator
from sync import SchemaSyncer

console = Console()


def main():
    console.print(Panel.fit("[bold cyan]PostgreSQL → KingBase 结构同步工具[/bold cyan]"))

    # 1. 加载配置
    console.print("\n[yellow]1. 加载配置...[/yellow]")
    config = load_config()

    # 2. 连接数据库
    console.print("\n[yellow]2. 连接数据库...[/yellow]")
    db = DatabaseManager(config)
    if not db.connect():
        console.print("[red]数据库连接失败！[/red]")
        sys.exit(1)

    # 3. 获取结构信息
    console.print("\n[yellow]3. 获取结构信息...[/yellow]")
    pg_schema = db.get_pg_schema()
    kb_schema = db.get_kb_schema()
    console.print(f"[green]PostgreSQL: {len(pg_schema.get('tables', []))} 张表, {len(pg_schema.get('views', []))} 个视图[/green]")
    console.print(f"[green]KingBase: {len(kb_schema.get('tables', []))} 张表, {len(kb_schema.get('views', []))} 个视图[/green]")

    # 4. 比对差异
    console.print("\n[yellow]4. 比对结构差异...[/yellow]")
    comparator = SchemaComparator(config)
    diff = comparator.compare(pg_schema, kb_schema)

    # 5. 生成报告
    console.print("\n[yellow]5. 生成差异报告...[/yellow]")
    reporter = ReportGenerator(config)
    reporter.print_summary(diff)
    reporter.save_report(diff)

    # 6. 确认并执行
    if diff['missing_fields']:
        console.print("\n[yellow]6. 同步缺失字段...[/yellow]")
        syncer = SchemaSyncer(db, config)
        syncer.sync(diff)
    else:
        console.print("[green]没有发现缺失字段，无需同步。[/green]")

    db.close()
    console.print("\n[green]✓ 完成！[/green]")


if __name__ == "__main__":
    main()
