"""报告生成模块"""

from datetime import datetime
from pathlib import Path
from typing import Dict, List
from rich.console import Console
from rich.table import Table as RichTable
from rich.panel import Panel


class ReportGenerator:
    """报告生成器"""

    def __init__(self, config: dict):
        self.config = config
        self.console = Console()
        self.reports_dir = Path(__file__).parent / "reports"
        self.reports_dir.mkdir(exist_ok=True)

    def print_summary(self, diff: dict):
        """打印差异摘要"""
        summary = diff.get('summary', {})

        # 创建摘要表格
        table = RichTable(title="\n📊 结构差异摘要", show_header=True)
        table.add_column("差异类型", style="cyan")
        table.add_column("数量", justify="right", style="yellow")

        table.add_row("缺失的表", str(summary.get('missing_tables_count', 0)))
        table.add_row("缺失的视图", str(summary.get('missing_views_count', 0)))
        table.add_row("缺失的字段", str(summary.get('missing_fields_count', 0)))

        self.console.print(table)

        # 显示缺失字段详情（前 20 条）
        missing_fields = diff.get('missing_fields', [])
        if missing_fields:
            self._print_missing_fields(missing_fields[:20])

            if len(missing_fields) > 20:
                self.console.print(f"\n... 还有 {len(missing_fields) - 20} 个缺失字段未显示")

    def _print_missing_fields(self, fields: List[Dict]):
        """打印缺失字段详情"""
        table = RichTable(title="\n⚠️ 缺失字段详情（部分）", show_header=True)
        table.add_column("表", style="cyan")
        table.add_column("字段名", style="yellow")
        table.add_column("类型", style="green")
        table.add_column("位置", justify="right")
        table.add_column("可空", style="magenta")

        for field in fields:
            pg_def = field['pg_definition']
            table.add_row(
                field['table'],
                field['column'],
                self._format_type(pg_def),
                str(field['position']),
                pg_def['is_nullable'],
            )

        self.console.print(table)

    def _format_type(self, col_def: Dict) -> str:
        """格式化类型显示"""
        data_type = col_def['data_type']

        # 添加长度信息
        if col_def['character_maximum_length']:
            return f"{data_type}({col_def['character_maximum_length']})"

        if col_def['numeric_precision'] and col_def['numeric_scale']:
            return f"{data_type}({col_def['numeric_precision']}, {col_def['numeric_scale']})"

        if col_def['numeric_precision'] and data_type in ('numeric', 'decimal'):
            return f"{data_type}({col_def['numeric_precision']})"

        return data_type

    def save_report(self, diff: dict):
        """保存详细报告到文件"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = self.reports_dir / f"diff_report_{timestamp}.txt"

        with open(report_path, 'w', encoding='utf-8') as f:
            f.write("=" * 60 + "\n")
            f.write("PostgreSQL → KingBase 结构差异报告\n")
            f.write(f"生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 60 + "\n\n")

            # 摘要
            summary = diff.get('summary', {})
            f.write("【摘要】\n")
            f.write(f"  缺失的表: {summary.get('missing_tables_count', 0)}\n")
            f.write(f"  缺失的视图: {summary.get('missing_views_count', 0)}\n")
            f.write(f"  缺失的字段: {summary.get('missing_fields_count', 0)}\n\n")

            # 缺失的表
            if diff.get('missing_tables'):
                f.write("【缺失的表】\n")
                for table in diff['missing_tables']:
                    f.write(f"  - {table}\n")
                f.write("\n")

            # 缺失的视图
            if diff.get('missing_views'):
                f.write("【缺失的视图】\n")
                for view in diff['missing_views']:
                    f.write(f"  - {view}\n")
                f.write("\n")

            # 缺失的字段详情
            if diff.get('missing_fields'):
                f.write("【缺失的字段详情】\n")
                for field in diff['missing_fields']:
                    pg_def = field['pg_definition']
                    f.write(f"\n  表: {field['table']}\n")
                    f.write(f"    字段名: {field['column']}\n")
                    f.write(f"    类型: {self._format_type(pg_def)}\n")
                    f.write(f"    位置: {field['position']}\n")
                    f.write(f"    可空: {pg_def['is_nullable']}\n")
                    if pg_def['column_default']:
                        f.write(f"    默认值: {pg_def['column_default']}\n")

        self.console.print(f"\n📄 报告已保存到: [cyan]{report_path}[/cyan]")
