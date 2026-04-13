"""结构同步模块"""

from typing import Dict, List
from rich.console import Console
from rich.panel import Panel
from rich.table import Table as RichTable


class SchemaSyncer:
    """结构同步器"""

    def __init__(self, db, config: dict):
        self.db = db
        self.config = config
        self.console = Console()

    def sync(self, diff: dict):
        """执行同步操作"""
        missing_fields = diff.get('missing_fields', [])

        if not missing_fields:
            self.console.print("[yellow]没有需要同步的字段。[/yellow]")
            return

        # 区分表和视图
        table_fields = [f for f in missing_fields if f.get('object_type') == 'BASE TABLE']
        view_fields = [f for f in missing_fields if f.get('object_type') == 'VIEW']

        self.console.print(f"\n[cyan]缺失字段分类:[/cyan]")
        self.console.print(f"  表字段: {len(table_fields)} 个")
        self.console.print(f"  视图字段: {len(view_fields)} 个 (需重新创建视图)")

        # 处理表字段
        if table_fields:
            self._sync_table_fields(table_fields)

        # 处理视图字段
        if view_fields:
            self._sync_view_fields(view_fields)

    def _sync_table_fields(self, table_fields: List[Dict]):
        """同步表字段"""
        # 生成 ALTER 语句
        alter_statements = self._generate_alter_statements(table_fields)

        # 显示将要执行的 SQL
        self._show_preview(alter_statements)

        # 确认后执行
        if not self.config['options'].get('auto_execute', False):
            self.console.print("\n[yellow]请确认是否执行以上 SQL 语句？[y/N][/yellow]")
            response = input().strip().lower()
            if response != 'y':
                self.console.print("[yellow]已取消表字段同步操作。[/yellow]")
                return

        # 执行同步
        self._execute_sync(alter_statements)

    def _sync_view_fields(self, view_fields: List[Dict]):
        """同步视图字段（通过重新创建视图）"""
        # 收集需要同步的视图
        views_to_sync = {}
        for field in view_fields:
            view_name = field['table']
            if view_name not in views_to_sync:
                views_to_sync[view_name] = []
            views_to_sync[view_name].append(field['column'])

        self.console.print(f"\n[cyan]需要重新创建 {len(views_to_sync)} 个视图[/cyan]")

        # 获取视图定义
        view_sqls = []
        for view_name in views_to_sync.keys():
            schema, name = view_name.split('.')
            view_def = self.db.get_view_definition(self.db.pg_conn, schema, name)
            if view_def:
                view_sqls.append({
                    'name': view_name,
                    'sql': view_def,
                    'missing_count': len(views_to_sync[view_name])
                })

        # 显示预览
        self._show_view_preview(view_sqls)

        # 确认后执行
        if not self.config['options'].get('auto_execute', False):
            self.console.print("\n[yellow]请确认是否重新创建以上视图？[y/N][/yellow]")
            response = input().strip().lower()
            if response != 'y':
                self.console.print("[yellow]已取消视图同步操作。[/yellow]")
                return

        # 执行视图同步
        self._execute_view_sync(view_sqls)

    def _generate_alter_statements(self, missing_fields: List[Dict]) -> List[str]:
        """生成 ALTER TABLE 语句"""
        statements = []
        warnings = []

        for field in missing_fields:
            table = field['table']
            col_name = field['column']
            pg_def = field['pg_definition']

            # 构建 ADD COLUMN 子句
            column_def, needs_not_null = self._build_column_definition(pg_def)

            sql = f"ALTER TABLE {table} ADD COLUMN {column_def};"
            statements.append(sql)

            # 如果需要 NOT NULL 但没有默认值，记录警告
            if needs_not_null:
                warnings.append(f"⚠️ {table}.{col_name}: 添加为可空字段（原定义为 NOT NULL，但无默认值）")

        # 显示警告
        if warnings:
            self.console.print("\n[yellow]注意事项:[/yellow]")
            for warning in warnings:
                self.console.print(f"  {warning}")
            self.console.print("\n[yellow]提示: 对于 NOT NULL 字段，请在数据填充后手动执行:[/yellow]")
            for field in missing_fields:
                pg_def = field['pg_definition']
                if pg_def['is_nullable'] == 'NO' and not pg_def['column_default']:
                    self.console.print(f"  ALTER TABLE {field['table']} ALTER COLUMN {field['column']} SET NOT NULL;")
            self.console.print()

        return statements

    def _build_column_definition(self, col_def: Dict) -> tuple:
        """
        构建列定义字符串
        返回: (SQL语句, 需要后续设置NOT NULL的标志)
        """
        parts = [col_def['column_name']]

        # 数据类型
        data_type = col_def['data_type']

        if col_def['character_maximum_length']:
            data_type = f"{data_type}({col_def['character_maximum_length']})"
        elif col_def['numeric_precision'] and col_def['numeric_scale']:
            data_type = f"{data_type}({col_def['numeric_precision']}, {col_def['numeric_scale']})"
        elif col_def['numeric_precision'] and data_type in ('numeric', 'decimal'):
            data_type = f"{data_type}({col_def['numeric_precision']})"

        parts.append(data_type)

        # DEFAULT 值
        if col_def['column_default']:
            parts.append(f"DEFAULT {col_def['column_default']}")

        # NOT NULL 处理：如果无默认值，先添加为可空
        is_nullable = col_def['is_nullable'] == 'YES'
        has_default = bool(col_def['column_default'])
        needs_not_null = col_def['is_nullable'] == 'NO' and not has_default

        if not needs_not_null and not is_nullable:
            parts.append("NOT NULL")

        return (" ".join(parts), needs_not_null)

    def _show_preview(self, statements: List[str]):
        """显示预览"""
        self.console.print(Panel.fit(
            "\n".join(statements[:10]) + ("\n..." if len(statements) > 10 else ""),
            title=f"[yellow]将要执行的 SQL (共 {len(statements)} 条)[/yellow]",
            border_style="yellow"
        ))

    def _show_view_preview(self, view_sqls: List[Dict]):
        """显示视图预览"""
        preview_lines = []
        for i, view in enumerate(view_sqls[:10], 1):
            preview_lines.append(f"{i}. {view['name']} ({view['missing_count']} 字段)")

        if len(view_sqls) > 10:
            preview_lines.append(f"... 还有 {len(view_sqls) - 10} 个视图")

        self.console.print(Panel.fit(
            "\n".join(preview_lines),
            title=f"[yellow]将要重新创建的视图 (共 {len(view_sqls)} 个)[/yellow]",
            border_style="cyan"
        ))

    def _execute_sync(self, statements: List[str]):
        """执行同步"""
        total = len(statements)
        success = 0
        failed = 0

        self.console.print(f"\n[cyan]开始执行同步...[/cyan]")

        for i, sql in enumerate(statements, 1):
            if self.db.execute_alter(sql):
                success += 1
                self.console.print(f"  [{i}/{total}] ✓", end="\r")
            else:
                failed += 1
                self.console.print(f"  [{i}/{total}] ✗ 失败", end="\r")

        self.console.print(f"\n[green]同步完成![/green]")
        self.console.print(f"  成功: {success}, 失败: {failed}")

    def _execute_view_sync(self, view_sqls: List[Dict]):
        """执行视图同步"""
        total = len(view_sqls)
        success = 0
        failed = 0

        self.console.print(f"\n[cyan]开始重新创建视图...[/cyan]")

        for i, view_info in enumerate(view_sqls, 1):
            sql = view_info['sql']
            name = view_info['name']
            schema = name.split('.')[0]
            view_name = name.split('.')[1]

            # 先尝试 CREATE OR REPLACE VIEW
            if self.db.execute_alter(sql):
                success += 1
                self.console.print(f"  [{i}/{total}] {name} ✓", end="\r")
            else:
                # 如果失败，尝试先 DROP 再 CREATE
                self.console.print(f"\n  [{i}/{total}] {name} CREATE OR REPLACE 失败，尝试 DROP 后重建")

                drop_sql = f"DROP VIEW IF EXISTS {schema}.{view_name} CASCADE;"
                if self.db.execute_alter(drop_sql):
                    if self.db.execute_alter(sql):
                        success += 1
                        self.console.print(f"  [{i}/{total}] {name} ✓ (重建)")
                    else:
                        failed += 1
                        self.console.print(f"  [{i}/{total}] {name} ✗ 重建失败")
                else:
                    failed += 1
                    self.console.print(f"  [{i}/{total}] {name} ✗ DROP 失败")

        self.console.print(f"\n[green]视图同步完成![/green]")
        self.console.print(f"  成功: {success}, 失败: {failed}")
