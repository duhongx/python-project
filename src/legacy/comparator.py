"""结构比对模块"""

from typing import Dict, List, Any
from collections import defaultdict


class SchemaComparator:
    """结构比对器"""

    def __init__(self, config: dict):
        self.config = config

    def compare(self, pg_schema: dict, kb_schema: dict) -> dict:
        """
        比对 PostgreSQL 和 KingBase 的结构差异
        返回 KingBase 缺失的字段信息
        """
        result = {
            'missing_tables': [],
            'missing_views': [],
            'missing_fields': [],
            'summary': {}
        }

        # 比对表
        pg_tables = pg_schema.get('tables', {})
        kb_tables = kb_schema.get('tables', {})

        for full_name, pg_columns in pg_tables.items():
            if full_name not in kb_tables:
                result['missing_tables'].append(full_name)
                continue

            kb_columns = kb_tables[full_name]
            missing = self._compare_columns(full_name, pg_columns, kb_columns)
            result['missing_fields'].extend(missing)

        # 比对视图
        pg_views = pg_schema.get('views', {})
        kb_views = kb_schema.get('views', {})

        for full_name, pg_columns in pg_views.items():
            if full_name not in kb_views:
                result['missing_views'].append(full_name)
                continue

            kb_columns = kb_views[full_name]
            missing = self._compare_columns(full_name, pg_columns, kb_columns)
            result['missing_fields'].extend(missing)

        # 汇总统计
        result['summary'] = {
            'missing_tables_count': len(result['missing_tables']),
            'missing_views_count': len(result['missing_views']),
            'missing_fields_count': len(result['missing_fields']),
        }

        return result

    def _compare_columns(
        self,
        table_name: str,
        pg_columns: List[Dict],
        kb_columns: List[Dict]
    ) -> List[Dict]:
        """比对单个表的列差异"""
        missing = []

        # 构建 KingBase 列名映射，方便快速查找
        kb_columns_map = {col['column_name']: col for col in kb_columns}

        # 获取对象类型（从第一个列的定义中获取）
        object_type = None
        if pg_columns:
            object_type = pg_columns[0].get('object_type', 'UNKNOWN')

        for pg_col in pg_columns:
            col_name = pg_col['column_name']

            if col_name not in kb_columns_map:
                # KingBase 缺少此列
                missing.append({
                    'table': table_name,
                    'column': col_name,
                    'pg_definition': pg_col,
                    'position': pg_col['ordinal_position'],
                    'object_type': object_type,
                })

        return missing
