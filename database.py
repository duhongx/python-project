"""数据库连接和查询模块"""

import psycopg2
from collections import defaultdict
from typing import Optional, List, Dict, Any


class DatabaseManager:
    """数据库管理器"""

    def __init__(self, config: dict):
        self.config = config
        self.pg_conn: Optional[Any] = None
        self.kb_conn: Optional[Any] = None

    def connect(self) -> bool:
        """连接两个数据库"""
        try:
            # 连接 PostgreSQL
            pg_config = self.config['postgresql']
            self.pg_conn = psycopg2.connect(
                host=pg_config['host'],
                port=pg_config['port'],
                database=pg_config['database'],
                user=pg_config['user'],
                password=pg_config['password'],
                client_encoding='utf8'
            )

            # 连接 KingBase (使用 psycopg2 驱动，KingBase 兼容)
            kb_config = self.config['kingbase']
            self.kb_conn = psycopg2.connect(
                host=kb_config['host'],
                port=kb_config['port'],
                database=kb_config['database'],
                user=kb_config['user'],
                password=kb_config['password'],
                client_encoding='utf8'
            )

            return True

        except Exception as e:
            print(f"连接数据库失败: {e}")
            return False

    def close(self):
        """关闭连接"""
        if self.pg_conn:
            self.pg_conn.close()
        if self.kb_conn:
            self.kb_conn.close()

    def _get_schemas_to_check(self, conn) -> List[str]:
        """获取需要检查的 schema 列表"""
        user_prefix = self.config['filter']['user_prefix']
        schema_prefix = self.config['filter']['schema_prefix']

        with conn.cursor() as cur:
            # 获取指定前缀的用户拥有的 schema
            # nspowner 是 oid 类型，需要用 pg_get_userbyid() 转换
            cur.execute("""
                SELECT DISTINCT n.nspname
                FROM pg_namespace n
                WHERE pg_get_userbyid(n.nspowner) LIKE %s
                AND n.nspname LIKE %s
                ORDER BY n.nspname
            """, (f"{user_prefix}%", f"{schema_prefix}%"))
            return [row[0] for row in cur.fetchall()]

    def _get_tables_and_views(self, conn, schemas: List[str], object_type: str) -> Dict[str, List[Dict]]:
        """获取表或视图的结构信息"""
        result = defaultdict(list)

        with conn.cursor() as cur:
            for schema in schemas:
                cur.execute("""
                    SELECT
                        t.table_name,
                        c.column_name,
                        c.ordinal_position,
                        c.data_type,
                        c.character_maximum_length,
                        c.numeric_precision,
                        c.numeric_scale,
                        c.is_nullable,
                        c.column_default
                    FROM information_schema.tables t
                    JOIN information_schema.columns c ON c.table_name = t.table_name
                        AND c.table_schema = t.table_schema
                    WHERE t.table_schema = %s
                        AND t.table_type = %s
                    ORDER BY t.table_name, c.ordinal_position
                """, (schema, 'BASE TABLE' if object_type == 'table' else 'VIEW'))

                rows = cur.fetchall()
                for row in rows:
                    table_name = row[0]
                    column_default = row[8] if len(row) > 8 else None
                    # 检测是否为 serial 字段
                    is_serial = column_default and 'nextval' in column_default

                    result[f"{schema}.{table_name}"].append({
                        'column_name': row[1],
                        'ordinal_position': row[2],
                        'data_type': row[3],
                        'character_maximum_length': row[4],
                        'numeric_precision': row[5],
                        'numeric_scale': row[6],
                        'is_nullable': row[7],
                        'column_default': column_default,
                        'is_serial': is_serial,
                        'object_type': 'BASE TABLE' if object_type == 'table' else 'VIEW',
                    })

        return dict(result)

    def get_pg_schema(self) -> dict:
        """获取 PostgreSQL 的结构信息"""
        schemas = self._get_schemas_to_check(self.pg_conn)
        print(f"  发现 {len(schemas)} 个 PostgreSQL schema: {schemas[:3]}...")

        result = {
            'schemas': schemas,
            'tables': self._get_tables_and_views(self.pg_conn, schemas, 'table'),
            'views': self._get_tables_and_views(self.pg_conn, schemas, 'view'),
        }

        return result

    def get_kb_schema(self) -> dict:
        """获取 KingBase 的结构信息"""
        schemas = self._get_schemas_to_check(self.kb_conn)
        print(f"  发现 {len(schemas)} 个 KingBase schema: {schemas[:3]}...")

        result = {
            'schemas': schemas,
            'tables': self._get_tables_and_views(self.kb_conn, schemas, 'table'),
            'views': self._get_tables_and_views(self.kb_conn, schemas, 'view'),
        }

        return result

    def get_view_definition(self, conn, schema: str, view_name: str) -> str:
        """
        获取视图的完整定义（CREATE VIEW 语句）
        """
        with conn.cursor() as cur:
            # 获取视图定义
            cur.execute("""
                SELECT view_definition
                FROM information_schema.views
                WHERE table_schema = %s AND table_name = %s
            """, (schema, view_name))
            result = cur.fetchone()

            if not result or not result[0]:
                return None

            view_definition = result[0]

            # 构建完整的 CREATE VIEW 语句
            create_view = f"CREATE OR REPLACE VIEW {schema}.{view_name} AS\n{view_definition}"

            return create_view

    def execute_alter(self, sql: str) -> bool:
        """在 KingBase 上执行 ALTER 语句"""
        try:
            with self.kb_conn.cursor() as cur:
                cur.execute(sql)
                self.kb_conn.commit()
                return True
        except Exception as e:
            print(f"执行失败: {sql}")
            print(f"错误: {e}")
            self.kb_conn.rollback()
            return False
