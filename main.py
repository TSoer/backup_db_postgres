#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Python-дампер PostgreSQL без pg_dump.
psql -U postgres -d test_backup -f "file.sql" отрабатывает пробовал на не больших база ~100MB
"""

import os
import sys
import json
import gzip
import datetime
from typing import Dict, Tuple, List, Iterable, Optional

import psycopg2
from psycopg2.extras import DictCursor
from psycopg2.extensions import adapt, Binary

# ----------------------- Параметры -----------------------

DEFAULT_SCHEMAS = ("public", "auditlog")
DEFAULT_BATCH_SIZE = 5000

# ----------------------- Утилиты -------------------------


def qident(name: str) -> str:
    """Кавычки для SQL-идентификатора."""
    return '"' + name.replace('"', '""') + '"'


def now_stamp() -> str:
    return datetime.datetime.now().strftime("%Y%m%d_%H%M%S")


def sql_literal(value) -> str:
    if value is None:
        return "NULL"

    # dict/list/tuple -> JSON
    if isinstance(value, (dict, list, tuple)):
        try:
            js = json.dumps(value, ensure_ascii=False)
        except Exception:
            js = str(value)
        return "'" + js.replace("'", "''") + "'"

    # memoryview/bytes -> bytea
    if isinstance(value, memoryview):
        value = bytes(value)
    if isinstance(value, (bytes, bytearray)):
        return adapt(Binary(bytes(value))).getquoted().decode("utf-8")

    # строки → вручную экранируем
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"

    # остальные типы оставляем через adapt
    try:
        q = adapt(value).getquoted()
        return q.decode("utf-8") if isinstance(q, bytes) else str(q)
    except Exception:
        return "'" + str(value).replace("'", "''") + "'"


def _short_pg_ver(ver_str: str) -> str:
    """
    Приводит '15.4 (Debian ...)' -> '15.4', '13.12' -> '13.12', '12' -> '12'
    """
    v = ver_str.strip().split()[0]
    parts = v.split(".")
    if len(parts) >= 2 and parts[1].isdigit():
        return parts[0] + "." + parts[1]
    return parts[0]


# ----------------------- Класс дампера -------------------

class PostgresBackupManager:
    """
    Делает SQL-дамп максимально близкий к pg_dump, только через psycopg2.
    Порядок объектов подобран под корректное восстановление с зависимостями.
    """

    def __init__(
        self,
        host: str,
        port: int,
        dbname: str,
        user: str,
        password: str,
        schemas: Tuple[str, ...] = DEFAULT_SCHEMAS,
        batch_size: int = DEFAULT_BATCH_SIZE,
        pretend_pg_dump_version: Optional[str] = None,  # строка вида '15.4' для шапки
    ):
        self.host = host
        self.port = int(port)
        self.dbname = dbname
        self.user = user
        self.password = password
        self.schemas = tuple(schemas)
        if isinstance(schemas, str):
            self.schemas = (str(schemas),)
        self.batch_size = int(batch_size)
        self._server_version_str = ""
        self._pg_dump_version_str = pretend_pg_dump_version or ""

        # Поднимаем соединение и жёстко ставим UTF-8
        self.conn = psycopg2.connect(
            host=self.host, port=self.port, dbname=self.dbname,
            user=self.user, password=self.password,
            options="-c search_path=pg_catalog"
        )
        self.conn.set_session(autocommit=True)
        try:
            self.conn.set_client_encoding('UTF8')
        except Exception:
            pass

        # На всякий случай — окружение для pg-клиента
        os.environ["PGCLIENTENCODING"] = "UTF8"

        # Версии для шапки
        with self.conn.cursor() as cur:
            cur.execute("SHOW server_version;")
            self._server_version_str = cur.fetchone()[0]
        if not self._pg_dump_version_str:
            # По умолчанию считаем, что "Dumped by pg_dump version" совпадает с серверной
            self._pg_dump_version_str = _short_pg_ver(self._server_version_str)

    # -------------------- Публичный API -------------------

    def backup(self, out_sql_path: str, compress: bool = False):
        """
        Полноценный дамп с правильным порядком объектов.
        """
        writer = open(out_sql_path, "w", encoding="utf-8", newline="\n")

        with writer as f:
            self._prolog_like_pg_dump(f)           # 0) Пролог как у pg_dump (важно!)
            self._dump_extensions(f)               # 1) EXTENSIONS
            self._dump_schemas(f)                  # 2) SCHEMAS
            # self._dump_types(f)                    # 3) TYPES (enum/domain/composite)
            self._dump_sequences_create(f)         # 4) CREATE SEQUENCE (без OWNED BY!)
            self._dump_tables(f)                   # 5) CREATE TABLE
            self._dump_comments_tables_columns(f)  # 6) COMMENTS
            self._dump_routines(f)                 # 7) FUNCTIONS/PROCEDURES
            self._dump_data(f)                     # 8) COPY-данные
            self._dump_sequences_state(f)          # 9) setval + INCREMENT BY
            # self._dump_indexes(f)                # 10) INDEXES
            self._dump_constraints(f)              # 11) CONSTRAINTS (FK в самом конце этого блока)
            self._dump_views(f)                    # 12) VIEWS
            self._dump_matviews(f)                 # 13) MATERIALIZED VIEWS
            self._dump_triggers(f)                 # 14) TRIGGERS
            self._dump_sequences_ownedby(f)        # 15) ALTER SEQUENCE ... OWNED BY (после таблиц и т.п.)
            self._dump_grants(f)                   # 16) GRANTS
            self._dump_roles_basic(f)              # 17) ROLES (basic)
            f.write("\n-- Done.\n")

    # -------------------- Блоки дампа ---------------------

    def _prolog_like_pg_dump(self, f):
        """
        Пролог максимально как у pg_dump.
        """
        db_ver = _short_pg_ver(self._server_version_str)
        pgdump_ver = self._pg_dump_version_str

        f.write(f"-- Dumped from database version {db_ver}\n")
        f.write(f"-- Dumped by pg_dump version {pgdump_ver}\n\n")

        f.write("SET statement_timeout = 0;\n")
        f.write("SET lock_timeout = 0;\n")
        f.write("SET idle_in_transaction_session_timeout = 0;\n")
        f.write("SET client_encoding = 'UTF8';\n")
        f.write("SET standard_conforming_strings = on;\n")
        f.write("SET search_path = pg_catalog;\n")
        # f.write("SELECT pg_catalog.set_config('search_path', '', false);\n")
        f.write("SET check_function_bodies = false;\n")
        f.write("SET xmloption = content;\n")
        f.write("SET client_min_messages = warning;\n")
        f.write("SET row_security = off;\n\n")

    def _dump_extensions(self, f):
        cur = self.conn.cursor(cursor_factory=DictCursor)
        cur.execute("SELECT extname, extversion FROM pg_extension ORDER BY extname;")
        rows = cur.fetchall()
        if rows:
            f.write("-- EXTENSIONS\n")
            for r in rows:
                f.write(
                    f"CREATE EXTENSION IF NOT EXISTS {qident(r['extname'])} "
                    f"WITH VERSION {sql_literal(r['extversion'])};\n"
                )
            f.write("\n")
        cur.close()

    def _dump_schemas(self, f):
        if self.schemas:
            f.write("-- SCHEMAS\n")
            for sch in self.schemas:
                f.write(f"CREATE SCHEMA IF NOT EXISTS {qident(sch)};\n")
            f.write("\n")

    def _dump_types(self, f):
        cur = self.conn.cursor(cursor_factory=DictCursor)

        # ENUM
        cur.execute("""
            SELECT n.nspname AS schema, t.typname AS name, e.enumlabel AS label
            FROM pg_type t
            JOIN pg_namespace n ON n.oid = t.typnamespace
            JOIN pg_enum e ON e.enumtypid = t.oid
            WHERE n.nspname = ANY(%s)
            ORDER BY schema, name, e.enumsortorder;
        """, (list(self.schemas),))
        enums: Dict[Tuple[str, str], List[str]] = {}
        for r in cur.fetchall():
            enums.setdefault((r["schema"], r["name"]), []).append(r["label"])
        if enums:
            f.write("-- ENUM TYPES\n")
            for (schema, name), labels in enums.items():
                lbls = ", ".join(sql_literal(x) for x in labels)
                f.write(f"CREATE TYPE {qident(schema)}.{qident(name)} AS ENUM ({lbls});\n")
            f.write("\n")

        # DOMAIN
        cur.execute("""
            SELECT n.nspname AS schema, t.typname AS name,
                   pg_catalog.format_type(t.typbasetype, t.typtypmod) AS base,
                   t.typnotnull, t.typdefault
            FROM pg_type t
            JOIN pg_namespace n ON n.oid = t.typnamespace
            WHERE n.nspname = ANY(%s) AND t.typtype = 'd';
        """, (list(self.schemas),))
        rows = cur.fetchall()
        if rows:
            f.write("-- DOMAIN TYPES\n")
            for r in rows:
                notnull = " NOT NULL" if r["typnotnull"] else ""
                default = f" DEFAULT {r['typdefault']}" if r["typdefault"] else ""
                f.write(f"CREATE DOMAIN {qident(r['schema'])}.{qident(r['name'])} AS {r['base']}{default}{notnull};\n")
            f.write("\n")

        # COMPOSITE
        cur.execute("""
            SELECT n.nspname AS schema, t.typname AS name, a.attname,
                   pg_catalog.format_type(a.atttypid, a.atttypmod) AS atype, a.attnum
            FROM pg_type t
            JOIN pg_namespace n ON n.oid = t.typnamespace
            JOIN pg_class c ON c.oid = t.typrelid
            JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE n.nspname = ANY(%s) AND t.typtype = 'c'
              AND a.attnum > 0 AND NOT a.attisdropped
            ORDER BY schema, name, a.attnum;
        """, (list(self.schemas),))
        comp: Dict[Tuple[str, str], List[Tuple[str, str]]] = {}
        for r in cur.fetchall():
            comp.setdefault((r["schema"], r["name"]), []).append((r["attname"], r["atype"]))
        if comp:
            f.write("-- COMPOSITE TYPES\n")
            for (schema, name), cols in comp.items():
                cols_sql = ", ".join(f"{qident(c)} {t}" for c, t in cols)
                f.write(f"CREATE TYPE {qident(schema)}.{qident(name)} AS ({cols_sql});\n")
            f.write("\n")
        cur.close()

    def _dump_sequences_create(self, f):
        """
        Создание всех пользовательских последовательностей (без состояния и без OWNED BY),
        но с параметрами (как у pg_dump).
        """
        cur = self.conn.cursor(cursor_factory=DictCursor)
        cur.execute("""
            SELECT n.nspname AS schema_name,
                   c.relname AS sequence_name,
                   s.seqstart AS start_value,
                   s.seqincrement AS increment_by,
                   s.seqmin AS min_value,
                   s.seqmax AS max_value,
                   s.seqcache AS cache_value,
                   s.seqcycle AS is_cycled
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_sequence s ON s.seqrelid = c.oid
            WHERE c.relkind = 'S'
              AND n.nspname = ANY(%s)
            ORDER BY schema_name, sequence_name;
        """, (list(self.schemas),))
        seqs = cur.fetchall()
        if seqs:
            f.write("-- SEQUENCES (create)\n")
            for r in seqs:
                sch, seq = r["schema_name"], r["sequence_name"]
                if not sch or not seq:
                    continue
                f.write(f"CREATE SEQUENCE {sch}.{seq}\n")
                f.write(f"    START WITH {r['start_value']}\n")
                f.write(f"    INCREMENT BY {r['increment_by']}\n")
                if r["min_value"] is None:
                    f.write("    NO MINVALUE\n")
                else:
                    f.write(f"    MINVALUE {r['min_value']}\n")
                if r["max_value"] is None:
                    f.write("    NO MAXVALUE\n")
                else:
                    f.write(f"    MAXVALUE {r['max_value']}\n")
                f.write(f"    CACHE {r['cache_value']}\n")
                if r["is_cycled"]:
                    f.write("    CYCLE\n")
                f.write(";\n\n")
        cur.close()

    def _dump_tables(self, f):
        """
        Определения таблиц: колонки, типы, DEFAULT, NOT NULL.
        """

        # Сначала соберем информацию о всех последовательностях в дампируемых схемах
        seq_map = {}
        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                SELECT n.nspname AS schema_name, c.relname AS sequence_name
                FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE c.relkind = 'S' AND n.nspname = ANY(%s)
            """, (list(self.schemas),))
            for r in cur.fetchall():
                seq_map[r['sequence_name']] = r['schema_name']

        with self.conn.cursor(cursor_factory=DictCursor) as cur:
            cur.execute("""
                  SELECT n.nspname AS schema_name,
                         c.relname AS table_name,
                         c.oid      AS table_oid,
                         c.relkind  AS relkind,
                         a.attnum,
                         a.attname,
                         pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type,
                         a.attnotnull,
                         pg_get_expr(d.adbin, d.adrelid) AS default_src
                  FROM pg_attribute a
                  JOIN pg_class c ON c.oid = a.attrelid
                  JOIN pg_namespace n ON n.oid = c.relnamespace
                  LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
                  WHERE a.attnum > 0
                    AND NOT a.attisdropped
                    AND c.relkind IN ('r','p','f')
                    AND n.nspname = ANY(%s)
                  ORDER BY schema_name, table_name, a.attnum;
              """, (list(self.schemas),))

            by_table = {}
            for r in cur.fetchall():
                by_table.setdefault((r["schema_name"], r["table_name"]), []).append(r)

            if by_table:
                f.write("-- TABLES\n")
                for (schema, table), cols in by_table.items():
                    parts = []
                    for c in cols:
                        col = f"{qident(c['attname'])} {c['data_type']}"

                        # Обрабатываем DEFAULT значения, особенно nextval для последовательностей
                        if c["default_src"]:
                            default_src = c["default_src"]
                            # Если это nextval для последовательности, добавляем схему явно
                            if "nextval" in default_src:
                                # Ищем имя последовательности в nextval
                                import re
                                match = re.search(r"nextval\('([^']+)'::regclass\)", default_src)
                                if match:
                                    seq_name = match.group(1)
                                    # Если последовательность в нашей карте, добавляем схему
                                    if seq_name in seq_map:
                                        schema_seq = seq_map[seq_name]
                                        default_src = default_src.replace(
                                            f"nextval('{seq_name}'",
                                            f"nextval('{schema_seq}.{seq_name}'"
                                        )

                            col += f" DEFAULT {default_src}"

                        if c["attnotnull"]:
                            col += " NOT NULL"
                        parts.append(col)

                    f.write(f"CREATE TABLE {schema}.{table} (\n  " +
                            ",\n  ".join(parts) + "\n);\n\n")

    def _dump_comments_tables_columns(self, f):
        cur = self.conn.cursor(cursor_factory=DictCursor)
        # Комментарии на таблицы/представления/матвью/секвенсы/foreign tables
        cur.execute("""
            SELECT n.nspname AS schema, c.relname AS name, obj_description(c.oid, 'pg_class') AS comment, c.relkind
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s)
              AND c.relkind IN ('r','v','m','S','f');
        """, (list(self.schemas),))
        for r in cur.fetchall():
            if r["comment"]:
                kind = r["relkind"]
                keyword = "TABLE"
                if kind == 'v': keyword = "VIEW"
                elif kind == 'm': keyword = "MATERIALIZED VIEW"
                elif kind == 'S': keyword = "SEQUENCE"
                elif kind == 'f': keyword = "FOREIGN TABLE"
                f.write(f"COMMENT ON {keyword} {qident(r['schema'])}.{qident(r['name'])} "
                        f"IS {sql_literal(r['comment'])};\n")

        # Комментарии на колонки
        cur.execute("""
            SELECT n.nspname AS schema, c.relname AS table, a.attname AS column,
                   col_description(c.oid, a.attnum) AS comment
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_attribute a ON a.attrelid = c.oid
            WHERE n.nspname = ANY(%s)
              AND c.relkind = 'r'
              AND a.attnum > 0 AND NOT a.attisdropped;
        """, (list(self.schemas),))
        rows = cur.fetchall()
        for r in rows:
            if r["comment"]:
                f.write(f"COMMENT ON COLUMN {qident(r['schema'])}.{qident(r['table'])}.{qident(r['column'])} "
                        f"IS {sql_literal(r['comment'])};\n")
        if rows:
            f.write("\n")
        cur.close()

    def _dump_data(self, f, batch_size=None):
        if batch_size is None:
            batch_size = self.batch_size

        cur = self.conn.cursor(cursor_factory=DictCursor)
        # все таблицы из указанных схем
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema = ANY(%s)
              AND table_type = 'BASE TABLE'
            ORDER BY table_schema, table_name;
        """, (list(self.schemas),))
        tables = cur.fetchall()

        for tbl in tables:
            schema = tbl["table_schema"]
            table = tbl["table_name"]
            full_table = f"{schema}.{table}"

            f.write(f"\n--\n-- Data for table {full_table}\n--\n\n")

            # колонки
            with self.conn.cursor(cursor_factory=DictCursor) as cur_cols:
                cur_cols.execute("""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_schema = %s AND table_name = %s
                    ORDER BY ordinal_position;
                """, (schema, table))
                cols = [r["column_name"] for r in cur_cols.fetchall()]

            col_list = ", ".join([qident(c) for c in cols])
            f.write(f"COPY {full_table} ({col_list}) FROM stdin;\n")

            # Данные батчами
            with self.conn.cursor(cursor_factory=DictCursor) as data_cur:
                data_cur.itersize = batch_size
                data_cur.execute(f"SELECT * FROM {full_table};")

                for row in data_cur:
                    vals = []
                    for v in row.values():
                        if v is None:
                            vals.append(r"\N")
                        elif isinstance(v, (dict, list, tuple)):
                            s = json.dumps(v, ensure_ascii=False)
                            s = s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
                            vals.append(s)
                        elif isinstance(v, (bytes, bytearray, memoryview)):
                            b = bytes(v)
                            vals.append(r"\x" + b.hex())
                        else:
                            s = str(v)
                            s = s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")
                            vals.append(s)
                    f.write("\t".join(vals) + "\n")

            # Завершение COPY
            f.write("\\.\n\n")

        cur.close()

    def _dump_sequences_state(self, f):
        """
        Восстановление состояния последовательностей (last_value/increment/is_called).
        Для каждой sequence читаем increment из pg_sequence и текущее состояние из самой последовательности.
        """
        cur = self.conn.cursor(cursor_factory=DictCursor)
        cur.execute("""
            SELECT n.nspname AS schemaname,
                   c.relname AS sequencename,
                   s.seqincrement AS increment_by
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            JOIN pg_sequence s ON s.seqrelid = c.oid
            WHERE n.nspname = ANY(%s) AND c.relkind = 'S'
            ORDER BY schemaname, sequencename;
        """, (list(self.schemas),))
        rows = cur.fetchall()
        if rows:
            f.write("-- SEQUENCES (state)\n")
            c2 = self.conn.cursor()
            for r in rows:
                sch = r["schemaname"]
                seq = r["sequencename"]
                inc = r["increment_by"]
                full = f"{sch}.{seq}"

                # increment_by (как делает pg_dump)
                f.write(f"ALTER SEQUENCE {full} INCREMENT BY {inc};\n")

                # читаем текущее состояние из последовательности (last_value, is_called)
                c2.execute(f"SELECT last_value, is_called FROM {qident(sch)}.{qident(seq)};")
                last_value, is_called = c2.fetchone()
                f.write(f"SELECT setval({sql_literal(f'{sch}.{seq}')}, {int(last_value)}, {'true' if is_called else 'false'});\n")
            f.write("\n")
            c2.close()
        cur.close()

    def _dump_indexes(self, f):
        cur = self.conn.cursor(cursor_factory=DictCursor)
        cur.execute("""
            SELECT n.nspname AS schema, c.relname AS indexname, pg_get_indexdef(c.oid) AS idxdef
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s) AND c.relkind = 'i'
            ORDER BY schema, indexname;
        """, (list(self.schemas),))
        rows = cur.fetchall()
        if rows:
            f.write("-- INDEXES\n")
            for r in rows:
                f.write(r["idxdef"] + ";\n")
            f.write("\n")
        cur.close()

    def _dump_constraints(self, f):

        cur = self.conn.cursor(cursor_factory=DictCursor)
        cur.execute("""
            SELECT 
                n.nspname AS schema_name,
                c.relname AS table_name,
                con.conname AS constraint_name,
                con.contype AS constraint_type,
                pg_get_constraintdef(con.oid, false) AS definition
            FROM pg_constraint con
            JOIN pg_class c ON c.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s)
            ORDER BY 
                CASE WHEN con.contype = 'p' THEN 1
                     WHEN con.contype = 'u' THEN 2
                     WHEN con.contype = 'c' THEN 3
                     WHEN con.contype = 'f' THEN 4
                     ELSE 5 END,
                schema_name, table_name, constraint_name;
        """, (list(self.schemas),))

        constraints = cur.fetchall()
        if constraints:
            f.write("-- CONSTRAINTS\n")
            for r in constraints:
                full_table = f"{r['schema_name']}.{r['table_name']}"

                # Для первичных ключей используем
                if r["constraint_type"] in ('p', 'u', 'c', 'f'):
                    f.write(
                        f"ALTER TABLE ONLY {full_table} ADD CONSTRAINT {r['constraint_name']} {r['definition']};\n")

            f.write("\n")
        cur.close()

    def _dump_views(self, f):
        cur = self.conn.cursor(cursor_factory=DictCursor)
        cur.execute("""
            SELECT n.nspname AS schema, c.relname AS view, pg_get_viewdef(c.oid, true) AS vdef
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s) AND c.relkind = 'v'
            ORDER BY schema, view;
        """, (list(self.schemas),))
        rows = cur.fetchall()
        if rows:
            f.write("-- VIEWS\n")
            for r in rows:
                f.write(f"CREATE VIEW {r['schema']}.{r['view']} AS\n{r['vdef']}\n")
            f.write("\n\n")
        cur.close()

    def _dump_matviews(self, f):
        cur = self.conn.cursor(cursor_factory=DictCursor)
        cur.execute("""
            SELECT n.nspname AS schema, c.relname AS mv, pg_get_viewdef(c.oid, true) AS vdef
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s) AND c.relkind = 'm'
            ORDER BY schema, mv;
        """, (list(self.schemas),))
        rows = cur.fetchall()
        if rows:
            f.write("-- MATERIALIZED VIEWS\n")
            for r in rows:
                f.write(f"CREATE MATERIALIZED VIEW {qident(r['schema'])}.{qident(r['mv'])} AS\n{r['vdef']};\n")
            f.write("\n\n")
        cur.close()

    def _dump_routines(self, f):
        cur = self.conn.cursor(cursor_factory=DictCursor)
        cur.execute("""
            SELECT p.oid, n.nspname AS schema, p.proname AS name
            FROM pg_proc p
            JOIN pg_namespace n ON n.oid = p.pronamespace
            WHERE n.nspname = ANY(%s)
            ORDER BY schema, name;
        """, (list(self.schemas),))
        rows = cur.fetchall()
        if rows:
            f.write("-- FUNCTIONS/PROCEDURES\n")
            for r in rows:
                c2 = self.conn.cursor()
                c2.execute("SELECT pg_get_functiondef(%s);", (r["oid"],))
                funcdef = c2.fetchone()[0]
                c2.close()
                f.write(funcdef + ";\n\n")
        cur.close()

    def _dump_triggers(self, f):
        cur = self.conn.cursor(cursor_factory=DictCursor)
        cur.execute("""
            SELECT 
                n.nspname AS schema_name,
                c.relname AS table_name,
                t.tgname AS trigger_name,
                pg_get_triggerdef(t.oid, true) AS definition
            FROM pg_trigger t
            JOIN pg_class c ON c.oid = t.tgrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE NOT t.tgisinternal
              AND n.nspname = ANY(%s)
            ORDER BY schema_name, table_name, trigger_name;
        """, (list(self.schemas),))
        triggers = cur.fetchall()

        if triggers:
            f.write("-- TRIGGERS\n")
            for r in triggers:
                f.write(r["definition"] + ";\n")
            f.write("\n")

        cur.close()

    def _dump_sequences_ownedby(self, f):
        """
        Привязка последовательностей к колонкам (после создания таблиц/данных/ограничений).
        """
        cur = self.conn.cursor(cursor_factory=DictCursor)
        cur.execute("""
            SELECT
                n.nspname AS schema_name,
                c.relname AS sequence_name,
                ns.nspname AS tbl_schema,
                t.relname AS tbl_name,
                a.attname AS col_name
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            LEFT JOIN pg_depend d ON d.objid = c.oid AND d.deptype = 'a'
            LEFT JOIN pg_class t ON t.oid = d.refobjid
            LEFT JOIN pg_namespace ns ON ns.oid = t.relnamespace
            LEFT JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
            WHERE c.relkind = 'S'
              AND n.nspname = ANY(%s)
        """, (list(self.schemas),))
        owned = cur.fetchall()
        if owned:
            f.write("-- SEQUENCES (owned by)\n")
            for r in owned:
                sch, seq = r["schema_name"], r["sequence_name"]
                ts, tn, cn = r["tbl_schema"], r["tbl_name"], r["col_name"]
                if sch and seq and ts and tn and cn:
                    f.write(f"ALTER SEQUENCE {qident(sch)}.{qident(seq)} "
                            f"OWNED BY {qident(ts)}.{qident(tn)}.{qident(cn)};\n")
            f.write("\n")
        cur.close()

    def _dump_grants(self, f):
        cur = self.conn.cursor(cursor_factory=DictCursor)
        # GRANT на схемы
        cur.execute("""
            SELECT n.nspname AS schema, n.nspacl::text AS acl
            FROM pg_namespace n
            WHERE n.nspname = ANY(%s);
        """, (list(self.schemas),))
        for r in cur.fetchall():
            if r["acl"]:
                f.write(self._acl_to_grants_schema(r["schema"], r["acl"]))

        # GRANT на объекты
        cur.execute("""
            SELECT n.nspname AS schema, c.relname, c.relkind, c.relacl::text AS acl
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = ANY(%s)
              AND c.relkind IN ('r','S','v','m','f');
        """, (list(self.schemas),))
        for r in cur.fetchall():
            if r["acl"]:
                f.write(self._acl_to_grants_rel(r["schema"], r["relname"], r["relkind"], r["acl"]))
        f.write("\n")
        cur.close()

    def _acl_to_grants_schema(self, schema: str, acl_text: str) -> str:
        grants = []
        entries = [e for e in acl_text.strip("{}").split(",") if e]
        for e in entries:
            if "=" not in e:
                continue
            role, perms = e.split("=")
            if "/" in perms:
                perms = perms.split("/")[0]
            role = role or "public"
            perms_map = {"U": "USAGE", "C": "CREATE"}
            for p in perms:
                if p in perms_map:
                    grants.append(f"GRANT {perms_map[p]} ON SCHEMA {schema} TO {role};")
        return "\n".join(grants) + ("\n" if grants else "")

    def _acl_to_grants_rel(self, schema: str, rel: str, relkind: str, acl_text: str) -> str:
        grants = []
        entries = [e for e in acl_text.strip("{}").split(",") if e]
        for e in entries:
            if "=" not in e:
                continue
            role, perms = e.split("=")
            if "/" in perms:
                perms = perms.split("/")[0]
            role = role or "public"
            table_map = {
                "a": "INSERT", "r": "SELECT", "w": "UPDATE", "d": "DELETE",
                "D": "TRUNCATE", "x": "REFERENCES", "t": "TRIGGER"
            }
            seq_map = {"r": "SELECT", "w": "UPDATE", "U": "USAGE"}
            if relkind == "S":
                for p in perms:
                    if p in seq_map:
                        grants.append(f"GRANT {seq_map[p]} ON SEQUENCE {qident(schema)}.{qident(rel)} TO {qident(role)};")
            else:
                for p in perms:
                    if p in table_map:
                        grants.append(f"GRANT {table_map[p]} ON {qident(schema)}.{qident(rel)} TO {qident(role)};")
        return "\n".join(grants) + ("\n" if grants else "")

    def _dump_roles_basic(self, f):
        cur = self.conn.cursor(cursor_factory=DictCursor)
        cur.execute("""
            SELECT rolname, rolsuper, rolinherit, rolcreaterole, rolcreatedb,
                   rolcanlogin, rolreplication, rolbypassrls
            FROM pg_roles
            WHERE rolname NOT IN ('pg_signal_backend')
            ORDER BY rolname;
        """)
        rows = cur.fetchall()
        if rows:
            f.write("-- ROLES (basic)\n")
            for r in rows:
                opt = []
                opt.append("SUPERUSER" if r["rolsuper"] else "NOSUPERUSER")
                opt.append("CREATEDB" if r["rolcreatedb"] else "NOCREATEDB")
                opt.append("CREATEROLE" if r["rolcreaterole"] else "NOCREATEROLE")
                opt.append("INHERIT" if r["rolinherit"] else "NOINHERIT")
                opt.append("LOGIN" if r["rolcanlogin"] else "NOLOGIN")
                if r["rolreplication"]:
                    opt.append("REPLICATION")
                if r["rolbypassrls"]:
                    opt.append("BYPASSRLS")
                f.write(
                    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname="
                    + sql_literal(r['rolname'])
                    + f") THEN CREATE ROLE {qident(r['rolname'])} "
                    + " ".join(opt)
                    + "; END IF; END $$;\n"
                )
            f.write("\n")
        cur.close()


# ----------------------- Пример запуска -------------------

def example_run():
    # Настрой под себя
    host = "127.0.0.1"
    port = 5432
    dbname = "test_db"
    user = "postgres"
    password = "postgres"
    schemas = "public"

    out_sql = f"backup_{dbname}_{now_stamp()}.sql"  # .gz если хочешь gzip

    mgr = PostgresBackupManager(
        host=host, port=port, dbname=dbname, user=user, password=password,
        schemas=schemas, batch_size=DEFAULT_BATCH_SIZE,
        pretend_pg_dump_version=None  # можно '15.4' если хочешь жёстко
    )
    mgr.backup(out_sql_path=out_sql, compress=False)
    print("Backup done:", out_sql)


if __name__ == "__main__":
    example_run()
