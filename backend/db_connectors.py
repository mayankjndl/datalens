"""
db_connectors.py - Unified database connection layer
Supports: SQLite, Snowflake, PostgreSQL (stub)

Architecture
------------
BaseConnector   — abstract interface + shared ANSI-SQL statistics engine
SQLiteConnector — SQLite via stdlib sqlite3
SnowflakeConnector — Snowflake via snowflake-connector-python (read-only)
PostgreSQLConnector — PostgreSQL via psycopg2 (stub, ready to extend)
"""

import math
import re
from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Enums & config
# ---------------------------------------------------------------------------

class DBType(str, Enum):
    SQLITE      = "sqlite"
    POSTGRESQL  = "postgresql"
    SQLSERVER   = "sqlserver"
    SNOWFLAKE   = "snowflake"
    MYSQL       = "mysql"


@dataclass
class ConnectionConfig:
    db_type: DBType
    # SQLite
    file_path: Optional[str] = None
    # Network databases
    host: Optional[str] = None
    port: Optional[int] = None
    database: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    # Snowflake extras
    account: Optional[str] = None
    warehouse: Optional[str] = None
    schema: Optional[str] = None


# ---------------------------------------------------------------------------
# Base connector — interface + shared statistics engine
# ---------------------------------------------------------------------------

class BaseConnector:
    """
    Abstract base for all database connectors.

    All metadata methods return normalised dicts with consistent keys so
    that quality_analyzer.py and the rest of the system never need to
    know which database they are talking to.

    get_column_stats() is implemented here in pure ANSI SQL and is
    inherited by every subclass automatically.  Subclasses only need to
    override it if they have a more efficient native approach.
    """

    def __init__(self, config: ConnectionConfig):
        self.config = config
        self._conn = None

    # ── Connection lifecycle ────────────────────────────────────────────────

    def connect(self) -> "BaseConnector":
        raise NotImplementedError

    def disconnect(self) -> None:
        if self._conn:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    # ── Query execution ─────────────────────────────────────────────────────

    def execute(self, sql: str, params=None) -> List[Dict]:
        """Run SQL and return a list of row dicts."""
        raise NotImplementedError

    # ── Metadata extraction — must be implemented by each subclass ──────────

    def get_schemas(self) -> List[str]:
        raise NotImplementedError

    def get_tables(self, schema: Optional[str] = None) -> List[Dict]:
        """
        Returns list of dicts with keys:
            table_name, table_type, schema, row_count
        """
        raise NotImplementedError

    def get_columns(self, table: str, schema: Optional[str] = None) -> List[Dict]:
        """
        Returns list of dicts with keys:
            column_name, data_type, is_nullable (bool),
            default_value, is_primary_key (bool), ordinal_position
        """
        raise NotImplementedError

    def get_foreign_keys(self, table: str, schema: Optional[str] = None) -> List[Dict]:
        """
        Returns list of dicts with keys:
            column, referenced_table, referenced_column,
            on_delete, on_update
        """
        raise NotImplementedError

    def get_indexes(self, table: str, schema: Optional[str] = None) -> List[Dict]:
        """
        Returns list of dicts with keys:
            index_name, is_unique (bool), columns (list of str)
        """
        raise NotImplementedError

    def sample_data(self, table: str, schema: Optional[str] = None, n: int = 5) -> List[Dict]:
        raise NotImplementedError

    # ── Identifier quoting — subclasses may override ────────────────────────

    def _quote(self, name: str) -> str:
        """Wrap an identifier in double-quotes (ANSI SQL standard)."""
        return f'"{name}"'

    def _qualified_table(self, table: str, schema: Optional[str] = None) -> str:
        """Return a schema-qualified, quoted table reference for data queries."""
        if schema:
            return f"{self._quote(schema)}.{self._quote(table)}"
        return self._quote(table)

    # ── Read-only guard — subclasses may call this in execute() ────────────

    _WRITE_PATTERN = re.compile(
        r"^\s*(INSERT|UPDATE|DELETE|DROP|CREATE|ALTER|TRUNCATE|MERGE|REPLACE|CALL|EXEC|EXECUTE|GRANT|REVOKE)\b",
        re.IGNORECASE,
    )

    def _assert_read_only(self, sql: str) -> None:
        """
        Raise ValueError if sql is a write or DDL statement.

        Allowed prefixes (not exhaustive — blacklist approach):
            SELECT, WITH, SHOW, DESCRIBE, EXPLAIN, PRAGMA, VALUES
        """
        if self._WRITE_PATTERN.match(sql):
            first = sql.strip().split()[0].upper()
            raise ValueError(
                f"Write operation '{first}' is not permitted. "
                "This connector is read-only."
            )

    # ── Shared ANSI-SQL statistics engine (inherited by all subclasses) ─────

    def get_column_stats(
        self,
        table: str,
        column: str,
        dtype: str,
        schema: Optional[str] = None,
    ) -> Dict:
        """
        Compute per-column quality statistics using pure ANSI SQL.

        Works for SQLite, Snowflake, PostgreSQL, and any other connector
        that implements execute().  Subclasses may override for native
        optimisations (e.g. Snowflake APPROX_COUNT_DISTINCT).

        Returns
        -------
        dict with keys: total_rows, null_count, null_pct, completeness_pct,
                        distinct_count, uniqueness_pct,
                        min, max, avg, sum, std_dev  (numeric columns),
                        top_values                   (low-cardinality columns),
                        min_date, max_date            (date/time columns)
        """
        stats: Dict[str, Any] = {}
        tbl = self._qualified_table(table, schema)
        col = self._quote(column)

        try:
            # ── Row and null counts ─────────────────────────────────────────
            total = self.execute(f"SELECT COUNT(*) AS c FROM {tbl}")[0]["c"]
            nulls = self.execute(
                f"SELECT COUNT(*) AS c FROM {tbl} WHERE {col} IS NULL"
            )[0]["c"]

            stats["total_rows"]      = total
            stats["null_count"]      = nulls
            stats["null_pct"]        = round(nulls / total * 100, 2) if total else 0
            stats["completeness_pct"] = (
                round((total - nulls) / total * 100, 2) if total else 0
            )

            # ── Distinct / cardinality ───────────────────────────────────────
            non_null = total - nulls
            distinct = self.execute(
                f"SELECT COUNT(DISTINCT {col}) AS c FROM {tbl}"
            )[0]["c"]
            stats["distinct_count"]  = distinct
            stats["uniqueness_pct"]  = (
                round(distinct / non_null * 100, 2) if non_null > 0 else 0
            )

            # ── Numeric statistics ───────────────────────────────────────────
            _NUMERIC = (
                "int", "real", "float", "double", "decimal", "numeric",
                "number", "money", "fixed", "bigint", "smallint", "tinyint",
                "byteint",
            )
            if any(h in dtype.lower() for h in _NUMERIC):
                agg = self.execute(f"""
                    SELECT
                        MIN({col})  AS mn,
                        MAX({col})  AS mx,
                        AVG({col})  AS avg,
                        SUM({col})  AS total
                    FROM {tbl}
                    WHERE {col} IS NOT NULL
                """)
                if agg and agg[0]["mn"] is not None:
                    avg_val = float(agg[0]["avg"] or 0)
                    stats.update({
                        "min": agg[0]["mn"],
                        "max": agg[0]["mx"],
                        "avg": round(avg_val, 4),
                        "sum": agg[0]["total"],
                    })
                    # Population standard deviation via two-pass variance
                    try:
                        var_rows = self.execute(f"""
                            SELECT AVG(
                                ({col} - {avg_val}) * ({col} - {avg_val})
                            ) AS v
                            FROM {tbl}
                            WHERE {col} IS NOT NULL
                        """)
                        if var_rows and var_rows[0]["v"] is not None:
                            stats["std_dev"] = round(
                                math.sqrt(float(var_rows[0]["v"])), 4
                            )
                    except Exception:
                        pass  # std_dev is optional

            # ── Top values for low-cardinality columns ───────────────────────
            if distinct <= 50:
                top = self.execute(f"""
                    SELECT {col} AS val, COUNT(*) AS freq
                    FROM {tbl}
                    WHERE {col} IS NOT NULL
                    GROUP BY {col}
                    ORDER BY freq DESC
                    LIMIT 5
                """)
                stats["top_values"] = [
                    {"value": str(r["val"]), "count": r["freq"]} for r in top
                ]

            # ── Date range ───────────────────────────────────────────────────
            _DATE = ("date", "time", "datetime", "timestamp")
            if any(h in dtype.lower() for h in _DATE):
                date_agg = self.execute(
                    f"SELECT MIN({col}) AS mn, MAX({col}) AS mx FROM {tbl}"
                )
                if date_agg:
                    stats["min_date"] = date_agg[0]["mn"]
                    stats["max_date"] = date_agg[0]["mx"]

        except Exception as exc:
            stats["error"] = str(exc)

        return stats


# ---------------------------------------------------------------------------
# SQLite connector (unchanged behaviour, now inherits shared stats engine)
# ---------------------------------------------------------------------------

class SQLiteConnector(BaseConnector):
    """
    SQLite via Python stdlib sqlite3.
    Uses PRAGMA commands for metadata (SQLite-specific, intentionally).
    All data queries go through the inherited ANSI-SQL stats engine.
    """

    def connect(self) -> "SQLiteConnector":
        import sqlite3
        self._conn = sqlite3.connect(
            self.config.file_path, check_same_thread=False
        )
        self._conn.row_factory = sqlite3.Row
        return self

    def execute(self, sql: str, params=None) -> List[Dict]:
        self._assert_read_only(sql)
        cur = self._conn.cursor()
        cur.execute(sql, params or [])
        rows = cur.fetchall()
        return [dict(r) for r in rows] if rows else []

    def get_schemas(self) -> List[str]:
        return ["main"]

    def get_tables(self, schema: Optional[str] = None) -> List[Dict]:
        rows = self.execute(
            "SELECT name, 'TABLE' AS type "
            "FROM sqlite_master "
            "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
        results = []
        for r in rows:
            try:
                cnt = self.execute(
                    f'SELECT COUNT(*) AS cnt FROM "{r["name"]}"'
                )
                row_count = cnt[0]["cnt"] if cnt else 0
            except Exception:
                row_count = 0
            results.append({
                "table_name": r["name"],
                "table_type": r["type"],
                "schema": "main",
                "row_count": row_count,
            })
        return results

    def get_columns(self, table: str, schema: Optional[str] = None) -> List[Dict]:
        rows = self.execute(f'PRAGMA table_info("{table}")')
        return [
            {
                "column_name":    r["name"],
                "data_type":      r["type"],
                "is_nullable":    not r["notnull"],
                "default_value":  r["dflt_value"],
                "is_primary_key": bool(r["pk"]),
                "ordinal_position": r["cid"] + 1,
            }
            for r in rows
        ]

    def get_foreign_keys(self, table: str, schema: Optional[str] = None) -> List[Dict]:
        rows = self.execute(f'PRAGMA foreign_key_list("{table}")')
        return [
            {
                "column":            r["from"],
                "referenced_table":  r["table"],
                "referenced_column": r["to"],
                "on_delete":         r.get("on_delete", ""),
                "on_update":         r.get("on_update", ""),
            }
            for r in rows
        ]

    def get_indexes(self, table: str, schema: Optional[str] = None) -> List[Dict]:
        rows = self.execute(f'PRAGMA index_list("{table}")')
        results = []
        for r in rows:
            idx_cols = self.execute(f'PRAGMA index_info("{r["name"]}")')
            results.append({
                "index_name": r["name"],
                "is_unique":  bool(r["unique"]),
                "columns":    [i["name"] for i in idx_cols],
            })
        return results

    def sample_data(self, table: str, schema: Optional[str] = None, n: int = 5) -> List[Dict]:
        try:
            return self.execute(f'SELECT * FROM "{table}" LIMIT {n}')
        except Exception:
            return []


# ---------------------------------------------------------------------------
# Snowflake connector — full production implementation
# ---------------------------------------------------------------------------

class SnowflakeConnector(BaseConnector):
    """
    Snowflake via snowflake-connector-python.

    Design decisions
    ----------------
    * Strictly read-only: every execute() call is checked before running.
    * Uses INFORMATION_SCHEMA throughout — no SHOW commands — so metadata
      is stable, filterable, and consistent with other connectors.
    * Identifiers in INFORMATION_SCHEMA are stored UPPERCASE for objects
      created without quoting (the Snowflake default), so all WHERE clauses
      use UPPER(?) to handle both cases transparently.
    * Data queries use schema-qualified names without forced quoting so
      Snowflake's case-folding resolves names correctly.

    Install
    -------
        pip install snowflake-connector-python
    """

    # Snowflake normalises unquoted object names to UPPERCASE in the catalog.
    # We normalise all user-supplied identifiers the same way before sending
    # them to INFORMATION_SCHEMA to avoid mismatches.
    def _sf_upper(self, name: str) -> str:
        return name.upper()

    def _qualified_table(self, table: str, schema: Optional[str] = None) -> str:
        """
        Returns an unquoted, uppercase schema.table reference.
        Snowflake resolves unquoted identifiers case-insensitively, which
        covers the vast majority of real-world schemas.
        """
        sch = (schema or self.config.schema or "PUBLIC").upper()
        return f"{sch}.{table.upper()}"

    def connect(self) -> "SnowflakeConnector":
        import snowflake.connector as _sf
        import snowflake.connector.errors as _sf_errors

        self._sf = _sf  # keep reference for DictCursor

        try:
            self._conn = _sf.connect(
                account=self.config.account,
                user=self.config.username,
                password=self.config.password,
                database=self.config.database,
                warehouse=self.config.warehouse,
                schema=self.config.schema or "PUBLIC",
                # Disable browser-based auth so failures surface as exceptions
                # rather than silently hanging or triggering the NoneType bug.
                login_timeout=30,
                # Tag every query so it's easy to audit in Query History.
                session_parameters={
                    "QUERY_TAG": "datalens-readonly",
                },
            )
        except _sf_errors.DatabaseError as exc:
            # DatabaseError is what the connector *should* raise on auth/network
            # failures.  Re-raise with a clean message that surfaces in the API.
            raise ConnectionError(
                f"Snowflake connection failed: {exc.msg or str(exc)}"
            ) from exc
        except AttributeError:
            # snowflake-connector-python bug: when authentication fails in
            # certain code paths (e.g. wrong account format, unreachable host),
            # the library's own error handler calls .find() on a None response
            # object, producing "'NoneType' object has no attribute 'find'".
            # We catch AttributeError here and surface a meaningful message.
            raise ConnectionError(
                "Snowflake connection failed. "
                "Please verify your account identifier, username, password, "
                "warehouse, and database. "
                "Account format should be 'orgname-accountname' or "
                "'xy12345.us-east-1' — do not include .snowflakecomputing.com"
            )
        except Exception as exc:
            raise ConnectionError(
                f"Snowflake connection failed: {type(exc).__name__}: {exc}"
            ) from exc

        return self

    def execute(self, sql: str, params=None) -> List[Dict]:
        """
        Execute SQL and return rows as a list of dicts.

        Read-only guard runs before every query.  Params must be a list or
        tuple — snowflake-connector-python uses %s positional placeholders.
        """
        self._assert_read_only(sql)

        if self._conn is None:
            raise ConnectionError(
                "Snowflake connector is not connected. Call connect() first."
            )

        cur = self._conn.cursor(self._sf.DictCursor)
        try:
            cur.execute(sql, params or ())
            rows = cur.fetchall()
            # DictCursor rows are already dicts; keys are uppercase strings.
            # Normalise to lowercase to keep the rest of the codebase uniform.
            return [
                {k.lower(): v for k, v in row.items()}
                for row in (rows or [])
            ]
        except AttributeError as exc:
            # Guard against the same .find()-on-None library bug that can
            # surface during query execution on a degraded connection.
            raise ConnectionError(
                f"Snowflake query failed with an internal connector error. "
                f"The session may have expired — try reconnecting. "
                f"SQL: {sql[:120]}"
            ) from exc
        finally:
            cur.close()

    # ── Schema listing ───────────────────────────────────────────────────────

    def get_schemas(self) -> List[str]:
        rows = self.execute("""
            SELECT SCHEMA_NAME
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE SCHEMA_NAME != 'INFORMATION_SCHEMA'
            ORDER BY SCHEMA_NAME
        """)
        return [r["schema_name"] for r in rows]

    # ── Table listing ────────────────────────────────────────────────────────

    def get_tables(self, schema: Optional[str] = None) -> List[Dict]:
        """
        Uses INFORMATION_SCHEMA.TABLES for reliable row counts and types.
        ROW_COUNT in Snowflake is the actual count stored in metadata —
        no need to run COUNT(*) on every table.
        """
        sch = self._sf_upper(schema or self.config.schema or "PUBLIC")
        rows = self.execute(
            """
            SELECT
                TABLE_NAME,
                TABLE_TYPE,
                TABLE_SCHEMA     AS schema,
                ROW_COUNT        AS row_count,
                BYTES            AS size_bytes,
                COMMENT          AS table_comment,
                CLUSTERING_KEY
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = %s
              AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')
            ORDER BY TABLE_NAME
            """,
            (sch,),
        )
        return [
            {
                "table_name":    r["table_name"],
                "table_type":    "TABLE" if r["table_type"] == "BASE TABLE" else r["table_type"],
                "schema":        r["schema"],
                "row_count":     r["row_count"] or 0,
                "size_bytes":    r["size_bytes"],
                "comment":       r["table_comment"],
                "clustering_key": r["clustering_key"],
            }
            for r in rows
        ]

    # ── Column metadata ──────────────────────────────────────────────────────

    def get_columns(self, table: str, schema: Optional[str] = None) -> List[Dict]:
        """
        Pulls column metadata from INFORMATION_SCHEMA.COLUMNS.
        Primary key membership is resolved via a LEFT JOIN to
        TABLE_CONSTRAINTS + KEY_COLUMN_USAGE in the same query so we
        make a single round-trip per table.
        """
        sch = self._sf_upper(schema or self.config.schema or "PUBLIC")
        tbl = self._sf_upper(table)

        rows = self.execute(
            """
            SELECT
                c.COLUMN_NAME,
                c.DATA_TYPE,
                c.IS_NULLABLE,
                c.COLUMN_DEFAULT,
                c.ORDINAL_POSITION,
                c.CHARACTER_MAXIMUM_LENGTH,
                c.NUMERIC_PRECISION,
                c.NUMERIC_SCALE,
                c.COMMENT                          AS column_comment,
                CASE WHEN pk.COLUMN_NAME IS NOT NULL
                     THEN TRUE ELSE FALSE END       AS is_primary_key
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN (
                SELECT kcu.COLUMN_NAME
                FROM   INFORMATION_SCHEMA.TABLE_CONSTRAINTS  tc
                JOIN   INFORMATION_SCHEMA.KEY_COLUMN_USAGE   kcu
                       ON  kcu.CONSTRAINT_NAME   = tc.CONSTRAINT_NAME
                       AND kcu.TABLE_SCHEMA      = tc.TABLE_SCHEMA
                       AND kcu.TABLE_NAME        = tc.TABLE_NAME
                WHERE  tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
                  AND  tc.TABLE_SCHEMA    = %s
                  AND  tc.TABLE_NAME      = %s
            ) pk ON pk.COLUMN_NAME = c.COLUMN_NAME
            WHERE c.TABLE_SCHEMA = %s
              AND c.TABLE_NAME   = %s
            ORDER BY c.ORDINAL_POSITION
            """,
            (sch, tbl, sch, tbl),
        )

        return [
            {
                "column_name":    r["column_name"],
                "data_type":      r["data_type"],
                "is_nullable":    r["is_nullable"] == "YES",
                "default_value":  r["column_default"],
                "is_primary_key": bool(r["is_primary_key"]),
                "ordinal_position": r["ordinal_position"],
                "character_maximum_length": r.get("character_maximum_length"),
                "numeric_precision": r.get("numeric_precision"),
                "numeric_scale":     r.get("numeric_scale"),
                "comment":           r.get("column_comment"),
            }
            for r in rows
        ]

    # ── Foreign keys ─────────────────────────────────────────────────────────

    def get_foreign_keys(self, table: str, schema: Optional[str] = None) -> List[Dict]:
        """
        Resolves FK relationships via REFERENTIAL_CONSTRAINTS joined to
        KEY_COLUMN_USAGE (child side) and UNIQUE_CONSTRAINT_NAME (parent side).
        """
        sch = self._sf_upper(schema or self.config.schema or "PUBLIC")
        tbl = self._sf_upper(table)

        rows = self.execute(
            """
            SELECT
                kcu_child.COLUMN_NAME          AS fk_column,
                kcu_parent.TABLE_NAME          AS referenced_table,
                kcu_parent.COLUMN_NAME         AS referenced_column,
                rc.DELETE_RULE                 AS on_delete,
                rc.UPDATE_RULE                 AS on_update
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS rc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu_child
                 ON  kcu_child.CONSTRAINT_NAME  = rc.CONSTRAINT_NAME
                 AND kcu_child.CONSTRAINT_SCHEMA = rc.CONSTRAINT_SCHEMA
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu_parent
                 ON  kcu_parent.CONSTRAINT_NAME  = rc.UNIQUE_CONSTRAINT_NAME
                 AND kcu_parent.CONSTRAINT_SCHEMA = rc.UNIQUE_CONSTRAINT_SCHEMA
                 AND kcu_parent.ORDINAL_POSITION  = kcu_child.ORDINAL_POSITION
            WHERE kcu_child.TABLE_SCHEMA = %s
              AND kcu_child.TABLE_NAME   = %s
            ORDER BY kcu_child.ORDINAL_POSITION
            """,
            (sch, tbl),
        )

        return [
            {
                "column":            r["fk_column"],
                "referenced_table":  r["referenced_table"],
                "referenced_column": r["referenced_column"],
                "on_delete":         r.get("on_delete", ""),
                "on_update":         r.get("on_update", ""),
            }
            for r in rows
        ]

    # ── Indexes / clustering keys ────────────────────────────────────────────

    def get_indexes(self, table: str, schema: Optional[str] = None) -> List[Dict]:
        """
        Snowflake does not have traditional indexes.  Instead it uses automatic
        micro-partitioning and optional clustering keys.  We return:
          * One entry per PRIMARY KEY constraint (is_unique=True)
          * One entry for the clustering key if defined
        """
        sch = self._sf_upper(schema or self.config.schema or "PUBLIC")
        tbl = self._sf_upper(table)

        results = []

        # Primary key
        pk_rows = self.execute(
            """
            SELECT kcu.COLUMN_NAME
            FROM   INFORMATION_SCHEMA.TABLE_CONSTRAINTS  tc
            JOIN   INFORMATION_SCHEMA.KEY_COLUMN_USAGE   kcu
                   ON  kcu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
                   AND kcu.TABLE_SCHEMA    = tc.TABLE_SCHEMA
                   AND kcu.TABLE_NAME      = tc.TABLE_NAME
            WHERE  tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
              AND  tc.TABLE_SCHEMA    = %s
              AND  tc.TABLE_NAME      = %s
            ORDER BY kcu.ORDINAL_POSITION
            """,
            (sch, tbl),
        )
        if pk_rows:
            results.append({
                "index_name": "PRIMARY_KEY",
                "is_unique":  True,
                "columns":    [r["column_name"] for r in pk_rows],
            })

        # Clustering key (if any)
        ck_rows = self.execute(
            """
            SELECT CLUSTERING_KEY
            FROM   INFORMATION_SCHEMA.TABLES
            WHERE  TABLE_SCHEMA = %s
              AND  TABLE_NAME   = %s
              AND  CLUSTERING_KEY IS NOT NULL
            """,
            (sch, tbl),
        )
        if ck_rows and ck_rows[0].get("clustering_key"):
            results.append({
                "index_name": "CLUSTERING_KEY",
                "is_unique":  False,
                "columns":    [ck_rows[0]["clustering_key"]],
            })

        return results

    # ── Sample data ──────────────────────────────────────────────────────────

    def sample_data(self, table: str, schema: Optional[str] = None, n: int = 5) -> List[Dict]:
        tbl_ref = self._qualified_table(table, schema)
        try:
            return self.execute(f"SELECT * FROM {tbl_ref} LIMIT {n}")
        except Exception:
            return []


# ---------------------------------------------------------------------------
# PostgreSQL connector (stub — extend get_column_stats if needed)
# ---------------------------------------------------------------------------

class PostgreSQLConnector(BaseConnector):
    """
    PostgreSQL via psycopg2.

    Inherits get_column_stats() from BaseConnector.  The ANSI SQL used there
    works in PostgreSQL without modification.  The only difference is that
    PostgreSQL uses %s placeholders (same as snowflake-connector-python).
    """

    def connect(self) -> "PostgreSQLConnector":
        import psycopg2
        import psycopg2.extras
        self._conn = psycopg2.connect(
            host=self.config.host,
            port=self.config.port or 5432,
            dbname=self.config.database,
            user=self.config.username,
            password=self.config.password,
        )
        self._dict_cursor_factory = psycopg2.extras.RealDictCursor
        return self

    def execute(self, sql: str, params=None) -> List[Dict]:
        with self._conn.cursor(cursor_factory=self._dict_cursor_factory) as cur:
            cur.execute(sql, params)
            try:
                return [dict(r) for r in cur.fetchall()]
            except Exception:
                return []

    def get_schemas(self) -> List[str]:
        rows = self.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name NOT IN ('pg_catalog','information_schema','pg_toast') "
            "ORDER BY schema_name"
        )
        return [r["schema_name"] for r in rows]

    def get_tables(self, schema: Optional[str] = None) -> List[Dict]:
        schema = schema or "public"
        return self.execute(
            """
            SELECT t.table_name,
                   t.table_type,
                   t.table_schema               AS schema,
                   COALESCE(s.n_live_tup, 0)    AS row_count
            FROM information_schema.tables t
            LEFT JOIN pg_stat_user_tables s
                   ON s.relname = t.table_name
                  AND s.schemaname = t.table_schema
            WHERE t.table_schema = %s
            ORDER BY t.table_name
            """,
            [schema],
        )

    def get_columns(self, table: str, schema: Optional[str] = None) -> List[Dict]:
        schema = schema or "public"
        rows = self.execute(
            """
            SELECT c.column_name,
                   c.data_type,
                   (c.is_nullable = 'YES')     AS is_nullable,
                   c.column_default            AS default_value,
                   c.ordinal_position,
                   c.character_maximum_length,
                   c.numeric_precision,
                   c.numeric_scale,
                   CASE WHEN kcu.column_name IS NOT NULL
                        THEN TRUE ELSE FALSE
                   END                         AS is_primary_key
            FROM information_schema.columns c
            LEFT JOIN (
                SELECT kcu.column_name
                FROM   information_schema.table_constraints  tc
                JOIN   information_schema.key_column_usage   kcu
                       ON  kcu.constraint_name = tc.constraint_name
                       AND kcu.table_schema    = tc.table_schema
                       AND kcu.table_name      = tc.table_name
                WHERE  tc.constraint_type = 'PRIMARY KEY'
                  AND  tc.table_schema    = %s
                  AND  tc.table_name      = %s
            ) pk ON pk.column_name = c.column_name
            WHERE c.table_schema = %s
              AND c.table_name   = %s
            ORDER BY c.ordinal_position
            """,
            [schema, table, schema, table],
        )
        return [
            {
                "column_name":    r["column_name"],
                "data_type":      r["data_type"],
                "is_nullable":    bool(r["is_nullable"]),
                "default_value":  r["default_value"],
                "is_primary_key": bool(r["is_primary_key"]),
                "ordinal_position": r["ordinal_position"],
            }
            for r in rows
        ]

    def get_foreign_keys(self, table: str, schema: Optional[str] = None) -> List[Dict]:
        schema = schema or "public"
        rows = self.execute(
            """
            SELECT kcu.column_name               AS fk_column,
                   ccu.table_name                AS referenced_table,
                   ccu.column_name               AS referenced_column
            FROM information_schema.table_constraints       tc
            JOIN information_schema.key_column_usage        kcu
                 ON  tc.constraint_name = kcu.constraint_name
                 AND tc.table_schema    = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                 ON  ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
              AND tc.table_name      = %s
              AND tc.table_schema    = %s
            """,
            [table, schema],
        )
        return [
            {
                "column":            r["fk_column"],
                "referenced_table":  r["referenced_table"],
                "referenced_column": r["referenced_column"],
                "on_delete":         "",
                "on_update":         "",
            }
            for r in rows
        ]

    def get_indexes(self, table: str, schema: Optional[str] = None) -> List[Dict]:
        schema = schema or "public"
        rows = self.execute(
            "SELECT indexname AS index_name, indexdef "
            "FROM pg_indexes "
            "WHERE tablename = %s AND schemaname = %s",
            [table, schema],
        )
        return [
            {
                "index_name": r["index_name"],
                "is_unique":  "UNIQUE" in (r.get("indexdef") or ""),
                "columns":    [],  # parsing indexdef is optional
            }
            for r in rows
        ]

    def sample_data(self, table: str, schema: Optional[str] = None, n: int = 5) -> List[Dict]:
        schema = schema or "public"
        return self.execute(
            f'SELECT * FROM "{schema}"."{table}" LIMIT %s', [n]
        )


# ---------------------------------------------------------------------------
# Connector factory
# ---------------------------------------------------------------------------

def get_connector(config: ConnectionConfig) -> BaseConnector:
    """Return the correct connector instance for config.db_type."""
    mapping = {
        DBType.SQLITE:      SQLiteConnector,
        DBType.POSTGRESQL:  PostgreSQLConnector,
        DBType.SNOWFLAKE:   SnowflakeConnector,
    }
    cls = mapping.get(config.db_type)
    if not cls:
        raise ValueError(
            f"Connector for '{config.db_type}' is not yet implemented. "
            f"Supported types: {', '.join(m.value for m in mapping)}"
        )
    return cls(config)