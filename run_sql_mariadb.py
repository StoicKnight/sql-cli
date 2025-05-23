#!/usr/bin/env python3

import argparse
import json
import sys
from typing import Any, Dict, List, Optional

import mariadb
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class DBSettings(BaseSettings):
    db_host: str = Field(validation_alias="DB_HOST")
    db_user: str = Field(validation_alias="DB_USER")
    db_password: str = Field(validation_alias="DB_PASSWORD")
    db_name: str = Field(validation_alias="DB_NAME")
    db_port: int = Field(default=3306, validation_alias="DB_PORT")

    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        case_sensitive=False,
    )


def main():
    parser = argparse.ArgumentParser(
        description="Run SQL from a file against a MariaDB database and output results of the last SELECT query."
    )
    parser.add_argument("sql_file", help="Path to the SQL file to execute.")
    args = parser.parse_args()

    try:
        settings = DBSettings()  # pyright: ignore
    except Exception as e:
        print(f"Error loading database settings: {e}", file=sys.stderr)
        print(
            "Please ensure DB_USER, DB_PASSWORD, and DB_NAME are set via environment variables or in a .env file.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        with open(args.sql_file, "r") as f:
            sql_query = f.read()
        if not sql_query.strip():
            print(
                f"SQL file '{args.sql_file}' is empty or contains only whitespace.",
                file=sys.stderr,
            )
            sys.exit(1)
    except FileNotFoundError:
        print(f"Error: SQL file '{args.sql_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Error reading SQL file '{args.sql_file}': {e}", file=sys.stderr)
        sys.exit(1)

    conn: Optional[mariadb.Connection] = None
    final_results_from_last_select: List[Dict[str, Any]] = []
    any_select_executed = False

    try:
        conn_params = {
            "user": settings.db_user,
            "password": settings.db_password,
            "host": settings.db_host,
            "port": settings.db_port,
            "database": settings.db_name,
        }
        print(
            f"Connecting to MariaDB: host={settings.db_host}, port={settings.db_port}, database='{settings.db_name}', user='{settings.db_user}'...",
            file=sys.stderr,
        )
        conn = mariadb.connect(**conn_params)
        cursor = conn.cursor()

        print(f"Executing SQL from '{args.sql_file}'...", file=sys.stderr)

        cursor.execute(sql_query)

        is_first_result_set = True
        while True:
            if cursor.description:
                any_select_executed = True
                column_names = [desc[0] for desc in cursor.description]
                rows = cursor.fetchall()
                current_statement_results = [
                    dict(zip(column_names, row)) for row in rows
                ]
                final_results_from_last_select = current_statement_results
                print(
                    f"Statement produced {len(current_statement_results)} rows. Columns: {column_names}",
                    file=sys.stderr,
                )
            else:
                if (
                    is_first_result_set
                    or cursor.lastrowid is not None
                    or cursor.rowcount != -1
                ):
                    print(
                        f"Statement executed. Rows affected: {cursor.rowcount}. Last inserted ID: {cursor.lastrowid}",
                        file=sys.stderr,
                    )

            is_first_result_set = False
            if not cursor.nextset():
                break

        conn.commit()
        print("SQL execution completed and transaction committed.", file=sys.stderr)

    except mariadb.Error as e:
        print(f"MariaDB Error: {e}", file=sys.stderr)
        if conn:
            try:
                print("Rolling back transaction due to error.", file=sys.stderr)
                conn.rollback()
            except mariadb.Error as re:
                print(f"Error during rollback: {re}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        if conn:
            try:
                print(
                    "Rolling back transaction due to unexpected error.", file=sys.stderr
                )
                conn.rollback()
            except mariadb.Error as re:
                print(f"Error during rollback: {re}", file=sys.stderr)
        sys.exit(1)
    finally:
        if conn:
            print("Closing MariaDB connection.", file=sys.stderr)
            conn.close()

    if not any_select_executed:
        print(
            "No SELECT statements were executed or they produced no structured result sets.",
            file=sys.stderr,
        )

    try:
        output_json = json.dumps(final_results_from_last_select, indent=4, default=str)
        print(output_json)
    except TypeError as e:
        print(f"Error serializing results to JSON: {e}", file=sys.stderr)
        print("Problematic data:", final_results_from_last_select, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
