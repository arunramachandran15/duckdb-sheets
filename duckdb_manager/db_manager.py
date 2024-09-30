import duckdb
import os
from datetime import datetime

class DuckDBManager:
    def __init__(self, sheet_id):
        self.db_file = f'{sheet_id}.db'
        self.conn = duckdb.connect(self.db_file)
        self.create_schema()

    def create_schema(self):
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS id_seq_tabs START 1 INCREMENT 1;")
        self.conn.execute("CREATE SEQUENCE IF NOT EXISTS id_seq_cells START 1 INCREMENT 1;")

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS tabs (
                tab_id INTEGER DEFAULT nextval('id_seq_tabs') PRIMARY KEY,
                tab_name  VARCHAR(255) UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        ''')

        self.conn.execute('''
            CREATE TABLE IF NOT EXISTS cells (
                cell_id INT DEFAULT nextval('id_seq_cells') PRIMARY KEY,
                tab_id INT REFERENCES tabs(tab_id),
                row_index INT NOT NULL,
                column_index INT NOT NULL,
                value TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE (tab_id, row_index, column_index)
            );
        ''')

    def create_tab(self, tab_name):
        self.conn.execute(
            "INSERT INTO tabs (tab_name) VALUES (?)", (tab_name,)
        )

    def insert_or_update_cell(self, tab_id, row_index, column_index, value):
        self.conn.execute('''
            INSERT INTO cells (tab_id, row_index, column_index, value, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (tab_id, row_index, column_index)
            DO UPDATE SET value = ?, updated_at = ?;
        ''', (tab_id, row_index, column_index, value, datetime.now(), value, datetime.now()))

    def read_tab(self, tab_id):
        result = self.conn.execute(
            "SELECT row_index, column_index, value FROM cells WHERE tab_id = ?", (tab_id,)
        ).fetchall()
        return result

    def list_tabs(self):
        result = self.conn.execute("SELECT * FROM tabs").fetchall()
        return result

    def delete_tab(self, tab_id):
        self.conn.execute("DELETE FROM cells WHERE tab_id = ?", (tab_id,))
        self.conn.execute("DELETE FROM tabs WHERE tab_id = ?", (tab_id,))

    def delete_sheet(self):
        if os.path.exists(self.db_file):
            os.remove(self.db_file)
