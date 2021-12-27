import json
import psycopg2
import os


class PgDatabase:
    def __init__(self, cfg):
        self.conn = psycopg2.connect(
            dbname=cfg["dbname"],
            user=cfg["user"],
            password=cfg["password"],
            host=cfg["host"],
            port=int(cfg["port"])
        )

    def close(self):
        self.conn.close()

    def validate(self, row):
        field_types = {
            'handle': str,
	        'solved_count': int,
	        'unsolved_count': int,
	        'submissions': int,
	        'average_solved': int
        }
        
        print(row)

        for field_name, field_type in field_types.items():
            if not field_name in row:
                return False

            if field_type != type(row[field_name]):
                return False

        return True

    def _insert(self, table, fields):
        columns_string = ""
        values_string = ""

        args = []

        for column_name, value in fields.items():
            columns_string += column_name
            columns_string += ","
            values_string += "%s,"
            args.append(value)

        columns_string = columns_string[:-1]
        values_string = values_string[:-1]

        sql_string = f"INSERT INTO {table} ({columns_string}) VALUES ({values_string}) ON CONFLICT DO NOTHING"
        print(sql_string)

        with self.conn.cursor() as cursor:
            cursor.execute(sql_string, tuple(args))

    def insert(self, assignment_id, row):
        if not self.validate(row):
            print("Can't insert row, check fields and their types")
            return False

        self._insert(table="statistic", fields={
            "assignment_id" : assignment_id,
            "handle": row["handle"],
            "solved_count": row["solved_count"],
            "unsolved_count": row["unsolved_count"],
            "submissions": row["submissions"], 
            "average_solved": row["average_solved"]
            })

        self.conn.commit()
