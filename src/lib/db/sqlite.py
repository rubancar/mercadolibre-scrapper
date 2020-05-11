import sqlite3
import logging
# create database connection
from sqlite3.dbapi2 import ProgrammingError

bridge = sqlite3.connect('scrapped_sites.db')

# create connection, this variable is a singleton according to Python export pattern


def query_read(query, options):
    # execute the query
    try:
        connection = bridge.cursor()
        connection.execute(query, options)
        result = connection.fetchall()
        connection.close()
        return None, result
    except ProgrammingError as err:
        logging.error("Error in SQL statement", err)
        return err, None


def query_write(query, options):
    # execute the query
    try:
        connection = bridge.cursor()
        connection.execute(query, options)
        # Save (commit) the changes
        bridge.commit()
        connection.close()
        return None, True
    except ProgrammingError as err:
        logging.error("Error in SQL statement", err)
        return err, None


def get_all_urls_one_by_one():
    connection = bridge.cursor()
    query_count = 'select count(*) as total from site_subsections'
    connection.execute(query_count)
    total_rows = connection.fetchone()[0]
    logging.debug("Total number of rows for fetching: %i", total_rows)
    query_select = 'select sub.id, sub.site_section_id, sub.name, sub.url, s.name as section_name' \
                   ' from site_subsections sub ' \
                   ' left join site_sections s on s.id = sub.site_section_id'
    connection.execute(query_select)
    for i in range(total_rows):
        row = connection.fetchone()
        yield row




