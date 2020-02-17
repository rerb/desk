import ast
import datetime
import json
import logging
import os

import psycopg2
import sqlalchemy
import requests


DESK_API_AUTH = {"username": os.environ["DESK_USERNAME"],
                 "password": os.environ["DESK_PASSWORD"]}

BASE_DESK_API_URL = "https://aashe-support.desk.com"

PAGINATION_LIMIT = 500

PG_USER = os.environ["PG_USER"]
PG_HOST = os.environ["PG_HOST"]
PG_PORT = os.environ["PG_PORT"]
PG_PASSWORD = os.environ["PG_PASSWORD"]
PG_DBNAME = os.environ["PG_DBNAME"]

logger = logging.getLogger()


def get_paginated_content(url,
                          since_updated_at=None,
                          passed_pagination_limit=False):
    """Returns a dictionary of `resp.json()`
    objects, keyed by page number.
    """
    full_url = BASE_DESK_API_URL + url

    # tack parameters onto url when since_updated_at.
    if (since_updated_at and
        not "since_updated_at" in url):  # noqa

        full_url += ("&sort_field=updated_at&since_updated_at=" +
                     str(int(since_updated_at)))

    logging.warning("full_url: {full_url}".format(
        full_url=full_url))

    resp = requests.get(url=full_url,
                        auth=(DESK_API_AUTH["username"],
                              DESK_API_AUTH["password"]))

    if resp.status_code != 200:
        raise Exception("response is {status_code} ({resp})".format(
            status_code=resp.status_code, resp=resp))

    json = resp.json()

    # get the next page href.
    try:
        next_page_href = json["_links"]["next"]["href"]
    except TypeError:  # 'NoneType' object is not subscriptable
        next_page_href = None

    pages_key = (json["page"] if not passed_pagination_limit
                 else json["page"] + PAGINATION_LIMIT)

    pages = {pages_key: json}

    # get the next page.
    if next_page_href:

        magic_page_marker = "page=" + str(PAGINATION_LIMIT + 1)

        # set since_updated_at if we've hit PAGINATION_LIMIT.
        if magic_page_marker in next_page_href:
            # strip page parameter.
            next_page_href = next_page_href.replace(magic_page_marker, '')
            # set since_updated_at.
            date_updated_at = json["_embedded"]["entries"][-1]["updated_at"]
            since_updated_at = datetime.datetime.strptime(
                date_updated_at, "%Y-%m-%dT%H:%S:%fZ").timestamp()
            # let recursive calls know.
            passed_pagination_limit = True

        # add the next page to pages.
        next_pages = get_paginated_content(
            url=next_page_href,
            since_updated_at=since_updated_at,
            passed_pagination_limit=passed_pagination_limit)

        pages.update(next_pages)

    return pages


def get_cases(per_page=100):
    """Returns a dictionary of cases, as dictionaries, indexed
    by ID.
    """
    endpoint_url = "/api/v2/cases?per_page=" + str(per_page)

    endpoint_url += "&embed=" + ','.join((
        "assigned_group",
        "assigned_user",
        "customer",
        "draft",
        "feedbacks",
        "locked_by",
        "message"))

    pages = get_paginated_content(endpoint_url)

    cases = {}

    for page_number, page in pages.items():
        logger.warning("collating page {page_number}".format(
            page_number=page_number))
        for entry in page["_embedded"]["entries"]:
            cases[entry["id"]] = entry

    return cases


def dump_cases(cases, filename):
    """Dump `cases` to `filename`.
    Return file.
    """
    with open(filename, "w") as f:
        print(cases, file=f)

    return f


def load_cases(filename):
    """Load cases from `filename`.
    Returns dict of cases.
    """
    cases = None

    with open(filename, "r") as f:
        cases = ast.literal_eval(f.read())

    return cases


def get_linked(case, link_type, per_page=100):
    """Returns a list of `link_type`s linked to `case`.
    """
    endpoint_url = (
        "/api/v2/cases/{case_id}/{link_type}?per_page={per_page}".format(
            case_id=case["id"],
            link_type=link_type,
            per_page=per_page))

    linked = []

    # if there's a `count` for this type of link, and
    # it's 0, we can short-circuit.
    count_available = "count" in case["_links"][link_type].keys()
    if count_available:
        count = int(case["_links"][link_type]["count"])
        if count < 1:
            return linked

    pages = get_paginated_content(endpoint_url)

    for page_number, page in pages.items():
        for entry in page["_embedded"]["entries"]:
            linked.append(entry)

    return linked


def embed_related_records_into_case(case):
    """Embed all the records related to `case`
    that can't be embedded via the Desk API
    (rather than provide a foreign key).
    """
    endpoint_url = "/api/v2/cases/{case_id}".format(
        case_id=case["id"])

    for link_type in ["notes", "attachments", "replies"]:

        case["_embedded"][link_type] = get_linked(case=case,
                                                  link_type=link_type)

    return case

##############################################################################
##############################################################################

# Database functions


def create_database():
    """Create the database.
    """
    try:
        cnx = psycopg2.connect(dbname=dbname,
                               user=PG_USER,
                               host=PG_HOST,
                               password=PG_PASSWORD)

        cnx.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

        cursor = cnx.cursor()

        try:
            cursor.execute('CREATE DATABASE ' + PG_DBNAME)
        except psycopg2.DuplicateDatabase:
            logger.warning("Database {db_name} already exists.".format(
                db_name=PG_DBNAME))
        finally:
            cursor.close()

    finally:
        if cnx:
            cnx.close()


def get_database_connection():

    engine = sqlalchemy.create_engine(
        "postgresql://{pg_user}:pg_password@"
        "{db_host}:{db_port}/{db_name}".format(
            pg_user=PG_USER, pg_password=PG_PASSWORD,
            db_host=PG_HOST, db_port=PG_PORT,
            db_name=PG_DBNAME))

    return engine.connect()


def get_case_table():
    """Get the case table.

    Creates the table if it doesn't exist.

    Returns sqlalchemy.Table for cases.
    """
    cnx = get_database_connection()

    meta = sqlalchemy.MetaData(cnx)

    case_table = sqlalchemy.Table(
        "case",
        meta,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("doc", sqlalchemy.dialects.postgresql.JSONB))

    meta.create_all()

    return case_table


def insert_case(case, case_table, cnx):
    """Insert `case` into `case_table` using
    database connection `cnx`.

    Returns a sqlalchemy.ResultProxy if successful.

    If `case` is already in `case_table`, a warning should
    be logged, and None returned.  However, that's not working.

    Make sure `case` isn't
    """
    try:
        return cnx.execute(case_table.insert(),
                           id=case["id"],
                           doc=case)
    except psycopg2.errors.UniqueViolation:
        logger.warning("Case {id} already exists; can't be inserted.".format(
            id=case["id"]))
        return None


##############################################################################
##############################################################################

# Main


def main(cases=None):

    cases = cases or get_cases()

    create_database()

    case_table = get_case_table()

    cnx = get_database_connection()

    # Wrap cases.values() in list() so it will be
    # serializeable (rather than a dict_items).
    for case in list(cases.values()):

        embed_related_records_into_case(case)

        logger.warning("Inserting case " + str(case["id"]))
        insert_case(case=case, case_table=case_table, cnx=cnx)
