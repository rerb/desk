"""
Export cases from Desk.com, and store them in a PostgreSQL
database.

`main()` will export and upsert all "new" cases.

"new" cases are those updated since the most recently updated case 
already in the database.  If there are no cases in the database,
all cases are exported and stored.

The following environmental variables must be set:

    - `DESK_USERNAME`
    - `DESK_PASSWORD`
    - `PG_USER`
    - `PG_HOST`
    - `PG_PORT`
    - `PG_PASSWORD`
    - `PG_DBNAME`
"""
import datetime
import logging
import os

import psycopg2
import sqlalchemy
import requests
from sqlalchemy.dialects.postgresql import insert as pg_insert


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
    """A generator that returns pages.
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

    page = resp.json()

    yield page

    # get the next page.
    next_page = page["_links"]["next"]

    if next_page:

        next_page_href = next_page["href"]

        magic_page_marker = "page=" + str(PAGINATION_LIMIT + 1)

        # set since_updated_at if we've hit PAGINATION_LIMIT.
        if magic_page_marker in next_page_href:
            # strip page parameter.
            next_page_href = next_page_href.replace(magic_page_marker, '')
            # set since_updated_at.
            date_updated_at = page["_embedded"]["entries"][-1]["updated_at"]
            since_updated_at = datetime.datetime.strptime(
                date_updated_at, "%Y-%m-%dT%H:%S:%fZ").timestamp()
            # let recursive calls know.
            passed_pagination_limit = True

        for page in get_paginated_content(
                url=next_page_href,
                since_updated_at=since_updated_at,
                passed_pagination_limit=passed_pagination_limit):

            yield page


def export_and_upsert_cases(per_page=100,
                            since_updated_at=None,
                            cnx=None):
    """Exports Desk cases, and stores them in database
    named `PG_DBNAME`.

    When `since_updated_at` is provided, only cases updated
    since `since_updated_at` are exported and saved.

    By default, all cases are exported and saved.
    """
    endpoint_url = "/api/v2/cases?per_page=" + str(per_page)

    endpoint_url += "&embed=" + ','.join((
        "assigned_group",
        "assigned_user",
        "customer",
        "draft",
        "feedbacks",
        "message"))

    cnx = cnx or get_database_connection()

    case_table = get_case_table(cnx=cnx)

    for page in get_paginated_content(endpoint_url, since_updated_at):

        for case in page["_embedded"]["entries"]:

            full_case = embed_related_records_into_case(case)

            upsert_case(case=full_case, case_table=case_table, cnx=cnx)


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

    for page in get_paginated_content(endpoint_url):
        for entry in page["_embedded"]["entries"]:
            linked.append(entry)

    return linked


def embed_related_records_into_case(case):
    """Embed all the records related to `case`
    that can't be embedded via the Desk API
    (rather than provide a foreign key).
    """
    for link_type in ["notes", "attachments", "replies"]:

        case["_embedded"][link_type] = get_linked(case=case,
                                                  link_type=link_type)

    return case


def export_and_upsert_new_cases(cnx=None, case_table=None):
    """Export and save all cases updated since the
    last case in the database.
    """
    cnx = cnx or get_database_connection()

    case_table = case_table or get_case_table(cnx=cnx)

    max_updated_at = sqlalchemy.func.max(
        case_table.c.doc["updated_at"].astext).execute().fetchone()[0]

    if max_updated_at:
        since_updated_at = datetime.datetime.strptime(
            max_updated_at, "%Y-%m-%dT%H:%S:%fZ").timestamp()
        logger.warning("Exporting/upserting cases updated since {}.".format(
            since_updated_at))
    else:
        since_updated_at = None
        logger.warning(
            "No extant cases found in database; "
            "exporting/upserting all cases.")

    export_and_upsert_cases(cnx=cnx,
                            since_updated_at=since_updated_at)


##############################################################################
##############################################################################

# Database functions


def get_database_connection():

    engine = sqlalchemy.create_engine(
        "postgresql://{pg_user}:{pg_password}@"
        "{db_host}:{db_port}/{db_name}".format(
            pg_user=PG_USER, pg_password=PG_PASSWORD,
            db_host=PG_HOST, db_port=PG_PORT,
            db_name=PG_DBNAME))

    return engine.connect()


def get_case_table(cnx=None):
    """Get the case table.

    Creates the table if it doesn't exist.

    Returns sqlalchemy.Table for cases.
    """
    cnx = cnx or get_database_connection()

    meta = sqlalchemy.MetaData(cnx)

    case_table = sqlalchemy.Table(
        "case",
        meta,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("doc", sqlalchemy.dialects.postgresql.JSONB))

    meta.create_all()

    return case_table


def upsert_case(case, case_table, cnx):
    """Upsert `case` into `case_table` using
    database connection `cnx`.

    Returns a sqlalchemy.ResultProxy if successful.
    """
    logger.warning("Inserting case " + str(case["id"]))

    upsert_statement = pg_insert(case_table).values(
        id=case["id"], doc=case).on_conflict_do_update(
            constraint="case_pkey",
            set_={"doc": case}
        )

    result = cnx.execute(upsert_statement,
                         id=case["id"],
                         doc=case)

    return result


##############################################################################
##############################################################################

# Main


def main():

    export_and_upsert_new_cases()


if __name__ == "__main__":

    main()
