import ast
import datetime
import logging
import os

import requests


AUTH = {"username": os.environ["DESK_USERNAME"],
        "password": os.environ["DESK_PASSWORD"]}

BASE_DESK_API_URL = "https://aashe-support.desk.com"

PAGINATION_LIMIT = 500


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
                        auth=(AUTH["username"], AUTH["password"]))

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
    pages = get_paginated_content("/api/v2/cases?per_page=" +
                                  str(per_page))

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
