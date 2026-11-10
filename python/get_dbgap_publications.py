"""
To run locally do python3 python/get_dbgap_publications.py --phs_ids phs002591 phs004621, or pass a file with one
PHS accession per line using --phs_id_file instead (one of the two is required).
You may need to install the required packages with pip install -r requirements.txt.

For each dbGaP PHS accession provided (with or without a version, e.g. phs002591 or phs002591.v2.p1), this looks
up the latest study version if needed, downloads its GapExchange XML from the dbGaP FTP site, and extracts any
PubMed IDs linked to the study. It also looks up the matching DUOS study for that PHS ID (skipping and logging a
warning if none is found, or if more than one DUOS study shares the PHS ID) and checks which of the study's
PubMed IDs are already registered as publications on that DUOS study. Metadata for each publication (title,
journal, year, DOI, authors, MeSH tags) is then pulled from NCBI E-utilities, and any publication not already on the
DUOS study is added to it. The publications that were not already in DUOS are printed per PHS ID and written to
a TSV file. Every PHS ID appears in the TSV: PHS IDs with nothing to add (no publications in dbGaP, no or multiple
matching DUOS studies, or all publications already in DUOS) or that failed get a single row explaining why in the
`status` column.

Adding publications requires being an admin, the creator, or a data custodian of the DUOS study; otherwise DUOS
responds with a 403. Use --dry_run to see what would be added without changing anything in DUOS. Use
--continue_on_error to log and record a failure for one PHS accession (e.g. a 404 from dbGaP) and keep going.
"""
import logging
from argparse import ArgumentParser, Namespace
from typing import Any, Optional

from ops_utils.csv_util import Csv
from ops_utils.duos_utils.duos_util import DUOS
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token

from utils.dbgap_utils import DbGaPStudy, get_pubmed_metadata

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


# Keys we add to each result for reporting that are not DUOS publication fields
NON_DUOS_FIELDS = {"phsId", "duosStudyId", "duosStudyName", "alreadyInDuos", "status"}

# Columns written to the output TSV. There is one row per publication not yet in DUOS, or a single row for a PHS ID
# with nothing to add or that failed. `status` says which of those it is.
TSV_COLUMNS = [
    "phsId", "status", "duosStudyId", "duosStudyName", "pubmedId", "title", "journal", "publishedDate", "doi", "url",
    "authors", "tags",
]


class DbGaPPublicationsToDuos:
    """Find the publications linked to a dbGaP study and add any that are missing to the matching DUOS study."""

    def __init__(self, duos: DUOS, skip_pubmed_metadata: bool, dry_run: bool):
        self.duos = duos
        self.skip_pubmed_metadata = skip_pubmed_metadata
        self.dry_run = dry_run

    def get_publications(self, phs_id: str) -> tuple[list[dict], Optional[str]]:
        """
        Look up the publications dbGaP links to a study, and which of them are already in the matching DUOS study.

        **Returns:**
        - tuple[list[dict], Optional[str]]: The publications, one entry per PMID with PubMed metadata unless
            `skip_pubmed_metadata` is set, and a reason the study was skipped. The list is empty and the reason is
            set if dbGaP has no publications for the study, or if there is not exactly one matching DUOS study.
            The reason is None otherwise.
        """
        logging.info(f"Looking up dbGaP publications for {phs_id}")
        full_accession, pmids = DbGaPStudy(phs_id).get_publication_pmids()
        logging.info(f"Found {len(pmids)} publication(s) for {full_accession} in dbGaP")

        if not pmids:
            logging.info(f"No publications found for {full_accession}")
            return [], f"No publications linked to {full_accession} in dbGaP"

        duos_studies = self.duos.find_studies_by_phs_id(phs_id)
        if not duos_studies:
            logging.warning(f"No DUOS study found for PHS ID {phs_id}, skipping")
            return [], f"No DUOS study found for {phs_id}"
        if len(duos_studies) > 1:
            matched_ids = ", ".join(str(study.get("studyId")) for study in duos_studies)
            logging.error(
                f"Found multiple DUOS studies for PHS ID {phs_id} (study IDs: {matched_ids}), skipping"
            )
            return [], f"Multiple DUOS studies found for {phs_id} (study IDs: {matched_ids})"
        duos_study = duos_studies[0]

        duos_publications = self.duos.get_study_publications(duos_study["studyId"])
        duos_pmids = {str(pub["pubmedId"]).strip() for pub in duos_publications if pub.get("pubmedId")}
        logging.info(f"DUOS study {duos_study.get('studyId')} already has {len(duos_pmids)} publication(s) with a PMID")

        publications = []
        for pmid in pmids:
            publication = {
                "phsId": full_accession,
                "duosStudyId": duos_study.get("studyId"),
                "duosStudyName": duos_study.get("studyName"),
                "pubmedId": pmid,
                "alreadyInDuos": str(pmid) in duos_pmids,
            }
            if not self.skip_pubmed_metadata:
                publication.update(get_pubmed_metadata(pmid))
            publications.append(publication)
        return publications, None

    def add_missing_to_duos(self, publications: list[dict]) -> None:
        """Add the publications from `get_publications` that are not already in their DUOS study."""
        if self.skip_pubmed_metadata or not publications:
            return
        duos_study_id = publications[0]["duosStudyId"]
        publications_to_add = [
            {key: value for key, value in publication.items() if key not in NON_DUOS_FIELDS}
            for publication in publications
            if not publication["alreadyInDuos"]
        ]
        if not publications_to_add:
            logging.info(f"All publications are already in DUOS study {duos_study_id}")
        elif self.dry_run:
            logging.info(
                f"Dry run: would add {len(publications_to_add)} publication(s) to DUOS study "
                f"{duos_study_id}: {[pub['pubmedId'] for pub in publications_to_add]}"
            )
        else:
            self.duos.add_study_publications(duos_study_id, publications_to_add)


def _tsv_value(value: Any) -> str:
    """Flatten a publication field to a single TSV-safe string."""
    if value is None:
        return ""
    if isinstance(value, list):
        value = "; ".join(item["name"] if isinstance(item, dict) else str(item) for item in value)
    return " ".join(str(value).split())


def write_missing_publications_tsv(publications: list[dict], output_tsv: str) -> None:
    """Write the rows (publications not yet in DUOS, or a status row for a PHS ID with none) to a TSV."""
    rows = [
        {column: _tsv_value(publication.get(column)) for column in TSV_COLUMNS}
        for publication in publications
    ]
    Csv(file_path=output_tsv).create_tsv_from_list_of_dicts(list_of_dicts=rows, header_list=TSV_COLUMNS)


def get_phs_ids(args: Namespace) -> list[str]:
    """Get the PHS IDs from `--phs_ids` or `--phs_id_file`, with duplicates removed and order kept."""
    if args.phs_id_file:
        with open(args.phs_id_file) as f:
            phs_ids = [line.strip() for line in f if line.strip()]
        if not phs_ids:
            raise ValueError(f"No PHS IDs found in {args.phs_id_file}")
    else:
        phs_ids = args.phs_ids
    return list(dict.fromkeys(phs_ids))


def get_args() -> Namespace:
    parser = ArgumentParser(description="Look up publications linked to dbGaP studies for one or more PHS accessions")
    phs_group = parser.add_mutually_exclusive_group(required=True)
    phs_group.add_argument(
        "--phs_ids", "-p", nargs="+",
        help="One or more dbGaP PHS accessions separated by spaces, with or without version "
             "(e.g. phs002591 phs004621.v1.p1). Provide this or --phs_id_file, not both"
    )
    phs_group.add_argument(
        "--phs_id_file", "-f",
        help="Path to a file with one dbGaP PHS accession per line, with or without version. Blank lines are "
             "ignored. Provide this or --phs_ids, not both"
    )
    parser.add_argument(
        "--output_tsv", "-o", default="dbgap_publications.tsv",
        help="Path to write a TSV with one row per publication found in dbGaP that is not yet in DUOS, and one row "
             "per PHS accession with nothing to add or that failed, with the reason in the `status` column. "
             "Defaults to dbgap_publications.tsv"
    )
    parser.add_argument(
        "--env", "-e", choices=["prod", "dev"], default="prod",
        help="DUOS environment to look studies and publications up in. Defaults to prod"
    )
    parser.add_argument(
        "--skip_pubmed_metadata", action="store_true",
        help="If set, only report the PMIDs found in dbGaP without fetching additional metadata from PubMed. "
             "Publications cannot be added to DUOS without that metadata, so nothing is added to DUOS"
    )
    parser.add_argument(
        "--continue_on_error", action="store_true",
        help="If set, a failure for one PHS accession (e.g. the study is not found in dbGaP, or adding publications "
             "to DUOS fails) is logged as a warning and the script moves on to the next accession instead of "
             "stopping. The failure is recorded in the `status` column of the output TSV"
    )
    parser.add_argument(
        "--dry_run", action="store_true",
        help="If set, log the publications that would be added to DUOS without adding them"
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()
    phs_ids = get_phs_ids(args)

    token = Token()
    request_util = RunRequest(token=token)
    duos = DUOS(request_util=request_util, env=args.env)
    publisher = DbGaPPublicationsToDuos(
        duos=duos, skip_pubmed_metadata=args.skip_pubmed_metadata, dry_run=args.dry_run
    )

    results = []
    # The TSV rows for each PHS ID: one per publication not yet in DUOS, or a single row saying why there is none
    rows_by_phs: dict[str, list[dict]] = {}
    print("=" * 80)
    for phs_id in phs_ids:
        try:
            publications, skip_reason = publisher.get_publications(phs_id)
        except Exception as e:
            if not args.continue_on_error:
                raise
            status = f"Failed to look up publications: {type(e).__name__}: {e}"
            logging.warning(f"{phs_id}: {status}")
            rows_by_phs[phs_id] = [{"phsId": phs_id, "status": status}]
            print("=" * 80)
            continue
        results.extend(publications)
        missing = [pub for pub in publications if not pub["alreadyInDuos"]]
        if not missing:
            status = skip_reason or f"All {len(publications)} publication(s) in dbGaP are already in DUOS"
            logging.info(f"{phs_id}: no publications to add to DUOS. {status}")
            rows_by_phs[phs_id] = [{"phsId": phs_id, "status": status}]
            print("=" * 80)
            continue

        try:
            publisher.add_missing_to_duos(publications)
            # Nothing is added in a dry run, or without PubMed metadata
            status = "Not in DUOS" if args.dry_run or args.skip_pubmed_metadata else "Added to DUOS"
        except Exception as e:
            if not args.continue_on_error:
                raise
            status = f"Failed to add publications to DUOS: {type(e).__name__}: {e}"
            logging.warning(f"{phs_id}: {status}")
        for pub in missing:
            pub["status"] = status
        rows_by_phs[phs_id] = missing
        print("=" * 80)

    logging.info(f"Found {len(results)} publication(s) across {len(phs_ids)} PHS accession(s)")

    tsv_rows = [row for rows in rows_by_phs.values() for row in rows]
    write_missing_publications_tsv(tsv_rows, args.output_tsv)

    print("Publications found in dbGaP that are not yet in DUOS:")
    for phs_id, rows in rows_by_phs.items():
        if not rows[0].get("pubmedId"):
            outcome = "FAILED" if rows[0]["status"].startswith("Failed") else "nothing to add"
            print(f"\n{phs_id}: {outcome}. {rows[0]['status']}")
            continue
        print(f"\n{phs_id}: {len(rows)} not in DUOS ({rows[0]['status']})")
        for pub in rows:
            print(f"  PMID {pub['pubmedId']}: {pub.get('title') or '(no title; PubMed metadata skipped)'}")
    logging.info(f"Wrote {len(tsv_rows)} row(s) for {len(phs_ids)} PHS accession(s) to {args.output_tsv}")
