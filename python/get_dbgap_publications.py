"""
To run locally do python3 python/get_dbgap_publications.py --phs_ids phs002591 phs004621.
You may need to install the required packages with pip install -r requirements.txt.

For each dbGaP PHS accession provided (with or without a version, e.g. phs002591 or phs002591.v2.p1), this looks
up the latest study version if needed, downloads its GapExchange XML from the dbGaP FTP site, and extracts any
PubMed IDs linked to the study. It also looks up the matching DUOS study for that PHS ID (skipping and logging a
warning if none is found, or if more than one DUOS study shares the PHS ID). Metadata for each publication (title,
journal, year, DOI, authors, MeSH tags) is then pulled from NCBI E-utilities. Results are written to a JSON file.
"""
import json
import logging
from argparse import ArgumentParser, Namespace

from ops_utils.duos_util import DUOS
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token

from utils.dbgap_utils import DbGaPStudy, get_pubmed_metadata

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


def get_args() -> Namespace:
    parser = ArgumentParser(description="Look up publications linked to dbGaP studies for one or more PHS accessions")
    parser.add_argument(
        "--phs_ids", "-p", nargs="+", required=True,
        help="One or more dbGaP PHS accessions, with or without version (e.g. phs002591 or phs002591.v2.p1)"
    )
    parser.add_argument(
        "--output_json", "-o", default="dbgap_publications.json",
        help="Path to write the resulting publication metadata JSON. Defaults to dbgap_publications.json"
    )
    parser.add_argument(
        "--skip_pubmed_metadata", action="store_true",
        help="If set, only report the PMIDs found in dbGaP without fetching additional metadata from PubMed"
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()

    token = Token()
    request_util = RunRequest(token=token)
    duos = DUOS(request_util=request_util)

    results = []
    for phs_id in args.phs_ids:
        logging.info(f"Looking up dbGaP publications for {phs_id}")
        try:
            full_accession, pmids = DbGaPStudy(phs_id).get_publication_pmids()
        except Exception as e:
            logging.error(f"Failed to look up publications for {phs_id}: {e}")
            continue

        if not pmids:
            logging.info(f"No publications found for {full_accession}")
            continue

        try:
            duos_studies = duos.find_studies_by_phs_id(phs_id)
        except Exception as e:
            logging.error(f"Failed to look up DUOS study for {phs_id}: {e}")
            continue
        if not duos_studies:
            logging.warning(f"No DUOS study found for PHS ID {phs_id}, skipping")
            continue
        if len(duos_studies) > 1:
            matched_ids = ", ".join(str(study.get("studyId")) for study in duos_studies)
            logging.error(
                f"Found multiple DUOS studies for PHS ID {phs_id} (study IDs: {matched_ids}), skipping"
            )
            continue
        duos_study = duos_studies[0]

        for pmid in pmids:
            publication = {
                "phsId": full_accession,
                "duosStudyId": duos_study.get("studyId"),
                "duosStudyName": duos_study.get("studyName"),
                "pubmedId": pmid,
            }
            if not args.skip_pubmed_metadata:
                try:
                    publication.update(get_pubmed_metadata(pmid))
                except Exception as e:
                    logging.warning(f"Failed to fetch PubMed metadata for PMID {pmid}: {e}")
            results.append(publication)

    logging.info(f"Found {len(results)} publication(s) across {len(args.phs_ids)} PHS accession(s)")
    with open(args.output_json, "w") as f:
        json.dump(results, f, indent=2)
    logging.info(f"Wrote results to {args.output_json}")
