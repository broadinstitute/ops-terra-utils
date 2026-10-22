"""
To run locally do python3 python/get_dbgap_publications.py --phs_ids phs002591 phs004621.
You may need to install the required packages with pip install -r requirements.txt.

For each dbGaP PHS accession provided (with or without a version, e.g. phs002591 or phs002591.v2.p1), this looks
up the latest study version if needed, downloads its GapExchange XML from the dbGaP FTP site, and extracts any
PubMed IDs linked to the study. It also looks up the matching DUOS study for that PHS ID (skipping and logging a
warning if none is found, or if more than one DUOS study shares the PHS ID) and checks which of the study's
PubMed IDs are already registered as publications on that DUOS study. Metadata for each publication (title,
journal, year, DOI, authors, MeSH tags) is then pulled from NCBI E-utilities, and any publication not already on the
DUOS study is added to it. Results are written to a JSON file.

Adding publications requires being an admin, the creator, or a data custodian of the DUOS study; otherwise DUOS
responds with a 403. Use --dry_run to see what would be added without changing anything in DUOS.
"""
import logging
from argparse import ArgumentParser, Namespace

from ops_utils.duos_utils.duos_util import DUOS
from ops_utils.request_util import RunRequest
from ops_utils.token_util import Token

from utils.dbgap_utils import DbGaPStudy, get_pubmed_metadata

logging.basicConfig(
    format="%(levelname)s: %(asctime)s : %(message)s", level=logging.INFO
)


# Keys we add to each result for reporting that are not DUOS publication fields
NON_DUOS_FIELDS = {"phsId", "duosStudyId", "duosStudyName", "alreadyInDuos"}


class DbGaPPublicationsToDuos:
    """Find the publications linked to a dbGaP study and add any that are missing to the matching DUOS study."""

    def __init__(self, duos: DUOS, skip_pubmed_metadata: bool, dry_run: bool):
        self.duos = duos
        self.skip_pubmed_metadata = skip_pubmed_metadata
        self.dry_run = dry_run

    def get_publications(self, phs_id: str) -> list[dict]:
        """
        Look up the publications dbGaP links to a study, and which of them are already in the matching DUOS study.

        **Returns:**
        - list[dict]: One entry per PMID, with PubMed metadata unless `skip_pubmed_metadata` is set. Empty if dbGaP
            has no publications for the study, or if there is not exactly one matching DUOS study.
        """
        logging.info(f"Looking up dbGaP publications for {phs_id}")
        full_accession, pmids = DbGaPStudy(phs_id).get_publication_pmids()
        logging.info(f"Found {len(pmids)} publication(s) for {full_accession} in dbGaP")

        if not pmids:
            logging.info(f"No publications found for {full_accession}")
            return []

        duos_studies = self.duos.find_studies_by_phs_id(phs_id)
        if not duos_studies:
            logging.warning(f"No DUOS study found for PHS ID {phs_id}, skipping")
            return []
        if len(duos_studies) > 1:
            matched_ids = ", ".join(str(study.get("studyId")) for study in duos_studies)
            logging.error(
                f"Found multiple DUOS studies for PHS ID {phs_id} (study IDs: {matched_ids}), skipping"
            )
            return []
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
        return publications

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
        "--env", "-e", choices=["prod", "dev"], default="prod",
        help="DUOS environment to look studies and publications up in. Defaults to prod"
    )
    parser.add_argument(
        "--skip_pubmed_metadata", action="store_true",
        help="If set, only report the PMIDs found in dbGaP without fetching additional metadata from PubMed. "
             "Publications cannot be added to DUOS without that metadata, so nothing is added to DUOS"
    )
    parser.add_argument(
        "--dry_run", action="store_true",
        help="If set, log the publications that would be added to DUOS without adding them"
    )
    return parser.parse_args()


if __name__ == '__main__':
    args = get_args()

    token = Token()
    request_util = RunRequest(token=token)
    duos = DUOS(request_util=request_util, env=args.env)
    publisher = DbGaPPublicationsToDuos(
        duos=duos, skip_pubmed_metadata=args.skip_pubmed_metadata, dry_run=args.dry_run
    )

    results = []
    print("=" * 80)
    for phs_id in args.phs_ids:
        publications = publisher.get_publications(phs_id)
        results.extend(publications)
        publisher.add_missing_to_duos(publications)
        print("=" * 80)

    logging.info(f"Found {len(results)} publication(s) across {len(args.phs_ids)} PHS accession(s)")
