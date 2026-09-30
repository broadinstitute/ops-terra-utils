"""Utilities for looking up dbGaP study metadata and linked publications."""
import logging
import re
import xml.etree.ElementTree as ET
from typing import Optional

import backoff
import requests

DBGAP_FTP_STUDIES_URL = "https://ftp.ncbi.nlm.nih.gov/dbgap/studies"
PUBMED_EFETCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi"
PUBMED_ARTICLE_URL = "https://pubmed.ncbi.nlm.nih.gov"

_FULL_ACCESSION_PATTERN = re.compile(r"^(phs\d+)\.v(\d+)\.p(\d+)$", re.IGNORECASE)
_BASE_ACCESSION_PATTERN = re.compile(r"^phs\d+$", re.IGNORECASE)


def _is_permanent_http_error(exc: Exception) -> bool:
    """Don't waste retries on a 4xx (e.g. 404 for a study/file that doesn't exist), other than rate-limiting."""
    if not isinstance(exc, requests.exceptions.HTTPError) or exc.response is None:
        return False
    return exc.response.status_code != 429 and 400 <= exc.response.status_code < 500


@backoff.on_exception(
    backoff.expo, requests.exceptions.RequestException, max_tries=5, factor=15, max_time=300,
    giveup=_is_permanent_http_error,
)
def _get(url: str) -> requests.Response:
    response = requests.get(url, timeout=60)
    response.raise_for_status()
    return response


def _get_text(element: Optional[ET.Element], path: str) -> Optional[str]:
    if element is None:
        return None
    found = element.find(path)
    return found.text.strip() if found is not None and found.text else None


class DbGaPStudy:
    """Look up a dbGaP study's GapExchange metadata and any publications linked to it."""

    def __init__(self, phs_id: str):
        self.phs_id = phs_id.strip().lower()
        if not (_FULL_ACCESSION_PATTERN.match(self.phs_id) or _BASE_ACCESSION_PATTERN.match(self.phs_id)):
            raise ValueError(
                f"'{phs_id}' is not a valid dbGaP PHS accession. Expected a format like 'phs002591' "
                f"or 'phs002591.v2.p1'."
            )

    def _resolve_full_accession(self) -> str:
        """Return the fully versioned accession (e.g. phs002591.v2.p1), resolving to the latest version if needed."""
        match = _FULL_ACCESSION_PATTERN.match(self.phs_id)
        if match:
            return self.phs_id

        listing_url = f"{DBGAP_FTP_STUDIES_URL}/{self.phs_id}/"
        response = _get(listing_url)
        version_pattern = re.compile(rf'href="({re.escape(self.phs_id)}\.v(\d+)\.p(\d+))/"', re.IGNORECASE)
        versions = version_pattern.findall(response.text)
        if not versions:
            raise ValueError(f"No study versions found for '{self.phs_id}' at {listing_url}")

        versions.sort(key=lambda v: (int(v[1]), int(v[2])))
        latest_accession = versions[-1][0]
        logging.info(f"Resolved '{self.phs_id}' to latest study version '{latest_accession}'")
        return latest_accession

    def _fetch_gap_exchange_xml(self, full_accession: str) -> Optional[str]:
        """Download the GapExchange XML for a fully versioned accession, if one exists."""
        base_accession = full_accession.split(".")[0]
        version_dir_url = f"{DBGAP_FTP_STUDIES_URL}/{base_accession}/{full_accession}/"
        expected_url = f"{version_dir_url}GapExchange_{full_accession}.xml"

        try:
            return _get(expected_url).text
        except requests.exceptions.HTTPError as e:
            if e.response is None or e.response.status_code != 404:
                raise
            logging.warning(f"{expected_url} not found, checking study directory listing instead")

        try:
            listing = _get(version_dir_url).text
        except requests.exceptions.HTTPError:
            raise ValueError(f"Could not find a study directory for '{full_accession}' at {version_dir_url}")

        match = re.search(r'href="(GapExchange[^"]*\.xml)"', listing, re.IGNORECASE)
        if not match:
            return None
        return _get(version_dir_url + match.group(1)).text

    @staticmethod
    def _parse_publication_pmids(gap_exchange_xml: str) -> list[str]:
        root = ET.fromstring(gap_exchange_xml)
        pmids = []
        for pubmed_element in root.findall(".//Publications/Publication/Pubmed"):
            pmid = pubmed_element.get("pmid")
            if pmid and pmid not in pmids:
                pmids.append(pmid)
        return pmids

    def get_publication_pmids(self) -> tuple[str, list[str]]:
        """
        Find the PubMed IDs linked to this study in dbGaP.

        **Returns:**
        - tuple[str, list[str]]: The fully versioned study accession, and the list of linked PMIDs (empty if none
            are found, or if the study has no GapExchange XML file).
        """
        full_accession = self._resolve_full_accession()
        gap_exchange_xml = self._fetch_gap_exchange_xml(full_accession)
        if gap_exchange_xml is None:
            logging.info(f"No GapExchange XML found for {full_accession}")
            return full_accession, []
        return full_accession, self._parse_publication_pmids(gap_exchange_xml)


def _get_published_date(article: ET.Element) -> Optional[str]:
    pub_date = article.find("Journal/JournalIssue/PubDate")
    if pub_date is None:
        return None
    medline_date = _get_text(pub_date, "MedlineDate")
    if medline_date:
        return medline_date
    date_parts = [
        part for part in (_get_text(pub_date, "Year"), _get_text(pub_date, "Month"), _get_text(pub_date, "Day"))
        if part
    ]
    return " ".join(date_parts) if date_parts else None


def get_pubmed_metadata(pmid: str) -> dict:
    """
    Fetch publication metadata for a PubMed ID from NCBI E-utilities, mapped to DUOS publication fields.

    **Args:**
    - pmid (str): The PubMed ID to look up.

    **Returns:**
    - dict: Publication metadata with keys `pubmedId`, `journal`, `title`, `url`, `publishedDate`, `doi`,
        `authors` (list of `{"name": ...}`), and `tags` (MeSH terms).
    """
    response = _get(f"{PUBMED_EFETCH_URL}?db=pubmed&id={pmid}&retmode=xml")
    root = ET.fromstring(response.text)
    medline_citation = root.find(".//PubmedArticle/MedlineCitation")
    if medline_citation is None:
        raise ValueError(f"No PubMed record found for PMID {pmid}")
    article = medline_citation.find("Article")
    if article is None:
        raise ValueError(f"No PubMed record found for PMID {pmid}")

    doi = None
    for elocation_id in article.findall("ELocationID"):
        if elocation_id.get("EIdType") == "doi":
            doi = elocation_id.text
            break

    authors = []
    for author in article.findall("AuthorList/Author"):
        last_name = _get_text(author, "LastName")
        fore_name = _get_text(author, "ForeName")
        if last_name:
            authors.append({"name": f"{fore_name} {last_name}" if fore_name else last_name})
        else:
            collective_name = _get_text(author, "CollectiveName")
            if collective_name:
                authors.append({"name": collective_name})

    tags = [
        descriptor.text.strip()
        for descriptor in medline_citation.findall("MeshHeadingList/MeshHeading/DescriptorName")
        if descriptor.text
    ]

    return {
        "pubmedId": pmid,
        "journal": _get_text(article, "Journal/Title"),
        "title": _get_text(article, "ArticleTitle"),
        "url": f"{PUBMED_ARTICLE_URL}/{pmid}/",
        "publishedDate": _get_published_date(article),
        "doi": doi,
        "authors": authors,
        "tags": tags,
    }
