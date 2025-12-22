import pandas as pd
import requests
from bs4 import BeautifulSoup
from urllib.parse import urlparse
import re
import json
import xml.etree.ElementTree as ET
import os
import time

INPUT_CSV = os.getenv("INPUT_CSV", "Account Information - 2024.csv")
BASE_CSV = os.getenv("BASE_CSV", "jobs_scraped.csv")
OUTPUT_CSV = os.getenv("OUTPUT_CSV", "jobs_diff.csv")
ZERO_CSV = os.getenv("ZERO_CSV", "jobs_zero.csv")
MAX_JOBS_PER_COMPANY = int(os.getenv("MAX_JOBS_PER_COMPANY", "50"))
MAX_TOTAL_JOBS = 1000
HTTP_HEADERS = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36"}
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL")
N8N_WEBHOOK_TOKEN = os.getenv("N8N_WEBHOOK_TOKEN")

def get_run_date():
    return time.strftime("%Y-%m-%d")

def post_diff_json(df):
    if not (N8N_WEBHOOK_URL and N8N_WEBHOOK_TOKEN):
        print("Webhook not configured; skipping")
        return None
    payload = df.to_dict(orient="records")
    try:
        print(f"Webhook posting {len(payload)} records")
        r = requests.post(N8N_WEBHOOK_URL, timeout=20, headers={"Authorization": f"Bearer {N8N_WEBHOOK_TOKEN}", "Content-Type": "application/json"}, data=json.dumps(payload))
        print(f"Webhook response status={r.status_code}")
        return r.status_code
    except Exception as e:
        print(f"Webhook error: {e}")
        return None

def normalize_domain(url):
    if not isinstance(url, str) or not url.strip():
        return None
    url = url.strip()
    if url.startswith("No website:"):
        return None
    if not url.startswith("http"):
        url = "https://" + url
    parsed = urlparse(url)
    host = parsed.netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    return host

def infer_lever_company(domain):
    if not domain:
        return None
    return domain.split(".")[0]

def infer_greenhouse_board_token(domain):
    if not domain:
        return None
    return domain.split(".")[0]

def infer_workable_subdomain(domain):
    if not domain:
        return None
    return domain.split(".")[0]

def slugify_name(name):
    if not isinstance(name, str) or not name.strip():
        return None
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s or None

def generate_slug_variants(name, domain_first_label=None):
    base = slugify_name(name) or ""
    variants = []
    if domain_first_label:
        variants.append(domain_first_label)
    if base:
        variants.append(base)
        variants.extend([
            f"{base}-labs",
            f"{base}-foundation",
            f"{base}-protocol",
            f"{base}-network",
            f"{base}-team",
        ])
    # De-duplicate while preserving order
    seen = set()
    uniq = []
    for v in variants:
        if v and v not in seen:
            uniq.append(v)
            seen.add(v)
    return uniq

def fetch_lever_jobs(company_slug, max_jobs=10):
    if not company_slug:
        return []
    base = f"https://api.lever.co/v0/postings/{company_slug}"
    headers = {**HTTP_HEADERS, "Accept": "application/json"}
    try:
        resp = requests.get(base, timeout=10, headers=headers, params={"limit": max_jobs})
        url_used = base
        if resp.status_code != 200:
            alt = f"https://api.eu.lever.co/v0/postings/{company_slug}"
            resp = requests.get(alt, timeout=10, headers=headers, params={"limit": max_jobs})
            url_used = alt
            if resp.status_code != 200:
                return []
        data = resp.json()
        jobs = []
        for posting in data[:max_jobs]:
            loc_field = posting.get("categories", {}).get("location")
            if isinstance(loc_field, list):
                job_location = ", ".join([l.get("text", "") for l in loc_field])
            else:
                job_location = loc_field
            jobs.append({
                "job_title": posting.get("text"),
                "job_location": job_location,
                "job_type": posting.get("categories", {}).get("commitment"),
                "job_salary": None,
                "job_description_short": BeautifulSoup(posting.get("description", "") or "", "html.parser").get_text()[:500],
                "job_url": posting.get("hostedUrl") or posting.get("applyUrl"),
                "source_raw": url_used,
            })
        return jobs
    except Exception:
        return []

def fetch_lever_jobs_html(subdomain, max_jobs=10):
    if not subdomain:
        return []
    url = f"https://jobs.lever.co/{subdomain}"
    try:
        resp = requests.get(url, timeout=10, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        jobs = []
        # Common Lever markup: div.posting + a.posting-apply and a.posting-title
        for posting in soup.select("div.posting")[:max_jobs]:
            title_tag = posting.select_one("h5") or posting.select_one("a.posting-title")
            title = (title_tag.get_text(strip=True) if title_tag else None)
            loc = None
            loc_tag = posting.select_one("span.posting-location")
            if loc_tag:
                loc = loc_tag.get_text(strip=True)
            link_tag = posting.select_one("a[data-qa='posting-name']") or posting.select_one("a.posting-apply") or posting.select_one("a")
            href = link_tag.get("href") if link_tag else None
            job_url = (href if href and href.startswith("http") else (f"https://jobs.lever.co{subdomain}{href}" if href else None))
            if not job_url and href:
                job_url = f"https://jobs.lever.co/{subdomain}{href}"
            if title or job_url:
                jobs.append({
                    "job_title": title,
                    "job_location": loc,
                    "job_type": None,
                    "job_salary": None,
                    "job_description_short": "",
                    "job_url": job_url,
                    "source_raw": url,
                })
        # Fallback: list items with anchors
        if not jobs:
            for a in soup.select("a[href]"):
                href = a.get("href")
                text = a.get_text(strip=True)
                if href and (subdomain in href or href.startswith("/")) and text:
                    job_url = href if href.startswith("http") else f"https://jobs.lever.co/{subdomain}{href}"
                    jobs.append({
                        "job_title": text,
                        "job_location": None,
                        "job_type": None,
                        "job_salary": None,
                        "job_description_short": "",
                        "job_url": job_url,
                        "source_raw": url,
                    })
                    if len(jobs) >= max_jobs:
                        break
        return jobs
    except Exception:
        return []

def fetch_greenhouse_jobs(board_token, max_jobs=10):
    if not board_token:
        return []
    url = f"https://boards-api.greenhouse.io/v1/boards/{board_token}/jobs"
    try:
        resp = requests.get(url, timeout=10, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return []
        data = resp.json()
        jobs_data = data.get("jobs", [])
        jobs = []
        for job in jobs_data[:max_jobs]:
            jobs.append({
                "job_title": job.get("title"),
                "job_location": job.get("location", {}).get("name"),
                "job_type": None,
                "job_salary": None,
                "job_description_short": (job.get("content") or "")[:500],
                "job_url": job.get("absolute_url"),
                "source_raw": url,
            })
        return jobs
    except Exception:
        return []

def fetch_greenhouse_jobs_html(board_token, max_jobs=10):
    if not board_token:
        return []
    url = f"https://boards.greenhouse.io/{board_token}"
    try:
        resp = requests.get(url, timeout=10, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        jobs = []
        for opening in soup.select("div.opening a[href]")[:max_jobs]:
            title = opening.get_text(strip=True)
            href = opening.get("href")
            job_url = href if href.startswith("http") else f"https://boards.greenhouse.io{href}"
            # Location often adjacent in the DOM; try siblings or parent
            loc = None
            parent = opening.find_parent(class_="opening")
            if parent:
                loc_tag = parent.find(class_="location")
                if loc_tag:
                    loc = loc_tag.get_text(strip=True)
            jobs.append({
                "job_title": title,
                "job_location": loc,
                "job_type": None,
                "job_salary": None,
                "job_description_short": "",
                "job_url": job_url,
                "source_raw": url,
            })
        return jobs
    except Exception:
        return []

def fetch_workable_jobs(subdomain, max_jobs=10):
    if not subdomain:
        return []
    url = f"https://apply.workable.com/api/v1/widget/accounts/{subdomain}"
    try:
        resp = requests.get(url, timeout=10, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return []
        data = resp.json()
        jobs_data = data.get("jobs") or data
        if not isinstance(jobs_data, list):
            return []
        jobs = []
        for job in jobs_data[:max_jobs]:
            jobs.append({
                "job_title": job.get("title"),
                "job_location": job.get("city") or job.get("location"),
                "job_type": job.get("employment_type"),
                "job_salary": None,
                "job_description_short": (job.get("full_description") or job.get("description") or "")[:500],
                "job_url": job.get("url") or job.get("application_url"),
                "source_raw": url,
            })
        return jobs
    except Exception:
        return []

def fetch_ashby_jobs(org_slug, max_jobs=10):
    if not org_slug:
        return []
    url = f"https://api.ashbyhq.com/posting-api/job-board/{org_slug}?includeCompensation=true"
    try:
        resp = requests.get(url, timeout=10, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return []
        data = resp.json()
        jobs_data = data.get("jobs", [])
        jobs = []
        for job in jobs_data[:max_jobs]:
            jobs.append({
                "job_title": job.get("title"),
                "job_location": job.get("location"),
                "job_type": job.get("employmentType"),
                "job_salary": None,
                "job_description_short": (job.get("descriptionPlain") or "")[:500],
                "job_url": job.get("jobUrl") or job.get("applyUrl"),
                "source_raw": url,
            })
        return jobs
    except Exception:
        return []

def fetch_recruitee_jobs(subdomain, max_jobs=10):
    if not subdomain:
        return []
    url = f"https://{subdomain}.recruitee.com/api/offers/"
    try:
        resp = requests.get(url, timeout=10, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return []
        data = resp.json()
        offers = data.get("offers") or data.get("jobs") or []
        jobs = []
        for offer in offers[:max_jobs]:
            title = offer.get("title") or offer.get("name")
            location = None
            loc = offer.get("location") or offer.get("city")
            if isinstance(loc, dict):
                location = loc.get("city") or loc.get("location_str") or loc.get("country")
            else:
                location = loc
            job_url = offer.get("url")
            if not job_url:
                slug = offer.get("slug") or offer.get("id")
                if slug:
                    job_url = f"https://{subdomain}.recruitee.com/o/{slug}"
            jobs.append({
                "job_title": title,
                "job_location": location,
                "job_type": offer.get("kind") or offer.get("employment_type"),
                "job_salary": None,
                "job_description_short": (offer.get("description") or "")[:500],
                "job_url": job_url,
                "source_raw": url,
            })
        return jobs
    except Exception:
        return []

def fetch_personio_jobs(subdomain, max_jobs=10):
    if not subdomain:
        return []
    url = f"https://{subdomain}.jobs.personio.de/xml?language=en"
    try:
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            return []
        # Parse XML feed for positions
        root = ET.fromstring(resp.content)
        jobs = []
        # The XML structure uses <position> elements
        for pos in root.findall("position")[:max_jobs]:
            title = (pos.findtext("name") or "").strip()
            # Personio doesn't always include a single location; use office or city if present
            location = (pos.findtext("office") or pos.findtext("city") or pos.findtext("location") or "").strip()
            emp_type = (pos.findtext("employmentType") or pos.findtext("schedule") or None)
            job_id = pos.findtext("id")
            job_url = f"https://{subdomain}.jobs.personio.de/job/{job_id}" if job_id else None
            description = (pos.findtext("description") or "")
            jobs.append({
                "job_title": title,
                "job_location": location,
                "job_type": emp_type,
                "job_salary": None,
                "job_description_short": description[:500],
                "job_url": job_url,
                "source_raw": url,
            })
        return jobs
    except Exception:
        return []

def discover_ats_from_website(domain):
    if not domain:
        return None, None
    candidates = [
        f"https://{domain}/careers",
        f"https://{domain}/jobs",
        f"https://{domain}/join-us",
        f"https://{domain}/careers/",
        f"https://{domain}/work-with-us",
        f"https://{domain}/open-roles",
        f"https://{domain}/about/careers",
    ]
    for url in candidates:
        try:
            resp = requests.get(url, timeout=8, headers=HTTP_HEADERS)
            if resp.status_code != 200:
                continue
            html = resp.text
            soup = BeautifulSoup(html, "html.parser")
            # Greenhouse embed or links
            m = re.search(r"boards\.greenhouse\.io/embed/job_board/js\?for=([a-zA-Z0-9_-]+)", html)
            if m:
                return "greenhouse", m.group(1)
            m = re.search(r"boards\.greenhouse\.io/([a-zA-Z0-9_-]+)/jobs", html)
            if m:
                return "greenhouse", m.group(1)
            # Lever links
            m = re.search(r"jobs\.lever\.co/([A-Za-z0-9_-]+)/?", html)
            if m:
                return "lever", m.group(1)
            m = re.search(r"jobs\.eu\.lever\.co/([A-Za-z0-9_-]+)", html)
            if m:
                return "lever", m.group(1)
            m = re.search(r"api\.lever\.co/v0/postings/([A-Za-z0-9_-]+)", html)
            if m:
                return "lever", m.group(1)
            # Workable
            m = re.search(r"apply\.workable\.com/([A-Za-z0-9_-]+)/?", html)
            if m:
                return "workable", m.group(1)
            m = re.search(r'workable\.com/api/[^"]*accounts/([A-Za-z0-9_-]+)', html)
            if m:
                return "workable", m.group(1)
            # Ashby
            m = re.search(r"jobs\.ashbyhq\.com/([A-Za-z0-9_-]+)/?", html)
            if m:
                return "ashby", m.group(1)
            m = re.search(r"api\.ashbyhq\.com/posting-api/job-board/([A-Za-z0-9_-]+)", html)
            if m:
                return "ashby", m.group(1)
            # Recruitee
            m = re.search(r"([A-Za-z0-9_-]+)\.recruitee\.com", html)
            if m:
                return "recruitee", m.group(1)
            # Personio
            m = re.search(r"([A-Za-z0-9_-]+)\.jobs\.personio\.(de|com)", html)
            if m:
                return "personio", m.group(1)
            # Breezy
            m = re.search(r"([A-Za-z0-9_-]+)\.breezy\.hr", html)
            if m:
                return "breezy", m.group(1)
            # Workday
            m = re.search(r"https://([A-Za-z0-9.-]+)\.myworkdayjobs\.com/[^\s\"']*?/([A-Za-z0-9_-]+)", html)
            if m:
                host = f"{m.group(1)}.myworkdayjobs.com"
                tenant = m.group(1).split(".")[0]
                site = m.group(2)
                return "workday", f"{host}|{tenant}|{site}"
            # BambooHR
            m = re.search(r"([A-Za-z0-9_-]+)\.bamboohr\.com", html)
            if m:
                return "bamboohr", m.group(1)
            # SmartRecruiters
            m = re.search(r"careers\.smartrecruiters\.com/([A-Za-z0-9_-]+)/?", html)
            if m:
                return "smartrecruiters", m.group(1)
            m = re.search(r"jobs\.smartrecruiters\.com/([A-Za-z0-9_-]+)/?", html)
            if m:
                return "smartrecruiters", m.group(1)
            # Deel job boards
            m = re.search(r"jobs\.deel\.com/job-boards/([A-Za-z0-9_-]+)/?", html)
            if m:
                return "deel", m.group(1)
            # Keka
            m = re.search(r"([A-Za-z0-9_-]+)\.keka\.com/careers", html)
            if m:
                return "keka", m.group(1)
            # Polymer
            m = re.search(r"jobs\.polymer\.co/([A-Za-z0-9_-]+)/?", html)
            if m:
                return "polymer", m.group(1)
            for tag in soup.find_all(["a", "script", "iframe"]):
                val = tag.get("href") or tag.get("src") or tag.get("data-src")
                if not val:
                    continue
                gh = re.search(r"boards\.greenhouse\.io/(?:embed/job_board/js\?for=)?([a-zA-Z0-9_-]+)", val)
                if gh:
                    return "greenhouse", gh.group(1)
                lv = re.search(r"jobs\.(?:eu\.)?lever\.co/([A-Za-z0-9_-]+)", val)
                if lv:
                    return "lever", lv.group(1)
                wk = re.search(r"apply\.workable\.com/([A-Za-z0-9_-]+)", val)
                if wk:
                    return "workable", wk.group(1)
                asb = re.search(r"jobs\.ashbyhq\.com/([A-Za-z0-9_-]+)", val)
                if asb:
                    return "ashby", asb.group(1)
                rct = re.search(r"([A-Za-z0-9_-]+)\.recruitee\.com", val)
                if rct:
                    return "recruitee", rct.group(1)
                prs = re.search(r"([A-Za-z0-9_-]+)\.jobs\.personio\.(?:de|com)", val)
                if prs:
                    return "personio", prs.group(1)
                br = re.search(r"([A-Za-z0-9_-]+)\.breezy\.hr", val)
                    
                if br:
                    return "breezy", br.group(1)
                wd = re.search(r"([A-Za-z0-9.-]+)\.myworkdayjobs\.com/.+?/([A-Za-z0-9_-]+)", val)
                if wd:
                    host = f"{wd.group(1)}.myworkdayjobs.com"
                    tenant = wd.group(1).split(".")[0]
                    site = wd.group(2)
                    return "workday", f"{host}|{tenant}|{site}"
                sm = re.search(r"careers\.smartrecruiters\.com/([A-Za-z0-9_-]+)|jobs\.smartrecruiters\.com/([A-Za-z0-9_-]+)", val)
                if sm:
                    ident = sm.group(1) or sm.group(2)
                    return "smartrecruiters", ident
                wf = re.search(r"wellfound\.com/company/([A-Za-z0-9_-]+)/jobs", val)
                if wf:
                    return "wellfound", wf.group(1)
                dl = re.search(r"jobs\.deel\.com/job-boards/([A-Za-z0-9_-]+)", val)
                if dl:
                    return "deel", dl.group(1)
                kk = re.search(r"([A-Za-z0-9_-]+)\.keka\.com/careers", val)
                if kk:
                    return "keka", kk.group(1)
                pm = re.search(r"jobs\.polymer\.co/([A-Za-z0-9_-]+)", val)
                if pm:
                    return "polymer", pm.group(1)
        except Exception:
            continue
    return None, None

def fetch_workday_jobs(host, tenant, site, max_jobs=10):
    if not host or not tenant or not site:
        return []
    url = f"https://{host}/wday/cxs/{tenant}/{site}/jobs"
    body = {"appliedFacets": {}, "limit": max_jobs, "offset": 0, "searchText": ""}
    headers = {"Accept": "application/json", "Content-Type": "application/json", "User-Agent": HTTP_HEADERS["User-Agent"]}
    try:
        resp = requests.post(url, json=body, headers=headers, timeout=12)
        if resp.status_code != 200:
            return []
        data = resp.json()
        postings = data.get("jobPostings") or []
        jobs = []
        for p in postings[:max_jobs]:
            title = p.get("title")
            loc = p.get("locationsText") or p.get("location")
            job_url = p.get("externalUrl") or p.get("absoluteJobUrl")
            jobs.append({
                "job_title": title,
                "job_location": loc,
                "job_type": None,
                "job_salary": None,
                "job_description_short": (p.get("description") or "")[:500],
                "job_url": job_url,
                "source_raw": url,
            })
        return jobs
    except Exception:
        return []

def fetch_bamboohr_jobs(subdomain, max_jobs=10):
    if not subdomain:
        return []
    url = f"https://{subdomain}.bamboohr.com/jobs/list"
    try:
        resp = requests.get(url, timeout=10, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return []
        data = resp.json()
        jobs = []
        for job in (data if isinstance(data, list) else [])[:max_jobs]:
            jobs.append({
                "job_title": job.get("jobTitle") or job.get("title"),
                "job_location": job.get("location"),
                "job_type": job.get("department") or None,
                "job_salary": None,
                "job_description_short": (job.get("description") or "")[:500],
                "job_url": job.get("link") or job.get("applyUrl"),
                "source_raw": url,
            })
        return jobs
    except Exception:
        return []

def fetch_breezy_jobs(subdomain, max_jobs=10):
    if not subdomain:
        return []

def fetch_wellfound_jobs_html(slug, max_jobs=10):
    if not slug:
        return []
    url = f"https://wellfound.com/company/{slug}/jobs"
    try:
        resp = requests.get(url, timeout=10, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        jobs = []
        seen = set()
        for a in soup.select("a[href]"):
            href = a.get("href")
            text = a.get_text(strip=True)
            if not href or not text:
                continue
            if "/jobs" not in href:
                continue
            if slug not in href:
                continue
            job_url = href if href.startswith("http") else f"https://wellfound.com{href}"
            if job_url in seen:
                continue
            seen.add(job_url)
            jobs.append({
                "job_title": text,
                "job_location": None,
                "job_type": None,
                "job_salary": None,
                "job_description_short": "",
                "job_url": job_url,
                "source_raw": url,
            })
            if len(jobs) >= max_jobs:
                break
        return jobs
    except Exception:
        return []
    url = f"https://{subdomain}.breezy.hr/"
    try:
        resp = requests.get(url, timeout=10, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        jobs = []
        # Typical Breezy positions are anchors linking to /p/{slug}
        for a in soup.select("a[href*='/p/']")[:max_jobs]:
            title = a.get_text(strip=True)
            href = a.get("href")
            job_url = href if href.startswith("http") else f"https://{subdomain}.breezy.hr{href}"
            # Attempt to find nearby location metadata
            loc = None
            parent = a.find_parent()
            if parent:
                loc_tag = parent.find(attrs={"class": re.compile(r"location|city|office", re.I)})
                if loc_tag:
                    loc = loc_tag.get_text(strip=True)
            jobs.append({
                "job_title": title or None,
                "job_location": loc,
                "job_type": None,
                "job_salary": None,
                "job_description_short": "",
                "job_url": job_url,
                "source_raw": url,
            })
        return jobs
    except Exception:
        return []

def fetch_smartrecruiters_jobs(identifier, max_jobs=10):
    if not identifier:
        return []
    url = f"https://api.smartrecruiters.com/v1/companies/{identifier}/postings?limit={max_jobs}"
    try:
        resp = requests.get(url, timeout=12, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return []
        data = resp.json()
        content = data.get("content") or []
        jobs = []
        for p in content[:max_jobs]:
            loc = p.get("location") or {}
            city = loc.get("city")
            region = loc.get("region")
            country = loc.get("country")
            parts = [x for x in [city, region, country] if x]
            job_url = p.get("ref") or p.get("applyUrl")
            jobs.append({
                "job_title": p.get("name"),
                "job_location": ", ".join(parts) if parts else None,
                "job_type": p.get("typeOfEmployment", {}).get("label"),
                "job_salary": None,
                "job_description_short": "",
                "job_url": job_url,
                "source_raw": url,
            })
        return jobs
    except Exception:
        return []

def fetch_keka_jobs(subdomain, max_jobs=10):
    if not subdomain:
        return []
    base = f"https://{subdomain}.keka.com"
    urls = [f"{base}/careers", f"{base}/careers/"]
    for url in urls:
        try:
            resp = requests.get(url, timeout=10, headers=HTTP_HEADERS)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            jobs = []
            seen = set()
            keka_links = soup.select("a[href*='/careers/jobdetails/'], a[href]")
            for a in keka_links:
                href = a.get("href")
                text = a.get_text(strip=True)
                if not href or not text:
                    continue
                if not ("/careers/jobdetails/" in href or "/job" in href or "/careers/" in href or href.startswith("/") or href.startswith("http")):
                    continue
                job_url = href if href.startswith("http") else f"{base}{href}"
                if job_url in seen:
                    continue
                seen.add(job_url)
                loc = None
                typ = None
                parent = a.find_parent(["li", "article", "div"]) or a.parent
                if parent:
                    block_text = parent.get_text(" ", strip=True)
                    mloc = re.search(r"Location[:\s]+([^|\n]+)", block_text, re.I)
                    if mloc:
                        loc = mloc.group(1).strip()
                    mtyp = re.search(r"(Employment Type|Type|Department)[:\s]+([^|\n]+)", block_text, re.I)
                    if mtyp:
                        typ = mtyp.group(2).strip()
                jobs.append({
                    "job_title": text,
                    "job_location": loc,
                    "job_type": typ,
                    "job_salary": None,
                    "job_description_short": "",
                    "job_url": job_url,
                    "source_raw": url,
                })
                if len(jobs) >= max_jobs:
                    break
            if jobs:
                return jobs
        except Exception:
            continue
    return []

def fetch_deel_jobs(slug, max_jobs=10):
    if not slug:
        return []
    urls = [
        f"https://jobs.deel.com/job-boards/{slug}/",
        f"https://jobs.deel.com/job-boards/{slug}",
    ]
    for url in urls:
        try:
            resp = requests.get(url, timeout=10, headers=HTTP_HEADERS)
            if resp.status_code != 200:
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            jobs = []
            seen = set()
            for a in soup.select("a[href]"):
                href = a.get("href")
                text = a.get_text(strip=True)
                if not href or not text:
                    continue
                if "/job-boards/" in href:
                    continue
                if not ("/jobs" in href or "/job" in href or "/positions" in href or "/careers" in href or href.startswith("/")):
                    continue
                job_url = href if href.startswith("http") else f"https://jobs.deel.com{href}"
                if job_url in seen:
                    continue
                seen.add(job_url)
                title = text
                loc = None
                typ = None
                card = a.find_parent(["li", "article", "div"]) or a.parent
                if card:
                    bt = card.get_text(" ", strip=True)
                    mloc = re.search(r"Location[:\s]+([^|\n]+)", bt, re.I)
                    if mloc:
                        loc = mloc.group(1).strip()
                    mtyp = re.search(r"(Employment Type|Type|Department)[:\s]+([^|\n]+)", bt, re.I)
                    if mtyp:
                        typ = mtyp.group(2).strip()
                    if not title:
                        ht = card.find(["h1","h2","h3"]) 
                        if ht:
                            title = ht.get_text(strip=True)
                jobs.append({
                    "job_title": title,
                    "job_location": loc,
                    "job_type": typ,
                    "job_salary": None,
                    "job_description_short": "",
                    "job_url": job_url,
                    "source_raw": url,
                })
                if len(jobs) >= max_jobs:
                    break
            if jobs:
                return jobs
        except Exception:
            continue
    return []

def fetch_polymer_jobs(slug, max_jobs=10):
    if not slug:
        return []
    api = f"https://api.polymer.co/v1/hire/organizations/{slug}/jobs"
    try:
        resp = requests.get(api, timeout=10, headers={"Accept": "application/json", "User-Agent": HTTP_HEADERS["User-Agent"]})
        if resp.status_code != 200:
            return []
        data = resp.json()
        items = data.get("items") or []
        jobs = []
        for it in items[:max_jobs]:
            jobs.append({
                "job_title": it.get("title"),
                "job_location": it.get("display_location") or it.get("remoteness_pretty"),
                "job_type": it.get("kind_pretty"),
                "job_salary": None,
                "job_description_short": "",
                "job_url": it.get("job_post_url") or it.get("job_application_description_url"),
                "source_raw": api,
            })
        return jobs
    except Exception:
        return []

def main():
    start = time.perf_counter()
    run_date = get_run_date()
    df = pd.read_csv(INPUT_CSV)
    only_companies = os.getenv("ONLY_COMPANIES")
    only_ats = os.getenv("ONLY_ATS")
    if only_companies:
        names = [x.strip() for x in only_companies.split(",") if x.strip()]
        if names:
            df = df[df["Company Name"].astype(str).isin(names)]
    if only_ats:
        ats_list = [x.strip().lower() for x in only_ats.split(",") if x.strip()]
        if ats_list:
            df = df[df["ATS used"].astype(str).str.lower().isin(ats_list)]
    all_rows = []
    total_jobs = 0
    zero_rows = []
    lever_overrides = {
        "aave": "aavelabs",
        "aave labs": "aavelabs",
        "aragon": "aragon",
        "wintermute": "wintermute-trading",
        "wintermute trading": "wintermute-trading",
        "seilabs": "SeiLabs",
        "sei labs": "SeiLabs",
        "zerion": "zerion",
        "crypto.com": "cryptocom",
        "bebop": "wintermute-trading",
        "bebob": "wintermute-trading",
    }
    company_overrides = {
        "chainlink": ("ashby", "chainlink-labs"),
        "chainlink labs": ("ashby", "chainlink-labs"),
        "aave": ("lever", "aavelabs"),
        "aave labs": ("lever", "aavelabs"),
        "optimism": ("ashby", "opfoundation"),
        "op labs": ("ashby", "opfoundation"),
        "bebop": ("lever", "wintermute-trading"),
        "bebob": ("lever", "wintermute-trading"),
        # add more verified overrides here as we confirm them
    }
    for _, row in df.iterrows():
        if total_jobs >= MAX_TOTAL_JOBS:
            break
        company = str(row.get("Company Name", "")).strip()
        website = str(row.get("Company Website", "")).strip()
        lc = str(row.get("Lead Contact (LC)", "")).strip() if not pd.isna(row.get("Lead Contact (LC)")) else ""
        ats = str(row.get("ATS used", "")).strip() if not pd.isna(row.get("ATS used")) else ""
        ats_clean = ats.strip().lower()
        override = company_overrides.get(company.lower())
        override_token = None
        if override:
            ats_clean, override_token = override[0], override[1]
            ats = override[0].capitalize()
        domain = normalize_domain(website)
        jobs = []
        attempt_links = []
        if ats_clean == "lever":
            lever_slug = override_token or infer_lever_company(domain)
            if company:
                key = company.lower()
                if key in lever_overrides:
                    lever_slug = lever_overrides[key]
            if lever_slug:
                attempt_links.append(f"https://api.lever.co/v0/postings/{lever_slug}")
            jobs = fetch_lever_jobs(lever_slug, MAX_JOBS_PER_COMPANY)
            if not jobs:
                for cand in generate_slug_variants(company, domain.split(".")[0] if domain else None):
                    attempt_links.append(f"https://api.lever.co/v0/postings/{cand}")
                    jobs = fetch_lever_jobs(cand, MAX_JOBS_PER_COMPANY)
                    if jobs:
                        break
            if not jobs:
                if lever_slug:
                    attempt_links.append(f"https://jobs.lever.co/{lever_slug}")
                jobs = fetch_lever_jobs_html(lever_slug, MAX_JOBS_PER_COMPANY)
        elif ats_clean == "greenhouse":
            gh_token = override_token or infer_greenhouse_board_token(domain)
            if gh_token:
                attempt_links.append(f"https://boards-api.greenhouse.io/v1/boards/{gh_token}/jobs")
            jobs = fetch_greenhouse_jobs(gh_token, MAX_JOBS_PER_COMPANY)
            if not jobs:
                for cand in generate_slug_variants(company, domain.split(".")[0] if domain else None):
                    attempt_links.append(f"https://boards-api.greenhouse.io/v1/boards/{cand}/jobs")
                    jobs = fetch_greenhouse_jobs(cand, MAX_JOBS_PER_COMPANY)
                    if jobs:
                        break
            if not jobs:
                if gh_token:
                    attempt_links.append(f"https://boards.greenhouse.io/{gh_token}")
                jobs = fetch_greenhouse_jobs_html(gh_token, MAX_JOBS_PER_COMPANY)
        elif ats_clean == "workable":
            subdomain = override_token or infer_workable_subdomain(domain)
            if subdomain:
                attempt_links.append(f"https://apply.workable.com/api/v1/widget/accounts/{subdomain}")
            jobs = fetch_workable_jobs(subdomain, MAX_JOBS_PER_COMPANY)
            if not jobs:
                for cand in generate_slug_variants(company, domain.split(".")[0] if domain else None):
                    attempt_links.append(f"https://apply.workable.com/api/v1/widget/accounts/{cand}")
                    jobs = fetch_workable_jobs(cand, MAX_JOBS_PER_COMPANY)
                    if jobs:
                        break
        elif ats_clean == "ashby":
            org = override_token or (domain.split(".")[0] if domain else None)
            if org:
                attempt_links.append(f"https://api.ashbyhq.com/posting-api/job-board/{org}?includeCompensation=true")
            jobs = fetch_ashby_jobs(org, MAX_JOBS_PER_COMPANY)
            if not jobs:
                for cand in generate_slug_variants(company, domain.split(".")[0] if domain else None):
                    attempt_links.append(f"https://api.ashbyhq.com/posting-api/job-board/{cand}?includeCompensation=true")
                    jobs = fetch_ashby_jobs(cand, MAX_JOBS_PER_COMPANY)
                    if jobs:
                        break
        elif ats_clean == "recruitee":
            subdomain = override_token or (domain.split(".")[0] if domain else None)
            if subdomain:
                attempt_links.append(f"https://{subdomain}.recruitee.com/api/offers/")
            jobs = fetch_recruitee_jobs(subdomain, MAX_JOBS_PER_COMPANY)
            if not jobs:
                for cand in generate_slug_variants(company, domain.split(".")[0] if domain else None):
                    attempt_links.append(f"https://{cand}.recruitee.com/api/offers/")
                    jobs = fetch_recruitee_jobs(cand, MAX_JOBS_PER_COMPANY)
                    if jobs:
                        break
        elif ats_clean == "personio":
            subdomain = override_token or (domain.split(".")[0] if domain else None)
            if subdomain:
                attempt_links.append(f"https://{subdomain}.jobs.personio.de/xml?language=en")
            jobs = fetch_personio_jobs(subdomain, MAX_JOBS_PER_COMPANY)
            if not jobs:
                for cand in generate_slug_variants(company, domain.split(".")[0] if domain else None):
                    attempt_links.append(f"https://{cand}.jobs.personio.de/xml?language=en")
                    jobs = fetch_personio_jobs(cand, MAX_JOBS_PER_COMPANY)
                    if jobs:
                        break
        elif ats_clean == "breezy":
            subdomain = override_token or (domain.split(".")[0] if domain else None)
            if subdomain:
                attempt_links.append(f"https://{subdomain}.breezy.hr/")
            jobs = fetch_breezy_jobs(subdomain, MAX_JOBS_PER_COMPANY)
            if not jobs:
                for cand in generate_slug_variants(company, domain.split(".")[0] if domain else None):
                    attempt_links.append(f"https://{cand}.breezy.hr/")
                    jobs = fetch_breezy_jobs(cand, MAX_JOBS_PER_COMPANY)
                    if jobs:
                        break
        elif ats_clean == "wellfound":
            slug = override_token or (domain.split(".")[0] if domain else slugify_name(company))
            if slug:
                attempt_links.append(f"https://wellfound.com/company/{slug}/jobs")
            jobs = fetch_wellfound_jobs_html(slug, MAX_JOBS_PER_COMPANY)
            if not jobs:
                for cand in generate_slug_variants(company, domain.split(".")[0] if domain else None):
                    attempt_links.append(f"https://wellfound.com/company/{cand}/jobs")
                    jobs = fetch_wellfound_jobs_html(cand, MAX_JOBS_PER_COMPANY)
                    if jobs:
                        break
        elif ats_clean == "keka":
            subdomain = override_token or (domain.split(".")[0] if domain else None)
            if subdomain:
                attempt_links.append(f"https://{subdomain}.keka.com/careers")
            jobs = fetch_keka_jobs(subdomain, MAX_JOBS_PER_COMPANY)
            if not jobs:
                for cand in generate_slug_variants(company, domain.split(".")[0] if domain else None):
                    attempt_links.append(f"https://{cand}.keka.com/careers")
                    jobs = fetch_keka_jobs(cand, MAX_JOBS_PER_COMPANY)
                    if jobs:
                        break
        elif ats_clean == "deel":
            slug = override_token or (domain.split(".")[0] if domain else slugify_name(company))
            if slug:
                attempt_links.append(f"https://jobs.deel.com/job-boards/{slug}/")
            jobs = fetch_deel_jobs(slug, MAX_JOBS_PER_COMPANY)
            if not jobs:
                for cand in generate_slug_variants(company, domain.split(".")[0] if domain else None):
                    attempt_links.append(f"https://jobs.deel.com/job-boards/{cand}/")
                    jobs = fetch_deel_jobs(cand, MAX_JOBS_PER_COMPANY)
                    if jobs:
                        break
        elif ats_clean == "polymer":
            slug = override_token or (domain.split(".")[0] if domain else slugify_name(company))
            if slug:
                attempt_links.append(f"https://api.polymer.co/v1/hire/organizations/{slug}/jobs")
            jobs = fetch_polymer_jobs(slug, MAX_JOBS_PER_COMPANY)
            if not jobs:
                for cand in generate_slug_variants(company, domain.split(".")[0] if domain else None):
                    attempt_links.append(f"https://api.polymer.co/v1/hire/organizations/{cand}/jobs")
                    jobs = fetch_polymer_jobs(cand, MAX_JOBS_PER_COMPANY)
                    if jobs:
                        break
        else:
            jobs = []
        if not jobs:
            # Attempt discovery from careers page markup for better slugs/tokens
            discovered_ats, discovered_token = discover_ats_from_website(domain)
            if discovered_ats == "greenhouse":
                if discovered_token:
                    attempt_links.append(f"https://boards-api.greenhouse.io/v1/boards/{discovered_token}/jobs")
                jobs = fetch_greenhouse_jobs(discovered_token, MAX_JOBS_PER_COMPANY)
                ats = "Greenhouse"
            elif discovered_ats == "lever":
                if discovered_token:
                    attempt_links.append(f"https://api.lever.co/v0/postings/{discovered_token}")
                jobs = fetch_lever_jobs(discovered_token, MAX_JOBS_PER_COMPANY)
                ats = "Lever"
            elif discovered_ats == "workable":
                if discovered_token:
                    attempt_links.append(f"https://apply.workable.com/api/v1/widget/accounts/{discovered_token}")
                jobs = fetch_workable_jobs(discovered_token, MAX_JOBS_PER_COMPANY)
                ats = "Workable"
            elif discovered_ats == "ashby":
                if discovered_token:
                    attempt_links.append(f"https://api.ashbyhq.com/posting-api/job-board/{discovered_token}?includeCompensation=true")
                jobs = fetch_ashby_jobs(discovered_token, MAX_JOBS_PER_COMPANY)
                ats = "Ashby"
            elif discovered_ats == "recruitee":
                if discovered_token:
                    attempt_links.append(f"https://{discovered_token}.recruitee.com/api/offers/")
                jobs = fetch_recruitee_jobs(discovered_token, MAX_JOBS_PER_COMPANY)
                ats = "Recruitee"
            elif discovered_ats == "personio":
                if discovered_token:
                    attempt_links.append(f"https://{discovered_token}.jobs.personio.de/xml?language=en")
                jobs = fetch_personio_jobs(discovered_token, MAX_JOBS_PER_COMPANY)
                ats = "Personio"
            elif discovered_ats == "workday":
                host, tenant, site = (discovered_token.split("|") + [None, None, None])[:3]
                if host and tenant and site:
                    attempt_links.append(f"https://{host}/wday/cxs/{tenant}/{site}/jobs")
                jobs = fetch_workday_jobs(host, tenant, site, MAX_JOBS_PER_COMPANY)
                ats = "Workday"
            elif discovered_ats == "bamboohr":
                if discovered_token:
                    attempt_links.append(f"https://{discovered_token}.bamboohr.com/jobs/list")
                jobs = fetch_bamboohr_jobs(discovered_token, MAX_JOBS_PER_COMPANY)
                ats = "BambooHR"
            elif discovered_ats == "smartrecruiters":
                if discovered_token:
                    attempt_links.append(f"https://api.smartrecruiters.com/v1/companies/{discovered_token}/postings?limit={MAX_JOBS_PER_COMPANY}")
                jobs = fetch_smartrecruiters_jobs(discovered_token, MAX_JOBS_PER_COMPANY)
                ats = "SmartRecruiters"
            elif discovered_ats == "breezy":
                if discovered_token:
                    attempt_links.append(f"https://{discovered_token}.breezy.hr/")
                jobs = fetch_breezy_jobs(discovered_token, MAX_JOBS_PER_COMPANY)
                ats = "Breezy"
            elif discovered_ats == "keka":
                if discovered_token:
                    attempt_links.append(f"https://{discovered_token}.keka.com/careers")
                jobs = fetch_keka_jobs(discovered_token, MAX_JOBS_PER_COMPANY)
                ats = "Keka"
            elif discovered_ats == "deel":
                if discovered_token:
                    attempt_links.append(f"https://jobs.deel.com/job-boards/{discovered_token}/")
                jobs = fetch_deel_jobs(discovered_token, MAX_JOBS_PER_COMPANY)
                ats = "Deel"
            elif discovered_ats == "polymer":
                if discovered_token:
                    attempt_links.append(f"https://api.polymer.co/v1/hire/organizations/{discovered_token}/jobs")
                jobs = fetch_polymer_jobs(discovered_token, MAX_JOBS_PER_COMPANY)
                ats = "Polymer"
            else:
                page_url = find_careers_page(domain)
                jobs = fetch_jsonld_jobs(page_url, MAX_JOBS_PER_COMPANY) if page_url else []
                if not jobs and page_url:
                    jobs = fetch_dom_jobs(page_url, MAX_JOBS_PER_COMPANY)
                if page_url and not jobs:
                    attempt_links.append(page_url)
                if jobs:
                    ats = ats or "HTML"
        if not jobs:
            unique_attempts = list(dict.fromkeys(attempt_links))
            zero_rows.append({
                "company": company,
                "ats": ats or "unknown",
                "scrapped_link": "; ".join(unique_attempts) if unique_attempts else (website or None),
            })
            if unique_attempts:
                print(f"Attempted links: {'; '.join(unique_attempts)}")
        print(f"Scraped {len(jobs)} jobs for {company} via {ats or 'unknown'}")
        for job in jobs:
            if total_jobs >= MAX_TOTAL_JOBS:
                break
            loc_val = job.get("job_location")
            if not (loc_val and str(loc_val).strip()):
                continue
            all_rows.append({
                "company_name": company,
                "company_website": website,
                "ats": ats,
                "job_title": job.get("job_title"),
                "job_location": loc_val,
                "job_type": job.get("job_type"),
                "job_salary": job.get("job_salary"),
                "job_description_short": job.get("job_description_short"),
                "job_contact_person": lc or None,
                "job_contact_email": None,
                "job_url": job.get("job_url"),
                "source_raw": job.get("source_raw"),
                "date": run_date,
            })
            total_jobs += 1
    out_cols = [
        "company_name",
        "company_website",
        "ats",
        "job_title",
        "job_location",
        "job_type",
        "job_salary",
        "job_description_short",
        "job_contact_person",
        "job_contact_email",
        "job_url",
        "source_raw",
        "date",
    ]
    out_df = pd.DataFrame(all_rows)
    out_df["date"] = run_date
    if not out_df.empty:
        out_df["key"] = (
            out_df["company_name"].fillna("").str.lower()
            + "|"
            + out_df["job_url"].fillna("").str.lower()
            + "|"
            + out_df["job_title"].fillna("").str.lower()
            + "|"
            + out_df["job_location"].fillna("").str.lower()
        )
    else:
        out_df["key"] = pd.Series(dtype=str)
    try:
        base_df = pd.read_csv(BASE_CSV)
    except Exception:
        base_df = pd.DataFrame(columns=out_cols)
    if not base_df.empty:
        if "key" not in base_df.columns:
            base_df["key"] = (
                base_df["company_name"].fillna("").str.lower()
                + "|"
                + base_df["job_url"].fillna("").str.lower()
                + "|"
                + base_df["job_title"].fillna("").str.lower()
                + "|"
                + base_df["job_location"].fillna("").str.lower()
            )
    else:
        base_df["key"] = pd.Series(dtype=str)
    base_keys = set(base_df["key"].tolist())
    diff_df = out_df[~out_df["key"].isin(base_keys)].copy()
    diff_df.drop(columns=["key"], inplace=True, errors="ignore")
    updated_base = pd.concat([base_df, out_df], ignore_index=True)
    updated_base = updated_base.drop_duplicates(subset=["key"])
    updated_base = updated_base.drop(columns=["key"])
    diff_df.to_csv(OUTPUT_CSV, index=False)
    updated_base.to_csv(BASE_CSV, index=False)
    status = post_diff_json(diff_df)
    if status is not None:
        print(f"Webhook status: {status}")
    zero_df = pd.DataFrame(zero_rows, columns=["company", "ats", "scrapped_link"])
    zero_df.to_csv(ZERO_CSV, index=False)
    print(f"Wrote {len(zero_df)} zero-job rows to {ZERO_CSV}")
    print(f"Wrote {len(diff_df)} new rows to {OUTPUT_CSV}")
    print(f"Updated base to {len(updated_base)} rows at {BASE_CSV}")
    end = time.perf_counter()
    print(f"Total runtime: {end - start:.2f}s")

 
def find_careers_page(domain):
    if not domain:
        return None
    candidates = [
        f"https://{domain}/careers",
        f"https://{domain}/jobs",
        f"https://{domain}/join-us",
        f"https://{domain}/careers/",
        f"https://{domain}/work-with-us",
        f"https://{domain}/open-roles",
        f"https://{domain}/about/careers",
    ]
    for url in candidates:
        try:
            r = requests.get(url, timeout=8, headers=HTTP_HEADERS)
            if r.status_code == 200:
                return url
        except Exception:
            continue
    return None

def fetch_dom_jobs(page_url, max_jobs=10):
    if not page_url:
        return []
    try:
        resp = requests.get(page_url, timeout=10, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        jobs = []
        seen_urls = set()
        containers = []
        containers.extend(soup.select("[class*='job'], [id*='job']"))
        containers.extend(soup.select("[class*='opening'], [id*='opening']"))
        containers.extend(soup.select("[class*='position'], [id*='position']"))
        containers.extend(soup.select("[class*='career'], [id*='career']"))
        containers.extend(soup.select("[class*='opportunity'], [id*='opportunity']"))
        containers.extend(soup.select("[class*='vacancy'], [id*='vacancy']"))
        containers.extend(soup.select("[class*='role'], [id*='role']"))
        anchors = []
        for c in containers:
            anchors.extend(c.select("a[href]"))
        if not anchors:
            anchors = soup.select("a[href]")
        for a in anchors:
            href = a.get("href")
            text = a.get_text(strip=True)
            if not href or not text:
                continue
            if len(text) < 2:
                continue
            if not ("job" in href or "career" in href or "join" in href or "position" in href or href.startswith("/")):
                continue
            if href.startswith("http"):
                job_url = href
            else:
                parsed = urlparse(page_url)
                base = f"{parsed.scheme}://{parsed.netloc}"
                job_url = base + href
            if job_url in seen_urls:
                continue
            seen_urls.add(job_url)
            loc = None
            parent = a.find_parent()
            if parent:
                loc_tag = parent.find(attrs={"class": re.compile(r"location", re.I)}) or parent.find(attrs={"data-location": True})
                if loc_tag:
                    loc = loc_tag.get_text(strip=True) if hasattr(loc_tag, "get_text") else loc_tag
            if not loc:
                m = re.search(r"\b(Remote|Hybrid|Onsite|On-site)\b", parent.get_text(" ", strip=True) if parent else "")
                if m:
                    loc = m.group(1)
            jobs.append({
                "job_title": text,
                "job_location": loc,
                "job_type": None,
                "job_salary": None,
                "job_description_short": "",
                "job_url": job_url,
                "source_raw": page_url,
            })
            if len(jobs) >= max_jobs:
                break
        return jobs
    except Exception:
        return []

def fetch_jsonld_jobs(page_url, max_jobs=10):
    if not page_url:
        return []
    try:
        resp = requests.get(page_url, timeout=10, headers=HTTP_HEADERS)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.text, "html.parser")
        jobs = []
        scripts = soup.find_all("script", attrs={"type": "application/ld+json"})
        for s in scripts:
            try:
                data = json.loads(s.string or "")
            except Exception:
                continue
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict) and (item.get("@type") == "JobPosting" or item.get("@type") == "jobPosting"):
                    title = item.get("title")
                    loc = None
                    jl = item.get("jobLocation")
                    if isinstance(jl, list) and jl:
                        addr = jl[0].get("address") or {}
                        loc = ", ".join([x for x in [addr.get("addressLocality"), addr.get("addressRegion"), addr.get("addressCountry")] if x])
                    elif isinstance(jl, dict):
                        addr = jl.get("address") or {}
                        loc = ", ".join([x for x in [addr.get("addressLocality"), addr.get("addressRegion"), addr.get("addressCountry")] if x])
                    emp = item.get("employmentType")
                    url = item.get("url") or page_url
                    desc = item.get("description") or ""
                    jobs.append({
                        "job_title": title,
                        "job_location": loc,
                        "job_type": emp,
                        "job_salary": None,
                        "job_description_short": BeautifulSoup(desc, "html.parser").get_text()[:500],
                        "job_url": url,
                        "source_raw": page_url,
                    })
                    if len(jobs) >= max_jobs:
                        break
            if len(jobs) >= max_jobs:
                break
        return jobs
    except Exception:
        return []

if __name__ == "__main__":
    main()
