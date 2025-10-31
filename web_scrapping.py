#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Berkeley faculty finder (crawl + extract + generate)
- Respeta robots.txt
- Crawling BFS controlado (profundidad + nº páginas)
- Extrae emails reales @berkeley.edu
- Heurísticas de nombres (encabezados, microdatos, títulos)
- Genera emails firstinitial+lastname@berkeley.edu (marcados como 'generated')
- Guarda en SQLite y exporta a CSV

Requisitos:
  pip install requests beautifulsoup4 tldextract unidecode pandas

Uso:
  python berkeley_faculty_scraper.py \
      --seeds https://www.berkeley.edu/ https://eecs.berkeley.edu/people/faculty https://vcresearch.berkeley.edu/faculty-expertise https://www.berkeley.edu/ https://www.berkeley.edu/academics/faculty\
      --max-pages 200 --max-depth 2 --delay 1.0 \
      --out-csv berkeley_faculty.csv --db berkeley_faculty.db
      
      https://vcresearch.berkeley.edu/faculty-expertise
      https://www.berkeley.edu/
      https://www.berkeley.edu/academics/faculty
"""

import argparse
import re
import time
import sqlite3
import hashlib
import tldextract
from urllib.parse import urljoin, urlparse
from urllib import robotparser

import requests
from bs4 import BeautifulSoup
from unidecode import unidecode
import pandas as pd

BERKELEY_DOMAIN = "berkeley.edu"
EMAIL_RE = re.compile(r'\b([A-Za-z0-9._%+\-]+@berkeley\.edu)\b', re.I)

# Heurísticos de nombres: encabezados, roles, microdatos
ROLE_HINTS = re.compile(
    r'\b(Professor|Assistant Professor|Associate Professor|Lecturer|Faculty|Staff|Researcher|Chair|Dean)\b',
    re.I
)

EXCLUDE_URL_PATTERNS = [
    re.compile(r'\.(pdf|jpg|jpeg|png|gif|svg|mp4|zip|pptx?|docx?)$', re.I),
    re.compile(r'/login|/signin|/calendar|/events|/news|/media|/files|/wp-json', re.I),
]

INCLUDE_URL_HINTS = [
    re.compile(r'faculty', re.I),
    re.compile(r'people', re.I),
    re.compile(r'directory', re.I),
    re.compile(r'staff', re.I),
    re.compile(r'department', re.I),
]

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--seeds", nargs="+", required=True,
                    help="URLs semilla bajo berkeley.edu (ej. https://eecs.berkeley.edu/people/faculty)")
    ap.add_argument("--max-pages", type=int, default=300)
    ap.add_argument("--max-depth", type=int, default=2)
    ap.add_argument("--delay", type=float, default=1.0, help="Segundos entre peticiones (rate limit)")
    ap.add_argument("--timeout", type=float, default=12.0)
    ap.add_argument("--out-csv", default="berkeley_faculty.csv")
    ap.add_argument("--db", default="berkeley_faculty.db")
    return ap.parse_args()

def same_registered_domain(url, target_domain):
    host = urlparse(url).netloc
    ext = tldextract.extract(host)
    reg = ".".join([p for p in [ext.domain, ext.suffix] if p])
    return reg.lower() == target_domain.lower()

def allowed_by_robots(url, robots_cache):
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    if base not in robots_cache:
        rp = robotparser.RobotFileParser()
        rp.set_url(urljoin(base, "/robots.txt"))
        try:
            rp.read()
        except Exception:
            # Si falla robots.txt, mejor ser conservador
            robots_cache[base] = None
            return False
        robots_cache[base] = rp
    rp = robots_cache[base]
    return rp.can_fetch("*", url) if rp else False

def should_skip_url(url):
    if not same_registered_domain(url, BERKELEY_DOMAIN):
        return True
    for rx in EXCLUDE_URL_PATTERNS:
        if rx.search(url):
            return True
    return False

def is_candidate_listing(url):
    # Para priorizar páginas de gente/facultad
    return any(rx.search(url) for rx in INCLUDE_URL_HINTS)

def fetch(url, timeout=12.0):
    headers = {
        "User-Agent": "SophIA-ResearchBot/1.0 (+contact: outreach@sophia.ai)"
    }
    resp = requests.get(url, headers=headers, timeout=timeout)
    resp.raise_for_status()
    if "text/html" not in resp.headers.get("Content-Type", ""):
        return None
    return resp.text

def extract_links(html, base_url):
    soup = BeautifulSoup(html, "html.parser")
    out = []
    for a in soup.find_all("a", href=True):
        href = urljoin(base_url, a["href"])
        out.append(href)
    return out

def extract_emails(html):
    return set(m.group(1).lower() for m in EMAIL_RE.finditer(html or ""))

def extract_names_and_titles(html):
    """Heurística: busca posibles nombres en encabezados y microdatos."""
    soup = BeautifulSoup(html, "html.parser")
    candidates = set()

    # Microdatos schema.org/Person
    for person in soup.select('[itemscope][itemtype*="Person" i]'):
        name_tag = person.select_one('[itemprop="name"]')
        if name_tag and name_tag.get_text(strip=True):
            candidates.add(name_tag.get_text(" ", strip=True))

    # Encabezados típicos
    for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
        txt = tag.get_text(" ", strip=True)
        if not txt:
            continue
        # Filtra ruido largo
        if len(txt.split()) > 8:
            continue
        # Señales de rol cerca
        neighbor = tag.find_next_sibling()
        near = " ".join([txt, neighbor.get_text(" ", strip=True) if neighbor else ""])
        if ROLE_HINTS.search(near) or ROLE_HINTS.search(txt):
            candidates.add(txt)

    # Metas
    for meta in soup.find_all("meta"):
        name_attr = meta.get("name", "") or meta.get("property", "")
        if "title" in name_attr.lower():
            val = meta.get("content", "")
            if val and 1 <= len(val.split()) <= 5:
                candidates.add(val.strip())

    clean = set()
    for c in candidates:
        c = re.sub(r'[,;|]+', ' ', c)
        c = re.sub(r'\s+', ' ', c).strip()
        # Quita títulos delante/detrás
        c = re.sub(r'\b(Dr\.?|Prof\.?|Professor|PhD|MSc|BSc|MA|MS)\b\.?', '', c, flags=re.I).strip()
        if 2 <= len(c.split()) <= 4:
            clean.add(c)
    return clean

def split_first_last(full_name):
    """Muy simple: 'First [Middle] Last'."""
    parts = [p for p in full_name.split() if p]
    if len(parts) < 2:
        return None, None
    first = parts[0]
    last = parts[-1]
    return first, last

def generate_email(first_name, last_name):
    if not first_name or not last_name:
        return None
    f = unidecode(first_name.strip().lower())
    l = unidecode(last_name.strip().lower())
    # quita apóstrofes/espacios/guiones
    l = re.sub(r"[^a-z0-9]", "", l)
    return f"{f[0]}{l}@berkeley.edu"

def init_db(path):
    con = sqlite3.connect(path)
    cur = con.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS contacts (
        id TEXT PRIMARY KEY,
        source_url TEXT,
        full_name TEXT,
        first_name TEXT,
        last_name TEXT,
        email_found TEXT,
        email_generated TEXT,
        method TEXT,
        confidence REAL,
        notes TEXT
    )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_email_found ON contacts(email_found)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_email_generated ON contacts(email_generated)")
    con.commit()
    return con

def upsert_contact(con, rec):
    # id = hash estable de (source_url + full_name + email_found + email_generated)
    key = "|".join([
        rec.get("source_url",""),
        rec.get("full_name","") or "",
        rec.get("email_found","") or "",
        rec.get("email_generated","") or "",
    ])
    rid = hashlib.sha256(key.encode("utf-8")).hexdigest()
    rec["id"] = rid
    cols = ["id","source_url","full_name","first_name","last_name",
            "email_found","email_generated","method","confidence","notes"]
    vals = [rec.get(c) for c in cols]
    con.execute(f"""
        INSERT INTO contacts ({",".join(cols)})
        VALUES ({",".join(["?"]*len(cols))})
        ON CONFLICT(id) DO NOTHING
    """, vals)
    con.commit()

def export_csv(con, path_csv):
    df = pd.read_sql_query("SELECT * FROM contacts", con)
    df.to_csv(path_csv, index=False)
    return df

def crawl(seeds, max_pages, max_depth, delay, timeout, db_path, out_csv):
    con = init_db(db_path)
    robots_cache = {}
    seen = set()
    queue = []

    for s in seeds:
        if not s.startswith("http"):
            s = "https://" + s
        queue.append((s, 0))

    pages = 0
    session = requests.Session()

    while queue and pages < max_pages:
        url, depth = queue.pop(0)
        if url in seen or depth > max_depth:
            continue
        seen.add(url)
        if should_skip_url(url) or not same_registered_domain(url, BERKELEY_DOMAIN):
            continue
        if not allowed_by_robots(url, robots_cache):
            continue

        try:
            time.sleep(delay)
            resp = session.get(url, headers={"User-Agent":"SophIA-ResearchBot/1.0"}, timeout=timeout)
            if "text/html" not in resp.headers.get("Content-Type",""):
                continue
            html = resp.text
        except Exception:
            continue

        pages += 1

        # 1) Emails reales
        emails = extract_emails(html)
        for e in emails:
            upsert_contact(con, {
                "source_url": url,
                "full_name": None,
                "first_name": None,
                "last_name": None,
                "email_found": e,
                "email_generated": None,
                "method": "email_found",
                "confidence": 1.0,
                "notes": None
            })

        # 2) Nombres y generación de email
        names = extract_names_and_titles(html)
        for full in names:
            fn, ln = split_first_last(full)
            gen = generate_email(fn, ln) if fn and ln else None
            upsert_contact(con, {
                "source_url": url,
                "full_name": full,
                "first_name": fn,
                "last_name": ln,
                "email_found": None,
                "email_generated": gen,
                "method": "name_generated" if gen else "name_only",
                "confidence": 0.4 if gen else 0.2,  # baja confianza para generados
                "notes": "pattern:firstinitial+lastname"
            })

        # 3) Enlaces siguientes (BFS controlado)
        if depth < max_depth:
            links = extract_links(html, url)
            for link in links:
                if should_skip_url(link):
                    continue
                # Priorizamos páginas con hints de faculty/people
                if is_candidate_listing(link):
                    queue.append((link, depth+1))
                # (opcional) también añadir otras páginas internas del mismo host:
                # else:
                #     queue.append((link, depth+1))

    # Exporta CSV
    df = export_csv(con, out_csv)
    con.close()
    print(f"[OK] Páginas procesadas: {pages} | Registros: {len(df)} | CSV: {out_csv}")

if __name__ == "__main__":
    args = parse_args()
    crawl(
        seeds=args.seeds,
        max_pages=args.max_pages,
        max_depth=args.max_depth,
        delay=args.delay,
        timeout=args.timeout,
        db_path=args.db,
        out_csv=args.out_csv
    )
