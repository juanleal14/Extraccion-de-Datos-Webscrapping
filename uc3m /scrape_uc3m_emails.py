"""
scrape_uc3m_emails.py
Uso:
    python scrape_uc3m_emails.py            # hace crawl desde la home (cuidado con robots.txt)
    python scrape_uc3m_emails.py --seed seeds.txt  # usa lista de URLs semilla (una por línea)

Salida:
    uc3m/emails.csv  (csv con columnas: nombre_pagina, url, email)
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import csv
import argparse
import os
import urllib.robotparser
from urllib.parse import urljoin, urlparse
from collections import deque
from tqdm import tqdm

BASE_DOMAIN = "uc3m.es"
BASE_URL = "https://www.uc3m.es/"

HEADERS = {
    "User-Agent": "uc3m-scraper/1.0 (+https://github.com/tu-repo) - Contact: tu.email@example.com"
}

EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")  # robust email regex

OUT_DIR = "uc3m"
OUT_FILE = os.path.join(OUT_DIR, "emails.csv")

# robots.txt comprobar
def allowed_by_robots(url, user_agent=HEADERS["User-Agent"]):
    robots_url = urljoin(BASE_URL, "robots.txt")
    rp = urllib.robotparser.RobotFileParser()
    try:
        rp.set_url(robots_url)
        rp.read()
    except Exception as e:
        # si no se puede leer robots.txt
        print(f"[WARN] No se pudo leer robots.txt ({robots_url}): {e}. Procede con precaución.")
        return False
    return rp.can_fetch(user_agent, url)


def is_internal_url(url):
    try:
        p = urlparse(urljoin(BASE_URL, url))
        return p.netloc.endswith(BASE_DOMAIN)
    except:
        return False

def normalize_url(url):
    return urljoin(BASE_URL, url)


def fetch(url, timeout=15, max_retries=2):
    for attempt in range(max_retries + 1):
        try:
            resp = requests.get(url, headers=HEADERS, timeout=timeout)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            if attempt < max_retries:
                time.sleep(1 + attempt*2)
            else:
                print(f"[ERROR] Falló al obtener {url}: {e}")
                return None

# parse emails from HTML text
def extract_emails(text):
    # extraer emails y normalizar (lower, únicos)
    found = set(m.group(0).lower() for m in EMAIL_REGEX.finditer(text))
    return found

# main crawler (breadth-first, con límite)
def crawl(start_urls, max_pages=1000, delay=0.7):
    os.makedirs(OUT_DIR, exist_ok=True)
    seen = set()
    q = deque()
    for u in start_urls:
        q.append(normalize_url(u))
    emails_found = {}  # email -> set(urls)
    pages_scanned = 0

    pbar = tqdm(total=max_pages, desc="Pages")

    while q and pages_scanned < max_pages:
        url = q.popleft()
        if url in seen:
            continue
        if not is_internal_url(url):
            continue
        # respetar robots
        if not allowed_by_robots(url):
            # si robots.txt prohíbe, saltar
            print(f"[SKIP robots] {url}")
            seen.add(url)
            continue

        resp = fetch(url)
        seen.add(url)
        pages_scanned += 1
        pbar.update(1)

        if resp is None:
            continue

        content_type = resp.headers.get("Content-Type","")
        text = resp.text

        # extraer emails de la página
        emails = extract_emails(text)
        for e in emails:
            emails_found.setdefault(e, set()).add(url)

        # parsear enlaces internos para seguir crawling
        try:
            soup = BeautifulSoup(text, "lxml")
        except Exception:
            soup = BeautifulSoup(text, "html.parser")

        # extrae enlaces relevantes
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            # ignorar enlaces mailto (los podemos extraer con regex) o anchors
            if href.startswith("mailto:"):
                mail = href.split(":",1)[1]
                if EMAIL_REGEX.search(mail):
                    emails_found.setdefault(mail.lower(), set()).add(url)
                continue
            if href.startswith("#"):
                continue
            full = normalize_url(href)
            if is_internal_url(full) and full not in seen:
                # opcional: limitar a URLs que contienen /ListadoPersonalDept/ o 'personal' para priorizar listados
                q.append(full)

        time.sleep(delay)

    pbar.close()

    # guardar CSV
    with open(OUT_FILE, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["email", "found_on_urls"])
        for email, urls in sorted(emails_found.items()):
            w.writerow([email, " | ".join(sorted(urls))])

    print(f"[DONE] {len(emails_found)} emails guardados en {OUT_FILE}")
    return emails_found

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scraper de correos UC3M (respetando robots.txt)")
    parser.add_argument("--seed", type=str, help="archivo con URLs semilla (una por línea)")
    parser.add_argument("--max", type=int, default=200, help="max páginas a escanear")
    parser.add_argument("--delay", type=float, default=0.8, help="segundos de espera entre peticiones")
    args = parser.parse_args()

    if args.seed:
        if not os.path.exists(args.seed):
            print(f"[ERROR] No existe {args.seed}")
            exit(1)
        with open(args.seed, "r", encoding="utf-8") as f:
            seeds = [l.strip() for l in f if l.strip()]
    else:
        # páginas de departamentos / listados de personal
        seeds = [
            "https://www.uc3m.es/ss/Satellite/DeptMatematicas/es/ListadoPersonalDept/1371321047774/Profesores_Titulares",
            "https://www.uc3m.es/departamento-ciencias-sociales/personal-tiempo-completo",
            "https://www.uc3m.es/ss/Satellite/DeptIngElectrica/es/Profesorado_a_tiempo_completo",
            "https://www.uc3m.es/ss/Satellite/DeptCienIngMatIngQuim/es/ListadoPersonalDept/1371322829661/Profesores_permanentes",
            # añade aquí otras URLs semilla si las conoces
        ]

    # comprobar robots.txt base antes de comenzar
    if not allowed_by_robots(BASE_URL):
        print("[ERROR] robots.txt bloquea el scraping del dominio base. Revisa antes de continuar.")
        exit(1)

    crawl(seeds, max_pages=args.max, delay=args.delay)
