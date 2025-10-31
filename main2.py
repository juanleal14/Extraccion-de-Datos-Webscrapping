#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Berkeley faculty crawler (Selenium) for:
- https://vcresearch.berkeley.edu/faculty-expertise
Works with "Load more" and/or infinite scroll.
Extracts @berkeley.edu emails, names (heuristics), and generates firstinitial+lastname@berkeley.edu.
Stores results in SQLite and CSV.

Usage example:
  python berkeley_faculty_vcresearch.py \
    --url https://vcresearch.berkeley.edu/faculty-expertise \
    --max-clicks 300 --wait-after-click 1.7 \
    --scroll-tries 80 --scroll-wait 1.0 \
    --card-css ".view-content .views-row, .card, article, .profile, .person" \
    --out-csv berkeley_faculty_vcr.csv --db berkeley_faculty_vcr.db \
    --headless 1
"""

import argparse
import re
import sqlite3
import hashlib
import time
from urllib.parse import urlparse

import pandas as pd
import tldextract
from bs4 import BeautifulSoup
from unidecode import unidecode

# --- Selenium ---
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException, ElementClickInterceptedException
)
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait

BERKELEY_DOMAIN = "berkeley.edu"
EMAIL_RE = re.compile(r'\b([A-Za-z0-9._%+\-]+@berkeley\.edu)\b', re.I)
ROLE_HINTS = re.compile(r'\b(Professor|Assistant Professor|Associate Professor|Lecturer|Faculty|Staff|Researcher|Chair|Dean)\b', re.I)

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--headless", type=int, default=1)
    ap.add_argument("--max-clicks", type=int, default=250)
    ap.add_argument("--wait-after-click", type=float, default=1.4)
    ap.add_argument("--scroll-tries", type=int, default=60)
    ap.add_argument("--scroll-wait", type=float, default=1.0)
    # selector(es) para contar tarjetas (separados por coma)
    ap.add_argument("--card-css", default=".view-content .views-row, .card, article, .profile, .person")
    ap.add_argument("--page-ready-css", default="", help="CSS que indica que el listado inicial cargó (opcional)")
    # si conoces el botón exacto:
    ap.add_argument("--load-more-xpath", default="")
    ap.add_argument("--load-more-css", default="")
    ap.add_argument("--out-csv", default="berkeley_faculty.csv")
    ap.add_argument("--db", default="berkeley_faculty.db")
    return ap.parse_args()

def same_registered_domain(url, target_domain):
    host = urlparse(url).netloc
    ext = tldextract.extract(host)
    reg = ".".join([p for p in [ext.domain, ext.suffix] if p])
    return reg.lower() == target_domain.lower()

from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import ChromeType
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.common.options import ArgOptions as BaseOptions  # Selenium >=4.25
import os
import shutil

import os, shutil
from selenium import webdriver

import os, shutil
from selenium import webdriver

def build_driver(headless=True):
    # Localiza Chrome (variable de entorno o PATH)
    chrome_bin = os.environ.get("CHROME_BINARY") or shutil.which("google-chrome") or shutil.which("google-chrome-stable")
    if not chrome_bin:
        raise RuntimeError("No se encontró Google Chrome. Define CHROME_BINARY o instala google-chrome-stable.")

    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    # Flags robustas para WSL/containers
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--remote-debugging-port=0")
    opts.add_argument("--no-zygote")
    opts.add_argument("--window-size=1440,2400")
    # Perfiles temporales en /tmp (evita permisos)
    opts.add_argument("--user-data-dir=/tmp/chrome-profile")
    opts.add_argument("--data-path=/tmp/chrome-data")
    opts.add_argument("--disk-cache-dir=/tmp/chrome-cache")
    opts.binary_location = chrome_bin

    # Selenium Manager se encarga del driver automáticamente
    return webdriver.Chrome(options=opts)



from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.common.exceptions import TimeoutException, NoSuchElementException, StaleElementReferenceException, ElementClickInterceptedException
import time

def count_cards(driver, css_list):
    total = 0
    for sel in css_list:
        try:
            elems = driver.find_elements(By.CSS_SELECTOR, sel.strip())
            total += len(elems)
        except Exception:
            pass
    return total

def find_candidate_buttons(driver):
    cands = []
    elems = driver.find_elements(By.XPATH, "//button|//a|//*[@role='button']")
    for el in elems:
        try:
            txt = (el.text or "").strip().lower()
            if not txt:
                txt = ((el.get_attribute("aria-label") or el.get_attribute("title")) or "").strip().lower()
            cls = (el.get_attribute("class") or "").lower()
            data_action = (el.get_attribute("data-action") or "").lower()
            if any(s in txt for s in ["load more","show more","more results","view more","load additional"]) \
               or "load-more" in cls or "loadmore" in cls or "pager__item--more" in cls \
               or "load-more" in data_action:
                cands.append(el)
        except Exception:
            continue
    return cands

def click_load_more_until_end(driver, css_list, max_clicks=250, wait_after_click=1.4,
                              explicit_xpath=None, explicit_css=None):
    css_list = [s for s in (css_list or []) if s and s.strip()]
    prev_count = count_cards(driver, css_list)
    print(f"[CLICK] Tarjetas iniciales: {prev_count}")
    clicks = 0
    clicked_any = False

    def click_element(el):
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.2)
        el.click()

    while clicks < max_clicks:
        btn = None
        if explicit_xpath:
            try: btn = driver.find_element(By.XPATH, explicit_xpath)
            except Exception: btn = None
        if not btn and explicit_css:
            try: btn = driver.find_element(By.CSS_SELECTOR, explicit_css)
            except Exception: btn = None
        if not btn:
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(0.5)
            cands = find_candidate_buttons(driver)
            print(f"[CLICK] Candidatos botón: {len(cands)}")
            if not cands:
                print("[CLICK] No hay botón 'Load more' visible. Paro clicks.")
                break
            btn = cands[0]

        try:
            click_element(btn)
        except (StaleElementReferenceException, ElementClickInterceptedException, NoSuchElementException):
            driver.execute_script("window.scrollBy(0, -200);")
            time.sleep(0.3)
            try:
                click_element(btn)
            except Exception as e:
                print(f"[CLICK] Falló el click: {e}. Paro clicks.")
                break

        clicks += 1
        clicked_any = True
        time.sleep(wait_after_click)

        cur = count_cards(driver, css_list)
        print(f"[CLICK] Click #{clicks} ⇒ tarjetas: {cur}")
        if cur <= prev_count:
            print("[CLICK] No aumentan tarjetas después del click. Paro clicks.")
            break
        prev_count = cur

    print(f"[CLICK] Hecho. Clicks: {clicks}, tarjetas: {prev_count}")
    return clicked_any

def infinite_scroll(driver, css_list, tries=60, wait=1.0):
    css_list = [s for s in (css_list or []) if s and s.strip()]
    prev = count_cards(driver, css_list)
    print(f"[SCROLL] Inicio con {prev} tarjetas")
    grew_steps = 0
    for i in range(tries):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(wait)
        cur = count_cards(driver, css_list)
        print(f"[SCROLL] Paso {i+1}/{tries} ⇒ tarjetas: {cur}")
        if cur > prev:
            grew_steps += 1
            prev = cur
        else:
            print("[SCROLL] No crece el nº de tarjetas. Paro scroll.")
            break
    print(f"[SCROLL] Hecho. Pasos con crecimiento: {grew_steps}, total tarjetas: {prev}")
    return grew_steps


# --- Extracción de datos ---
def extract_emails(html):
    return set(m.group(1).lower() for m in EMAIL_RE.finditer(html or ""))

def extract_names_and_titles(html):
    soup = BeautifulSoup(html, "html.parser")
    candidates = set()

    # Microdatos schema.org/Person
    for person in soup.select('[itemscope][itemtype*="Person" i]'):
        name_tag = person.select_one('[itemprop="name"]')
        if name_tag and name_tag.get_text(strip=True):
            candidates.add(name_tag.get_text(" ", strip=True))

    # Encabezados
    for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
        txt = tag.get_text(" ", strip=True)
        if not txt or len(txt.split()) > 8:
            continue
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
        c = re.sub(r'\b(Dr\.?|Prof\.?|Professor|PhD|MSc|BSc|MA|MS)\b\.?', '', c, flags=re.I).strip()
        if 2 <= len(c.split()) <= 4:
            clean.add(c)
    return clean

def split_first_last(full_name):
    parts = [p for p in full_name.split() if p]
    if len(parts) < 2:
        return None, None
    return parts[0], parts[-1]

def generate_email(first_name, last_name):
    if not first_name or not last_name:
        return None
    f = unidecode(first_name.strip().lower())
    l = unidecode(last_name.strip().lower())
    l = re.sub(r"[^a-z0-9]", "", l)
    return f"{f[0]}{l}@berkeley.edu"

# --- Storage (SQLite + CSV) ---
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
    key = "|".join([
        rec.get("source_url","") or "",
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

def main():
    args = parse_args()
    if not same_registered_domain(args.url, BERKELEY_DOMAIN):
        raise SystemExit("La URL no pertenece a berkeley.edu")

    driver = build_driver(headless=bool(args.headless))
    con = init_db(args.db)
    try:
        driver.get(args.url)
        css_list = [s.strip() for s in args.card_css.split(",") if s.strip()]

        print("[MAIN] Página cargada. Intento 'Load more'...")
        clicked = click_load_more_until_end(
            driver,
            css_list=css_list,
            max_clicks=args.max_clicks,
            wait_after_click=args.wait_after_click,
            explicit_xpath=args.load_more_xpath or None,
            explicit_css=args.load_more_css or None
        )

        #print("[MAIN] Intento scroll infinito...")
        #infinite_scroll(driver, css_list=css_list, tries=args.scroll_tries, wait=args.scroll_wait)

        print("[MAIN] Extrayendo HTML final y parseando...")


    #    html = driver.page_source
    #    source_url = driver.current_url
    #    
    #    # --- Extracción ---
    #    for e in extract_emails(html):
    #        upsert_contact(con, {
    #            "source_url": source_url,
    #            "full_name": None,
    #            "first_name": None,
    #            "last_name": None,
    #            "email_found": e,
    #            "email_generated": None,
    #            "method": "email_found",
    #            "confidence": 1.0,
    #            "notes": "selenium: load-more/scroll"
    #        })
#
    #    for full in extract_names_and_titles(html):
    #        fn, ln = split_first_last(full)
    #        gen = generate_email(fn, ln) if fn and ln else None
    #        upsert_contact(con, {
    #            "source_url": source_url,
    #            "full_name": full,
    #            "first_name": fn,
    #            "last_name": ln,
    #            "email_found": None,
    #            "email_generated": gen,
    #            "method": "name_generated" if gen else "name_only",
    #            "confidence": 0.4 if gen else 0.2,
    #            "notes": "pattern:firstinitial+lastname"
    #        })
        from urllib.parse import urljoin, urlparse
        import requests
        from bs4 import BeautifulSoup
        import time
        import re
        html = driver.page_source
        source_url = driver.current_url

        # --- 1) Extrae emails visibles en la PÁGINA DE LISTA (mantén tu regex existente) ---
        for e in extract_emails(html):
            upsert_contact(con, {
                "source_url": source_url,
                "full_name": None,
                "first_name": None,
                "last_name": None,
                "email_found": e,
                "email_generated": None,
                "method": "email_found_list",
                "confidence": 1.0,
                "notes": "selenium: list page"
            })

        # --- 2) Extrae NOMBRES en la lista (para generar patrón inicial+apellido) ---
        soup = BeautifulSoup(html, "html.parser")

        name_selectors = [
            ".view-content .views-row h3 a",
            ".view-content .views-row h3",
            ".view-content .views-row .field--name-title a",
            ".view-content .views-row .field--name-title",
        ]
        card_names = set()
        for sel in name_selectors:
            for el in soup.select(sel):
                txt = (el.get_text(" ", strip=True) or "").strip()
                if 2 <= len(txt.split()) <= 4:
                    card_names.add(txt)

        for full in card_names:
            fn, ln = split_first_last(full)
            gen = generate_email(fn, ln) if fn and ln else None
            upsert_contact(con, {
                "source_url": source_url,
                "full_name": full,
                "first_name": fn,
                "last_name": ln,
                "email_found": None,
                "email_generated": gen,
                "method": "card_name_generated",
                "confidence": 0.5 if gen else 0.25,
                "notes": "name from list card"
            })

        print(f"[DETAIL] Nombres en lista: {len(card_names)}")

        # --- 3) Encuentra ENLACES a fichas personales y visítalos (AQUÍ ESTÁN LOS EMAILS) ---
        def is_profile_like(href: str) -> bool:
            href_l = href.lower()
            # Ajusta si ves otros patrones en DevTools
            return any(p in href_l for p in [
                "/faculty/", "/people/", "/profile", "/profiles/", "/user/", "/directory/"
            ])

        profile_links = set()
        for a in soup.select(".view-content .views-row a[href]"):
            href = a.get("href")
            if not href: 
                continue
            if href.startswith("#"): 
                continue
            abs_url = urljoin(source_url, href)
            if "berkeley.edu" not in abs_url: 
                continue
            if is_profile_like(abs_url):
                profile_links.add(abs_url)

        print(f"[DETAIL] Fichas detectadas: {len(profile_links)}")

        # --- 4) Visita cada ficha y extrae email + nombre limpio ---
        HEADERS = {"User-Agent": "SophIA-ResearchBot/1.0 (+contact: outreach@sophia.ai)"}
        MAX_PROFILES = 1000          # sube/baja según quieras
        PROFILE_DELAY = 0.5          # respeta rate-limit del sitio
        EMAIL_RE = re.compile(r'\b([A-Za-z0-9._%+\-]+@berkeley\.edu)\b', re.I)

        visited = 0
        found_detail_emails = 0

        for link in list(profile_links)[:MAX_PROFILES]:
            try:
                time.sleep(PROFILE_DELAY)
                r = requests.get(link, headers=HEADERS, timeout=12)
                if r.status_code != 200:
                    continue
                if "text/html" not in r.headers.get("Content-Type",""):
                    continue
                detail_html = r.text
                visited += 1

                # a) Emails reales en la ficha
                emails = set(m.group(1).lower() for m in EMAIL_RE.finditer(detail_html))
                for e in emails:
                    upsert_contact(con, {
                        "source_url": link,
                        "full_name": None,
                        "first_name": None,
                        "last_name": None,
                        "email_found": e,
                        "email_generated": None,
                        "method": "email_found_profile",
                        "confidence": 1.0,
                        "notes": "profile page"
                    })
                if emails:
                    found_detail_emails += len(emails)

                # b) Nombre en la ficha (para generar patrón si no hay email)
                detail_soup = BeautifulSoup(detail_html, "html.parser")
                # prueba encabezados típicos de fichas
                title = None
                for sel in ["h1", "h2", ".page-title", ".node--title", "header h1", ".title"]:
                    el = detail_soup.select_one(sel)
                    if el:
                        title = (el.get_text(" ", strip=True) or "").strip()
                        if title: break

                # Microdatos Person si existe
                if not title:
                    el = detail_soup.select_one('[itemscope][itemtype*="Person" i] [itemprop="name"]')
                    if el:
                        title = (el.get_text(" ", strip=True) or "").strip()

                if title and 2 <= len(title.split()) <= 5:
                    fn, ln = split_first_last(title)
                    if fn and ln:
                        gen = generate_email(fn, ln)
                        upsert_contact(con, {
                            "source_url": link,
                            "full_name": title,
                            "first_name": fn,
                            "last_name": ln,
                            "email_found": None,
                            "email_generated": gen,
                            "method": "profile_name_generated",
                            "confidence": 0.6 if gen else 0.3,
                            "notes": "name from profile"
                        })

            except Exception:
                continue

        print(f"[DETAIL] Fichas visitadas: {visited} | Emails reales en fichas: {found_detail_emails}")

        df = export_csv(con, args.out_csv)
        print(f"[OK] Saved CSV: {args.out_csv} | Rows: {len(df)}")
    finally:
        driver.quit()
        con.close()

if __name__ == "__main__":
    main()
