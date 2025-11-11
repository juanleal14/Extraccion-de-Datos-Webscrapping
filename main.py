#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Crawler para https://vcresearch.berkeley.edu/faculty-expertise
- Selenium para clicar "Load more" y cargar toda la lista
- Extrae emails/nombres en la lista
- Visita fichas personales y extrae email + nombre + departamento
- Guarda en SQLite y exporta CSV raw + CSV CRM

Ejemplo:
  python3 main.py \
    --url https://vcresearch.berkeley.edu/faculty-expertise \
    --max-clicks 5--wait-after-click 1.8 \
    --card-css ".view-content .views-row" \
    --out-csv mails.csv \
    --db mails.db \
    --headless 1
"""

import argparse
import hashlib
import os
import re
import shutil
import sqlite3
import time
import uuid
from urllib.parse import urlparse, urljoin

import pandas as pd
import requests
import tldextract
from bs4 import BeautifulSoup
from unidecode import unidecode

# Selenium (usando Selenium Manager; no webdriver_manager)
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    TimeoutException, NoSuchElementException, StaleElementReferenceException, ElementClickInterceptedException
)

BERKELEY_DOMAIN = "berkeley.edu"
EMAIL_RE = re.compile(r'\b([A-Za-z0-9._%+\-]+@berkeley\.edu)\b', re.I)
ROLE_HINTS = re.compile(r'\b(Professor|Assistant Professor|Associate Professor|Lecturer|Faculty|Staff|Researcher|Chair|Dean)\b', re.I)


# ---------------------------
# CLI
# ---------------------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="Listado, p.ej. https://vcresearch.berkeley.edu/faculty-expertise")
    ap.add_argument("--headless", type=int, default=1)
    ap.add_argument("--max-clicks", type=int, default=250)
    ap.add_argument("--wait-after-click", type=float, default=1.4)
    ap.add_argument("--card-css", default=".view-content .views-row", help="Selectores (separados por coma) para contar 'tarjetas'")
    ap.add_argument("--out-csv", default="berkeley_faculty.csv")
    ap.add_argument("--db", default="berkeley_faculty.db")
    # tuning fichas
    ap.add_argument("--max-profiles", type=int, default=1000, help="Máx. fichas a visitar")
    ap.add_argument("--profile-delay", type=float, default=0.5, help="Delay entre fichas (seg)")
    return ap.parse_args()


def same_registered_domain(url, target_domain):
    host = urlparse(url).netloc
    ext = tldextract.extract(host)
    reg = ".".join([p for p in [ext.domain, ext.suffix] if p])
    return reg.lower() == target_domain.lower()


# ---------------------------
# Driver Chrome robusto (WSL/containers)
# ---------------------------
def build_driver(headless=True):
    chrome_bin = os.environ.get("CHROME_BINARY") or shutil.which("google-chrome") or shutil.which("google-chrome-stable")
    if not chrome_bin:
        raise RuntimeError("No se encontró Google Chrome. Instálalo o define CHROME_BINARY.")

    opts = webdriver.ChromeOptions()
    if headless:
        opts.add_argument("--headless=new")
    # flags robustas
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--remote-debugging-port=0")
    opts.add_argument("--no-zygote")
    opts.add_argument("--window-size=1440,2400")
    # perfiles temporales en /tmp
    opts.add_argument("--user-data-dir=/tmp/chrome-profile")
    opts.add_argument("--data-path=/tmp/chrome-data")
    opts.add_argument("--disk-cache-dir=/tmp/chrome-cache")
    opts.binary_location = chrome_bin

    return webdriver.Chrome(options=opts)


# ---------------------------
# Utilidades de listado (click & count)
# ---------------------------
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
            if any(s in txt for s in ["load more", "show more", "more results", "view more", "load additional"]) \
               or "load-more" in cls or "loadmore" in cls or "pager__item--more" in cls \
               or "load-more" in data_action:
                cands.append(el)
        except Exception:
            continue
    return cands


def click_load_more_until_end(driver, css_list, max_clicks=250, wait_after_click=1.4):
    css_list = [s for s in (css_list or []) if s and s.strip()]
    prev_count = count_cards(driver, css_list)
    print(f"[CLICK] Tarjetas iniciales: {prev_count}")
    clicks = 0

    def click_element(el):
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
        time.sleep(0.2)
        el.click()

    while clicks < max_clicks:
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
        time.sleep(wait_after_click)

        cur = count_cards(driver, css_list)
        print(f"[CLICK] Click #{clicks} ⇒ tarjetas: {cur}")
        if cur <= prev_count:
            print("[CLICK] No aumentan tarjetas después del click. Paro clicks.")
            break
        prev_count = cur

    print(f"[CLICK] Hecho. Clicks: {clicks}, tarjetas: {prev_count}")
    return clicks


# ---------------------------
# Extracción básica
# ---------------------------
def extract_emails_from_html(html):
    return set(m.group(1).lower() for m in EMAIL_RE.finditer(html or ""))


def extract_names_from_list_html(html):
    """Nombres desde encabezados/enlaces comunes de tarjeta."""
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        ".view-content .views-row h3 a",
        ".view-content .views-row h3",
        ".view-content .views-row .field--name-title a",
        ".view-content .views-row .field--name-title",
    ]
    names = set()
    for sel in selectors:
        for el in soup.select(sel):
            txt = (el.get_text(" ", strip=True) or "").strip()
            if 2 <= len(txt.split()) <= 4:
                names.add(txt)
    return names, soup


def split_first_last(full_name):
    if not full_name:
        return None, None
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


def is_profile_like(href: str) -> bool:
    href_l = href.lower()
    return any(p in href_l for p in ["/faculty/", "/people/", "/profile", "/profiles/", "/user/", "/directory/"])


def extract_department(detail_soup):
    candidates = []
    for sel in ["dl", ".field", ".profile-meta", ".node__meta", ".sidebar", ".field--name-field-department"]:
        for block in detail_soup.select(sel):
            txt = block.get_text(" ", strip=True)
            if not txt:
                continue
            if re.search(r"\bDepartment\b", txt, flags=re.I) or re.search(r"\bAffiliation\b|\bSchool\b|\bDivision\b", txt, flags=re.I):
                candidates.append(txt)

    for txt in candidates:
        m = re.search(r"Department(?: of)?[:\s]+(.+?)(?:\s{2,}|$)", txt, flags=re.I)
        if m:
            dep = m.group(1).strip()
            if 0 < len(dep) <= 200:
                return dep

    el = detail_soup.select_one(".department, .field--name-field-department")
    if el:
        v = el.get_text(" ", strip=True)
        if v:
            return v
    return ""


# ---------------------------
# Storage: SQLite + export
# ---------------------------
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

    cur.execute("""
    CREATE TABLE IF NOT EXISTS profiles (
        source_url TEXT PRIMARY KEY,
        full_name TEXT,
        first_name TEXT,
        last_name TEXT,
        university TEXT,
        department TEXT
    )
    """)
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


def upsert_profile(con, source_url, full_name=None, first_name=None, last_name=None,
                   university=None, department=None):
    con.execute("""
        INSERT INTO profiles (source_url, full_name, first_name, last_name, university, department)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(source_url) DO UPDATE SET
            full_name=COALESCE(excluded.full_name, profiles.full_name),
            first_name=COALESCE(excluded.first_name, profiles.first_name),
            last_name=COALESCE(excluded.last_name, profiles.last_name),
            university=COALESCE(excluded.university, profiles.university),
            department=COALESCE(excluded.department, profiles.department)
    """, (source_url, full_name, first_name, last_name, university, department))
    con.commit()


def export_raw_csv(con, path_csv):
    df = pd.read_sql_query("SELECT * FROM contacts", con)
    df.to_csv(path_csv, index=False)
    return df


def export_crm_csv(con, path_csv, default_university="UC Berkeley"):
    df_c = pd.read_sql_query("SELECT * FROM contacts", con)
    df_p = pd.read_sql_query("SELECT * FROM profiles", con)

    # Preferimos email_found; si no, email_generated
    df_c["Email"] = df_c["email_found"].fillna(df_c["email_generated"])

    df = df_c.merge(df_p.add_prefix("prof_"), how="left",
                    left_on="source_url", right_on="prof_source_url")

    f_name = df["prof_first_name"].fillna(df["first_name"])
    l_name = df["prof_last_name"].fillna(df["last_name"])
    full = df["prof_full_name"].fillna(df["full_name"])

    # Completar Nombre/Apellidos desde full_name si faltan
    mask_need = f_name.isna() & l_name.isna() & full.notna()
    if mask_need.any():
        split_vals = full[mask_need].apply(lambda s: pd.Series(split_first_last(s)))
        f_name.loc[mask_need] = split_vals[0]
        l_name.loc[mask_need]  = split_vals[1]

    uni = df["prof_university"].fillna(default_university)
    dept = df["prof_department"].fillna("")

    def make_id(row):
        key = (row.get("Email") or "").strip().lower()
        if not key:
            key = f"{(row.get('prof_full_name') or row.get('full_name') or '').strip()}|{row.get('source_url') or ''}"
        return str(uuid.uuid5(uuid.NAMESPACE_URL, key))

    out = pd.DataFrame({
        "id":        df.apply(make_id, axis=1),
        "Nombre":    f_name.fillna(""),
        "Apellidos": l_name.fillna(""),
        "Email":     df["Email"].fillna(""),
        "Universidad": uni,
        "Departamento": dept,
        "Desuscrito": "FALSE",
        "Enviado": "FALSE",
        "open_count": 0,
        "first_open": "",
        "last_open": "",
        "click_count": 0,
        "first_click": "",
        "last_click": "",
        "last_ip": "",
        "last_ua": "",
        "last_clicked_url": ""
    })

    out["__k"] = out["Email"].where(out["Email"].str.len() > 0, out["id"])
    out = out.sort_values(["__k"]).drop_duplicates("__k").drop(columns="__k")

    out.to_csv(path_csv, index=False)
    return out


# ---------------------------
# Main
# ---------------------------
def main():
    args = parse_args()
    if not same_registered_domain(args.url, BERKELEY_DOMAIN):
        raise SystemExit("La URL no pertenece a berkeley.edu")

    con = init_db(args.db)
    driver = build_driver(headless=bool(args.headless))
    try:
        driver.get(args.url)
        css_list = [s.strip() for s in args.card_css.split(",") if s.strip()]

        print("[MAIN] Página cargada. Intento 'Load more'...")
        click_load_more_until_end(
            driver,
            css_list=css_list,
            max_clicks=args.max_clicks,
            wait_after_click=args.wait_after_click
        )

        print("[MAIN] Extrayendo HTML final y parseando...")
        html = driver.page_source
        source_url = driver.current_url

        # 1) Emails visibles en la lista
        for e in extract_emails_from_html(html):
            upsert_contact(con, {
                "source_url": source_url,
                "full_name": None,
                "first_name": None,
                "last_name": None,
                "email_found": e,
                "email_generated": None,
                "method": "email_found_list",
                "confidence": 1.0,
                "notes": "list page"
            })

        # 2) Nombres en la lista
        names, soup = extract_names_from_list_html(html)
        for full in names:
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

        # 3) Fichas personales
        profile_links = set()
        for a in soup.select(".view-content .views-row a[href]"):
            href = a.get("href")
            if not href or href.startswith("#"):
                continue
            abs_url = urljoin(source_url, href)
            if "berkeley.edu" not in abs_url:
                continue
            if is_profile_like(abs_url):
                profile_links.add(abs_url)

        print(f"[DETAIL] Fichas detectadas: {len(profile_links)}")

        HEADERS = {"User-Agent": "SophIA-ResearchBot/1.0 (+contact: outreach@sophia.ai)"}
        visited = 0
        found_detail_emails = 0

        for link in list(profile_links)[:args.max_profiles]:
            try:
                time.sleep(args.profile_delay)
                r = requests.get(link, headers=HEADERS, timeout=12)
                if r.status_code != 200:
                    continue
                if "text/html" not in r.headers.get("Content-Type",""):
                    continue
                detail_html = r.text
                visited += 1

                # emails en ficha
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

                # nombre y departamento en ficha
                detail_soup = BeautifulSoup(detail_html, "html.parser")
                title = None
                for sel in ["h1", "h2", ".page-title", ".node--title", "header h1", ".title"]:
                    el = detail_soup.select_one(sel)
                    if el:
                        title = (el.get_text(" ", strip=True) or "").strip()
                        if title:
                            break
                if not title:
                    el = detail_soup.select_one('[itemscope][itemtype*="Person" i] [itemprop="name"]')
                    if el:
                        title = (el.get_text(" ", strip=True) or "").strip()

                dep = extract_department(detail_soup)
                fn_p, ln_p = split_first_last(title) if title else (None, None)

                upsert_profile(
                    con,
                    source_url=link,
                    full_name=title,
                    first_name=fn_p,
                    last_name=ln_p,
                    university="UC Berkeley",
                    department=dep or None
                )

                if fn_p and ln_p:
                    gen = generate_email(fn_p, ln_p)
                    upsert_contact(con, {
                        "source_url": link,
                        "full_name": title,
                        "first_name": fn_p,
                        "last_name": ln_p,
                        "email_found": None,
                        "email_generated": gen,
                        "method": "profile_name_generated",
                        "confidence": 0.6 if gen else 0.3,
                        "notes": "name from profile"
                    })

            except Exception:
                continue

        print(f"[DETAIL] Fichas visitadas: {visited} | Emails reales en fichas: {found_detail_emails}")

        # Export
        df_raw = export_raw_csv(con, args.out_csv)
        crm_path = args.out_csv.replace(".csv", "_CRM.csv")
        df_crm = export_crm_csv(con, crm_path)
        print(f"[OK] Saved RAW CSV: {args.out_csv} | Rows: {len(df_raw)}")
        print(f"[OK] Saved CRM CSV: {crm_path} | Rows: {len(df_crm)}")

    finally:
        driver.quit()
        con.close()


if __name__ == "__main__":
    main()
