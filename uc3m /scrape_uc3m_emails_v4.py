#!/usr/bin/env python3
"""
Script mejorado para extraer emails de profesores de UC3M
Versión 4: Extracción optimizada con limpieza en tiempo real
"""

import requests
from bs4 import BeautifulSoup
import re
import time
import csv
from urllib.parse import urljoin, urlparse
from collections import defaultdict

BASE_URL = "https://www.uc3m.es"

# Headers para evitar bloqueos
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
}

# Regex para emails válidos
EMAIL_PATTERN = re.compile(r"[a-zA-Z0-9._%+-]+@(?:[a-zA-Z0-9.-]+\.)+[a-zA-Z]{2,}")


def get_soup(url, retries=2):
    """Obtiene el soup de una URL con reintentos"""
    for attempt in range(retries):
        try:
            r = requests.get(url, headers=HEADERS, timeout=15)
            r.raise_for_status()
            return BeautifulSoup(r.text, "html.parser")
        except Exception as e:
            if attempt < retries - 1:
                time.sleep(1)
                continue
            print(f"[ERROR] No se pudo acceder a {url}: {e}")
            return None
    return None


def clean_email(email_text):
    """Limpia y normaliza un email en el momento de extracción"""
    if not email_text:
        return None
    
    # Buscar el primer email válido en el texto
    match = EMAIL_PATTERN.search(str(email_text))
    if not match:
        return None
    
    email = match.group(0)
    
    # Limpiar: quitar puntos finales, espacios, y convertir a minúsculas
    email = email.rstrip('. ').lower().strip()
    
    # Validar que sea un email de UC3M o dominio relacionado
    if '@uc3m.es' in email or '@pa.uc3m.es' in email:
        return email
    
    return None


def normalize_name(name):
    """Normaliza un nombre a formato título"""
    if not name:
        return None
    
    name = str(name).strip()
    
    # Eliminar "Departamento" como nombre
    if name.lower() in ['departamento', 'dept', 'depto']:
        return None
    
    # Limpiar espacios múltiples
    name = re.sub(r'\s+', ' ', name)
    
    # Validar longitud y número de palabras
    words = name.split()
    if len(words) < 2 or len(words) > 8:
        return None
    
    # Convertir a formato título (Title Case)
    name = name.title()
    
    # Eliminar caracteres extraños al inicio/final
    name = name.strip('.,;:()[]{}')
    
    return name if len(name) > 3 else None


def extract_name_advanced(node, email):
    """Extrae el nombre usando múltiples estrategias avanzadas"""
    if node is None:
        return None
    
    # Estrategia 1: Buscar en el mismo nodo y hermanos
    for candidate in [node, node.previous_sibling, node.next_sibling]:
        if candidate and hasattr(candidate, 'get_text'):
            text = candidate.get_text(separator=" ", strip=True)
            text = text.replace(email, "").strip()
            name = normalize_name(text)
            if name:
                return name
    
    # Estrategia 2: Buscar en el padre y sus hermanos
    parent = node.parent
    if parent:
        # Buscar en elementos hermanos del padre
        for sibling in [parent.previous_sibling, parent.next_sibling]:
            if sibling and hasattr(sibling, 'get_text'):
                text = sibling.get_text(separator=" ", strip=True)
                text = text.replace(email, "").strip()
                name = normalize_name(text)
                if name:
                    return name
        
        # Buscar en el texto del padre
        text = parent.get_text(separator=" ", strip=True)
        text = text.replace(email, "").strip()
        name = normalize_name(text)
        if name:
            return name
    
    # Estrategia 3: Buscar en estructuras comunes (tablas, listas)
    # Buscar en la fila de tabla (td o th)
    if parent and parent.name in ['td', 'th']:
        row = parent.find_parent('tr')
        if row:
            cells = row.find_all(['td', 'th'])
            for cell in cells:
                text = cell.get_text(separator=" ", strip=True)
                if email not in text:  # No debe contener el email
                    name = normalize_name(text)
                    if name:
                        return name
    
    # Estrategia 4: Buscar en elementos li (listas)
    if parent and parent.name == 'li':
        text = parent.get_text(separator=" ", strip=True)
        text = text.replace(email, "").strip()
        name = normalize_name(text)
        if name:
            return name
    
    # Estrategia 5: Buscar en divs con clases comunes
    container = node.find_parent(['div', 'span', 'p'])
    if container:
        # Buscar texto antes del email en el mismo contenedor
        full_text = container.get_text(separator=" ", strip=True)
        email_pos = full_text.find(email)
        if email_pos > 0:
            before_email = full_text[:email_pos].strip()
            # Tomar las últimas 2-4 palabras antes del email
            words = before_email.split()[-4:]
            if len(words) >= 2:
                name = normalize_name(" ".join(words))
                if name:
                    return name
    
    # Estrategia 6: Recurrir al padre
    if parent:
        return extract_name_advanced(parent, email)
    
    return None


def extract_emails_from_text(text):
    """Extrae emails válidos de un texto"""
    if not text:
        return []
    emails = EMAIL_PATTERN.findall(text)
    return [clean_email(e) for e in emails if clean_email(e)]


def obtener_departamentos_con_nombres():
    """Obtiene lista de departamentos con sus nombres reales"""
    url = f"{BASE_URL}/conocenos/departamentos"
    soup = get_soup(url)
    if not soup:
        return []
    
    departamentos = []  # Lista de tuplas (url, nombre)
    
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        if "/Detalle/Organismo_C" in href:
            full_url = urljoin(BASE_URL, href)
            nombre = a.get_text(strip=True)
            
            # Validar que el nombre sea razonable
            if nombre and len(nombre) > 3 and len(nombre) < 100:
                departamentos.append((full_url, nombre))
    
    # Eliminar duplicados manteniendo el orden
    seen = set()
    unique_deps = []
    for url, nombre in departamentos:
        if url not in seen:
            seen.add(url)
            unique_deps.append((url, nombre))
    
    return unique_deps


def info_departamento(url):
    """Extrae email y web del departamento, limpiando el email en el momento"""
    soup = get_soup(url)
    if not soup:
        return None, None
    
    # Email del departamento (limpiado)
    mail = None
    m = soup.find("a", href=lambda x: x and x.startswith("mailto:"))
    if m:
        email_raw = m.get("href", "").replace("mailto:", "").strip()
        mail = clean_email(email_raw)
    
    # Web del departamento
    web = None
    for dt in soup.find_all("dt"):
        if "Web del departamento" in dt.get_text():
            dd = dt.find_next("dd")
            if dd:
                a = dd.find("a", href=True)
                if a:
                    web = urljoin(BASE_URL, a["href"])
    
    return mail, web


def crawl_personal(start_url, departamento_nombre, max_pages=50, seen_emails=None):
    """
    Crawler mejorado que extrae emails y nombres limpiados en tiempo real
    """
    if not start_url:
        return []
    
    if seen_emails is None:
        seen_emails = set()
    
    visited = set()
    to_visit = [start_url]
    domain = urlparse(start_url).netloc
    
    resultados = []
    
    print(f"  [Crawling] Iniciando en: {start_url}")
    
    while to_visit and len(visited) < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)
        
        soup = get_soup(url)
        if not soup:
            continue
        
        # Estrategia 1: Buscar enlaces mailto: (más confiable)
        for a in soup.find_all("a", href=True):
            if a["href"].startswith("mailto:"):
                email_raw = a["href"].replace("mailto:", "").strip()
                email = clean_email(email_raw)
                
                if email and email not in seen_emails:
                    seen_emails.add(email)
                    name = extract_name_advanced(a, email)
                    if name:
                        resultados.append((departamento_nombre, name, email))
                    else:
                        # Intentar inferir desde el email
                        name_inferred = infer_name_from_email(email)
                        resultados.append((departamento_nombre, name_inferred, email))
        
        # Estrategia 2: Buscar emails en texto plano
        page_text = soup.get_text()
        text_emails = extract_emails_from_text(page_text)
        
        for email in text_emails:
            if email not in seen_emails:
                seen_emails.add(email)
                # Buscar el nodo que contiene el email
                node = soup.find(string=lambda t: t and email in str(t))
                if node:
                    name = extract_name_advanced(node.parent, email)
                else:
                    name = None
                
                if not name:
                    name = infer_name_from_email(email)
                
                resultados.append((departamento_nombre, name, email))
        
        # Estrategia 3: Buscar en estructuras específicas (tablas de personal)
        # Buscar tablas que puedan contener información de personal
        tables = soup.find_all("table")
        for table in tables:
            rows = table.find_all("tr")
            for row in rows:
                cells = row.find_all(["td", "th"])
                if len(cells) >= 2:
                    # Asumir que el nombre está en la primera columna y email en otra
                    potential_name = None
                    potential_email = None
                    
                    for cell in cells:
                        text = cell.get_text(strip=True)
                        email_found = clean_email(text)
                        if email_found:
                            potential_email = email_found
                        elif not email_found and len(text.split()) >= 2:
                            potential_name = normalize_name(text)
                    
                    if potential_email and potential_email not in seen_emails:
                        seen_emails.add(potential_email)
                        name = potential_name if potential_name else infer_name_from_email(potential_email)
                        resultados.append((departamento_nombre, name, potential_email))
        
        # Añadir enlaces internos relevantes
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            text = a.get_text(strip=True).lower()
            
            # Priorizar enlaces que parecen ser de personal
            keywords = ["personal", "profesor", "profesores", "plantilla", "staff", 
                       "miembros", "equipo", "directorio", "listado"]
            
            is_relevant = any(kw in text or kw in href.lower() for kw in keywords)
            
            new_url = urljoin(url, href)
            parsed = urlparse(new_url)
            
            # Solo enlaces del mismo dominio
            if parsed.netloc == domain and new_url not in visited:
                if is_relevant or len(visited) < 10:  # Priorizar relevantes o primeros 10
                    to_visit.append(new_url)
        
        time.sleep(0.3)  # Pausa más corta pero respetuosa
    
    print(f"  [Crawling] Completado: {len(resultados)} emails encontrados en {len(visited)} páginas")
    return resultados


def infer_name_from_email(email):
    """Intenta inferir el nombre del profesor desde su email"""
    if not email:
        return None
    
    # Patrón común: nombre.apellido@uc3m.es
    local_part = email.split("@")[0]
    
    # Dividir por puntos
    parts = local_part.split(".")
    
    if len(parts) >= 2:
        # Tomar las primeras partes como nombre y apellido
        name_parts = []
        for part in parts[:3]:  # Máximo 3 partes
            if part and len(part) > 2:
                name_parts.append(part.capitalize())
        
        if len(name_parts) >= 2:
            return " ".join(name_parts)
    
    return None


def guardar_csv(data, filename="profesores_uc3m_v4.csv"):
    """Guarda los datos en CSV con encoding UTF-8"""
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["departamento", "profesor", "email"])
        writer.writerows(data)


def main():
    """Función principal mejorada"""
    print("=" * 60)
    print("SCRAPER UC3M - VERSIÓN 4 (MEJORADA)")
    print("=" * 60)
    
    # Obtener departamentos con nombres
    print("\n[1/4] Obteniendo lista de departamentos...")
    departamentos = obtener_departamentos_con_nombres()
    print(f"✓ Encontrados {len(departamentos)} departamentos")
    
    if not departamentos:
        print("[ERROR] No se encontraron departamentos. Abortando.")
        return
    
    # Conjunto global para evitar duplicados entre departamentos
    seen_emails_global = set()
    datos_finales = []
    estadisticas = defaultdict(int)
    
    print("\n[2/4] Procesando departamentos...")
    for idx, (dep_url, dep_nombre) in enumerate(departamentos, 1):
        print(f"\n[{idx}/{len(departamentos)}] {dep_nombre}")
        print(f"  URL: {dep_url}")
        
        # Obtener info del departamento
        email_dep, web_dep = info_departamento(dep_url)
        
        if email_dep:
            print(f"  Email dep: {email_dep}")
            if email_dep not in seen_emails_global:
                seen_emails_global.add(email_dep)
                datos_finales.append((dep_nombre, "Departamento", email_dep))
                estadisticas['emails_departamento'] += 1
        
        if web_dep:
            print(f"  Web dep: {web_dep}")
            # Scraping del personal
            data_personal = crawl_personal(web_dep, dep_nombre, 
                                          max_pages=50, 
                                          seen_emails=seen_emails_global)
            datos_finales.extend(data_personal)
            estadisticas['emails_personal'] += len(data_personal)
        else:
            print("  ⚠ No se encontró web del departamento")
            estadisticas['sin_web'] += 1
        
        time.sleep(0.5)  # Pausa entre departamentos
    
    print("\n[3/4] Eliminando duplicados finales...")
    # Eliminar duplicados finales (por si acaso)
    emails_vistos = set()
    datos_unicos = []
    for dep, prof, email in datos_finales:
        if email and email not in emails_vistos:
            emails_vistos.add(email)
            datos_unicos.append((dep, prof, email))
    
    duplicados_eliminados = len(datos_finales) - len(datos_unicos)
    if duplicados_eliminados > 0:
        print(f"  ✓ Eliminados {duplicados_eliminados} duplicados adicionales")
    
    print("\n[4/4] Guardando resultados...")
    guardar_csv(datos_unicos, "profesores_uc3m_v4.csv")
    
    # Estadísticas finales
    print("\n" + "=" * 60)
    print("ESTADÍSTICAS FINALES")
    print("=" * 60)
    print(f"Total registros: {len(datos_unicos)}")
    print(f"Emails únicos: {len(emails_vistos)}")
    
    # Contar profesores con nombre
    con_nombre = sum(1 for _, prof, _ in datos_unicos if prof and prof != "Departamento")
    sin_nombre = len(datos_unicos) - con_nombre
    print(f"Profesores con nombre: {con_nombre}")
    print(f"Profesores sin nombre: {sin_nombre}")
    print(f"Departamentos procesados: {len(departamentos)}")
    print(f"Departamentos sin web: {estadisticas['sin_web']}")
    print(f"Emails de departamentos: {estadisticas['emails_departamento']}")
    print(f"Emails de personal: {estadisticas['emails_personal']}")
    
    # Distribución por departamento
    dept_counts = defaultdict(int)
    for dep, _, _ in datos_unicos:
        dept_counts[dep] += 1
    
    print(f"\nTop 5 departamentos con más emails:")
    for dep, count in sorted(dept_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f"  {dep}: {count} emails")
    
    print(f"\n✓ CSV generado: profesores_uc3m_v4.csv")
    print("=" * 60)


if __name__ == "__main__":
    main()

