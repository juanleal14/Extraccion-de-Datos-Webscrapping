#!/usr/bin/env python3
import requests
from bs4 import BeautifulSoup
import csv, os, time, re
from urllib.parse import urljoin, urlparse

BASE = "https://www.uc3m.es"
DEPT_LIST_URL = "https://www.uc3m.es/conocenos/departamentos"
DIRECTORIO_URL = "https://www.uc3m.es/directorio"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")

def get_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"[ERROR] No se pudo cargar {url}: {e}")
        return None


# 1) Encontrar enlaces a departamentos
def get_departments():
    print(f"[INFO] Cargando HTML desde {DEPT_LIST_URL}")
    soup = get_html(DEPT_LIST_URL)
    if not soup:
        return []

    dept_links = {}
    
    # Lista de nombres conocidos de departamentos
    dept_keywords = [
        'Biblioteconomía', 'Bioingeniería', 'Ciencia e Ingeniería', 'Ciencias Sociales',
        'Comunicación', 'Derecho Internacional', 'Derecho Penal', 'Derecho Privado',
        'Derecho Público', 'Derecho Social', 'Economía', 'Estadística', 'Física',
        'Humanidades', 'Informática', 'Ingeniería Aeroespacial', 'Ingeniería de Sistemas',
        'Ingeniería Eléctrica', 'Ingeniería Mecánica', 'Ingeniería Telemática',
        'Ingeniería Térmica', 'Matemáticas', 'Mecánica de Medios', 'Neurociencia',
        'Tecnología Electrónica', 'Teoría de la Señal'
    ]

    print("[INFO] Buscando enlaces de departamentos...")
    
    for a in soup.find_all("a", href=True):
        href = a.get("href", "")
        text = a.get_text(" ", strip=True)
        
        # Buscar enlaces que contengan nombres de departamentos
        if any(keyword.lower() in text.lower() for keyword in dept_keywords) and text:
            full_url = urljoin(BASE, href)
            # Evitar duplicados y enlaces no válidos
            if full_url.startswith(BASE) and text not in dept_links.values():
                dept_links[full_url] = text

    print(f"[INFO] Departamentos detectados: {len(dept_links)}")
    return list(dept_links.items())


# 2) Encontrar página de personal dentro de cada departamento

def find_personal_pages(dept_url, dept_name):
    """Busca páginas de personal usando múltiples estrategias"""
    personal_urls = []
    visited = set()
    
    # Estrategia 1: Buscar enlaces en la página del departamento
    soup = get_html(dept_url)
    if soup:
        for a in soup.find_all("a", href=True):
            text = a.get_text(" ", strip=True).lower()
            href = a.get("href", "").lower()
            
            keywords = ["personal", "profesorado", "profesor", "profesores", 
                       "titular", "asociado", "plantilla", "staff", "miembros",
                       "listado", "directorio", "equipo"]
            
            if any(k in text or k in href for k in keywords):
                full_url = urljoin(dept_url, a["href"])
                if full_url.startswith(BASE) and full_url not in visited:
                    personal_urls.append(full_url)
                    visited.add(full_url)
        
        # También buscar todos los enlaces internos que puedan ser relevantes
        for a in soup.find_all("a", href=True):
            href = a.get("href", "")
            full_url = urljoin(dept_url, href)
            if (full_url.startswith(BASE) and 
                full_url not in visited and
                ("satellite" in href.lower() or "departamento" in href.lower() or 
                 "personal" in href.lower() or "listado" in href.lower())):
                # Verificar que no sea un enlace genérico
                if len(href) > 20:  # URLs largas suelen ser más específicas
                    personal_urls.append(full_url)
                    visited.add(full_url)
    
    # Estrategia 2: Intentar patrones conocidos de URL basados en el nombre del departamento
    dept_short = dept_name.lower().replace(" ", "").replace(":", "").replace(",", "").replace("é", "e").replace("á", "a")
    
    # Mapeo de nombres de departamentos a patrones conocidos
    dept_patterns = {
        "informatica": ["DeptInformatica"],
        "matematicas": ["DeptMatematicas"],
        "cienciasingenieriademateriales": ["DeptCienIngMatIngQuim"],
        "cienciassociales": ["departamento-ciencias-sociales"],
        "economia": ["DeptEconomia"],
        "fisica": ["DeptFisica"],
        "derecho": ["DeptDerecho"],
    }
    
    patterns = []
    for key, dept_codes in dept_patterns.items():
        if key in dept_short:
            for code in dept_codes:
                if "Dept" in code:
                    patterns.extend([
                        f"{BASE}/ss/Satellite/{code}/es/ListadoPersonalDept/1371321047774/Profesores_Titulares",
                        f"{BASE}/ss/Satellite/{code}/es/ListadoPersonalDept/1371321047774/Profesores_Asociados",
                        f"{BASE}/ss/Satellite/{code}/es/ListadoPersonalDept/1371321047774/Profesores_permanentes",
                    ])
                else:
                    patterns.extend([
                        f"{BASE}/{code}/personal-tiempo-completo",
                        f"{BASE}/{code}/personal-asociado",
                    ])
            break
    
    # Probar los patrones
    for pattern_url in patterns:
        if pattern_url not in visited:
            test_soup = get_html(pattern_url)
            if test_soup:
                # Verificar que la página tiene contenido relevante
                text = test_soup.get_text().lower()
                if any(k in text for k in ["profesor", "personal", "email", "@", "correo"]):
                    personal_urls.append(pattern_url)
                    visited.add(pattern_url)
                    time.sleep(0.3)
    
    # Estrategia 3: Si no encontramos nada, usar la misma URL del departamento
    if not personal_urls:
        personal_urls.append(dept_url)
    
    return list(set(personal_urls))  # Eliminar duplicados

# 3) Extraer nombres y correos de una página
def extract_contacts_from_page(page_url, dept_name):
    """Extrae nombres y correos de una página, intentando asociarlos"""
    soup = get_html(page_url)
    if not soup:
        return []
    
    results = []
    raw_text = soup.get_text(" ")
    
    # Buscar tablas con información de personal
    tables = soup.find_all("table")
    for table in tables:
        rows = table.find_all("tr")
        for row in rows:
            cells = row.find_all(["td", "th"])
            if len(cells) >= 2:
                text_content = " ".join([cell.get_text(" ", strip=True) for cell in cells])
                emails = EMAIL_RE.findall(text_content)
                
                # Intentar extraer nombre (generalmente está antes del email)
                for email in emails:
                    name = ""
                    for cell in cells:
                        cell_text = cell.get_text(" ", strip=True)
                        if email.lower() in cell_text.lower():
                            # El nombre podría estar en la misma celda o en una anterior
                            parts = cell_text.split(email)
                            if parts[0].strip():
                                name = parts[0].strip()
                            else:
                                # Buscar en celdas anteriores
                                cell_idx = cells.index(cell)
                                if cell_idx > 0:
                                    name = cells[cell_idx - 1].get_text(" ", strip=True)
                            break
                    
                    if not name:
                        # Si no encontramos nombre, usar texto antes del email
                        email_idx = text_content.lower().find(email.lower())
                        if email_idx > 0:
                            name = text_content[:email_idx].strip()
                            # Limpiar el nombre
                            name = re.sub(r'\s+', ' ', name)
                            name = name.split('\n')[0].strip()
                    
                    # Limpiar nombre
                    if name:
                        name = re.sub(r'[^\w\s\-.,]', '', name).strip()
                        if len(name) > 2 and len(name) < 100:
                            results.append({
                                "nombre": name,
                                "correo": email.lower(),
                                "departamento": dept_name,
                            })
    
    # Buscar en listas (ul, ol)
    if not results or len(results) < 5:  # Si encontramos pocos, buscar más
        lists = soup.find_all(["ul", "ol"])
        for list_elem in lists:
            items = list_elem.find_all("li")
            for item in items:
                item_text = item.get_text(" ", strip=True)
                emails = EMAIL_RE.findall(item_text)
                for email in emails:
                    # Buscar nombre en el mismo item
                    email_pos = item_text.lower().find(email.lower())
                    if email_pos > 0:
                        name_part = item_text[:email_pos].strip()
                        # Limpiar
                        name_part = re.sub(r'[^\w\s\-.,]', '', name_part).strip()
                        if 2 < len(name_part) < 100 and '@' not in name_part:
                            results.append({
                                "nombre": name_part,
                                "correo": email.lower(),
                                "departamento": dept_name,
                            })
    
    # Buscar en divs con clases comunes de perfiles
    profile_selectors = [
        "div.person", "div.profile", "div.staff-member", "div.contact",
        "div[class*='person']", "div[class*='profile']", "div[class*='staff']",
        "div[class*='member']", "div[class*='faculty']"
    ]
    
    for selector in profile_selectors:
        try:
            elements = soup.select(selector)
            for elem in elements:
                elem_text = elem.get_text(" ", strip=True)
                emails = EMAIL_RE.findall(elem_text)
                for email in emails:
                    # Buscar nombre (generalmente en h2, h3, h4, o strong dentro del elemento)
                    name = ""
                    for tag in elem.find_all(["h1", "h2", "h3", "h4", "h5", "strong", "b"]):
                        tag_text = tag.get_text(" ", strip=True)
                        if '@' not in tag_text and 2 < len(tag_text) < 100:
                            name = tag_text
                            break
                    
                    if not name:
                        # Buscar texto antes del email
                        email_pos = elem_text.lower().find(email.lower())
                        if email_pos > 0:
                            name = elem_text[:email_pos].strip()
                            name = re.sub(r'[^\w\s\-.,]', '', name).strip()
                            # Tomar las últimas palabras
                            words = name.split()
                            if len(words) > 4:
                                name = " ".join(words[-4:])
                    
                    if name and 2 < len(name) < 100:
                        results.append({
                            "nombre": name,
                            "correo": email.lower(),
                            "departamento": dept_name,
                        })
        except:
            continue
    
    # Si aún no encontramos suficientes, buscar en todo el texto
    if not results:
        all_text = soup.get_text(" ")
        emails = EMAIL_RE.findall(all_text)
        
        for email in emails:
            email_lower = email.lower()
            text_lower = all_text.lower()
            email_pos = text_lower.find(email_lower)
            
            if email_pos > 0:
                # Buscar nombre antes del email (hasta 300 caracteres antes)
                context_before = all_text[max(0, email_pos - 300):email_pos]
                # Buscar líneas que puedan contener nombres
                lines = context_before.split('\n')
                name = ""
                for line in reversed(lines[-8:]):  # Últimas 8 líneas antes del email
                    line = line.strip()
                    if line and 2 < len(line) < 100:
                        # Verificar si parece un nombre
                        if ('@' not in line and 
                            not line.replace('.', '').replace(',', '').replace('-', '').isdigit() and
                            not line.lower().startswith(('email', 'correo', 'tel', 'phone', 'fax'))):
                            name = re.sub(r'[^\w\s\-.,]', '', line).strip()
                            if 2 < len(name) < 100:
                                break
                
                if not name:
                    # Extraer texto justo antes del email
                    before_email = context_before[-80:].strip()
                    before_email = re.sub(r'\s+', ' ', before_email)
                    parts = before_email.split()
                    if parts:
                        # Tomar las últimas 2-4 palabras como posible nombre
                        name = " ".join(parts[-4:]) if len(parts) >= 4 else " ".join(parts)
                        name = re.sub(r'[^\w\s\-.,]', '', name).strip()
                
                if name and 2 < len(name) < 100:
                    results.append({
                        "nombre": name,
                        "correo": email.lower(),
                        "departamento": dept_name,
                    })
    
    # Eliminar duplicados por email
    seen_emails = set()
    unique_results = []
    for r in results:
        if r["correo"] not in seen_emails:
            seen_emails.add(r["correo"])
            unique_results.append(r)
    
    return unique_results


# 4) Buscar también en el directorio general
def extract_from_directorio():
    """Extrae contactos del directorio general de la universidad"""
    print(f"\n[INFO] Buscando en directorio general: {DIRECTORIO_URL}")
    results = []
    
    soup = get_html(DIRECTORIO_URL)
    if soup:
        contacts = extract_contacts_from_page(DIRECTORIO_URL, "Varios")
        results.extend(contacts)
        print(f"  [DIRECTORIO] Encontrados {len(contacts)} contactos")
    
    return results


# MAIN
def main():
    os.makedirs("uc3m", exist_ok=True)

    # Obtener departamentos
    departamentos = get_departments()
    if not departamentos:
        print("[ERROR] No se encontraron departamentos")
        return
    
    all_contacts = {}  # email -> contacto (para evitar duplicados)
    
    print(f"\n[INFO] Procesando {len(departamentos)} departamentos...\n")
    
    for dept_url, dept_name in departamentos:
        print(f"\n[DEPARTAMENTO] {dept_name}")
        print(f"  URL: {dept_url}")
        
        # Buscar páginas de personal
        personal_pages = find_personal_pages(dept_url, dept_name)
        print(f"  Páginas de personal encontradas: {len(personal_pages)}")
        
        for personal_url in personal_pages:
            print(f"    Procesando: {personal_url[:80]}...")
            contacts = extract_contacts_from_page(personal_url, dept_name)
            
            for contact in contacts:
                email = contact["correo"]
                # Si ya existe, actualizar si el nuevo tiene más información
                if email not in all_contacts:
                    all_contacts[email] = contact
                elif not all_contacts[email]["nombre"] and contact["nombre"]:
                    all_contacts[email] = contact
            
            print(f"    Contactos encontrados: {len(contacts)}")
            time.sleep(0.5)  # Respetar el servidor
    
    # También buscar en el directorio general
    directorio_contacts = extract_from_directorio()
    for contact in directorio_contacts:
        email = contact["correo"]
        if email not in all_contacts:
            all_contacts[email] = contact
    
    # Guardar CSV
    out = "uc3m/profesores_uc3m.csv"
    with open(out, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=["nombre", "correo", "departamento"]
        )
        writer.writeheader()
        for contact in sorted(all_contacts.values(), key=lambda x: x["correo"]):
            writer.writerow(contact)
    
    print(f"\n[FIN] Total contactos únicos: {len(all_contacts)}")
    print(f"[CSV] Guardado en: {out}")
    
    # Estadísticas
    with_names = sum(1 for c in all_contacts.values() if c["nombre"])
    print(f"[ESTADÍSTICAS]")
    print(f"  - Con nombre: {with_names}")
    print(f"  - Sin nombre: {len(all_contacts) - with_names}")

if __name__ == "__main__":
    main()
