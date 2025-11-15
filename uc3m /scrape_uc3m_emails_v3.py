import requests
from bs4 import BeautifulSoup
import re
import time
import csv
from urllib.parse import urljoin, urlparse

BASE_URL = "https://www.uc3m.es"

def get_soup(url):
    try:
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return BeautifulSoup(r.text, "html.parser")
    except Exception as e:
        print(f"[ERROR] No se pudo acceder a {url}: {e}")
        return None


def extract_emails(text):
    if not text:
        return []
    pattern = r"[a-zA-Z0-9._%+-]+@(?:[a-zA-Z0-9.-]+\.)+[a-zA-Z]{2,}"
    return re.findall(pattern, text)


def extract_name_from_node(node, email):
    """Intenta obtener el nombre desde el nodo o padres cercanos."""
    if node is None:
        return None

    # Texto sin el email
    text = node.get_text(separator=" ", strip=True)
    text = text.replace(email, "").strip()

    # Heurísticas: evitar textos demasiado largos o irrelevantes
    if 2 < len(text.split()) <= 8:
        return text

    # Subir al padre y reintentar
    return extract_name_from_node(node.parent, email)


# 1. Obtener lista de departamentos

def obtener_departamentos():
    url = "https://www.uc3m.es/conocenos/departamentos"
    soup = get_soup(url)
    if not soup:
        return []

    departamentos = []

    for a in soup.find_all("a", href=True):
        if "/Detalle/Organismo_C" in a["href"]:
            departamentos.append(urljoin(BASE_URL, a["href"]))

    return list(set(departamentos))



# 2. Extraer email del departamento + web del departamento

def info_departamento(url):
    soup = get_soup(url)
    if not soup:
        return None, None

    # Email del departamento
    mail = None
    m = soup.find("a", href=lambda x: x and x.startswith("mailto:"))
    if m:
        mail = m.get_text(strip=True)

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


# --------------------------------------------------------------
# 3. Crawler de la web del departamento para extraer emails + nombres
# --------------------------------------------------------------

def crawl_personal(start_url, departamento, max_pages=40):
    if not start_url:
        return []

    visited = set()
    to_visit = [start_url]
    domain = urlparse(start_url).netloc

    resultados = []

    while to_visit and len(visited) < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)

        soup = get_soup(url)
        if not soup:
            continue

        # Buscar correos en nodos con nombres cercanos
        for a in soup.find_all("a", href=True):
            if a["href"].startswith("mailto:"):
                email = a["href"].replace("mailto:", "").strip()
                name = extract_name_from_node(a, email)
                resultados.append((departamento, name, email))

        # También buscar correos en texto
        text_emails = extract_emails(soup.get_text())
        for email in text_emails:
            if not any(email == r[2] for r in resultados):
                # Intentar localizar el nodo contenedor
                node = soup.find(string=lambda t: email in t)
                if node:
                    name = extract_name_from_node(node.parent, email)
                else:
                    name = None

                resultados.append((departamento, name, email))

        # Añadir enlaces internos
        for a in soup.find_all("a", href=True):
            new_url = urljoin(url, a["href"])
            if urlparse(new_url).netloc == domain:
                if new_url not in visited:
                    to_visit.append(new_url)

        time.sleep(0.5)

    return resultados



# 4. Guardar CSV

def guardar_csv(data, filename="profesores_uc3m_v3.csv"):
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["departamento", "profesor", "email"])
        writer.writerows(data)



def main():
    departamentos = obtener_departamentos()
    print(f"Encontrados {len(departamentos)} departamentos.")

    datos_finales = []

    for dep_url in departamentos:
        print("\n====================================================")
        print("Procesando departamento:", dep_url)

        email_dep, web_dep = info_departamento(dep_url)
        print("Email dep:", email_dep, " | Web:", web_dep)

        # Extraer nombre del departamento (último fragmento de la URL)
        nombre_departamento = dep_url.split("/")[-2]

        # Registrar email del departamento
        if email_dep:
            datos_finales.append((nombre_departamento, "Departamento", email_dep))

        # Scraping del personal
        if web_dep:
            data_personal = crawl_personal(web_dep, nombre_departamento)
            datos_finales.extend(data_personal)

    # Guardar CSV
    guardar_csv(datos_finales)
    print("\nCSV generado: profesores_uc3m_v3.csv")


if __name__ == "__main__":
    main()

