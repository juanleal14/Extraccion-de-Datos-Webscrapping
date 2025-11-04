# Extraccion-de-Datos-Webscrapping
Código actual mejorado: main3.py
## Requisitos
1.
```
sudo apt-get update
sudo apt-get install -y wget gnupg
wget -qO- https://dl.google.com/linux/linux_signing_key.pub | sudo gpg --dearmor -o /usr/share/keyrings/google-linux.gpg
echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-linux.gpg] http://dl.google.com/linux/chrome/deb/ stable main" | sudo tee /etc/apt/sources.list.d/google-chrome.list
sudo apt-get update && sudo apt-get install -y google-chrome-stable
export CHROME_BINARY=/usr/bin/google-chrome  # ajusta si es /usr/bin/google-chrome-stable
```
2.
```
pip install "selenium>=4.13" beautifulsoup4 unidecode pandas tldextract requests
```