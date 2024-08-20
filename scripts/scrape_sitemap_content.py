import requests
import logging
import json
import anthropic
import time
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
import re
import random
from logging.handlers import RotatingFileHandler


# Nastavení loggeru
log_file = 'logs/scraper.log'
os.makedirs(os.path.dirname(log_file), exist_ok=True)
logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        RotatingFileHandler(log_file, maxBytes=10000000, backupCount=5),
                        logging.StreamHandler()
                    ])
logger = logging.getLogger(__name__)

# Přepínač pro kontrolu data modifikace
CHECK_MODIFIED_DATE = False  # Nastavte na False pro vypnutí kontroly data modifikace

# API klíče a konstanty
CLAUDE_API_KEY = ""
FIRECRAWL_API_KEY = ""
VOICEFLOW_API_KEY = ""

# Definice sekcí a jejich sitemapů
SECTIONS = {
    "Services": ["https://icuk.cz/pro-firmy-sitemap.xml", "https://icuk.cz/pro-region-sitemap.xml", "https://icuk.cz/pro-skoly-sitemap.xml"],
    "References": ["https://icuk.cz/reference-sitemap.xml"],
    "SuccessStories": ["https://icuk.cz/success-story-sitemap.xml"],
    "Events": ["https://icuk.cz/udalost-sitemap.xml"],
    "Articles": ["https://icuk.cz/post-sitemap.xml"]
}

def create_directories():
    directories = ['logs', 'payloads']
    for directory in directories:
        if not os.path.exists(directory):
            os.makedirs(directory)
            logger.info(f"Vytvořena složka: {directory}")
        else:
            logger.info(f"Složka {directory} již existuje")

def parse_sitemap(sitemap_url):
    logger.info(f"Parsování sitemapy: {sitemap_url}")
    try:
        response = requests.get(sitemap_url)
        response.raise_for_status()
        logger.debug(f"Sitemap response: {response.text[:500]}...")  # Log first 500 characters
        root = ET.fromstring(response.content)
        
        urls = []
        for url in root.findall('.//{http://www.sitemaps.org/schemas/sitemap/0.9}url'):
            loc = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}loc').text
            lastmod = url.find('{http://www.sitemaps.org/schemas/sitemap/0.9}lastmod')
            lastmod = lastmod.text if lastmod is not None else None
            
            urls.append({
                'url': loc,
                'lastmod': lastmod
            })
        
        logger.debug(f"Parsed URLs: {urls}")
        return urls
    except Exception as e:
        logger.error(f"Chyba při parsování sitemapy {sitemap_url}: {str(e)}")
        return []

def get_html_content(url):
    logger.info(f"Získávání HTML obsahu z URL: {url}")
    api_url = "https://api.firecrawl.dev/v0/scrape"
    payload = {
        "url": url,
        "pageOptions": {
            "includeHtml": True,
            "includeRawHtml": True,
            "replaceAllPathsWithAbsolutePaths": True
        },
        "extractorOptions": {
            "mode": "markdown"
        }
    }
    headers = {
        "Authorization": f"Bearer {FIRECRAWL_API_KEY}",
        "Content-Type": "application/json"
    }
    
    logger.debug(f"Firecrawl API Request: {json.dumps(payload, indent=2)}")
    
    try:
        response = requests.post(api_url, json=payload, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        logger.debug(f"Firecrawl API Response: {json.dumps(data, indent=2)}")  # Log full response
        
        if data.get('success'):
            markdown_content = data.get('data', {}).get('markdown')
            metadata = data.get('data', {}).get('metadata', {})
            if markdown_content:
                logger.debug(f"Markdown content: {markdown_content[:1000]}...")  # Log first 1000 characters
                return markdown_content, metadata
            else:
                logger.error("Markdown obsah nebyl nalezen v odpovědi API")
                raise ValueError("Markdown obsah chybí v odpovědi API")
        else:
            error_message = data.get('data', {}).get('warning', 'Neznámá chyba')
            logger.error(f"Chyba při získávání obsahu: {error_message}")
            raise ValueError(f"Chyba API: {error_message}")
    except requests.RequestException as e:
        logger.error(f"Chyba při volání Firecrawl API: {str(e)}")
        raise

def preprocess_markdown(markdown_content):
    # Remove control characters
    markdown_content = ''.join(ch for ch in markdown_content if ord(ch) >= 32)
    # Escape quotation marks
    markdown_content = markdown_content.replace('"', '\\"')
    return markdown_content

def extract_json_from_response(response_text):
    json_match = re.search(r'\[[\s\S]*\]', response_text)
    if json_match:
        try:
            return json.loads(json_match.group())
        except json.JSONDecodeError:
            logger.error("Nalezená JSON struktura není validní.")
    logger.error("Nebyla nalezena žádná validní JSON struktura.")
    return None

def convert_to_qa(markdown_content, max_retries=3, base_delay=1):
    client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
    
    preprocessed_content = preprocess_markdown(markdown_content)
    
    prompt = f"""Převeďte následující markdown obsah na pole otázek a odpovědí ve formátu JSON.

Instrukce:
1. Analyzujte poskytnutý markdown obsah.
2. Vytvořte otázky a odpovědi pokrývající klíčové informace z obsahu.
3. Formátujte každý pár otázka-odpověď jako JSON objekt.
4. Kombinujte všechny objekty do JSON pole.
5. Vynechtejte Q/As, které obsahují jakýkoli zdrojový kód, cookies, footer, header, gdpr informace.

Očekávaný formát výstupu:
[
  {{
    "Question": "Otázka v češtině?",
    "Answer": "Odpověď v češtině, může obsahovat **tučný text**, *kurzívu*, nebo [odkazy](https://example.com)."
  }},
  {{
    "Question": "Další otázka v češtině?",
    "Answer": "Další odpověď v češtině. Může obsahovat odrážky:\\n* První bod\\n* Druhý bod"
  }}
]

Markdown obsah k zpracování:

{preprocessed_content}

Poskytněte pouze JSON pole bez jakéhokoli dalšího textu nebo vysvětlení."""

    logger.debug(f"Claude API Prompt (first 500 characters): {prompt[:500]}...")
    
    for attempt in range(max_retries):
        try:
            message = client.messages.create(
                model="claude-3-5-sonnet-20240620",
                max_tokens=4000,
                temperature=0,
                messages=[
                    {"role": "user", "content": prompt}
                ]
            )
            
            response_text = message.content[0].text.strip()
            logger.debug(f"Claude API Response (first 500 characters): {response_text[:500]}...")
            
            qa_json = extract_json_from_response(response_text)
            if qa_json:
                logger.debug(f"Parsed Q&A JSON (first 500 characters): {json.dumps(qa_json, indent=2, ensure_ascii=False)[:500]}...")
                return qa_json
            else:
                raise ValueError("Nepodařilo se získat validní JSON odpověď")
        
        except (anthropic.APIError, anthropic.APIConnectionError, ValueError) as e:
            if attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Pokus {attempt + 1} selhal. Opakování za {delay:.2f} sekund...")
                time.sleep(delay)
            else:
                logger.error(f"Všech {max_retries} pokusů selhalo. Poslední chyba: {str(e)}")
                return []
    
    logger.error("Všechny pokusy o získání odpovědi selhaly")
    return []

def convert_to_event(markdown_content):
    client = anthropic.Anthropic(api_key=CLAUDE_API_KEY)
    
    prompt = f"""Extrahujte následující informace z daného markdown obsahu a formátujte je jako JSON objekt.

Požadovaná struktura:
{{
    "Name": "Název události v češtině",
    "RegistrationURL": "URL pro registraci na událost",
    "TimeFrom": "Čas začátku události v češtině",
    "TimeTo": "Čas konce události v češtině",
    "Place": "Místo konání události v češtině",
    "Summary": "Stručné shrnutí nebo popis události v češtině",
    "Speakers": ["Seznam řečníků na události v češtině"],
    "Images": ["URL adresy obrázků spojené s událostí"]
}}

Pokud některá informace není k dispozici, použijte null.

Markdown obsah k zpracování:

{markdown_content}

Poskytněte pouze JSON objekt bez jakéhokoli dalšího textu nebo vysvětlení."""

    logger.debug(f"Claude API Event Prompt: {prompt[:500]}...")
    
    message = client.messages.create(
        model="claude-3-5-sonnet-20240620",
        max_tokens=1000,
        temperature=0,
        messages=[
            {"role": "user", "content": prompt}
        ]
    )
    
    response_text = message.content[0].text.strip()
    logger.debug(f"Claude API Event Response: {response_text[:500]}...")
    
    event_json = extract_json_from_response(response_text)
    if event_json:
        logger.debug(f"Parsed Event JSON: {json.dumps(event_json, indent=2, ensure_ascii=False)}")
        return [event_json]
    else:
        logger.error("Nepodařilo se najít JSON strukturu v odpovědi pro událost")
        return []

def save_payload_to_file(url, content, section, metadata):
    og_title = metadata.get('ogTitle', '')
    if not og_title:
        og_title = url.replace('/', '_').replace(':', '_')
    else:
        og_title = og_title.replace(' ', '_').replace('/', '_').replace(':', '_')
    
    filename = f"payloads/{section.lower()}_{og_title}_payload.json"
    payload = {
        "data": {
            "schema": {
                "searchableFields": ["Question", "Answer"] if section != "Events" else ["Name", "Summary"]
            },
            "name": f"{section.lower()}_{og_title}",
            "tags": [section],
            "items": content
        }
    }
    with open(filename, 'w', encoding='utf-8') as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    logger.info(f"Uložen payload pro URL '{url}' do souboru: {filename}")
    logger.debug(f"Payload content: {json.dumps(payload, indent=2, ensure_ascii=False)}")
    print(f"Vytvořen nový payload: {filename}")
    return filename

def upload_to_voiceflow(filename):
    logger.info(f"Nahrávání souboru '{filename}' do Voiceflow")
    url = 'https://api.voiceflow.com/v1/knowledge-base/docs/upload/table?overwrite=true'
    headers = {
        'Authorization': VOICEFLOW_API_KEY,
        'accept': 'application/json',
        'content-type': 'application/json'
    }
    
    with open(filename, 'r', encoding='utf-8') as f:
        payload = json.load(f)
    
    logger.debug(f"Voiceflow Upload Request: {json.dumps(payload, indent=2)}")
    
    response = requests.post(url, headers=headers, json=payload)
    
    logger.debug(f"Voiceflow Upload Response: {response.text}")
    
    if response.status_code == 200:
        logger.info(f"Úspěšně nahráno {len(payload['data']['items'])} položek pro soubor '{filename}'")
    else:
        logger.error(f"Chyba při nahrávání souboru '{filename}': {response.text}")

def is_url_modified(lastmod):
    if not CHECK_MODIFIED_DATE:
        return True  # Pokud je kontrola vypnutá, vždy vrátíme True
    
    if not lastmod:
        return True  # Pokud není k dispozici datum poslední modifikace, zpracujeme URL
    
    try:
        lastmod_date = datetime.strptime(lastmod, "%Y-%m-%dT%H:%M:%S%z")
        current_date = datetime.now(timezone.utc)
        return lastmod_date > current_date
    except ValueError:
        logger.error(f"Neplatný formát data poslední modifikace: {lastmod}")
        return True  # V případě chyby zpracujeme URL pro jistotu

def main():
    create_directories()
    
    start_time = datetime.now()
    logger.info(f"Začátek zpracování: {start_time}")

    for section, sitemaps in SECTIONS.items():
        for sitemap_url in sitemaps:
            try:
                urls = parse_sitemap(sitemap_url)
                
                for url_data in urls:
                    url = url_data['url']
                    lastmod = url_data['lastmod']
                    
                    if not is_url_modified(lastmod):
                        logger.info(f"Přeskakuji URL {url}, nebyla modifikována od posledního zpracování")
                        continue
                    
                    try:
                        markdown_content, metadata = get_html_content(url)
                        
                        if not markdown_content:
                            logger.warning(f"Prázdný markdown obsah pro URL {url}, přeskakuji")
                            continue
                        
                        if section == "Events":
                            content = convert_to_event(markdown_content)
                        else:
                            content = convert_to_qa(markdown_content)
                        
                        if content:
                            payload_file = save_payload_to_file(url, content, section, metadata)
                            upload_to_voiceflow(payload_file)
                        else:
                            logger.warning(f"Prázdný obsah po konverzi pro URL {url}, přeskakuji")
                        
                        time.sleep(60)  # 1 minuta pauza mezi zpracováním URL
                    except Exception as e:
                        logger.error(f"Chyba při zpracování URL {url}: {str(e)}", exc_info=True)
            except Exception as e:
                logger.error(f"Chyba při zpracování sitemapy {sitemap_url}: {str(e)}", exc_info=True)
    
    end_time = datetime.now()
    logger.info(f"Konec zpracování: {end_time}")
    logger.info(f"Celková doba zpracování: {end_time - start_time}")

if __name__ == "__main__":
    main()
