"""
Google Maps Lead Scraper — Focado em empresas com WhatsApp
Autor: Senior Software Engineer
Stack: Python + Playwright
"""

import asyncio
import csv
import random
import re
import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
import urllib.request
import urllib.error
import json
from dotenv import load_dotenv

load_dotenv()

from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeout

# ─── Configuração de Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("scraper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

# ─── Constantes ─────────────────────────────────────────────────────────────
OUTPUT_FILE = "leads_prospeccao.csv"
CSV_HEADERS = [
    "nome_empresa",
    "telefone",
    "tipo_telefone",
    "whatsapp_link",
    "site",
    "endereco",
    "avaliacao",
    "total_avaliacoes",
    "categoria",
    "data_extracao",
]

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

# ─── Utilitários ─────────────────────────────────────────────────────────────

def delay(min_s: float = 1.0, max_s: float = 3.5):
    """Delay aleatório para simular comportamento humano."""
    # Adiciona micro-jitter gaussiano para tornar o padrão menos previsível
    base = random.uniform(min_s, max_s)
    jitter = random.gauss(0, 0.3)
    total = max(min_s, base + jitter)
    return asyncio.sleep(total)


def normalizar_telefone(raw: str) -> str:
    """Remove tudo que não for dígito."""
    return re.sub(r"\D", "", raw)


def classificar_telefone(numero: str) -> str:
    """
    Classifica o número conforme padrão brasileiro.
    Retorna: 'celular', 'fixo' ou 'invalido'
    """
    # Remove código do país se presente
    if numero.startswith("55") and len(numero) > 11:
        numero = numero[2:]

    if len(numero) == 11:
        # DDD (2 dígitos) + 9 + 8 dígitos = celular
        if numero[2] == "9":
            return "celular"
    elif len(numero) == 10:
        # DDD + 8 dígitos = fixo
        return "fixo"
    return "invalido"


def e_celular_valido(numero: str) -> bool:
    """Retorna True apenas para celulares com 9º dígito (padrão BR)."""
    return classificar_telefone(numero) == "celular"


def extrair_whatsapp_de_texto(texto: str) -> Optional[str]:
    """
    Procura padrões de WhatsApp em texto.
    Detecta: wa.me, api.whatsapp.com, wa.link
    """
    padrao = re.compile(
        r"(?:https?://)?(?:api\.whatsapp\.com/send\?phone=|wa\.me/|wa\.link/)[\w/?=&+%-]+",
        re.IGNORECASE,
    )
    match = padrao.search(texto)
    if match:
        return match.group(0)
    return None


def montar_link_whatsapp(numero: str) -> str:
    """Monta link wa.me limpo a partir de um número."""
    digits = normalizar_telefone(numero)
    if not digits.startswith("55"):
        digits = "55" + digits
    return f"https://wa.me/{digits}"


def inicializar_csv():
    """Cria o CSV com cabeçalho se ainda não existir."""
    if not Path(OUTPUT_FILE).exists():
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()
        log.info(f"Arquivo {OUTPUT_FILE} criado.")


def telefones_ja_salvos() -> set:
    """Retorna o conjunto de telefones já presentes no CSV."""
    if not Path(OUTPUT_FILE).exists():
        return set()
    with open(OUTPUT_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["telefone"] for row in reader if row.get("telefone")}


N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")


def enviar_para_n8n(lead: dict):
    """Envia o lead ao webhook do n8n (Sheets → GHL). Falha silenciosa."""
    try:
        payload = json.dumps(lead).encode("utf-8")
        req = urllib.request.Request(
            N8N_WEBHOOK_URL,
            data=payload,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode("utf-8")
            log.info(f"  n8n: {body}")
    except urllib.error.URLError as exc:
        log.warning(f"  n8n webhook falhou: {exc}")


def salvar_lead(lead: dict, telefones_vistos: set) -> bool:
    """Salva um lead no CSV se o telefone ainda não foi registrado. Retorna True se salvou."""
    tel = lead.get("telefone", "")
    if tel in telefones_vistos:
        log.info(f"  Duplicata ignorada — {lead.get('nome_empresa')} | {tel}")
        return False
    with open(OUTPUT_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
        writer.writerow(lead)
    telefones_vistos.add(tel)
    enviar_para_n8n(lead)
    return True


# ─── Scraper de Site da Empresa ──────────────────────────────────────────────

async def verificar_whatsapp_no_site(page, url: str) -> Optional[str]:
    """
    Abre o site da empresa e procura links/botões de WhatsApp.
    Retorna o link wa.me encontrado ou None.
    """
    if not url or url == "N/A":
        return None
    try:
        await page.goto(url, timeout=15000, wait_until="domcontentloaded")
        await delay(1.0, 2.5)

        conteudo = await page.content()
        link = extrair_whatsapp_de_texto(conteudo)
        if link:
            log.info(f"  WhatsApp encontrado no site: {link}")
            return link

        # Tenta encontrar href com whatsapp em âncoras
        hrefs = await page.eval_on_selector_all(
            "a[href*='whatsapp'], a[href*='wa.me'], a[href*='wa.link']",
            "els => els.map(e => e.href)",
        )
        if hrefs:
            log.info(f"  WhatsApp via âncora: {hrefs[0]}")
            return hrefs[0]

    except Exception as exc:
        log.debug(f"  Falha ao verificar site {url}: {exc}")
    return None


# ─── Parser de Detalhes do Local ─────────────────────────────────────────────

async def extrair_detalhes(page) -> dict:
    """
    Com o painel de detalhes aberto no Maps, extrai todos os dados do local.
    """
    dados = {
        "nome_empresa": "N/A",
        "telefone": "N/A",
        "tipo_telefone": "N/A",
        "whatsapp_link": "N/A",
        "site": "N/A",
        "endereco": "N/A",
        "avaliacao": "N/A",
        "total_avaliacoes": "N/A",
        "categoria": "N/A",
        "data_extracao": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }

    try:
        await page.wait_for_selector("h1.DUwDvf", timeout=8000)
    except PlaywrightTimeout:
        log.warning("Timeout aguardando h1 do painel de detalhes.")
        return dados

    # Nome
    try:
        dados["nome_empresa"] = await page.inner_text("h1.DUwDvf")
    except Exception:
        pass

    # Categoria
    try:
        dados["categoria"] = await page.inner_text("button.DkEaL")
    except Exception:
        pass

    # Avaliação
    try:
        dados["avaliacao"] = await page.inner_text("div.F7nice span[aria-hidden='true']")
    except Exception:
        pass

    # Total de avaliações
    try:
        total_raw = await page.inner_text("div.F7nice span[aria-label*='avalia']")
        dados["total_avaliacoes"] = re.sub(r"\D", "", total_raw)
    except Exception:
        pass

    # Endereço
    try:
        dados["endereco"] = await page.inner_text(
            "button[data-item-id='address'] .Io6YTe"
        )
    except Exception:
        pass

    # Site
    try:
        site_el = page.locator("a[data-item-id='authority']")
        if await site_el.count() > 0:
            dados["site"] = await site_el.get_attribute("href")
    except Exception:
        pass

    # Telefone
    try:
        tel_el = page.locator("button[data-item-id*='phone'] .Io6YTe")
        if await tel_el.count() > 0:
            tel_raw = await tel_el.inner_text()
            tel_norm = normalizar_telefone(tel_raw)
            dados["telefone"] = tel_norm
            dados["tipo_telefone"] = classificar_telefone(tel_norm)

            # Se for celular, já monta link WhatsApp provisório
            if dados["tipo_telefone"] == "celular":
                dados["whatsapp_link"] = montar_link_whatsapp(tel_norm)
    except Exception:
        pass

    return dados


# ─── Loop Principal de Scraping ───────────────────────────────────────────────

async def scrape(query: str, max_resultados: int = 120, verificar_site: bool = False):
    """
    Parâmetros:
        query            — Ex: "clínicas estéticas São Paulo"
        max_resultados   — Limite de empresas a extrair
        verificar_site   — Se True, abre o site de cada empresa para caçar link WA
    """
    inicializar_csv()
    ja_processados: set[str] = set()
    telefones_vistos: set[str] = telefones_ja_salvos()
    total_salvos = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ],
        )

        user_agent = random.choice(USER_AGENTS)
        context = await browser.new_context(
            user_agent=user_agent,
            viewport={"width": 1366, "height": 768},
            locale="pt-BR",
            timezone_id="America/Sao_Paulo",
        )

        # Bloqueia recursos pesados para velocidade
        await context.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,eot}",
            lambda route: route.abort(),
        )

        page = await context.new_page()

        # Anti-detecção básica
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        url_busca = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        log.info(f"Acessando: {url_busca}")
        await page.goto(url_busca, wait_until="networkidle", timeout=30000)
        await delay(2, 4)

        # Painel lateral de resultados
        SELETOR_PAINEL = "div[role='feed']"
        SELETOR_ITEM = "div[role='feed'] > div > div > a"

        try:
            await page.wait_for_selector(SELETOR_PAINEL, timeout=15000)
        except PlaywrightTimeout:
            log.error("Painel de resultados não encontrado. Encerrando.")
            await browser.close()
            return

        log.info("Iniciando scroll para carregar todos os resultados...")

        # ── Scroll para carregar mais resultados ──────────────────────────
        anteriores = 0
        sem_mudanca = 0
        while True:
            itens = await page.query_selector_all(SELETOR_ITEM)
            atual = len(itens)

            if atual >= max_resultados:
                log.info(f"Limite de {max_resultados} resultados atingido.")
                break

            # Verifica se chegou ao fim da lista
            fim = await page.query_selector("span.HlvSq")
            if fim:
                log.info("Fim da lista detectado pelo Maps.")
                break

            if atual == anteriores:
                sem_mudanca += 1
                if sem_mudanca >= 5:
                    log.info("Sem novos resultados após 5 tentativas. Continuando.")
                    break
            else:
                sem_mudanca = 0

            anteriores = atual

            # Scroll dentro do painel lateral
            await page.eval_on_selector(
                SELETOR_PAINEL,
                "el => el.scrollBy(0, 800)",
            )
            await delay(1.5, 3.0)
            log.info(f"  Carregados até agora: {atual} resultados...")

        # ── Iterar sobre cada resultado ───────────────────────────────────
        itens = await page.query_selector_all(SELETOR_ITEM)
        log.info(f"Total de itens a processar: {len(itens)}")

        for idx, item in enumerate(itens[:max_resultados]):
            try:
                nome_preview = await item.inner_text()
                nome_preview = nome_preview.strip().split("\n")[0]

                if nome_preview in ja_processados:
                    continue

                log.info(f"[{idx+1}] Processando: {nome_preview}")
                await item.click()
                await delay(2.0, 4.0)

                dados = await extrair_detalhes(page)

                # ── Filtro principal: só celular válido ───────────────────
                if dados["tipo_telefone"] != "celular":
                    log.info(
                        f"  Ignorado (tipo: {dados['tipo_telefone']}) — {dados['nome_empresa']}"
                    )
                    ja_processados.add(nome_preview)
                    continue

                # ── Verificação opcional no site ──────────────────────────
                if verificar_site and dados["site"] != "N/A":
                    site_page = await context.new_page()
                    wa_site = await verificar_whatsapp_no_site(site_page, dados["site"])
                    await site_page.close()
                    if wa_site:
                        dados["whatsapp_link"] = wa_site

                if salvar_lead(dados, telefones_vistos):
                    total_salvos += 1
                    log.info(
                        f"  SALVO — {dados['nome_empresa']} | {dados['telefone']} | WA: {dados['whatsapp_link']}"
                    )
                ja_processados.add(nome_preview)

                await delay(1.0, 2.5)

            except Exception as exc:
                log.error(f"  Erro ao processar item {idx+1}: {exc}")
                continue

        await browser.close()

    log.info(f"\nConcluído! {total_salvos} leads salvos em '{OUTPUT_FILE}'")


# ─── Entry Point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Google Maps Lead Scraper — Focado em WhatsApp/Celular BR"
    )
    parser.add_argument(
        "query",
        nargs="?",
        default="loja de rodas e pneus premium Brasil",
        help='Busca no Maps. Ex: "barbearias Curitiba"',
    )
    parser.add_argument(
        "--max",
        type=int,
        default=120,
        help="Número máximo de resultados (padrão: 120)",
    )
    parser.add_argument(
        "--verificar-site",
        action="store_true",
        help="Acessa o site de cada empresa para verificar links de WhatsApp",
    )

    args = parser.parse_args()

    asyncio.run(
        scrape(
            query=args.query,
            max_resultados=args.max,
            verificar_site=args.verificar_site,
        )
    )
