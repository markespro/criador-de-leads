#!/bin/bash
# ─── Instalador do Google Maps Lead Scraper ─────────────────────────────────
# Cole este script inteiro no terminal da VPS e execute:
#   bash instalar.sh
# ─────────────────────────────────────────────────────────────────────────────

set -e
PASTA="$HOME/scraper"
mkdir -p "$PASTA"
cd "$PASTA"

echo "==> Criando scraper.py..."
cat > scraper.py << 'PYEOF'
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
    base = random.uniform(min_s, max_s)
    jitter = random.gauss(0, 0.3)
    total = max(min_s, base + jitter)
    return asyncio.sleep(total)


def normalizar_telefone(raw: str) -> str:
    return re.sub(r"\D", "", raw)


def classificar_telefone(numero: str) -> str:
    if numero.startswith("55") and len(numero) > 11:
        numero = numero[2:]
    if len(numero) == 11:
        if numero[2] == "9":
            return "celular"
    elif len(numero) == 10:
        return "fixo"
    return "invalido"


def e_celular_valido(numero: str) -> bool:
    return classificar_telefone(numero) == "celular"


def extrair_whatsapp_de_texto(texto: str) -> Optional[str]:
    padrao = re.compile(
        r"(?:https?://)?(?:api\.whatsapp\.com/send\?phone=|wa\.me/|wa\.link/)[\w/?=&+%-]+",
        re.IGNORECASE,
    )
    match = padrao.search(texto)
    if match:
        return match.group(0)
    return None


def montar_link_whatsapp(numero: str) -> str:
    digits = normalizar_telefone(numero)
    if not digits.startswith("55"):
        digits = "55" + digits
    return f"https://wa.me/{digits}"


def inicializar_csv():
    if not Path(OUTPUT_FILE).exists():
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_HEADERS)
            writer.writeheader()
        log.info(f"Arquivo {OUTPUT_FILE} criado.")


def telefones_ja_salvos() -> set:
    if not Path(OUTPUT_FILE).exists():
        return set()
    with open(OUTPUT_FILE, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return {row["telefone"] for row in reader if row.get("telefone")}


N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")


def enviar_para_n8n(lead: dict):
    if not N8N_WEBHOOK_URL:
        return
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

    try:
        dados["nome_empresa"] = await page.inner_text("h1.DUwDvf")
    except Exception:
        pass

    try:
        dados["categoria"] = await page.inner_text("button.DkEaL")
    except Exception:
        pass

    try:
        dados["avaliacao"] = await page.inner_text("div.F7nice span[aria-hidden='true']")
    except Exception:
        pass

    try:
        total_raw = await page.inner_text("div.F7nice span[aria-label*='avalia']")
        dados["total_avaliacoes"] = re.sub(r"\D", "", total_raw)
    except Exception:
        pass

    try:
        dados["endereco"] = await page.inner_text(
            "button[data-item-id='address'] .Io6YTe"
        )
    except Exception:
        pass

    try:
        site_el = page.locator("a[data-item-id='authority']")
        if await site_el.count() > 0:
            dados["site"] = await site_el.get_attribute("href")
    except Exception:
        pass

    try:
        tel_el = page.locator("button[data-item-id*='phone'] .Io6YTe")
        if await tel_el.count() > 0:
            tel_raw = await tel_el.inner_text()
            tel_norm = normalizar_telefone(tel_raw)
            dados["telefone"] = tel_norm
            dados["tipo_telefone"] = classificar_telefone(tel_norm)
            if dados["tipo_telefone"] == "celular":
                dados["whatsapp_link"] = montar_link_whatsapp(tel_norm)
    except Exception:
        pass

    return dados


# ─── Loop Principal de Scraping ───────────────────────────────────────────────

async def scrape(query: str, max_resultados: int = 120, verificar_site: bool = False):
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

        await context.route(
            "**/*.{png,jpg,jpeg,gif,webp,svg,woff,woff2,ttf,eot}",
            lambda route: route.abort(),
        )

        page = await context.new_page()
        await page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )

        url_busca = f"https://www.google.com/maps/search/{query.replace(' ', '+')}"
        log.info(f"Acessando: {url_busca}")
        await page.goto(url_busca, wait_until="networkidle", timeout=30000)
        await delay(2, 4)

        SELETOR_PAINEL = "div[role='feed']"
        SELETOR_ITEM = "div[role='feed'] > div > div > a"

        try:
            await page.wait_for_selector(SELETOR_PAINEL, timeout=15000)
        except PlaywrightTimeout:
            log.error("Painel de resultados não encontrado. Encerrando.")
            await browser.close()
            return

        log.info("Iniciando scroll para carregar todos os resultados...")

        anteriores = 0
        sem_mudanca = 0
        while True:
            itens = await page.query_selector_all(SELETOR_ITEM)
            atual = len(itens)

            if atual >= max_resultados:
                log.info(f"Limite de {max_resultados} resultados atingido.")
                break

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
            await page.eval_on_selector(SELETOR_PAINEL, "el => el.scrollBy(0, 800)")
            await delay(1.5, 3.0)
            log.info(f"  Carregados até agora: {atual} resultados...")

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

                if dados["tipo_telefone"] != "celular":
                    log.info(
                        f"  Ignorado (tipo: {dados['tipo_telefone']}) — {dados['nome_empresa']}"
                    )
                    ja_processados.add(nome_preview)
                    continue

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
PYEOF

echo "==> Criando agendador.py..."
cat > agendador.py << 'PYEOF'
"""
Agendador Furtivo — Distribui as buscas ao longo do dia
Estratégia: N lotes pequenos em horários aleatórios dentro de uma janela segura.

Uso:
    python3 agendador.py "barbearias São Paulo" --meta 50
    python3 agendador.py "clínicas estéticas Curitiba" --meta 30 --lote 3 --inicio 09:00 --fim 18:00
"""

import asyncio
import argparse
import logging
import random
import sys
from datetime import datetime, timedelta

log = logging.getLogger("agendador")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [AGENDADOR] %(message)s",
    handlers=[
        logging.FileHandler("agendador.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

from scraper import scrape, inicializar_csv


def parse_hora(s: str) -> datetime:
    h, m = map(int, s.split(":"))
    return datetime.now().replace(hour=h, minute=m, second=0, microsecond=0)


def gerar_horarios_aleatorios(inicio: datetime, fim: datetime, n: int) -> list:
    janela_segundos = int((fim - inicio).total_seconds())
    if janela_segundos <= 0:
        raise ValueError("Horário de fim deve ser posterior ao de início.")

    horarios = set()
    tentativas = 0
    while len(horarios) < n and tentativas < 10000:
        candidato = inicio + timedelta(seconds=random.randint(0, janela_segundos))
        muito_proximo = any(
            abs((candidato - h).total_seconds()) < 300 for h in horarios
        )
        if not muito_proximo:
            horarios.add(candidato)
        tentativas += 1

    horarios = sorted(horarios)

    if len(horarios) < n:
        log.warning(
            f"Só foi possível agendar {len(horarios)} lotes na janela informada "
            f"(pedido: {n}). Aumente a janela ou reduza a meta."
        )

    return horarios


async def aguardar_ate(momento: datetime):
    agora = datetime.now()
    espera = (momento - agora).total_seconds()
    if espera <= 0:
        return
    log.info(
        f"Próximo lote agendado para {momento.strftime('%H:%M:%S')} "
        f"— aguardando {int(espera // 60)}m {int(espera % 60)}s..."
    )
    await asyncio.sleep(espera)


async def orquestrar(
    query: str,
    meta: int,
    tamanho_lote: int,
    inicio: datetime,
    fim: datetime,
    verificar_site: bool,
):
    n_lotes = -(-meta // tamanho_lote)
    log.info("=" * 60)
    log.info(f"Query       : {query}")
    log.info(f"Meta total  : {meta} leads")
    log.info(f"Tamanho lote: {tamanho_lote} leads por lote")
    log.info(f"Nº de lotes : {n_lotes}")
    log.info(f"Janela      : {inicio.strftime('%H:%M')} → {fim.strftime('%H:%M')}")
    log.info("=" * 60)

    inicializar_csv()

    horarios = gerar_horarios_aleatorios(inicio, fim, n_lotes)

    if not horarios:
        log.error("Não foi possível gerar nenhum horário. Encerrando.")
        return

    log.info("Plano de execução do dia:")
    for i, h in enumerate(horarios, 1):
        log.info(f"  Lote {i:02d} → {h.strftime('%H:%M:%S')} ({tamanho_lote} leads)")

    total_coletado = 0

    for i, horario in enumerate(horarios, 1):
        leads_restantes = meta - total_coletado
        if leads_restantes <= 0:
            log.info("Meta atingida antes do último lote. Encerrando.")
            break

        lote_atual = min(tamanho_lote, leads_restantes)

        await aguardar_ate(horario)

        log.info(f"--- Iniciando lote {i}/{len(horarios)} ({lote_atual} leads) ---")

        try:
            await scrape(
                query=query,
                max_resultados=lote_atual,
                verificar_site=verificar_site,
            )
            total_coletado += lote_atual
            log.info(f"Lote {i} concluído. Total acumulado: {total_coletado}/{meta}")
        except Exception as exc:
            log.error(f"Erro no lote {i}: {exc}. Pulando para o próximo.")

        if i < len(horarios):
            pausa_extra = random.randint(30, 120)
            log.info(f"Pausa pós-lote: {pausa_extra}s de segurança...")
            await asyncio.sleep(pausa_extra)

    log.info(f"\nDia encerrado. Total de leads coletados: {total_coletado}/{meta}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Agendador furtivo — distribui buscas ao longo do dia"
    )
    parser.add_argument(
        "query",
        nargs="?",
        default="loja de rodas e pneus premium Brasil",
        help='Busca no Maps. Ex: "salões de beleza Curitiba"',
    )
    parser.add_argument("--meta", type=int, default=50)
    parser.add_argument("--lote", type=int, default=5)
    parser.add_argument("--inicio", type=str, default="09:00")
    parser.add_argument("--fim", type=str, default="18:00")
    parser.add_argument("--verificar-site", action="store_true")

    args = parser.parse_args()

    inicio_dt = parse_hora(args.inicio)
    fim_dt = parse_hora(args.fim)

    agora = datetime.now()
    if inicio_dt < agora:
        log.warning(
            f"Horário de início ({args.inicio}) já passou. "
            f"Usando agora ({agora.strftime('%H:%M')}) como início."
        )
        inicio_dt = agora + timedelta(seconds=5)

    asyncio.run(
        orquestrar(
            query=args.query,
            meta=args.meta,
            tamanho_lote=args.lote,
            inicio=inicio_dt,
            fim=fim_dt,
            verificar_site=args.verificar_site,
        )
    )
PYEOF

echo "==> Criando requirements.txt..."
cat > requirements.txt << 'EOF'
playwright==1.44.0
python-dotenv==1.0.1
EOF

echo "==> Criando .env.example..."
cat > .env.example << 'EOF'
N8N_WEBHOOK_URL=https://seu-n8n.com/webhook/SEU_ID/webhook/leads-scraper
EOF

echo "==> Criando .gitignore..."
cat > .gitignore << 'EOF'
# Credenciais
.env

# Dados gerados
leads_prospeccao.csv
*.log

# Python
venv/
__pycache__/
*.pyc

# macOS
.DS_Store
EOF

echo "==> Instalando dependências do sistema..."
sudo apt-get update -qq && sudo apt-get install -y -qq \
  python3 python3-pip python3-venv \
  libglib2.0-0 libnss3 libnspr4 libdbus-1-3 libatk1.0-0 \
  libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
  libxdamage1 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 \
  libcairo2 libasound2 libatspi2.0-0

echo "==> Criando ambiente virtual..."
python3 -m venv venv
source venv/bin/activate

echo "==> Instalando dependências Python..."
pip install -q playwright==1.44.0 python-dotenv==1.0.1

echo "==> Instalando Chromium..."
python3 -m playwright install chromium

echo ""
echo "Instalacao concluida! Arquivos criados em: $PASTA"
echo ""
echo "Proximo passo — configure o webhook:"
echo "  cp .env.example .env"
echo "  nano .env  # preencha com sua URL do n8n"
echo ""
echo "Para rodar:"
echo "  cd ~/scraper"
echo "  source venv/bin/activate"
echo "  python3 agendador.py \"barbearias Curitiba\" --meta 50 --lote 5"
echo ""
echo "Para rodar em background:"
echo "  nohup python3 agendador.py \"barbearias Curitiba\" --meta 50 > agendador_output.log 2>&1 &"
echo "  tail -f agendador.log"
