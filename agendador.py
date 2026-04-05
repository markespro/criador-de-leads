"""
Agendador Furtivo — Distribui as buscas ao longo do dia
Estratégia: N lotes pequenos em horários aleatórios dentro de uma janela segura.

Uso:
    python3 agendador.py "barbearias São Paulo" --meta 50
    python3 agendador.py "clínicas estéticas Curitiba" --meta 30 --lote 3 --inicio 09:00 --fim 18:00
"""

import asyncio
import argparse
import csv
import logging
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

log = logging.getLogger("agendador")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [AGENDADOR] %(message)s",
    handlers=[
        logging.FileHandler("agendador.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)

# ─── Importa a função principal do scraper ───────────────────────────────────
from scraper import scrape, inicializar_csv, OUTPUT_FILE

# ─── Cidades para rotação de queries ─────────────────────────────────────────
CIDADES = [
    "São Paulo", "Rio de Janeiro", "Belo Horizonte", "Curitiba",
    "Porto Alegre", "Brasília", "Salvador", "Fortaleza", "Recife",
    "Manaus", "Belém", "Goiânia", "Florianópolis", "Campinas",
    "Santos", "São Bernardo do Campo", "Ribeirão Preto", "Uberlândia",
    "Niterói", "Natal", "Maceió", "Teresina", "Campo Grande",
    "João Pessoa", "Aracaju", "Cuiabá", "Macapá", "Porto Velho",
]


def _contar_leads() -> int:
    p = Path(OUTPUT_FILE)
    if not p.exists():
        return 0
    with open(p, newline="", encoding="utf-8") as f:
        return sum(1 for _ in csv.reader(f)) - 1  # desconta cabeçalho


# ─── Helpers de horário ───────────────────────────────────────────────────────

def parse_hora(s: str) -> datetime:
    """Converte 'HH:MM' para datetime de hoje."""
    h, m = map(int, s.split(":"))
    return datetime.now().replace(hour=h, minute=m, second=0, microsecond=0)


def gerar_horarios_aleatorios(inicio: datetime, fim: datetime, n: int) -> list:
    """
    Gera N datetimes aleatórios dentro da janela [inicio, fim],
    ordenados e com espaçamento mínimo de 5 minutos entre eles.
    """
    janela_segundos = int((fim - inicio).total_seconds())
    if janela_segundos <= 0:
        raise ValueError("Horário de fim deve ser posterior ao de início.")

    horarios = set()
    tentativas = 0
    while len(horarios) < n and tentativas < 10000:
        candidato = inicio + timedelta(seconds=random.randint(0, janela_segundos))
        # Garante espaçamento mínimo de 5 minutos de qualquer horário já escolhido
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
    """Dorme até o momento especificado."""
    agora = datetime.now()
    espera = (momento - agora).total_seconds()
    if espera <= 0:
        return
    log.info(
        f"Próximo lote agendado para {momento.strftime('%H:%M:%S')} "
        f"— aguardando {int(espera // 60)}m {int(espera % 60)}s..."
    )
    await asyncio.sleep(espera)


# ─── Orquestrador principal ───────────────────────────────────────────────────

async def orquestrar(
    nicho: str,
    meta: int,
    tamanho_lote: int,
    inicio: datetime,
    fim: datetime,
    verificar_site: bool,
):
    """
    Divide a meta em lotes distribuídos na janela de tempo.
    Cada lote usa uma cidade diferente, ampliando o pool de resultados.
    """
    n_lotes = -(-meta // tamanho_lote)  # ceil sem math
    cidades_rotacao = random.sample(CIDADES, min(n_lotes, len(CIDADES)))
    # Se precisar de mais lotes que cidades, repete embaralhado
    while len(cidades_rotacao) < n_lotes:
        cidades_rotacao += random.sample(CIDADES, min(n_lotes - len(cidades_rotacao), len(CIDADES)))

    log.info("=" * 60)
    log.info(f"Nicho       : {nicho}")
    log.info(f"Meta total  : {meta} leads")
    log.info(f"Tamanho lote: {tamanho_lote} leads por lote")
    log.info(f"Nº de lotes : {n_lotes}")
    log.info(f"Janela      : {inicio.strftime('%H:%M')} → {fim.strftime('%H:%M')}")
    log.info(f"Cidades     : {', '.join(cidades_rotacao[:n_lotes])}")
    log.info("=" * 60)

    inicializar_csv()

    horarios = gerar_horarios_aleatorios(inicio, fim, n_lotes)

    if not horarios:
        log.error("Não foi possível gerar nenhum horário. Encerrando.")
        return

    log.info("Plano de execução do dia:")
    for i, (h, cidade) in enumerate(zip(horarios, cidades_rotacao), 1):
        log.info(f"  Lote {i:02d} → {h.strftime('%H:%M:%S')} | {nicho} {cidade}")

    total_coletado = 0

    for i, (horario, cidade) in enumerate(zip(horarios, cidades_rotacao), 1):
        leads_restantes = meta - total_coletado
        if leads_restantes <= 0:
            log.info("Meta atingida antes do último lote. Encerrando.")
            break

        query_lote = f"{nicho} {cidade}"

        await aguardar_ate(horario)

        log.info(f"--- Lote {i}/{n_lotes} | Query: {query_lote} ---")

        try:
            salvos_antes = _contar_leads()
            await scrape(
                query=query_lote,
                max_resultados=50,
                verificar_site=verificar_site,
            )
            salvos_depois = _contar_leads()
            novos = salvos_depois - salvos_antes
            total_coletado += novos
            log.info(f"Lote {i} concluído. Novos: {novos}. Total acumulado: {total_coletado}/{meta}")
        except Exception as exc:
            log.error(f"Erro no lote {i}: {exc}. Pulando para o próximo.")

        # Pausa extra aleatória pós-lote (comportamento humano entre sessões)
        if i < len(horarios):
            pausa_extra = random.randint(30, 120)  # 30s a 2min extra após cada lote
            log.info(f"Pausa pós-lote: {pausa_extra}s de segurança...")
            await asyncio.sleep(pausa_extra)

    log.info(f"\nDia encerrado. Total de leads coletados: {total_coletado}/{meta}")


# ─── Entry Point ─────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Agendador furtivo — distribui buscas ao longo do dia"
    )
    parser.add_argument(
        "nicho",
        nargs="?",
        default="loja de rodas e pneus",
        help='Nicho a buscar. Ex: "salões de beleza" (cidade é adicionada automaticamente)',
    )
    parser.add_argument(
        "--meta",
        type=int,
        default=50,
        help="Total de leads desejados no dia (padrão: 50)",
    )
    parser.add_argument(
        "--lote",
        type=int,
        default=5,
        help="Leads por lote/sessão (padrão: 5)",
    )
    parser.add_argument(
        "--inicio",
        type=str,
        default="09:00",
        help="Início da janela de operação (padrão: 09:00)",
    )
    parser.add_argument(
        "--fim",
        type=str,
        default="18:00",
        help="Fim da janela de operação (padrão: 18:00)",
    )
    parser.add_argument(
        "--verificar-site",
        action="store_true",
        help="Acessa o site de cada empresa para verificar links de WhatsApp",
    )

    args = parser.parse_args()

    inicio_dt = parse_hora(args.inicio)
    fim_dt = parse_hora(args.fim)

    # Se o horário de início já passou, avisa mas começa do agora
    agora = datetime.now()
    if inicio_dt < agora:
        log.warning(
            f"Horário de início ({args.inicio}) já passou. "
            f"Usando agora ({agora.strftime('%H:%M')}) como início."
        )
        inicio_dt = agora + timedelta(seconds=5)

    asyncio.run(
        orquestrar(
            nicho=args.nicho,
            meta=args.meta,
            tamanho_lote=args.lote,
            inicio=inicio_dt,
            fim=fim_dt,
            verificar_site=args.verificar_site,
        )
    )
