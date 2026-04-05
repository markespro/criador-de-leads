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
    query: str,
    meta: int,
    tamanho_lote: int,
    inicio: datetime,
    fim: datetime,
    verificar_site: bool,
):
    """
    Divide a meta em lotes e os distribui aleatoriamente na janela de tempo.

    Exemplo com meta=50 e lote=5:
        → 10 lotes de 5 leads cada
        → Distribuídos em horários aleatórios entre início e fim
    """
    n_lotes = -(-meta // tamanho_lote)  # Divisão com teto (ceil sem math)
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

    # Mostra o plano do dia antes de executar
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
            salvos_antes = _contar_leads()
            await scrape(
                query=query,
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
        "query",
        nargs="?",
        default="loja de rodas e pneus premium Brasil",
        help='Busca no Maps. Ex: "salões de beleza Curitiba"',
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
            query=args.query,
            meta=args.meta,
            tamanho_lote=args.lote,
            inicio=inicio_dt,
            fim=fim_dt,
            verificar_site=args.verificar_site,
        )
    )
