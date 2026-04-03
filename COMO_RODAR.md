# Como Rodar o Scraper na VPS Linux

## 1. Pré-requisitos na VPS

```bash
# Atualizar pacotes
sudo apt update && sudo apt upgrade -y

# Instalar Python 3.11+ e pip
sudo apt install -y python3 python3-pip python3-venv

# Instalar dependências do Playwright (Chromium headless)
sudo apt install -y \
    libglib2.0-0 libnss3 libnspr4 libdbus-1-3 libatk1.0-0 \
    libatk-bridge2.0-0 libcups2 libdrm2 libxkbcommon0 libxcomposite1 \
    libxdamage1 libxfixes3 libxrandr2 libgbm1 libpango-1.0-0 \
    libcairo2 libasound2 libatspi2.0-0
```

## 2. Setup do Projeto

```bash
# Criar e entrar na pasta do projeto
mkdir ~/scraper && cd ~/scraper

# Copiar os arquivos scraper.py e requirements.txt para esta pasta

# Criar ambiente virtual
python3 -m venv venv
source venv/bin/activate

# Instalar dependências
pip install -r requirements.txt

# Instalar o browser do Playwright
playwright install chromium
```

## 3. Rodando o Scraper

### Uso básico
```bash
python scraper.py "barbearias Curitiba"
```

### Com limite personalizado
```bash
python scraper.py "clínicas estéticas São Paulo" --max 200
```

### Com verificação de WhatsApp no site (mais lento, mais completo)
```bash
python scraper.py "academias Belo Horizonte" --max 100 --verificar-site
```

### Rodando em background (sem travar o terminal)
```bash
nohup python scraper.py "restaurantes Florianópolis" --max 150 > output.log 2>&1 &

# Verificar progresso
tail -f scraper.log
```

## 4. Saída dos Dados

O arquivo `leads_prospeccao.csv` é gerado/atualizado incrementalmente na mesma pasta.

Colunas geradas:
- `nome_empresa` — Nome do negócio
- `telefone` — Número limpo (somente dígitos)
- `tipo_telefone` — `celular` ou `fixo`
- `whatsapp_link` — Link `wa.me/55...` pronto para uso
- `site` — URL do site (se disponível)
- `endereco` — Endereço completo
- `avaliacao` — Nota (ex: 4.7)
- `total_avaliacoes` — Quantidade de reviews
- `categoria` — Categoria no Maps
- `data_extracao` — Timestamp da coleta

**Apenas leads com celular brasileiro válido (9º dígito) são salvos.**

## 5. Dicas de Uso

- Varie os queries para cobrir diferentes nichos: `"salões de beleza Campinas"`, `"dentistas Recife"`, etc.
- O arquivo CSV acumula resultados entre execuções — sem risco de perda.
- Monitore o `scraper.log` para acompanhar o progresso e erros.
- Para grandes volumes, use `--max 60` por execução e varie o horário para evitar bloqueios.
