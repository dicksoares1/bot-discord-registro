# =========================================================
# ==================== BOT VDR v3.3.1 ====================
# =========================================================
# Versão: 3.3.1 - Refatorado e Otimizado - ESTRUTURA MODULAR
# =========================================================

# =========================================================
# SEÇÃO 1: IMPORTAÇÕES E CONFIGURAÇÕES
# =========================================================

import os
import sys
import json
import gc
import re
import asyncio
import aiohttp
import asyncpg
import discord
import tweepy
import time as time_module
import logging
import psutil
import signal
from discord.ext import commands, tasks
from discord.utils import escape_markdown
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

# =========================================================
# SEÇÃO 2: CONFIGURAÇÃO DE LOG
# =========================================================

class JsonFormatter(logging.Formatter):
    def format(self, record):
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno
        }
        return json.dumps(log_entry)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

json_handler = logging.StreamHandler(sys.stdout)
json_handler.setFormatter(JsonFormatter())
logger = logging.getLogger('VDR_BOT')
logger.addHandler(json_handler)
logger.setLevel(logging.INFO)

# =========================================================
# SEÇÃO 3: CONSTANTES GLOBAIS
# =========================================================

# ---------------------------------------------------------
# 3.1: TOKENS E CREDENCIAIS
# ---------------------------------------------------------

TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    logger.error("❌ TOKEN não encontrado!")
    sys.exit(1)

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    logger.error("❌ DATABASE_URL não encontrada!")
    sys.exit(1)

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")
API_KEY = os.environ.get("API_KEY")
API_SECRET = os.environ.get("API_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_SECRET = os.environ.get("ACCESS_SECRET")

# ---------------------------------------------------------
# 3.2: CONFIGURAÇÕES DO BOT
# ---------------------------------------------------------

BRASIL = ZoneInfo("America/Sao_Paulo")
GUILD_ID = 1229526644193099880
BASE_PATH = "/mnt/data"
EMOJI_APROVACAO = "✅"

# ---------------------------------------------------------
# 3.3: CONSTANTES DE PRODUÇÃO
# ---------------------------------------------------------

PRECO_POLVORA = 80
PRECO_EMBALAGEM_POR_UNIDADE = 2000000 / 25000
TEMPO_BASE_NORTE = 65
TEMPO_BASE_SUL = 130

# ---------------------------------------------------------
# 3.4: ITENS E ALIASES
# ---------------------------------------------------------

ITENS_DISPONIVEIS = [
    "🔫 Fuzil", "🔫 M4", "🔫 SIG Sauer", "🔫 AK47", "🔫 Glock",
    "🔫 Shotgun", "🔫 Sniper", "🎯 Kit Reparos Comum", "🎯 Kit Reparos Raro",
    "🎯 Kit Reparos Épico", "🎯 Kit Reparos Lendário", "🛡️ Colete Leve",
    "🛡️ Colete Médio", "🛡️ Colete Pesado", "📦 Municao PT",
    "📦 Municao SUB", "🧨 Explosivo", "💊 Kit Médico", "🔑 Chave Mestra",
    "📡 Rádio", "🔦 Lanterna"
]

ALIASES = {
    "fuzil": "Fuzil", "m4": "M4", "sig": "SIG Sauer", "ak": "AK47",
    "ak47": "AK47", "glock": "Glock", "shotgun": "Shotgun", "sniper": "Sniper",
    "kit comum": "Kit Reparos Comum", "kit raro": "Kit Reparos Raro",
    "kit epico": "Kit Reparos Épico", "kit lendario": "Kit Reparos Lendário",
    "colete leve": "Colete Leve", "colete medio": "Colete Médio",
    "colete pesado": "Colete Pesado", "municao pt": "Municao PT",
    "municao sub": "Municao SUB", "explosivo": "Explosivo",
    "kit medico": "Kit Médico", "chave mestra": "Chave Mestra",
    "radio": "Rádio", "lanterna": "Lanterna", "pt": "Municao PT",
    "sub": "Municao SUB"
}

ITENS_COM_OPCOES = {
    "Kit Reparos": ["Kit Reparos Comum", "Kit Reparos Raro", "Kit Reparos Épico", "Kit Reparos Lendário"],
    "Colete": ["Colete Leve", "Colete Médio", "Colete Pesado"],
    "Municao": ["Municao PT", "Municao SUB"]
}

# ---------------------------------------------------------
# 3.5: IDs DOS CARGOS E CANAIS
# ---------------------------------------------------------

CANAL_SOLICITAR_SALA_ID = 1337374500366450741
RESULTADOS_METAS_ID = 1341403574483288125
CARGO_01_ID = 1258753233355014144
CARGO_02_ID = 1258753479082512394
CARGO_GERENTE_ID = 1324499473296134154
CARGO_GERENTE_GERAL_ID = 1462804425163935796
CARGO_RESP_METAS_ID = 1337407399656423485
CARGO_RESP_ACAO_ID = 1337379517274259509
CARGO_RESP_P1_ID = 1537563287393402920
CARGO_RESP_VENDAS_ID = 1337379530586980352
CARGO_RESP_PRODUCAO_ID = 1337379524949573662
CARGO_SOLDADO_ID = 1537803858611281940
CARGO_MEMBRO_ID = 1422847198789369926
CARGO_AGREGADO_ID = 1422847202937536532
CATEGORIA_META_GERENTE_ID = 1337374002422743122
CATEGORIA_META_RESPONSAVEIS_ID = 1462810826992783422
CATEGORIA_META_SOLDADO_ID = 1537807041022664835
CATEGORIA_META_MEMBRO_ID = 1461335697209163900
CATEGORIA_META_AGREGADO_ID = 1461335748870541323
AGREGADO_ROLE_ID = 1422847202937536532

CARGOS_PERMITIDOS_REMOVER = [
    CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID, CARGO_01_ID, CARGO_02_ID
]

CANAL_REGISTRO_ID = 1229556030397218878
CANAL_LOG_REGISTRO_ID = 1462457604939841851
CONVIDADO_ROLE_ID = 1337382961456353342
EM_REGISTRO_ROLE_ID = 1337382961456353342

CANAL_FABRICACAO_ID = 1466421612566810634
CANAL_REGISTRO_GALPAO_ID = 1356174712337862819
CANAL_BAU_GALPAO_SUL_ID = 1356174937764794521
CANAL_BAU_GALPAO_ID = 1448561598384963747
CANAL_CALCULO_POLVORA_ID = 1462834441968943157
CANAL_REGISTRO_POLVORA_ID = 1448570795101261846

CANAL_CALCULADORA_ID = 1460984821458272347
CANAL_ENCOMENDAS_ID = 1460980984811098294
CANAL_VENDAS_ID = CANAL_CALCULADORA_ID
CANAL_TEXTOS_VENDAS_ID = 1499045083994001500

CANAL_ESCALACOES_ID = 1241406819545514064
CANAL_RELATORIO_ACOES_ID = 1477308788531921019

CANAL_INICIAR_LAVAGEM_ID = 1467152989499293768
CANAL_LAVAGEM_MEMBROS_ID = 1467159346923311216
CANAL_RELATORIO_LAVAGEM_ID = 1467150805273546878

CANAL_CADASTRO_LIVE_ID = 1466464557215256790
CANAL_DIVULGACAO_LIVE_ID = 1243325102917943335
ADM_ID = 467673818375389194

CANAL_BOTAO_AUSENCIA_ID = 1491427870277374162
CANAL_REGISTRO_AUSENCIA_ID = 1313854772545196032
CARGO_AUSENTE_ID = 1337420032212336823
CANAL_GERENCIA_ID = 1237393478414241854

CANAL_GRUPOS_ID = 1448563544386961479

CANAL_RELATORIO_FINANCEIRO_ID = 1498664038559776768
CANAL_REGISTRAR_COMPRA_ID = 1498668853465448560
CANAL_COMPRAS_REGISTRADAS_ID = 1270467793363669053

CANAL_CLIPES_ID = 1229526645837271134
CANAL_POSTAGEM_X = 1486353689680547900

# =========================================================
# SEÇÃO 4: BOT E INTENTS
# =========================================================

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.guilds = True
intents.presences = True
intents.reactions = True

bot = commands.Bot(command_prefix="!", intents=intents)

# =========================================================
# SEÇÃO 5: FUNÇÕES AUXILIARES GLOBAIS
# =========================================================

# ---------------------------------------------------------
# 5.1: FUNÇÕES DE DATA/HORA
# ---------------------------------------------------------

def agora():
    return datetime.now(BRASIL)

def agora_db():
    return datetime.now(BRASIL).replace(tzinfo=None)

def str_para_datetime(data_str):
    if not data_str:
        return None
    if isinstance(data_str, datetime):
        if data_str.tzinfo is None:
            return data_str.replace(tzinfo=BRASIL)
        return data_str
    dt = datetime.fromisoformat(data_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=BRASIL)
    return dt

def str_para_datetime_completa(data_str):
    if not data_str:
        return None
    if isinstance(data_str, datetime):
        if data_str.tzinfo is None:
            return data_str.replace(tzinfo=BRASIL)
        return data_str
    try:
        dt = datetime.fromisoformat(data_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=BRASIL)
        return dt
    except:
        return None

def datetime_para_str(dt):
    if not dt:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=BRASIL)
        return dt.isoformat()
    return str(dt)

def para_db_naive(dt):
    if dt.tzinfo is not None:
        dt = dt.astimezone(BRASIL)
    return dt.replace(tzinfo=None)

def calcular_semana_anterior():
    hoje = agora()
    dia_semana = hoje.weekday()
    dias_para_domingo_anterior = dia_semana + 1
    domingo_anterior = hoje - timedelta(days=dias_para_domingo_anterior)
    segunda_anterior = domingo_anterior - timedelta(days=6)
    segunda_anterior = segunda_anterior.replace(hour=0, minute=0, second=0, microsecond=0)
    domingo_anterior = domingo_anterior.replace(hour=23, minute=59, second=59, microsecond=0)
    return segunda_anterior, domingo_anterior

def calcular_barra_progresso(data_fim, dias_totais):
    agora_br = agora()
    if not data_fim or agora_br >= data_fim:
        return "❌ EXPIRADO"
    data_inicio = data_fim - timedelta(days=dias_totais)
    tempo_total = (data_fim - data_inicio).total_seconds()
    tempo_restante = (data_fim - agora_br).total_seconds()
    if tempo_total <= 0:
        porcentagem_restante = 0
    else:
        porcentagem_restante = (tempo_restante / tempo_total) * 100
        porcentagem_restante = max(0, min(100, porcentagem_restante))
    tamanho_barra = 20
    preenchidos = int((porcentagem_restante / 100) * tamanho_barra)
    preenchidos = max(0, min(tamanho_barra, preenchidos))
    if porcentagem_restante > 66:
        cor = "🟢"
    elif porcentagem_restante > 33:
        cor = "🟡"
    else:
        cor = "🔴"
    return f"{cor} `{'█' * preenchidos}{'░' * (tamanho_barra - preenchidos)}` {porcentagem_restante:.0f}%"

def formatar_tempo_detalhado(data_fim):
    agora_br = agora()
    if not data_fim or agora_br >= data_fim:
        return "⚠️ **EXPIRADO**"
    diferenca = data_fim - agora_br
    dias = diferenca.days
    horas = diferenca.seconds // 3600
    minutos = (diferenca.seconds % 3600) // 60
    if dias > 0:
        return f"**{dias} dia(s)** e **{horas}h** {minutos}m"
    elif horas > 0:
        return f"**{horas}h** {minutos}m"
    else:
        return f"**{minutos}m**"

# ---------------------------------------------------------
# 5.2: FUNÇÕES DE FORMATAÇÃO
# ---------------------------------------------------------

def formatar_dinheiro(valor):
    try:
        valor = float(valor)
    except:
        valor = 0
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_num(valor):
    try:
        return f"{int(valor):,.0f}".replace(",", ".")
    except:
        return "0"

def safe_int(valor, default=0):
    if not valor:
        return default
    if isinstance(valor, int):
        return valor
    try:
        if isinstance(valor, str):
            valor = valor.replace(".", "").replace(",", "")
        return int(valor)
    except (ValueError, TypeError):
        return default

def barra(pct, size=20):
    cheio = int(pct * size)
    if pct <= 0.35:
        cor = "🟢"
    elif pct <= 0.70:
        cor = "🟡"
    elif pct < 1:
        cor = "🔴"
    else:
        cor = "🔵"
    return cor + " " + ("▓" * cheio) + ("░" * (size - cheio))

def capitalizar_nome(texto):
    if not texto:
        return texto
    palavras = texto.strip().split()
    palavras_capitalizadas = []
    for palavra in palavras:
        if len(palavra) > 1:
            palavras_capitalizadas.append(palavra[0].upper() + palavra[1:].lower())
        else:
            palavras_capitalizadas.append(palavra.upper())
    return " ".join(palavras_capitalizadas)

# ---------------------------------------------------------
# 5.3: FUNÇÕES DE PLATAFORMA/LINK
# ---------------------------------------------------------

def detectar_plataforma(link):
    link = link.lower()
    if "twitch.tv" in link:
        return "twitch"
    if "kick.com" in link:
        return "kick"
    if "tiktok.com" in link:
        return "tiktok"
    return None

def extrair_canal(link):
    link = link.lower().strip()
    link = link.replace("https://", "").replace("http://", "").replace("www.", "")
    link = link.split("?")[0].rstrip("/")
    partes = link.split("/")
    if "twitch.tv" in link:
        return partes[1] if len(partes) > 1 else None
    if "kick.com" in link:
        if len(partes) > 1:
            canal = partes[1]
            if canal == "live" and len(partes) > 2:
                return partes[2]
            return canal
        return None
    if "tiktok.com" in link:
        return partes[1].replace("@", "") if len(partes) > 1 else None
    return None

# ---------------------------------------------------------
# 5.4: FUNÇÕES DE CARGO/PERMISSÃO
# ---------------------------------------------------------

def pode_remover_ausencia(member):
    if not member:
        return False
    return any(role.id in CARGOS_PERMITIDOS_REMOVER for role in member.roles)

def obter_categoria_meta(member):
    if not member:
        return None
    roles = [r.id for r in member.roles]
    if CARGO_GERENTE_ID in roles:
        return CATEGORIA_META_GERENTE_ID
    if any(r in roles for r in [CARGO_RESP_METAS_ID, CARGO_RESP_ACAO_ID, CARGO_RESP_P1_ID, CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID]):
        return CATEGORIA_META_RESPONSAVEIS_ID
    if CARGO_SOLDADO_ID in roles:
        return CATEGORIA_META_SOLDADO_ID
    if CARGO_MEMBRO_ID in roles:
        return CATEGORIA_META_MEMBRO_ID
    if AGREGADO_ROLE_ID in roles:
        return CATEGORIA_META_AGREGADO_ID
    return None

def membro_deve_ter_meta(member):
    if not member:
        return None
    cargos_com_meta = [
        CARGO_AGREGADO_ID, CARGO_MEMBRO_ID, CARGO_SOLDADO_ID,
        CARGO_RESP_METAS_ID, CARGO_RESP_P1_ID, CARGO_RESP_ACAO_ID,
        CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID
    ]
    cargos_isentos = [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID, CARGO_01_ID, CARGO_02_ID]
    roles = [r.id for r in member.roles]
    if any(r in roles for r in cargos_isentos):
        return "isento"
    if any(r in roles for r in cargos_com_meta):
        return "obrigado"
    return None

def tem_cargo_permitido(cargos_ids):
    async def predicate(ctx):
        return any(role.id in cargos_ids for role in ctx.author.roles)
    return commands.check(predicate)

# ---------------------------------------------------------
# 5.5: FUNÇÕES DE ITENS
# ---------------------------------------------------------

def normalizar_nome(texto_digitado):
    if not texto_digitado:
        return None
    texto = texto_digitado.lower().strip()
    if texto in ALIASES:
        return ALIASES[texto]
    for item in ITENS_DISPONIVEIS:
        item_nome = item.split(" ", 1)[1] if " " in item else item
        if item_nome.lower() == texto:
            return item_nome.upper()
    return texto.upper()

def verificar_opcoes(texto_digitado):
    texto = texto_digitado.lower().strip()
    if texto in ITENS_COM_OPCOES:
        return ITENS_COM_OPCOES[texto]
    if texto in ALIASES:
        nome_convertido = ALIASES[texto]
        for chave, opcoes in ITENS_COM_OPCOES.items():
            if nome_convertido in opcoes:
                return opcoes
    return None

# =========================================================
# SEÇÃO 6: RATE LIMITER GLOBAL
# =========================================================

class RateLimiter:
    def __init__(self, max_calls=5, period=10):
        self.max_calls = max_calls
        self.period = period
        self.calls = {}
        self.lock = asyncio.Lock()
    
    async def can_call(self, user_id):
        async with self.lock:
            now = time_module.time()
            if user_id not in self.calls:
                self.calls[user_id] = []
            self.calls[user_id] = [t for t in self.calls[user_id] if now - t < self.period]
            if len(self.calls[user_id]) >= self.max_calls:
                return False
            self.calls[user_id].append(now)
            return True
    
    async def wait_and_call(self, user_id):
        while not await self.can_call(user_id):
            await asyncio.sleep(1)
        return True

rate_limiter = RateLimiter(max_calls=5, period=10)

# =========================================================
# SEÇÃO 7: BANCO DE DADOS - GLOBAL
# =========================================================

db = None
db_lock = asyncio.Lock()
db_reconnect_attempts = 0
MAX_DB_RECONNECT_ATTEMPTS = 10

# ---------------------------------------------------------
# ASYNC: conectar_db
# ---------------------------------------------------------

async def conectar_db():
    global db, db_reconnect_attempts
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL não encontrada!")
        return None
    async with db_lock:
        if db and not db._closed:
            db_reconnect_attempts = 0
            return db
        try:
            db = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=2,
                max_size=10,
                command_timeout=30,
                max_inactive_connection_lifetime=300
            )
            db_reconnect_attempts = 0
            await inicializar_tabelas(db)
            return db
        except Exception as e:
            db_reconnect_attempts += 1
            logger.error(f"❌ Erro ao conectar ao PostgreSQL: {e}")
            if db_reconnect_attempts >= MAX_DB_RECONNECT_ATTEMPTS:
                logger.critical("🔴 Número máximo de tentativas de reconexão atingido!")
                return None
            await asyncio.sleep(5 * db_reconnect_attempts)
            return await conectar_db()

# ---------------------------------------------------------
# FUNÇÃO: get_db
# ---------------------------------------------------------

def get_db():
    global db
    if db and not db._closed:
        return db
    return None

# ---------------------------------------------------------
# ASYNC: get_pool
# ---------------------------------------------------------

async def get_pool():
    pool = get_db()
    if pool:
        return pool
    logger.warning("⚠️ Pool do banco fechado! Reconectando...")
    return await conectar_db()

# ---------------------------------------------------------
# ASYNC: inicializar_tabelas
# ---------------------------------------------------------

async def inicializar_tabelas(pool):
    async with pool.acquire() as conn:
        # Metas
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS metas (
                user_id VARCHAR(30) PRIMARY KEY,
                canal_id VARCHAR(30),
                dinheiro BIGINT DEFAULT 0,
                polvora BIGINT DEFAULT 0,
                acao TEXT,
                dinheiro_acoes BIGINT DEFAULT 0
            )
        """)
        await conn.execute("ALTER TABLE metas ADD COLUMN IF NOT EXISTS saldo_excedente BIGINT DEFAULT 0")

        # Histórico de metas
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS metas_historico (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                dinheiro BIGINT,
                polvora BIGINT,
                acao TEXT,
                dinheiro_acoes BIGINT,
                data_inicio TIMESTAMP,
                data_fim TIMESTAMP,
                data_fechamento TIMESTAMP
            )
        """)

        # Avisos de metas
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS metas_avisos (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                tipo VARCHAR(20),
                data TIMESTAMP
            )
        """)

        # Produções
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS producoes (
                pid VARCHAR(50) PRIMARY KEY,
                galpao TEXT,
                autor VARCHAR(30),
                inicio TIMESTAMP,
                fim TIMESTAMP,
                obs TEXT,
                msg_id VARCHAR(30),
                canal_id VARCHAR(30),
                segunda_task_user VARCHAR(30),
                segunda_task_time TIMESTAMP,
                polvora INTEGER DEFAULT 400,
                qtd_galpoes INTEGER DEFAULT 1,
                polvora_por_galpao INTEGER DEFAULT 400
            )
        """)

        # Produções finalizadas
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS producoes_finalizadas (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                capsulas INTEGER,
                data TIMESTAMP,
                polvora INTEGER,
                galpao TEXT
            )
        """)

        # Produção de munição
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS producao_municao (
                id SERIAL PRIMARY KEY,
                tipo VARCHAR(3),
                pacotes INTEGER,
                municoes INTEGER,
                produzido_por VARCHAR(30),
                obs TEXT,
                capsulas_consumidas INTEGER,
                embalagens_consumidas INTEGER,
                data TIMESTAMP DEFAULT NOW()
            )
        """)

        # Estoque de munições
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS estoque_municoes (
                tipo VARCHAR(3) PRIMARY KEY,
                quantidade INTEGER DEFAULT 0,
                ultima_atualizacao TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            INSERT INTO estoque_municoes (tipo, quantidade)
            VALUES ('PT', 0), ('SUB', 0)
            ON CONFLICT (tipo) DO NOTHING
        """)

        # Estoque de cápsulas
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS estoque_capsulas (
                id INTEGER PRIMARY KEY DEFAULT 1,
                quantidade INTEGER DEFAULT 0,
                ultima_atualizacao TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            INSERT INTO estoque_capsulas (id, quantidade)
            VALUES (1, 0)
            ON CONFLICT (id) DO NOTHING
        """)

        # Estoque de embalagens
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS estoque_embalagens (
                id INTEGER PRIMARY KEY DEFAULT 1,
                quantidade INTEGER DEFAULT 0,
                ultima_atualizacao TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            INSERT INTO estoque_embalagens (id, quantidade)
            VALUES (1, 0)
            ON CONFLICT (id) DO NOTHING
        """)

        # Entrada de insumos
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS entrada_insumos (
                id SERIAL PRIMARY KEY,
                tipo VARCHAR(20),
                quantidade INTEGER,
                registrado_por VARCHAR(30),
                obs TEXT,
                data TIMESTAMP DEFAULT NOW()
            )
        """)

        # Vendas
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS vendas (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                valor INTEGER,
                data VARCHAR(20),
                pedido_numero INTEGER
            )
        """)

        # Pedidos
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pedidos (
                id INTEGER PRIMARY KEY DEFAULT 1,
                ultimo INTEGER DEFAULT 1
            )
        """)
        await conn.execute("""
            INSERT INTO pedidos (id, ultimo)
            VALUES (1, 1)
            ON CONFLICT (id) DO NOTHING
        """)

        # Saída de estoque
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS saida_estoque (
                id SERIAL PRIMARY KEY,
                pedido_numero INTEGER,
                tipo VARCHAR(3),
                pacotes INTEGER,
                retirado_por VARCHAR(30),
                data TIMESTAMP DEFAULT NOW()
            )
        """)

        # Entregas parceladas
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS entregas_parceladas (
                id SERIAL PRIMARY KEY,
                pedido_original INTEGER,
                entrega_atual INTEGER,
                total_entregas INTEGER,
                pt_por_entrega INTEGER,
                sub_por_entrega INTEGER,
                vendedor_id VARCHAR(30),
                organizacao TEXT,
                observacoes TEXT,
                proxima_entrega TIMESTAMP,
                canal_id VARCHAR(30),
                mensagem_ids TEXT[] DEFAULT '{}',
                ativo BOOLEAN DEFAULT true,
                data_criacao TIMESTAMP DEFAULT NOW()
            )
        """)

        # Detalhes das entregas
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS entregas_detalhes (
                entrega_id INTEGER PRIMARY KEY,
                entregas_json TEXT,
                data_criacao TIMESTAMP DEFAULT NOW()
            )
        """)

        # Pólvoras
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS polvoras (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                quantidade INTEGER,
                valor INTEGER,
                data TEXT
            )
        """)

        # Pólvora vendas
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS polvora_vendas (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                quantidade INTEGER,
                valor INTEGER,
                status VARCHAR(20) DEFAULT 'pendente',
                data_venda TIMESTAMP DEFAULT NOW(),
                data_pagamento TIMESTAMP
            )
        """)

        # Lives
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lives (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                link TEXT,
                divulgado BOOLEAN DEFAULT false,
                data_cadastro TIMESTAMP DEFAULT NOW()
            )
        """)

        # Lives manual
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lives_manual (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30) NOT NULL,
                user_name VARCHAR(100) NOT NULL,
                plataforma VARCHAR(20) NOT NULL,
                link VARCHAR(255) NOT NULL,
                titulo VARCHAR(255),
                categoria VARCHAR(100),
                ativo BOOLEAN DEFAULT true,
                data_cadastro TIMESTAMP DEFAULT NOW()
            )
        """)

        # Ausências
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS ausencias (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                nome TEXT,
                motivo TEXT,
                data_inicio TIMESTAMP,
                data_fim TIMESTAMP,
                ativo BOOLEAN DEFAULT true,
                data_criacao TIMESTAMP DEFAULT NOW()
            )
        """)

        # Lavagens
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lavagens (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                valor INTEGER,
                taxa INTEGER,
                liquido INTEGER,
                data TIMESTAMP DEFAULT NOW()
            )
        """)

        # Ações da semana
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS acoes_semana (
                id SERIAL PRIMARY KEY,
                tipo TEXT,
                data TIMESTAMP DEFAULT NOW(),
                autor VARCHAR(30),
                status VARCHAR(20) DEFAULT 'aberta',
                resultado VARCHAR(20),
                valor INTEGER DEFAULT 0
            )
        """)

        # Participantes de ações
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS participantes_acoes (
                id SERIAL PRIMARY KEY,
                acao_id INTEGER,
                user_id VARCHAR(30)
            )
        """)

        # Paineis
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS paineis (
                nome VARCHAR(50) PRIMARY KEY,
                canal_id VARCHAR(30),
                mensagem_id VARCHAR(30),
                data_atualizacao TIMESTAMP DEFAULT NOW()
            )
        """)

        # Grupos
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS grupos (
                grupo_id VARCHAR(50) PRIMARY KEY,
                nome_org TEXT,
                lider_nome TEXT,
                lider_telefone TEXT,
                braco_nome TEXT,
                braco_telefone TEXT,
                produto TEXT,
                data_criacao TIMESTAMP DEFAULT NOW(),
                data_atualizacao TIMESTAMP,
                data_exclusao TIMESTAMP,
                ativo BOOLEAN DEFAULT true
            )
        """)
        await conn.execute("ALTER TABLE grupos ADD COLUMN IF NOT EXISTS tipo_org VARCHAR(30) DEFAULT 'PISTA SEM PAINEL'")
        await conn.execute("ALTER TABLE grupos ADD COLUMN IF NOT EXISTS observacoes TEXT")

        # Compras grupo
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS compras_grupo (
                id SERIAL PRIMARY KEY,
                grupo_id VARCHAR(50),
                tipo VARCHAR(3),
                quantidade INTEGER,
                valor INTEGER,
                data TIMESTAMP DEFAULT NOW()
            )
        """)

        # Compras
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS compras (
                id SERIAL PRIMARY KEY,
                produto TEXT,
                valor INTEGER,
                comprado_por VARCHAR(30),
                data TIMESTAMP DEFAULT NOW()
            )
        """)

        # Registros histórico
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS registros_historico (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                user_name TEXT,
                passaporte TEXT,
                nome TEXT,
                vulgo TEXT,
                telefone TEXT,
                indicado TEXT,
                tipo TEXT,
                data_registro TIMESTAMP DEFAULT NOW()
            )
        """)

        # Alugueis
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS alugueis (
                id SERIAL PRIMARY KEY,
                galpao TEXT NOT NULL,
                dias_alugados INTEGER DEFAULT 0,
                data_inicio TIMESTAMP DEFAULT NOW(),
                ativo BOOLEAN DEFAULT true,
                data_atualizacao TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            UPDATE alugueis
            SET ativo = false
            WHERE galpao NOT IN ('GALPÕES NORTE', 'GALPÕES SUL')
              AND ativo = true
        """)
        existe_norte = await conn.fetchval("SELECT 1 FROM alugueis WHERE galpao = 'GALPÕES NORTE'")
        if not existe_norte:
            await conn.execute("""
                INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo)
                VALUES ('GALPÕES NORTE', 0, NOW(), true)
            """)
        existe_sul = await conn.fetchval("SELECT 1 FROM alugueis WHERE galpao = 'GALPÕES SUL'")
        if not existe_sul:
            await conn.execute("""
                INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo)
                VALUES ('GALPÕES SUL', 0, NOW(), true)
            """)

        # Armas controle
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS armas_controle (
                id SERIAL PRIMARY KEY,
                tipo VARCHAR(10) NOT NULL,
                arma_nome VARCHAR(50) NOT NULL,
                quantidade INT NOT NULL,
                responsavel VARCHAR(100),
                observacao TEXT,
                data TIMESTAMP DEFAULT NOW()
            )
        """)

        # Armas emprestadas
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS armas_emprestadas (
                id SERIAL PRIMARY KEY,
                arma_nome VARCHAR(50) NOT NULL,
                quantidade INT NOT NULL,
                responsavel VARCHAR(100),
                data_retirada TIMESTAMP DEFAULT NOW(),
                data_prevista_devolucao TIMESTAMP,
                observacao TEXT,
                ativo BOOLEAN DEFAULT true
            )
        """)

        # Bau itens
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bau_itens (
                id SERIAL PRIMARY KEY,
                item_nome VARCHAR(100) NOT NULL,
                quantidade INT NOT NULL,
                tipo_movimento VARCHAR(10) NOT NULL,
                responsavel VARCHAR(100),
                observacao TEXT,
                data TIMESTAMP DEFAULT NOW()
            )
        """)

        # Bau estoque
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bau_estoque (
                id SERIAL PRIMARY KEY,
                item_nome VARCHAR(100) UNIQUE NOT NULL,
                quantidade INT DEFAULT 0,
                ultima_atualizacao TIMESTAMP DEFAULT NOW()
            )
        """)

# =========================================================
# SEÇÃO 8: CACHE E MÉTRICAS - GLOBAL
# =========================================================

# ---------------------------------------------------------
# CLASS: CacheManager
# ---------------------------------------------------------

class CacheManager:
    def __init__(self, default_ttl=300, max_size=1000):
        self._cache = {}
        self._default_ttl = default_ttl
        self._max_size = max_size
        self._lock = asyncio.Lock()

    async def get(self, key):
        async with self._lock:
            if key in self._cache:
                value = self._cache[key]
                if time_module.time() - value['timestamp'] < value.get('ttl', self._default_ttl):
                    return value.get('data')
                else:
                    del self._cache[key]
            return None

    async def set(self, key, data, ttl=None):
        async with self._lock:
            if len(self._cache) >= self._max_size:
                oldest = min(self._cache.keys(), key=lambda k: self._cache[k]['timestamp'])
                del self._cache[oldest]
            self._cache[key] = {
                'data': data,
                'ttl': ttl or self._default_ttl,
                'timestamp': time_module.time()
            }

    async def delete(self, key):
        async with self._lock:
            if key in self._cache:
                del self._cache[key]

    async def clear(self):
        async with self._lock:
            self._cache.clear()

    async def clean_expired(self):
        async with self._lock:
            now = time_module.time()
            expired = []
            for key, value in self._cache.items():
                if now - value['timestamp'] > value.get('ttl', self._default_ttl):
                    expired.append(key)
            for key in expired:
                del self._cache[key]
            return len(expired)

    def size(self):
        return len(self._cache)

cache = CacheManager(default_ttl=300, max_size=1000)

# ---------------------------------------------------------
# CLASS: Metricas
# ---------------------------------------------------------

class Metricas:
    def __init__(self):
        self.comandos_executados = 0
        self.erros = 0
        self.requests_api = 0
        self.start_time = time_module.time()

    def incrementar_comando(self):
        self.comandos_executados += 1

    def incrementar_erro(self):
        self.erros += 1

    def incrementar_api(self):
        self.requests_api += 1

    def get_uptime(self):
        return time_module.time() - self.start_time

metricas = Metricas()

# =========================================================
# SEÇÃO 9: VARIÁVEIS GLOBAIS
# =========================================================

http_session = None
user_cache = {}
edit_queue = asyncio.Queue()
fila_clipes = None
clips_postados = set()
metas_cache = {}
alugueis_ativos = {}
galpoes_ativos = set()
producoes_tasks = {}
mensagens_em_andamento = set()
mensagens_timers = {}
acoes_ativas = {}
cache_lives = {}
cache_lives_timestamp = 0
CACHE_LIVES_TTL = 120
twitch_token = None
twitch_token_expira = 0
lavagens_pendentes = {}

# =========================================================
# SEÇÃO 10: FUNÇÕES SEGURAS - GLOBAL
# =========================================================

# ---------------------------------------------------------
# ASYNC: safe_request
# ---------------------------------------------------------

async def safe_request(func, *args, max_retries=3, **kwargs):
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except discord.HTTPException as e:
            if e.status == 429:
                retry_after = e.retry_after or 5
                logger.warning(f"⚠️ Rate limit! Aguardando {retry_after}s (tentativa {attempt+1}/{max_retries})")
                await asyncio.sleep(retry_after + 1)
            else:
                raise
        except Exception as e:
            logger.error(f"Erro no safe_request: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(2 * (attempt + 1))
            else:
                raise
    return None

# ---------------------------------------------------------
# ASYNC: retry_operation
# ---------------------------------------------------------

async def retry_operation(func, *args, max_retries=3, delay=2, **kwargs):
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            logger.warning(f"⚠️ Tentativa {attempt+1}/{max_retries} falhou: {e}")
            if attempt < max_retries - 1:
                await asyncio.sleep(delay * (attempt + 1))
            else:
                raise

# ---------------------------------------------------------
# ASYNC: safe_fetch_message
# ---------------------------------------------------------

async def safe_fetch_message(canal, msg_id):
    try:
        return await canal.fetch_message(msg_id)
    except discord.NotFound:
        return None
    except discord.Forbidden:
        return None
    except Exception as e:
        logger.error(f"Erro ao buscar mensagem {msg_id}: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: responder_interacao
# ---------------------------------------------------------

async def responder_interacao(interaction: discord.Interaction, *, defer=False, ephemeral=False):
    try:
        if interaction.response.is_done():
            return
        if defer:
            await interaction.response.defer(ephemeral=ephemeral)
        else:
            await interaction.response.defer(ephemeral=True)
    except discord.errors.HTTPException:
        pass
    except Exception as e:
        logger.error(f"Erro responder_interacao: {e}")

# =========================================================
# SEÇÃO 11: EDIT WORKER - GLOBAL
# =========================================================

# ---------------------------------------------------------
# ASYNC: edit_worker
# ---------------------------------------------------------

async def edit_worker():
    while True:
        try:
            coro = await edit_queue.get()
            await coro
            await asyncio.sleep(1.2)
        except discord.NotFound:
            pass
        except discord.HTTPException as e:
            if e.status == 429:
                await asyncio.sleep(3)
            else:
                logger.error(f"Erro HTTP edit_worker: {e}")
        except Exception as e:
            logger.error(f"Erro no edit_worker: {e}")
        edit_queue.task_done()

# =========================================================
# SEÇÃO 12: ENVIAR/ATUALIZAR PAINEL - GLOBAL
# =========================================================

# ---------------------------------------------------------
# ASYNC: enviar_ou_atualizar_painel
# ---------------------------------------------------------

async def enviar_ou_atualizar_painel(nome, canal_id, embed, view):
    canal = bot.get_channel(canal_id)
    if not canal:
        logger.error(f"❌ Canal não encontrado para painel: {nome}")
        return
    pool = await get_pool()
    if not pool:
        logger.error(f"❌ Banco de dados não disponível para painel: {nome}")
        return
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT mensagem_id, canal_id FROM paineis WHERE nome=$1", nome)
            if row:
                try:
                    canal_salvo = bot.get_channel(int(row["canal_id"])) or canal
                    msg = await safe_fetch_message(canal_salvo, int(row["mensagem_id"]))
                    if msg:
                        await msg.edit(embed=embed, view=view)
                        return
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao atualizar painel {nome}: {e}")
            msg = await safe_request(canal.send, embed=embed, view=view)
            if msg:
                await conn.execute(
                    "INSERT INTO paineis (nome, canal_id, mensagem_id) VALUES ($1,$2,$3) ON CONFLICT (nome) DO UPDATE SET canal_id=$2, mensagem_id=$3",
                    nome, str(canal_id), str(msg.id)
                )
    except Exception as e:
        logger.error(f"❌ Erro crítico ao enviar painel {nome}: {e}")

# =========================================================
# SEÇÃO 13: PEAR USUÁRIO - GLOBAL
# =========================================================

# ---------------------------------------------------------
# ASYNC: pegar_usuario
# ---------------------------------------------------------

async def pegar_usuario(uid):
    if uid in user_cache:
        return user_cache[uid]
    try:
        user = await bot.fetch_user(uid)
        user_cache[uid] = user
        return user
    except:
        return None

# =========================================================
# ==================== SISTEMA DE METAS ===================
# =========================================================

# ---------------------------------------------------------
# ASYNC: carregar_metas_db
# ---------------------------------------------------------

async def carregar_metas_db():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM metas")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar metas: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: salvar_meta_db
# ---------------------------------------------------------

async def salvar_meta_db(user_id, canal_id, dinheiro, polvora, acao):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            if acao is not None:
                acao = str(acao)
            await conn.execute(
                """
                INSERT INTO metas (user_id, canal_id, dinheiro, polvora, acao, dinheiro_acoes, saldo_excedente)
                VALUES ($1,$2,$3,$4,$5,0,0)
                ON CONFLICT (user_id)
                DO UPDATE SET canal_id=$2, dinheiro=$3, polvora=$4, acao=$5
                """,
                str(user_id), str(canal_id), dinheiro, polvora, acao
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar meta: {e}")

# ---------------------------------------------------------
# ASYNC: depositar_na_meta_db
# ---------------------------------------------------------

async def depositar_na_meta_db(user_id, valor):
    pool = await get_pool()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT dinheiro FROM metas WHERE user_id = $1", str(user_id))
            if meta:
                novo_valor = meta["dinheiro"] + valor
                await conn.execute("UPDATE metas SET dinheiro = $1 WHERE user_id = $2", novo_valor, str(user_id))
                return True
            return False
    except Exception as e:
        logger.error(f"❌ Erro ao depositar na meta: {e}")
        return False

# ---------------------------------------------------------
# ASYNC: adicionar_polvora_meta
# ---------------------------------------------------------

async def adicionar_polvora_meta(user_id, quantidade):
    pool = await get_pool()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT polvora FROM metas WHERE user_id = $1", str(user_id))
            if meta:
                novo_valor = meta["polvora"] + quantidade
                await conn.execute("UPDATE metas SET polvora = $1 WHERE user_id = $2", novo_valor, str(user_id))
                return True
            return False
    except Exception as e:
        logger.error(f"❌ Erro ao adicionar pólvora: {e}")
        return False

# ---------------------------------------------------------
# ASYNC: adicionar_dinheiro_meta
# ---------------------------------------------------------

async def adicionar_dinheiro_meta(user_id, valor):
    pool = get_db()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT dinheiro, saldo_excedente FROM metas WHERE user_id = $1", str(user_id))
            if not meta:
                return False
            dinheiro_atual = meta["dinheiro"] or 0
            saldo_excedente = meta["saldo_excedente"] or 0
            META_LIMITE = 300000
            falta_para_meta = max(0, META_LIMITE - dinheiro_atual)
            if valor <= falta_para_meta:
                novo_dinheiro = dinheiro_atual + valor
                await conn.execute("UPDATE metas SET dinheiro = $1 WHERE user_id = $2", novo_dinheiro, str(user_id))
            else:
                novo_dinheiro = META_LIMITE
                novo_excedente = saldo_excedente + (valor - falta_para_meta)
                await conn.execute(
                    "UPDATE metas SET dinheiro = $1, saldo_excedente = $2 WHERE user_id = $3",
                    novo_dinheiro, novo_excedente, str(user_id)
                )
            return True
    except Exception as e:
        logger.error(f"❌ Erro ao adicionar dinheiro: {e}")
        return False

# ---------------------------------------------------------
# ASYNC: fechar_meta
# ---------------------------------------------------------

async def fechar_meta(user_id, data_inicio, data_fim):
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(user_id))
            if not meta:
                return None

            dinheiro = meta["dinheiro"] or 0
            polvora = meta["polvora"] or 0
            acao = meta.get("acao") or "N/A"
            dinheiro_acoes = meta.get("dinheiro_acoes") or 0
            saldo_excedente = meta.get("saldo_excedente") or 0
            
            data_fechamento = agora_db()

            await conn.execute(
                """
                INSERT INTO metas_historico (
                    user_id, dinheiro, polvora, acao, dinheiro_acoes, 
                    data_inicio, data_fim, data_fechamento
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                str(user_id), 
                min(dinheiro, 300000), 
                polvora, 
                str(acao), 
                dinheiro_acoes, 
                data_inicio,
                data_fim,
                data_fechamento
            )

            await conn.execute(
                """
                UPDATE metas 
                SET acao = NULL
                WHERE user_id = $1
                """,
                str(user_id)
            )

            return {
                "dinheiro": min(dinheiro, 300000),
                "polvora": polvora,
                "acao": acao,
                "dinheiro_acoes": dinheiro_acoes,
                "excedente": saldo_excedente
            }
    except Exception as e:
        logger.error(f"❌ Erro ao fechar meta: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: fechar_todas_metas
# ---------------------------------------------------------

async def fechar_todas_metas(data_inicio, data_fim):
    pool = await get_pool()
    if not pool:
        logger.error("❌ Pool do banco indisponível em fechar_todas_metas")
        return None, []
    try:
        async with pool.acquire() as conn:
            metas = await conn.fetch("SELECT * FROM metas")
            if not metas:
                logger.warning("📭 Nenhuma meta encontrada para fechar")
                return None, []

            data_inicio_naive = data_inicio.replace(tzinfo=None) if hasattr(data_inicio, 'replace') else data_inicio
            data_fim_naive = data_fim.replace(tzinfo=None) if hasattr(data_fim, 'replace') else data_fim
            data_fechamento = agora_db()

            relatorio = []
            guild = bot.get_guild(GUILD_ID)
            salvos = 0

            for meta in metas:
                user_id = meta["user_id"]
                member = guild.get_member(int(user_id)) if guild else None

                status = membro_deve_ter_meta(member) if member else None
                if status is None:
                    continue

                dinheiro = meta["dinheiro"] or 0
                polvora = meta["polvora"] or 0
                acao = meta["acao"] or "N/A"
                dinheiro_acoes = meta.get("dinheiro_acoes") or 0

                try:
                    await conn.execute(
                        """
                        INSERT INTO metas_historico (
                            user_id, dinheiro, polvora, acao, dinheiro_acoes, 
                            data_inicio, data_fim, data_fechamento
                        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        """,
                        user_id, 
                        min(dinheiro, 300000), 
                        polvora, 
                        acao, 
                        dinheiro_acoes, 
                        data_inicio_naive,
                        data_fim_naive,
                        data_fechamento
                    )
                    salvos += 1
                except Exception as e:
                    logger.error(f"❌ Erro ao salvar meta de {user_id} no histórico: {e}")

                relatorio.append({
                    "user_id": user_id,
                    "dinheiro": min(dinheiro, 300000),
                    "polvora": polvora,
                    "acao": acao,
                    "dinheiro_acoes": dinheiro_acoes,
                    "total_meta": min(dinheiro, 300000),
                    "status": status
                })

            membros_sem_meta = []
            if guild:
                cargos_meta = [CARGO_AGREGADO_ID, CARGO_MEMBRO_ID, CARGO_SOLDADO_ID, CARGO_01_ID, CARGO_02_ID,
                              CARGO_RESP_METAS_ID, CARGO_RESP_ACAO_ID, CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID]
                for member in guild.members:
                    if member.bot:
                        continue
                    tem_cargo = any(r.id in cargos_meta for r in member.roles)
                    if tem_cargo:
                        tem_meta = any(m["user_id"] == str(member.id) for m in metas)
                        if not tem_meta:
                            membros_sem_meta.append({
                                "user_id": str(member.id),
                                "nome": member.display_name,
                                "menção": member.mention
                            })

            return relatorio, membros_sem_meta
    except Exception as e:
        logger.error(f"❌ Erro ao fechar todas as metas: {e}")
        return None, []

# ---------------------------------------------------------
# ASYNC: zerar_todas_metas
# ---------------------------------------------------------

async def zerar_todas_metas():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id, canal_id FROM metas")
            await conn.execute("""
                UPDATE metas 
                SET dinheiro = 0, 
                    dinheiro_acoes = 0, 
                    polvora = 0, 
                    saldo_excedente = 0,
                    acao = NULL
            """)
            logger.info(f"⚠️ TODAS AS METAS FORAM ZERADAS! {len(rows)} metas resetadas.")
            return rows
    except Exception as e:
        logger.error(f"❌ Erro ao zerar metas: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: buscar_historico_metas
# ---------------------------------------------------------

async def buscar_historico_metas(data_inicio, data_fim):
    pool = await get_pool()
    if not pool:
        logger.error("❌ Pool do banco indisponível em buscar_historico_metas")
        return []
    try:
        async with pool.acquire() as conn:
            inicio_dt = data_inicio.replace(tzinfo=None) if hasattr(data_inicio, 'replace') else data_inicio
            fim_dt = data_fim.replace(tzinfo=None) if hasattr(data_fim, 'replace') else data_fim
            
            rows = await conn.fetch(
                """
                SELECT * FROM metas_historico 
                WHERE data_fechamento >= $1 
                AND data_fechamento <= $2
                ORDER BY data_fechamento DESC
                """,
                inicio_dt, fim_dt
            )
            
            return rows
    except Exception as e:
        logger.error(f"❌ Erro ao buscar histórico: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: carregar_metas_cache
# ---------------------------------------------------------

async def carregar_metas_cache():
    global metas_cache
    try:
        rows = await carregar_metas_db()
        metas_cache = {}
        for r in rows:
            metas_cache[str(r["user_id"])] = {
                "canal_id": int(r["canal_id"]),
                "dinheiro": r["dinheiro"],
                "polvora": r["polvora"],
                "acao": r["acao"],
                "dinheiro_acoes": r.get("dinheiro_acoes") or 0,
                "saldo_excedente": r.get("saldo_excedente") or 0
            }
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao recarregar cache de metas: {e}")
        return False

# ---------------------------------------------------------
# ASYNC: definir_valor_meta_por_cargo
# ---------------------------------------------------------

async def definir_valor_meta_por_cargo(member: discord.Member):
    roles = [r.id for r in member.roles]
    cargos_isentos = [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID, CARGO_01_ID, CARGO_02_ID]
    if any(r in roles for r in cargos_isentos):
        return 0
    cargos_responsaveis = [CARGO_RESP_METAS_ID, CARGO_RESP_ACAO_ID, CARGO_RESP_P1_ID, CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID]
    if any(r in roles for r in cargos_responsaveis):
        return 100000
    if CARGO_SOLDADO_ID in roles:
        return 300000
    return 300000

# ---------------------------------------------------------
# ASYNC: criar_sala_meta
# ---------------------------------------------------------

async def criar_sala_meta(member: discord.Member):
    guild = member.guild
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            meta_existente = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(member.id))
        if meta_existente:
            canal_id = int(meta_existente["canal_id"])
            canal_existe = guild.get_channel(canal_id)
            if canal_existe:
                metas_cache[str(member.id)] = {
                    "canal_id": canal_id,
                    "dinheiro": meta_existente["dinheiro"],
                    "polvora": meta_existente["polvora"],
                    "acao": meta_existente["acao"],
                    "dinheiro_acoes": meta_existente.get("dinheiro_acoes") or 0,
                    "saldo_excedente": meta_existente.get("saldo_excedente") or 0
                }
                await atualizar_embed_meta(member.id)
                
                cargo_resp = guild.get_role(CARGO_RESP_METAS_ID)
                if cargo_resp:
                    for resp_member in guild.members:
                        if cargo_resp in resp_member.roles:
                            try:
                                perms = canal_existe.permissions_for(resp_member)
                                if not perms.view_channel:
                                    await canal_existe.set_permissions(resp_member, view_channel=True, send_messages=True)
                            except Exception as e:
                                logger.error(f"❌ Erro ao dar acesso a {resp_member.display_name}: {e}")
                
                return canal_existe
            else:
                await conn.execute("DELETE FROM metas WHERE user_id = $1", str(member.id))
                if str(member.id) in metas_cache:
                    del metas_cache[str(member.id)]

        for canal in guild.text_channels:
            if member.display_name.lower() in canal.name.lower() and "📁" in canal.name:
                await salvar_meta_db(member.id, canal.id, 0, 0, 0)
                metas_cache[str(member.id)] = {
                    "canal_id": canal.id,
                    "dinheiro": 0,
                    "polvora": 0,
                    "acao": None,
                    "dinheiro_acoes": 0,
                    "saldo_excedente": 0
                }
                await atualizar_embed_meta(member.id)
                
                cargo_resp = guild.get_role(CARGO_RESP_METAS_ID)
                if cargo_resp:
                    for resp_member in guild.members:
                        if cargo_resp in resp_member.roles:
                            try:
                                perms = canal.permissions_for(resp_member)
                                if not perms.view_channel:
                                    await canal.set_permissions(resp_member, view_channel=True, send_messages=True)
                            except Exception as e:
                                logger.error(f"❌ Erro ao dar acesso a {resp_member.display_name}: {e}")
                
                return canal

        categoria_id = obter_categoria_meta(member)
        if not categoria_id:
            return None
        categoria = guild.get_channel(categoria_id)
        if not categoria:
            return None

        nome_canal = f"📁・{member.display_name.lower().replace(' ', '-')}"
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            member: discord.PermissionOverwrite(view_channel=True, send_messages=True)
        }
        gerente = guild.get_role(CARGO_GERENTE_ID)
        if gerente:
            overwrites[gerente] = discord.PermissionOverwrite(view_channel=True)
        gerente_geral = guild.get_role(CARGO_GERENTE_GERAL_ID)
        if gerente_geral:
            overwrites[gerente_geral] = discord.PermissionOverwrite(view_channel=True)

        canal = await guild.create_text_channel(nome_canal, category=categoria, overwrites=overwrites)

        await salvar_meta_db(member.id, canal.id, 0, 0, 0)
        metas_cache[str(member.id)] = {
            "canal_id": canal.id,
            "dinheiro": 0,
            "polvora": 0,
            "acao": None,
            "dinheiro_acoes": 0,
            "saldo_excedente": 0
        }
        await asyncio.sleep(1)
        await atualizar_embed_meta(member.id)

        cargo_resp = guild.get_role(CARGO_RESP_METAS_ID)
        if cargo_resp:
            for resp_member in guild.members:
                if cargo_resp in resp_member.roles:
                    try:
                        await canal.set_permissions(resp_member, view_channel=True, send_messages=True)
                    except Exception as e:
                        logger.error(f"❌ Erro ao dar acesso a {resp_member.display_name}: {e}")

        return canal
    except Exception as e:
        logger.error(f"❌ Erro ao criar sala meta: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: atualizar_embed_meta
# ---------------------------------------------------------

async def atualizar_embed_meta(user_id):
    try:
        if str(user_id) not in metas_cache:
            await carregar_metas_cache()
            if str(user_id) not in metas_cache:
                guild = bot.get_guild(GUILD_ID)
                member = guild.get_member(int(user_id))
                if member:
                    await criar_sala_meta(member)
                    await carregar_metas_cache()
                return
        dados = metas_cache[str(user_id)]
        canal = bot.get_channel(dados["canal_id"])
        if not canal:
            if str(user_id) in metas_cache:
                del metas_cache[str(user_id)]
            pool = await get_pool()
            if pool:
                async with pool.acquire() as conn:
                    await conn.execute("DELETE FROM metas WHERE user_id = $1", str(user_id))
            return
        pool = await get_pool()
        if not pool:
            return
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(user_id))
        if not meta:
            await salvar_meta_db(user_id, canal.id, 0, 0, 0)
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(user_id))
            if not meta:
                return
            metas_cache[str(user_id)] = {
                "canal_id": canal.id,
                "dinheiro": 0,
                "polvora": 0,
                "acao": None,
                "dinheiro_acoes": 0,
                "saldo_excedente": 0
            }
        pendente = await buscar_polvora_pendente(user_id)
        guild = bot.get_guild(GUILD_ID)
        member = guild.get_member(int(user_id))
        if member:
            nome = member.display_name
            is_soldado = CARGO_SOLDADO_ID in [r.id for r in member.roles]
        else:
            nome = str(user_id)
            is_soldado = False
        dinheiro_meta = meta["dinheiro"] or 0
        dinheiro_acoes = meta.get("dinheiro_acoes") or 0
        polvora = meta["polvora"] or 0
        saldo_excedente = meta.get("saldo_excedente") or 0
        acao = meta.get("acao")
        if acao is None:
            acao = "Nenhuma"
        else:
            acao = str(acao)
        meta_total = await definir_valor_meta_por_cargo(member) if member else 300000
        embed = discord.Embed(title=f"📊 META DE {nome.upper()}", color=0x3498db, timestamp=agora())
        embed.add_field(name="💰 DINHEIRO SUJO (Meta)", value=formatar_dinheiro(dinheiro_meta), inline=False)
        if is_soldado:
            embed.add_field(name="🎯 DINHEIRO DE AÇÕES (Meta do Soldado)", value=formatar_dinheiro(dinheiro_acoes), inline=False)
        if saldo_excedente > 0:
            embed.add_field(name="📦 SALDO EXCEDENTE (Próxima semana)", value=formatar_dinheiro(saldo_excedente), inline=False)
        if pendente and pendente["quantidade"] > 0:
            embed.add_field(
                name="💣 PÓLVORA",
                value=f"**Na meta:** {fmt_num(polvora)} unidades\n**Vendida (pendente):** {fmt_num(pendente['quantidade'])} unidades (R$ {formatar_dinheiro(pendente['valor'])})",
                inline=False
            )
        else:
            embed.add_field(name="💣 PÓLVORA", value=f"{fmt_num(polvora)} unidades" if polvora > 0 else "0 unidades", inline=False)
        if is_soldado:
            valor_progresso = dinheiro_acoes
        else:
            valor_progresso = dinheiro_meta
        if meta_total > 0:
            progresso = min(valor_progresso / meta_total, 1.0)
        else:
            progresso = 1.0
        barra_progresso = "▓" * int(progresso * 20) + "░" * (20 - int(progresso * 20))
        porcentagem = int(progresso * 100)
        if meta_total == 0:
            status_meta = "🟢 META ISENTA (Gerente)"
            cor_status = 0x2ecc71
        elif progresso >= 1:
            status_meta = "✅ META CONCLUÍDA! 🎉"
            cor_status = 0x2ecc71
        elif progresso >= 0.7:
            status_meta = "🟢 Quase lá!"
            cor_status = 0x2ecc71
        elif progresso >= 0.4:
            status_meta = "🟡 Vamos acelerar!"
            cor_status = 0xf1c40f
        elif progresso >= 0.1:
            status_meta = "🟠 Começando..."
            cor_status = 0xe67e22
        else:
            status_meta = "🔴 Comece já!"
            cor_status = 0xe74c3c
        embed.add_field(
            name="📊 PROGRESSO DA META",
            value=f"`{barra_progresso}` **{porcentagem}%**\n**{status_meta}**\n💰 {formatar_dinheiro(valor_progresso)} / {formatar_dinheiro(meta_total)}",
            inline=False
        )
        if is_soldado:
            texto_acao = "**🎯 Participar de Ações** - Sua meta é paga com ações realizadas\n**💰 Adicionar Dinheiro Sujo** - Registre dinheiro extra"
        else:
            texto_acao = "**💣 Vender Pólvora** - Venda pólvora para a facção\n**💰 Adicionar Dinheiro Sujo** - Registre dinheiro da meta\n**💰 Pólvora Paga** - Gerente paga a pólvora pendente"
        embed.add_field(name="📌 COMO USAR", value=texto_acao, inline=False)
        embed.set_footer(text=f"ID: {user_id}")
        async for msg in canal.history(limit=30):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    await asyncio.sleep(0.3)
                except:
                    pass
        await canal.send(embed=embed, view=MetaView(user_id))
        await verificar_meta_concluida(user_id, valor_progresso)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar embed da meta: {e}")

# ---------------------------------------------------------
# ASYNC: atualizar_categoria_meta
# ---------------------------------------------------------

async def atualizar_categoria_meta(member):
    try:
        if str(member.id) not in metas_cache:
            return
        dados = metas_cache[str(member.id)]
        canal = member.guild.get_channel(dados["canal_id"])
        if not canal:
            return
        nova_categoria_id = obter_categoria_meta(member)
        if not nova_categoria_id:
            return
        nova_categoria = member.guild.get_channel(nova_categoria_id)
        if not nova_categoria:
            return
        if canal.category_id == nova_categoria_id:
            return
        await canal.edit(category=nova_categoria)
        await atualizar_embed_meta(member.id)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar categoria de {member.name}: {e}")

# ---------------------------------------------------------
# ASYNC: fixar_painel_meta_no_final
# ---------------------------------------------------------

async def fixar_painel_meta_no_final(user_id):
    try:
        if str(user_id) not in metas_cache:
            return
        dados = metas_cache[str(user_id)]
        canal = bot.get_channel(dados["canal_id"])
        if not canal:
            return
        mensagem_painel = None
        async for msg in canal.history(limit=30):
            if msg.author == bot.user and msg.embeds and len(msg.embeds) > 0:
                if msg.embeds[0].title and "META DE" in msg.embeds[0].title.upper():
                    mensagem_painel = msg
                    break
        if not mensagem_painel:
            await atualizar_embed_meta(user_id)
            return
        ultima_msg = None
        async for msg in canal.history(limit=1):
            ultima_msg = msg
            break
        if ultima_msg and ultima_msg.id == mensagem_painel.id:
            return
        try:
            await mensagem_painel.delete()
            await asyncio.sleep(0.5)
            await atualizar_embed_meta(user_id)
        except Exception as e:
            logger.error(f"Erro ao recolocar painel: {e}")
    except Exception as e:
        logger.error(f"❌ Erro ao fixar painel: {e}")

# ---------------------------------------------------------
# ASYNC: depositar_na_meta
# ---------------------------------------------------------

async def depositar_na_meta(user_id, valor, motivo):
    pool = await get_pool()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT dinheiro, dinheiro_acoes, saldo_excedente FROM metas WHERE user_id = $1", str(user_id))
            if not meta:
                return False

            META_LIMITE = 300000
            dinheiro_atual = meta["dinheiro"] or 0
            dinheiro_acoes = meta["dinheiro_acoes"] or 0
            saldo_excedente = meta["saldo_excedente"] or 0

            if "Ação" in motivo:
                novo_acoes = dinheiro_acoes + valor
                await conn.execute("UPDATE metas SET dinheiro_acoes = $1 WHERE user_id = $2", novo_acoes, str(user_id))
            else:
                falta_para_meta = max(0, META_LIMITE - dinheiro_atual)

                if valor <= falta_para_meta:
                    novo_dinheiro = dinheiro_atual + valor
                    await conn.execute("UPDATE metas SET dinheiro = $1 WHERE user_id = $2", novo_dinheiro, str(user_id))
                else:
                    novo_dinheiro = META_LIMITE
                    novo_excedente = saldo_excedente + (valor - falta_para_meta)
                    await conn.execute(
                        "UPDATE metas SET dinheiro = $1, saldo_excedente = $2 WHERE user_id = $3",
                        novo_dinheiro, novo_excedente, str(user_id)
                    )

                canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(user_id))
                if canal_id:
                    canal = bot.get_channel(int(canal_id))
                    if canal:
                        await canal.send(f"💰 **Depósito recebido!**\n📝 Motivo: {motivo}\n💵 Valor: {formatar_dinheiro(valor)}\n✨ **Saldo atualizado na sua meta!**")

            return True
    except Exception as e:
        logger.error(f"❌ Erro ao depositar na meta: {e}")
        return False

# ---------------------------------------------------------
# ASYNC: verificar_meta_concluida
# ---------------------------------------------------------

async def verificar_meta_concluida(user_id, valor_total):
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return False
    member = guild.get_member(int(user_id))
    if not member:
        return False
    meta_total = await definir_valor_meta_por_cargo(member)
    if meta_total == 0:
        return False
    if valor_total >= meta_total:
        pool = await get_pool()
        if not pool:
            return False
        try:
            async with pool.acquire() as conn:
                ja_avisado = await conn.fetchval(
                    "SELECT 1 FROM metas_avisos WHERE user_id = $1 AND tipo = 'concluida' AND data > NOW() - INTERVAL '1 day'",
                    str(user_id)
                )
                if not ja_avisado:
                    await conn.execute(
                        "INSERT INTO metas_avisos (user_id, tipo, data) VALUES ($1, 'concluida', $2)",
                        str(user_id), agora_db()
                    )
                    canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(user_id))
                    if canal_id:
                        canal = bot.get_channel(int(canal_id))
                        if canal:
                            user = await pegar_usuario(user_id)
                            embed = discord.Embed(
                                title="🎉 META SEMANAL CONCLUÍDA!",
                                description=f"{user.mention} **parabéns!** Sua meta semanal foi atingida! 🎉",
                                color=0x2ecc71
                            )
                            embed.add_field(name="💰 Total atingido", value=formatar_dinheiro(valor_total), inline=True)
                            embed.add_field(name="📅 Data", value=agora().strftime('%d/%m/%Y %H:%M'), inline=True)
                            embed.add_field(name="🎯 Meta da semana", value=formatar_dinheiro(meta_total), inline=True)
                            await canal.send(embed=embed)
                            return True
            return False
        except Exception as e:
            logger.error(f"❌ Erro ao verificar meta concluída: {e}")
            return False
    return False

# ---------------------------------------------------------
# ASYNC: verificar_avisos_quarta
# ---------------------------------------------------------

async def verificar_avisos_quarta():
    hoje = agora()
    if hoje.weekday() != 2:
        return
    pool = await get_pool()
    if not pool:
        logger.error("❌ Banco de dados indisponível!")
        return
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            logger.error("❌ Guild não encontrada!")
            return
        cargos_obrigados = [
            CARGO_AGREGADO_ID, CARGO_MEMBRO_ID, CARGO_SOLDADO_ID,
            CARGO_01_ID, CARGO_02_ID, CARGO_RESP_METAS_ID, CARGO_RESP_P1_ID,
            CARGO_RESP_ACAO_ID, CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID
        ]
        async with pool.acquire() as conn:
            avisos_enviados = 0
            for member in guild.members:
                if member.bot:
                    continue
                tem_cargo = any(r.id in cargos_obrigados for r in member.roles)
                if not tem_cargo:
                    continue
                user_id = str(member.id)
                meta = await conn.fetchrow("SELECT dinheiro, dinheiro_acoes FROM metas WHERE user_id = $1", user_id)
                if not meta:
                    canal_existente = None
                    for canal in guild.text_channels:
                        if member.display_name.lower() in canal.name.lower() and "📁" in canal.name:
                            canal_existente = canal
                            break
                    if canal_existente:
                        await salvar_meta_db(member.id, canal_existente.id, 0, 0, 0)
                    else:
                        await criar_sala_meta(member)
                    meta = await conn.fetchrow("SELECT dinheiro, dinheiro_acoes FROM metas WHERE user_id = $1", user_id)
                    if not meta:
                        continue
                dinheiro = meta["dinheiro"] or 0
                dinheiro_acoes = meta.get("dinheiro_acoes") or 0
                total = dinheiro + dinheiro_acoes
                if total == 0:
                    ja_avisado = await conn.fetchval(
                        "SELECT 1 FROM metas_avisos WHERE user_id = $1 AND tipo = 'quarta' AND data::date = $2",
                        user_id, hoje.date()
                    )
                    if not ja_avisado:
                        await conn.execute(
                            "INSERT INTO metas_avisos (user_id, tipo, data) VALUES ($1, 'quarta', $2)",
                            user_id, agora_db()
                        )
                        canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", user_id)
                        if canal_id:
                            canal = bot.get_channel(int(canal_id))
                            if canal:
                                embed = discord.Embed(
                                    title="⚠️ AVISO DE META SEMANAL",
                                    description=f"{member.mention} **atenção!**",
                                    color=0xe74c3c
                                )
                                embed.add_field(
                                    name="📌 Você ainda NÃO fez nenhum depósito na sua meta esta semana!",
                                    value=(
                                        "⏰ **Você tem até domingo para completar sua meta!**\n\n"
                                        "⚠️ **Consequências:**\n"
                                        "• Se NÃO fechar a meta: **REBAIXAMENTO** na facção\n"
                                        "• Se atrasar 2 vezes: **REMOÇÃO** da facção\n\n"
                                        "💪 **Corra atrás do prejuízo!**"
                                    ),
                                    inline=False
                                )
                                embed.set_footer(text="Meta semanal • Vida Rasa")
                                await canal.send(embed=embed)
                                avisos_enviados += 1
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao verificar avisos de quarta: {e}")
        return False

# ---------------------------------------------------------
# ASYNC: zerar_exibicao_metas
# ---------------------------------------------------------

async def zerar_exibicao_metas():
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            logger.error("❌ Guild não encontrada para zerar exibição")
            return 0
        
        await carregar_metas_cache()
        
        contador = 0
        for uid in list(metas_cache.keys()):
            await atualizar_embed_meta(int(uid))
            contador += 1
            await asyncio.sleep(0.3)
        
        logger.info(f"✅ {contador} embeds de metas atualizados (exibição zerada)")
        return contador
    except Exception as e:
        logger.error(f"❌ Erro ao zerar exibição das metas: {e}")
        return 0

# ---------------------------------------------------------
# ASYNC: atualizar_acesso_responsaveis
# ---------------------------------------------------------

async def atualizar_acesso_responsaveis():
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            logger.error("❌ Guild não encontrada!")
            return
        
        cargo_resp = guild.get_role(CARGO_RESP_METAS_ID)
        if not cargo_resp:
            logger.error(f"❌ Cargo RESP_METAS não encontrado! ID: {CARGO_RESP_METAS_ID}")
            return
        
        membros_resp = [m for m in guild.members if cargo_resp in m.roles]
        if not membros_resp:
            return
        
        categorias_permitidas = [
            CATEGORIA_META_GERENTE_ID,
            CATEGORIA_META_RESPONSAVEIS_ID,
            CATEGORIA_META_SOLDADO_ID,
            CATEGORIA_META_MEMBRO_ID,
            CATEGORIA_META_AGREGADO_ID
        ]
        
        for uid, dados in metas_cache.items():
            canal = guild.get_channel(dados["canal_id"])
            if not canal:
                continue
            if canal.category_id not in categorias_permitidas:
                continue
            for membro in membros_resp:
                perms = canal.permissions_for(membro)
                if not perms.view_channel:
                    try:
                        await canal.set_permissions(membro, view_channel=True, send_messages=True)
                    except Exception as e:
                        logger.error(f"❌ Erro ao dar acesso a {membro.display_name}: {e}")
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar acesso dos responsáveis: {e}")

# ---------------------------------------------------------
# ASYNC: gerar_relatorio_metas
# ---------------------------------------------------------

async def gerar_relatorio_metas(interaction, data_inicio_str, data_fim_str, historico, titulo_extra=""):
    try:
        if not historico:
            await interaction.followup.send(f"📭 Nenhuma meta fechada no período **{data_inicio_str}** até **{data_fim_str}**.", ephemeral=True)
            return
        
        total_dinheiro = sum(r["dinheiro"] for r in historico)
        total_acoes = sum(r.get("dinheiro_acoes") or 0 for r in historico)
        total_geral = total_dinheiro + total_acoes
        
        guild = interaction.guild
        
        grupos = {
            "gerentes": {
                "cargos": [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID],
                "nome": "🟢 GERENTES (ISENTOS)",
                "cor": 0x2ecc71,
                "itens": [],
                "is_isento": True
            },
            "cargos_01_02": {
                "cargos": [CARGO_01_ID, CARGO_02_ID],
                "nome": "🟡 CARGOS 01/02 (ISENTOS)",
                "cor": 0xf1c40f,
                "itens": [],
                "is_isento": True
            },
            "responsaveis": {
                "cargos": [CARGO_RESP_METAS_ID, CARGO_RESP_ACAO_ID, CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID],
                "nome": "🔵 RESPONSÁVEIS",
                "cor": 0x3498db,
                "itens": [],
                "is_isento": False
            },
            "soldados": {
                "cargos": [CARGO_SOLDADO_ID],
                "nome": "🟠 SOLDADOS",
                "cor": 0xe67e22,
                "itens": [],
                "is_isento": False
            },
            "membros": {
                "cargos": [CARGO_MEMBRO_ID],
                "nome": "🔴 MEMBROS",
                "cor": 0xe74c3c,
                "itens": [],
                "is_isento": False
            },
            "agregados": {
                "cargos": [CARGO_AGREGADO_ID],
                "nome": "⚪ AGREGADOS",
                "cor": 0x95a5a6,
                "itens": [],
                "is_isento": False
            }
        }
        
        for item in historico:
            user_id = int(item["user_id"])
            member = guild.get_member(user_id) if guild else None
            if not member:
                continue
            
            total_meta = item["dinheiro"]
            total_acoes_item = item.get("dinheiro_acoes") or 0
            total_geral_item = total_meta + total_acoes_item
            
            item_dict = dict(item)
            item_dict["total_meta"] = total_meta
            item_dict["total_acoes"] = total_acoes_item
            item_dict["total_geral"] = total_geral_item
            item_dict["nome"] = member.display_name
            
            cargo_encontrado = False
            for grupo_key, grupo_data in grupos.items():
                if any(role.id in grupo_data["cargos"] for role in member.roles):
                    grupo_data["itens"].append(item_dict)
                    cargo_encontrado = True
                    break
            
            if not cargo_encontrado:
                if "outros" not in grupos:
                    grupos["outros"] = {
                        "nome": "📌 OUTROS",
                        "cor": 0x808080,
                        "itens": [],
                        "is_isento": False
                    }
                grupos["outros"]["itens"].append(item_dict)
        
        canal_resultados = interaction.guild.get_channel(RESULTADOS_METAS_ID)
        if not canal_resultados:
            canal_resultados = interaction.channel
        
        titulo = f"📊 RELATÓRIO DE METAS FECHADAS"
        if titulo_extra:
            titulo = f"📊 {titulo_extra}"
        
        embed_resumo = discord.Embed(
            title=titulo,
            description=f"📅 **Período:** {data_inicio_str} até {data_fim_str}",
            color=0x2ecc71, timestamp=agora()
        )
        
        total_nao_isentos = 0
        total_isentos = 0
        for grupo_key, grupo_data in grupos.items():
            if grupo_data["itens"]:
                if grupo_data.get("is_isento", False):
                    total_isentos += len(grupo_data["itens"])
                else:
                    total_nao_isentos += len(grupo_data["itens"])
        
        resumo_texto = (
            f"💰 **Dinheiro Sujo (Meta):** {formatar_dinheiro(total_dinheiro)}\n"
            f"🎯 **Dinheiro de Ações:** {formatar_dinheiro(total_acoes)}\n"
            f"📦 **Total Geral:** {formatar_dinheiro(total_geral)}\n"
            f"👥 **Total de metas fechadas:** {len(historico)}\n"
            f"🟡 **Isentos (Gerentes + 01/02):** {total_isentos}\n"
            f"📊 **Obrigados (Demais cargos):** {total_nao_isentos}"
        )
        embed_resumo.add_field(name="📊 RESUMO GERAL", value=resumo_texto, inline=False)
        
        resumo_grupos = ""
        for grupo_key, grupo_data in grupos.items():
            if grupo_data["itens"]:
                qtd = len(grupo_data["itens"])
                total_grupo = sum(item["total_geral"] for item in grupo_data["itens"])
                if grupo_data.get("is_isento", False):
                    resumo_grupos += f"{grupo_data['nome']}: {qtd} membros (ISENTOS)\n"
                else:
                    resumo_grupos += f"{grupo_data['nome']}: {qtd} membros | {formatar_dinheiro(total_grupo)}\n"
        
        if resumo_grupos:
            embed_resumo.add_field(name="📊 RESUMO POR CARGO", value=resumo_grupos, inline=False)
        
        embed_resumo.set_footer(text=f"Relatório gerado por {interaction.user.display_name}")
        await canal_resultados.send(embed=embed_resumo)
        await asyncio.sleep(0.5)
        
        for grupo_key, grupo_data in grupos.items():
            if not grupo_data["itens"]:
                continue
            
            itens_ordenados = sorted(grupo_data["itens"], key=lambda x: x["total_geral"], reverse=True)
            
            if grupo_data.get("is_isento", False):
                for i in range(0, len(itens_ordenados), 10):
                    grupo = itens_ordenados[i:i+10]
                    embed = discord.Embed(
                        title=f"🟡 {grupo_data['nome']} ({len(itens_ordenados)} membros) - Parte {i//10 + 1}",
                        color=grupo_data["cor"]
                    )
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        texto += f"**{idx}.** {item['nome']} - 🟡 ISENTO (não paga meta)\n"
                    embed.add_field(name="📋 LISTA DE ISENTOS", value=texto, inline=False)
                    await canal_resultados.send(embed=embed)
                    await asyncio.sleep(0.3)
                continue
            
            pagaram = [item for item in itens_ordenados if item["total_geral"] > 0]
            nao_pagaram = [item for item in itens_ordenados if item["total_geral"] == 0]
            
            if pagaram:
                for i in range(0, len(pagaram), 5):
                    grupo = pagaram[i:i+5]
                    embed = discord.Embed(
                        title=f"✅ {grupo_data['nome']} - QUEM PAGOU ({len(pagaram)} membros) - Parte {i//5 + 1}",
                        color=grupo_data["cor"]
                    )
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        texto += f"**{idx}.** {item['nome']}\n"
                        texto += f"   💰 Meta: {formatar_dinheiro(item['total_meta'])}\n"
                        texto += f"   🎯 Ações: {formatar_dinheiro(item['total_acoes'])}\n"
                        texto += f"   📦 Total: {formatar_dinheiro(item['total_geral'])}\n\n"
                    
                    if len(texto) > 1000:
                        parte1 = texto[:900]
                        parte2 = texto[900:]
                        embed.add_field(name="📋 LISTA (parte 1)", value=parte1, inline=False)
                        embed.add_field(name="📋 LISTA (parte 2)", value=parte2, inline=False)
                    else:
                        embed.add_field(name="📋 LISTA", value=texto, inline=False)
                    
                    await canal_resultados.send(embed=embed)
                    await asyncio.sleep(0.3)
            
            if nao_pagaram:
                for i in range(0, len(nao_pagaram), 10):
                    grupo = nao_pagaram[i:i+10]
                    embed = discord.Embed(
                        title=f"❌ {grupo_data['nome']} - QUEM NÃO PAGOU ({len(nao_pagaram)} membros) - Parte {i//10 + 1}",
                        color=0xe74c3c
                    )
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        texto += f"**{idx}.** {item['nome']} - ❌ ZERADO\n"
                    embed.add_field(name="📋 LISTA", value=texto, inline=False)
                    await canal_resultados.send(embed=embed)
                    await asyncio.sleep(0.3)
        
        total_embeds = 1
        for grupo_key, grupo_data in grupos.items():
            if grupo_data["itens"]:
                if grupo_data.get("is_isento", False):
                    total_embeds += (len(grupo_data["itens"]) + 9) // 10
                else:
                    pagaram = [item for item in grupo_data["itens"] if item["total_geral"] > 0]
                    nao_pagaram = [item for item in grupo_data["itens"] if item["total_geral"] == 0]
                    total_embeds += (len(pagaram) + 4) // 5
                    total_embeds += (len(nao_pagaram) + 9) // 10
        
        await interaction.followup.send(
            f"✅ **Relatório enviado com sucesso!**\n"
            f"📊 {len(historico)} metas processadas\n"
            f"📨 {total_embeds} mensagens enviadas",
            ephemeral=True
        )
        
    except Exception as e:
        logger.error(f"❌ Erro ao gerar relatório: {e}")
        await interaction.followup.send(f"❌ Erro ao gerar relatório: {str(e)}", ephemeral=True)

# ---------------------------------------------------------
# ASYNC: enviar_painel_solicitar_sala
# ---------------------------------------------------------

async def enviar_painel_solicitar_sala():
    canal = bot.get_channel(CANAL_SOLICITAR_SALA_ID)
    if not canal:
        logger.error("❌ Canal solicitar sala não encontrado")
        return
    embed = discord.Embed(title="📂 Solicitar Sala", description="Clique no botão para criar sua sala.", color=0x2ecc71)
    await enviar_ou_atualizar_painel("painel_solicitar_sala", CANAL_SOLICITAR_SALA_ID, embed, SolicitarSalaView())

# ---------------------------------------------------------
# ASYNC: enviar_painel_relatorio_metas
# ---------------------------------------------------------

async def enviar_painel_relatorio_metas():
    canal = bot.get_channel(1521495685092999279)
    if not canal:
        logger.error("❌ Canal de relatório de metas não encontrado")
        return
    
    embed = discord.Embed(
        title="📊 GERENCIAMENTO DE METAS",
        description=(
            "**Gerencie as metas de todos os membros.**\n\n"
            "📌 **Opções disponíveis:**\n"
            "• 📊 **Gerar Relatório** - Consulta metas já fechadas (com datas)\n"
            "• 🔒 **Fechar Metas (Automático)** - Fecha a semana anterior (NUNCA ZERA O BANCO)\n\n"
            "📋 **O relatório mostra:**\n"
            "• Quem pagou e quanto (META)\n"
            "• Quem pagou e quanto (AÇÕES)\n"
            "• Quem NÃO pagou\n"
            "• Isentos (Gerentes e 01/02)\n"
            "• Totais gerais separados por cargo"
        ),
        color=0x2ecc71
    )
    
    embed.add_field(
        name="📌 COMO USAR - FECHAR METAS (AUTOMÁTICO)",
        value=(
            "**Clique no botão verde e confirme:**\n"
            "• O sistema calcula a SEMANA ANTERIOR (Segunda a Domingo)\n"
            "• Fecha todas as metas do período\n"
            "• Gera o relatório automaticamente\n"
            "• ⚠️ **NUNCA ZERA O BANCO DE DADOS**\n"
            "• Apenas zera a exibição (embeds) no Discord\n\n"
            "**Exemplo:**\n"
            "• Se fechar hoje (20/08/2026) → Fecha 10/08 a 16/08\n"
            "• Se fechar amanhã (21/08/2026) → Fecha 10/08 a 16/08\n"
            "• Sempre a SEMANA ANTERIOR completa!"
        ),
        inline=False
    )
    
    embed.add_field(
        name="📌 COMO USAR - GERAR RELATÓRIO",
        value=(
            "**Clique no botão azul e informe as datas:**\n"
            "• Data INÍCIO (ex: 01/08/2026)\n"
            "• Data FIM (ex: 07/08/2026)\n\n"
            "O sistema vai buscar as metas já fechadas no período e gerar o relatório.\n"
            "⚠️ **NUNCA ALTERA NADA no banco ou nos embeds.**"
        ),
        inline=False
    )
    
    embed.add_field(
        name="📌 SEGURANÇA",
        value=(
            "✅ **Os dados NUNCA são perdidos**\n"
            "• Fechar Metas → Salva no histórico + Zera apenas os embeds\n"
            "• Gerar Relatório → Apenas consulta\n"
            "• Os dados permanecem no banco para sempre"
        ),
        inline=False
    )
    
    view = discord.ui.View(timeout=None)
    view.add_item(RelatorioMetasButton())
    view.add_item(FecharMetasAutomaticoButton())
    
    await enviar_ou_atualizar_painel("painel_relatorio_metas", 1521495685092999279, embed, view)

# ---------------------------------------------------------
# ASYNC: verificar_avisos_quarta_forcado
# ---------------------------------------------------------

async def verificar_avisos_quarta_forcado():
    logger.info("📨 TESTE FORÇADO: Verificando avisos de quarta-feira...")
    pool = await get_pool()
    if not pool:
        logger.error("❌ Banco de dados indisponível!")
        return False
    try:
        hoje = agora()
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            logger.error("❌ Guild não encontrada!")
            return False
        cargos_obrigados = [
            CARGO_AGREGADO_ID, CARGO_MEMBRO_ID, CARGO_SOLDADO_ID,
            CARGO_01_ID, CARGO_02_ID, CARGO_RESP_P1_ID, CARGO_RESP_METAS_ID,
            CARGO_RESP_ACAO_ID, CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID
        ]
        async with pool.acquire() as conn:
            avisos_enviados = 0
            for member in guild.members:
                if member.bot:
                    continue
                tem_cargo = any(r.id in cargos_obrigados for r in member.roles)
                if not tem_cargo:
                    continue
                user_id = str(member.id)
                meta = await conn.fetchrow("SELECT dinheiro, dinheiro_acoes FROM metas WHERE user_id = $1", user_id)
                if not meta:
                    continue
                dinheiro = meta["dinheiro"] or 0
                dinheiro_acoes = meta.get("dinheiro_acoes") or 0
                total = dinheiro + dinheiro_acoes
                if total == 0:
                    canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", user_id)
                    if canal_id:
                        canal = bot.get_channel(int(canal_id))
                        if canal:
                            embed = discord.Embed(
                                title="⚠️ [TESTE] AVISO DE META SEMANAL",
                                description=f"{member.mention} **atenção!**",
                                color=0xe74c3c
                            )
                            embed.add_field(
                                name="📌 Você ainda NÃO fez nenhum depósito na sua meta esta semana!",
                                value=(
                                    "⏰ **Você tem até domingo para completar sua meta!**\n\n"
                                    "⚠️ **Consequências:**\n"
                                    "• Se NÃO fechar a meta: **REBAIXAMENTO** na facção\n"
                                    "• Se atrasar 2 vezes: **REMOÇÃO** da facção\n\n"
                                    "💪 **Corra atrás do prejuízo!**\n\n"
                                    "🔴 **ESTE É UM TESTE**"
                                ),
                                inline=False
                            )
                            embed.set_footer(text="Meta semanal • Vida Rasa • TESTE")
                            await canal.send(embed=embed)
                            avisos_enviados += 1
        logger.info(f"✅ [TESTE] Avisos enviados: {avisos_enviados} membros")
        return True
    except Exception as e:
        logger.error(f"❌ Erro no teste de aviso: {e}")
        return False

# =========================================================
# CLASS: MetaView
# =========================================================

class MetaView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=None)
        self.user_id = user_id

    @discord.ui.button(label="💣 Vender Pólvora", style=discord.ButtonStyle.primary, custom_id="meta_vender_polvora_fixo", emoji="💣")
    async def vender_polvora(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            pool = await get_pool()
            if not pool:
                await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
                return
            
            async with pool.acquire() as conn:
                meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(self.user_id))
            
            if not meta:
                guild = interaction.guild
                member = guild.get_member(int(self.user_id))
                if member:
                    await criar_sala_meta(member)
                    await asyncio.sleep(1)
                    await carregar_metas_cache()
                    await interaction.response.send_message("✅ **Meta criada automaticamente!**\n💡 Tente novamente agora.", ephemeral=True)
                    return
                else:
                    await interaction.response.send_message("❌ **Meta não encontrada!**", ephemeral=True)
                    return
            
            await interaction.response.send_modal(VenderPolvoraMetaModal(self.user_id))
            
        except Exception as e:
            logger.error(f"❌ Erro no botão Vender Pólvora: {e}")
            try:
                await interaction.response.send_message(f"❌ Erro: {str(e)[:100]}", ephemeral=True)
            except:
                pass

    @discord.ui.button(label="💰 Adicionar Dinheiro Sujo", style=discord.ButtonStyle.success, custom_id="meta_adicionar_dinheiro_fixo", emoji="💰")
    async def adicionar_dinheiro(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            pool = await get_pool()
            if not pool:
                await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
                return
            
            async with pool.acquire() as conn:
                meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(self.user_id))
            
            if not meta:
                guild = interaction.guild
                member = guild.get_member(int(self.user_id))
                if member:
                    await criar_sala_meta(member)
                    await asyncio.sleep(1)
                    await carregar_metas_cache()
                    await interaction.response.send_message("✅ **Meta criada automaticamente!**\n💡 Tente novamente agora.", ephemeral=True)
                    return
                else:
                    await interaction.response.send_message("❌ **Meta não encontrada!**", ephemeral=True)
                    return
            
            await interaction.response.send_modal(AdicionarDinheiroModal(self.user_id))
            
        except Exception as e:
            logger.error(f"❌ Erro no botão Adicionar Dinheiro: {e}")
            try:
                await interaction.response.send_message(f"❌ Erro: {str(e)[:100]}", ephemeral=True)
            except:
                pass

    @discord.ui.button(label="💰 Pólvora Paga", style=discord.ButtonStyle.success, custom_id="meta_polvora_paga_fixo", emoji="✅")
    async def polvora_paga(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
            is_admin = interaction.user.guild_permissions.administrator

            if not is_gerente and not is_admin:
                await interaction.response.send_message("❌ Apenas **Gerentes** ou **ADM** podem marcar pólvora como paga!", ephemeral=True)
                return

            pendente = await buscar_polvora_pendente(self.user_id)
            if not pendente:
                await interaction.response.send_message("📭 Este membro não tem pólvora pendente para pagar!", ephemeral=True)
                return

            view = ConfirmarPagamentoPolvoraViewMeta(self.user_id, pendente)
            embed = discord.Embed(
                title="💰 CONFIRMAR PAGAMENTO DE PÓLVORA",
                description=f"👤 <@{self.user_id}>",
                color=0xf1c40f
            )
            embed.add_field(name="📦 Quantidade", value=f"{fmt_num(pendente['quantidade'])} unidades", inline=True)
            embed.add_field(name="💰 Valor total", value=formatar_dinheiro(pendente['valor']), inline=True)
            embed.add_field(name="💵 Preço por unidade", value=f"R$ {PRECO_POLVORA:.2f}", inline=True)
            embed.set_footer(text="Clique em ✅ Confirmar Pagamento para finalizar")

            await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
            
        except Exception as e:
            logger.error(f"❌ Erro no botão Pólvora Paga: {e}")
            try:
                await interaction.response.send_message(f"❌ Erro: {str(e)[:100]}", ephemeral=True)
            except:
                pass

    @discord.ui.button(label="✏️ Editar Meta", style=discord.ButtonStyle.primary, custom_id="meta_editar_fixo", emoji="✏️")
    async def editar_meta(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            is_dono = str(interaction.user.id) == str(self.user_id)
            is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
            is_admin = interaction.user.guild_permissions.administrator

            if not is_dono and not is_gerente and not is_admin:
                await interaction.response.send_message("❌ Apenas o dono da sala, gerentes ou ADM podem editar a meta!", ephemeral=True)
                return

            pool = await get_pool()
            if not pool:
                await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
                return

            async with pool.acquire() as conn:
                meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(self.user_id))

            if not meta:
                await interaction.response.send_message("❌ **Meta não encontrada!**", ephemeral=True)
                return

            dados = {
                "dinheiro": meta["dinheiro"] or 0,
                "polvora": meta["polvora"] or 0,
                "saldo_excedente": meta.get("saldo_excedente") or 0
            }

            await interaction.response.send_modal(EditarMetaModal(self.user_id, dados))
            
        except Exception as e:
            logger.error(f"❌ Erro no botão Editar Meta: {e}")
            try:
                await interaction.response.send_message(f"❌ Erro: {str(e)[:100]}", ephemeral=True)
            except:
                pass

# =========================================================
# CLASS: ConfirmarPagamentoPolvoraViewMeta
# =========================================================

class ConfirmarPagamentoPolvoraViewMeta(discord.ui.View):
    def __init__(self, user_id, pendente):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.pendente = pendente

    @discord.ui.button(label="✅ Confirmar Pagamento", style=discord.ButtonStyle.success, custom_id="confirmar_pagamento_polvora", emoji="✅")
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        sucesso = await pagar_polvora(self.user_id)
        if not sucesso:
            await interaction.followup.send("❌ Erro ao marcar pólvora como paga!", ephemeral=True)
            return
        canal_membro = None
        pool = await get_pool()
        if pool:
            async with pool.acquire() as conn:
                canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(self.user_id))
                if canal_id:
                    canal_membro = interaction.guild.get_channel(int(canal_id))
        if canal_membro:
            embed_notificacao = discord.Embed(
                title="✅ PÓLVORA PAGA!",
                description=f"👤 <@{self.user_id}>",
                color=0x2ecc71, timestamp=agora()
            )
            embed_notificacao.add_field(name="📦 Quantidade", value=f"{fmt_num(self.pendente['quantidade'])} unidades", inline=True)
            embed_notificacao.add_field(name="💰 Valor recebido", value=formatar_dinheiro(self.pendente['valor']), inline=True)
            embed_notificacao.add_field(name="💵 Preço por unidade", value=f"R$ {PRECO_POLVORA:.2f}", inline=True)
            embed_notificacao.set_footer(text="Pólvora paga! ✅")
            await canal_membro.send(embed=embed_notificacao)
        embed = discord.Embed(
            title="✅ PÓLVORA PAGA COM SUCESSO!",
            description=f"👤 <@{self.user_id}>",
            color=0x2ecc71, timestamp=agora()
        )
        embed.add_field(name="📦 Quantidade", value=f"{fmt_num(self.pendente['quantidade'])} unidades", inline=True)
        embed.add_field(name="💰 Valor pago", value=formatar_dinheiro(self.pendente['valor']), inline=True)
        embed.add_field(name="💵 Preço por unidade", value=f"R$ {PRECO_POLVORA:.2f}", inline=True)
        embed.set_footer(text="Pólvora paga! ✅")
        await interaction.followup.send(embed=embed, ephemeral=True)
        await atualizar_embed_meta(self.user_id)
        try:
            await interaction.message.delete()
        except:
            pass

    @discord.ui.button(label="❌ Cancelar", style=discord.ButtonStyle.danger, custom_id="cancelar_pagamento_polvora", emoji="❌")
    async def cancelar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ Pagamento cancelado.", ephemeral=True)
        try:
            await interaction.message.delete()
        except:
            pass

# =========================================================
# CLASS: EditarMetaModal
# =========================================================

class EditarMetaModal(discord.ui.Modal, title="✏️ Editar Meta"):
    def __init__(self, user_id, dados_atuais):
        super().__init__(timeout=300)
        self.user_id = user_id
        self.dinheiro = discord.ui.TextInput(
            label="💰 Dinheiro Sujo (Meta)",
            placeholder="Digite o valor correto",
            default=str(dados_atuais.get("dinheiro", 0)),
            required=True, max_length=15
        )
        self.polvora = discord.ui.TextInput(
            label="💣 Pólvora",
            placeholder="Digite a quantidade correta",
            default=str(dados_atuais.get("polvora", 0)),
            required=True, max_length=10
        )
        self.saldo_excedente = discord.ui.TextInput(
            label="📦 Saldo Excedente (Próxima semana)",
            placeholder="Digite o valor correto",
            default=str(dados_atuais.get("saldo_excedente", 0)),
            required=True, max_length=15
        )
        self.add_item(self.dinheiro)
        self.add_item(self.polvora)
        self.add_item(self.saldo_excedente)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            novo_dinheiro = safe_int(self.dinheiro.value)
            nova_polvora = safe_int(self.polvora.value)
            novo_saldo_excedente = safe_int(self.saldo_excedente.value)
            if novo_dinheiro < 0 or nova_polvora < 0 or novo_saldo_excedente < 0:
                raise ValueError("Valores não podem ser negativos")
        except ValueError as e:
            await interaction.followup.send(f"❌ **Valor inválido!** {str(e)}", ephemeral=True)
            return
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        try:
            async with pool.acquire() as conn:
                await conn.execute("""
                    UPDATE metas
                    SET dinheiro = $1, polvora = $2, saldo_excedente = $3
                    WHERE user_id = $4
                """, novo_dinheiro, nova_polvora, novo_saldo_excedente, str(self.user_id))
            if str(self.user_id) in metas_cache:
                metas_cache[str(self.user_id)]["dinheiro"] = novo_dinheiro
                metas_cache[str(self.user_id)]["polvora"] = nova_polvora
                metas_cache[str(self.user_id)]["saldo_excedente"] = novo_saldo_excedente
            await atualizar_embed_meta(self.user_id)
            embed = discord.Embed(
                title="✅ META ATUALIZADA COM SUCESSO!",
                description=f"**👤 <@{self.user_id}>**",
                color=0x2ecc71, timestamp=agora()
            )
            embed.add_field(name="💰 Dinheiro Sujo", value=formatar_dinheiro(novo_dinheiro), inline=True)
            embed.add_field(name="💣 Pólvora", value=f"{fmt_num(nova_polvora)} unidades", inline=True)
            embed.add_field(name="📦 Saldo Excedente", value=formatar_dinheiro(novo_saldo_excedente), inline=True)
            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            logger.error(f"❌ Erro ao editar meta: {e}")
            await interaction.followup.send(f"❌ Erro ao editar meta: {str(e)}", ephemeral=True)

# =========================================================
# CLASS: VenderPolvoraMetaModal
# =========================================================

class VenderPolvoraMetaModal(discord.ui.Modal, title="💣 Vender Pólvora"):
    def __init__(self, user_id):
        super().__init__(timeout=300)
        self.user_id = user_id
    quantidade = discord.ui.TextInput(
        label="📦 Quantidade de Pólvora",
        placeholder="Digite a quantidade (ex: 100)",
        required=True, max_length=10
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            qtd = safe_int(self.quantidade.value)
            if qtd <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ Quantidade inválida! Digite um número positivo.", ephemeral=True)
            return
        valor = qtd * PRECO_POLVORA
        sucesso = await salvar_venda_polvora(self.user_id, qtd)
        if not sucesso:
            await interaction.followup.send("❌ Erro ao registrar venda de pólvora!", ephemeral=True)
            return
        pendente = await buscar_polvora_pendente(self.user_id)
        embed = discord.Embed(
            title="💣 VENDA DE PÓLVORA REGISTRADA",
            description=f"👤 <@{self.user_id}>",
            color=0xe67e22, timestamp=agora()
        )
        embed.add_field(name="📦 Quantidade", value=f"{fmt_num(qtd)} unidades", inline=True)
        embed.add_field(name="💰 Valor a receber", value=formatar_dinheiro(valor), inline=True)
        embed.add_field(name="💵 Preço por unidade", value=f"R$ {PRECO_POLVORA:.2f}", inline=True)
        if pendente:
            embed.add_field(
                name="📊 TOTAL PENDENTE",
                value=f"📦 {fmt_num(pendente['quantidade'])} unidades\n💰 {formatar_dinheiro(pendente['valor'])}",
                inline=False
            )
        embed.set_footer(text="Aguardando pagamento...")
        await interaction.followup.send(embed=embed, ephemeral=True)
        await atualizar_embed_meta(self.user_id)

# =========================================================
# CLASS: AdicionarDinheiroModal
# =========================================================

class AdicionarDinheiroModal(discord.ui.Modal, title="💰 Adicionar Dinheiro Sujo"):
    quantidade = discord.ui.TextInput(label="Valor do Dinheiro Sujo", placeholder="Digite o valor (ex: 5000)", required=True)

    def __init__(self, user_id):
        super().__init__()
        self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        try:
            valor = safe_int(self.quantidade.value)
            if valor <= 0:
                raise ValueError
        except:
            await interaction.response.send_message("❌ Valor inválido!", ephemeral=True)
            return
        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(self.user_id))
        if not meta:
            guild = interaction.guild
            member = guild.get_member(int(self.user_id))
            if member:
                await criar_sala_meta(member)
                await asyncio.sleep(1)
                await carregar_metas_cache()
                await interaction.response.send_message("✅ **Meta criada automaticamente!**\n💡 Tente novamente agora.", ephemeral=True)
                return
            else:
                await interaction.response.send_message("❌ **Meta não encontrada!**\n\n💡 Clique em '➕ Criar Minha Sala' no canal de solicitar sala.", ephemeral=True)
                return
        sucesso = await adicionar_dinheiro_meta(self.user_id, valor)
        if not sucesso:
            await interaction.response.send_message("❌ Erro ao adicionar dinheiro!", ephemeral=True)
            return
        await carregar_metas_cache()
        await atualizar_embed_meta(self.user_id)
        await interaction.response.send_message(f"✅ **{formatar_dinheiro(valor)} adicionado à meta!**", ephemeral=True)

# =========================================================
# CLASS: RelatorioMetasButton
# =========================================================

class RelatorioMetasButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="📊 Gerar Relatório de Metas", style=discord.ButtonStyle.success, custom_id="relatorio_metas_btn", emoji="📊")

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(RelatorioMetasModal())

# =========================================================
# CLASS: RelatorioMetasModal
# =========================================================

class RelatorioMetasModal(discord.ui.Modal, title="📊 Relatório de Metas"):
    data_inicio = discord.ui.TextInput(label="📅 Data INÍCIO", placeholder="Ex: 01/07/2026", required=True)
    data_fim = discord.ui.TextInput(label="📅 Data FIM", placeholder="Ex: 31/07/2026", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            inicio = datetime.strptime(self.data_inicio.value.strip(), "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value.strip(), "%d/%m/%Y")
            inicio_dt = inicio.replace(hour=0, minute=0, second=0)
            fim_dt = fim.replace(hour=23, minute=59, second=59)
        except ValueError:
            await interaction.followup.send("❌ Formato de data inválido! Use DD/MM/AAAA", ephemeral=True)
            return
        if fim < inicio:
            await interaction.followup.send("❌ Data de FIM deve ser depois da data de INÍCIO!", ephemeral=True)
            return
        
        historico = await buscar_historico_metas(inicio_dt, fim_dt)
        await gerar_relatorio_metas(
            interaction=interaction,
            data_inicio_str=self.data_inicio.value,
            data_fim_str=self.data_fim.value,
            historico=historico,
            titulo_extra="📊 RELATÓRIO DE METAS FECHADAS (CONSULTA)"
        )

# =========================================================
# CLASS: FecharMetasAutomaticoButton
# =========================================================

class FecharMetasAutomaticoButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="🔒 Fechar Metas (Automático - Semana Anterior)", style=discord.ButtonStyle.success, custom_id="fechar_metas_automatico_btn", emoji="🔒")

    async def callback(self, interaction: discord.Interaction):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem fechar todas as metas!", ephemeral=True)
            return
        data_inicio, data_fim = calcular_semana_anterior()
        data_inicio_str = data_inicio.strftime("%d/%m/%Y")
        data_fim_str = data_fim.strftime("%d/%m/%Y")
        view = ConfirmarFechamentoAutomaticoView(data_inicio, data_fim, data_inicio_str, data_fim_str)
        embed = discord.Embed(
            title="🔒 FECHAR METAS - SEMANA ANTERIOR",
            description=f"📅 **Período a ser fechado:**\n**{data_inicio_str}** a **{data_fim_str}**\n\n⚠️ **ATENÇÃO:** Esta ação irá:\n• Fechar TODAS as metas deste período\n• Gerar o relatório completo\n• Resetar as metas dos membros\n\n🔄 **Esta semana é calculada automaticamente!**\n📌 Sempre a semana anterior (Segunda a Domingo)",
            color=0xe67e22
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# =========================================================
# CLASS: ConfirmarFechamentoAutomaticoView
# =========================================================

class ConfirmarFechamentoAutomaticoView(discord.ui.View):
    def __init__(self, data_inicio, data_fim, data_inicio_str, data_fim_str):
        super().__init__(timeout=60)
        self.data_inicio = data_inicio
        self.data_fim = data_fim
        self.data_inicio_str = data_inicio_str
        self.data_fim_str = data_fim_str

    @discord.ui.button(label="✅ Confirmar Fechamento", style=discord.ButtonStyle.danger, custom_id="confirmar_fechamento_auto", emoji="✅")
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        try:
            relatorio, membros_sem_meta = await fechar_todas_metas(self.data_inicio, self.data_fim)
            
            if not relatorio and not membros_sem_meta:
                await interaction.followup.send("📭 Nenhuma meta para fechar.", ephemeral=True)
                return
            
            await gerar_relatorio_metas(
                interaction=interaction,
                data_inicio_str=self.data_inicio_str,
                data_fim_str=self.data_fim_str,
                historico=relatorio,
                titulo_extra="📊 RELATÓRIO SEMANAL - METAS FECHADAS"
            )
            
            await zerar_exibicao_metas()
            
            if membros_sem_meta:
                canal_resultados = interaction.guild.get_channel(RESULTADOS_METAS_ID)
                if not canal_resultados:
                    canal_resultados = interaction.channel
                
                for i in range(0, len(membros_sem_meta), 10):
                    grupo = membros_sem_meta[i:i+10]
                    embed = discord.Embed(title=f"⚠️ MEMBROS SEM META ({len(membros_sem_meta)} membros) - Parte {i//10 + 1}", color=0xf1c40f)
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        member = interaction.guild.get_member(int(item["user_id"]))
                        if member:
                            nome = member.display_name
                        else:
                            nome = item['nome']
                        texto += f"**{idx}.** {nome} - ❌ SEM META\n"
                    embed.add_field(name="📋 LISTA", value=texto, inline=False)
                    await canal_resultados.send(embed=embed)
                    await asyncio.sleep(0.3)
            
            await interaction.followup.send(
                f"✅ **Metas fechadas com sucesso!**\n"
                f"📊 {len(relatorio)} metas salvas no histórico\n"
                f"🔄 Exibição zerada no Discord\n"
                f"📌 Os dados continuam salvos no banco para consulta",
                ephemeral=True
            )
                
        except Exception as e:
            logger.error(f"❌ Erro ao fechar metas automático: {e}")
            await interaction.followup.send(f"❌ Erro ao fechar metas: {e}", ephemeral=True)
    
    @discord.ui.button(label="❌ Cancelar", style=discord.ButtonStyle.secondary, custom_id="cancelar_fechamento_auto", emoji="❌")
    async def cancelar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ Operação cancelada.", ephemeral=True)

# =========================================================
# CLASS: SolicitarSalaView
# =========================================================

class SolicitarSalaView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="➕ Criar Minha Sala", style=discord.ButtonStyle.success, custom_id="criar_sala_manual")
    async def criar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(interaction.user.id))
        if meta:
            canal = interaction.guild.get_channel(meta["canal_id"])
            if canal:
                await interaction.followup.send(f"✅ Você já possui uma sala! {canal.mention}", ephemeral=True)
                await atualizar_embed_meta(interaction.user.id)
                return
            else:
                await conn.execute("DELETE FROM metas WHERE user_id = $1", str(interaction.user.id))
                if str(interaction.user.id) in metas_cache:
                    del metas_cache[str(interaction.user.id)]
        for canal in interaction.guild.text_channels:
            if interaction.user.display_name.lower() in canal.name.lower() and "📁" in canal.name:
                await salvar_meta_db(interaction.user.id, canal.id, 0, 0, 0)
                metas_cache[str(interaction.user.id)] = {"canal_id": canal.id, "dinheiro": 0, "polvora": 0, "acao": None, "dinheiro_acoes": 0}
                await atualizar_embed_meta(interaction.user.id)
                await interaction.followup.send(f"✅ Sala encontrada e meta criada! {canal.mention}", ephemeral=True)
                return
        await criar_sala_meta(interaction.user)
        await interaction.followup.send("✅ Sua sala foi criada com sucesso!", ephemeral=True)

# =========================================================
# CLASS: ConfirmarPagamentoPolvoraView
# =========================================================

class ConfirmarPagamentoPolvoraView(discord.ui.View):
    def __init__(self, user_id, pendente):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.pendente = pendente

    @discord.ui.button(label="✅ Confirmar Pagamento", style=discord.ButtonStyle.success, custom_id="confirmar_pagamento_polvora", emoji="✅")
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        sucesso = await pagar_polvora(self.user_id)
        if not sucesso:
            await interaction.followup.send("❌ Erro ao marcar pólvora como paga!", ephemeral=True)
            return
        canal_membro = None
        pool = await get_pool()
        if pool:
            async with pool.acquire() as conn:
                canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(self.user_id))
                if canal_id:
                    canal_membro = interaction.guild.get_channel(int(canal_id))
        if not canal_membro:
            member = interaction.guild.get_member(int(self.user_id))
            if member:
                for canal in interaction.guild.text_channels:
                    if member.display_name.lower() in canal.name.lower() and "📁" in canal.name:
                        canal_membro = canal
                        break
        if canal_membro:
            embed_notificacao = discord.Embed(
                title="✅ PÓLVORA PAGA!",
                description=f"👤 <@{self.user_id}> sua pólvora foi paga!",
                color=0x2ecc71, timestamp=agora()
            )
            embed_notificacao.add_field(name="📦 Quantidade", value=f"{fmt_num(self.pendente['quantidade'])} unidades", inline=True)
            embed_notificacao.add_field(name="💰 Valor recebido", value=formatar_dinheiro(self.pendente['valor']), inline=True)
            embed_notificacao.add_field(name="💵 Preço por unidade", value=f"R$ {PRECO_POLVORA:.2f}", inline=True)
            embed_notificacao.set_footer(text="Pólvora paga com sucesso! ✅")
            await canal_membro.send(embed=embed_notificacao)
        else:
            try:
                member = interaction.guild.get_member(int(self.user_id))
                if member:
                    embed_dm = discord.Embed(
                        title="✅ PÓLVORA PAGA!",
                        description=f"Sua pólvora foi paga!",
                        color=0x2ecc71, timestamp=agora()
                    )
                    embed_dm.add_field(name="📦 Quantidade", value=f"{fmt_num(self.pendente['quantidade'])} unidades", inline=True)
                    embed_dm.add_field(name="💰 Valor recebido", value=formatar_dinheiro(self.pendente['valor']), inline=True)
                    embed_dm.add_field(name="💵 Preço por unidade", value=f"R$ {PRECO_POLVORA:.2f}", inline=True)
                    await member.send(embed=embed_dm)
            except:
                pass
        embed = discord.Embed(
            title="✅ PÓLVORA PAGA COM SUCESSO!",
            description=f"👤 <@{self.user_id}>",
            color=0x2ecc71, timestamp=agora()
        )
        embed.add_field(name="📦 Quantidade", value=f"{fmt_num(self.pendente['quantidade'])} unidades", inline=True)
        embed.add_field(name="💰 Valor pago", value=formatar_dinheiro(self.pendente['valor']), inline=True)
        embed.add_field(name="💵 Preço por unidade", value=f"R$ {PRECO_POLVORA:.2f}", inline=True)
        embed.set_footer(text="Pólvora paga! ✅")
        await interaction.followup.send(embed=embed, ephemeral=True)
        await atualizar_embed_meta(self.user_id)
        try:
            await interaction.message.delete()
        except:
            pass

    @discord.ui.button(label="❌ Cancelar", style=discord.ButtonStyle.danger, custom_id="cancelar_pagamento_polvora", emoji="❌")
    async def cancelar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ Pagamento cancelado.", ephemeral=True)
        try:
            await interaction.message.delete()
        except:
            pass

# =========================================================
# TASK: verificar_avisos_meta
# =========================================================

@tasks.loop(hours=1)
async def verificar_avisos_meta():
    try:
        await verificar_avisos_quarta()
    except Exception as e:
        logger.error(f"❌ Erro ao verificar avisos de meta: {e}")

# =========================================================
# TASK: fechar_metas_semanais
# =========================================================

@tasks.loop(hours=1)
async def fechar_metas_semanais():
    """Task para fechar metas automaticamente no domingo à meia-noite."""
    agora_br = agora()
    if agora_br.weekday() == 6 and agora_br.hour == 23 and agora_br.minute == 59:
        logger.info("🔄 Fechando metas automaticamente...")
        try:
            data_inicio, data_fim = calcular_semana_anterior()
            relatorio, membros_sem_meta = await fechar_todas_metas(data_inicio, data_fim)
            if relatorio:
                logger.info(f"✅ {len(relatorio)} metas fechadas automaticamente")
                await zerar_exibicao_metas()
        except Exception as e:
            logger.error(f"❌ Erro ao fechar metas automaticamente: {e}")

# =========================================================
# ==================== SISTEMA DE PRODUÇÃO ================
# =========================================================

# ---------------------------------------------------------
# ASYNC: carregar_producao
# ---------------------------------------------------------

async def carregar_producao(pid):
    try:
        pool = await get_pool()
        if not pool:
            return None
        async with pool.acquire() as conn:
            r = await conn.fetchrow("SELECT * FROM producoes WHERE pid=$1", pid)
        if not r:
            return None
        if isinstance(r["inicio"], datetime):
            inicio = r["inicio"].isoformat()
        else:
            inicio = r["inicio"]
        if isinstance(r["fim"], datetime):
            fim = r["fim"].isoformat()
        else:
            fim = r["fim"]
        dados = {
            "galpao": r["galpao"],
            "autor": int(r["autor"]),
            "inicio": inicio,
            "fim": fim,
            "obs": r.get("obs") or "",
            "msg_id": int(r["msg_id"]),
            "canal_id": int(r["canal_id"]),
            "polvora": r.get("polvora") or 400,
            "qtd_galpoes": r.get("qtd_galpoes") or 1,
            "polvora_por_galpao": r.get("polvora_por_galpao") or 400
        }
        if r.get("segunda_task_user"):
            dados["segunda_task_confirmada"] = {
                "user": int(r["segunda_task_user"]),
                "time": r["segunda_task_time"]
            }
        return dados
    except Exception as e:
        logger.error(f"❌ Erro ao carregar produção {pid}: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: salvar_producao
# ---------------------------------------------------------

async def salvar_producao(pid, dados):
    if isinstance(dados["inicio"], datetime):
        inicio_str = dados["inicio"].isoformat()
    else:
        inicio_str = dados["inicio"]
    if isinstance(dados["fim"], datetime):
        fim_str = dados["fim"].isoformat()
    else:
        fim_str = dados["fim"]
    segunda_user = None
    segunda_time = None
    if "segunda_task_confirmada" in dados:
        segunda_user = str(dados["segunda_task_confirmada"]["user"])
        segunda_time = dados["segunda_task_confirmada"]["time"]
    qtd_galpoes = dados.get("qtd_galpoes", 1)
    polvora_por_galpao = dados.get("polvora_por_galpao", 400)
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO producoes
                (pid, galpao, autor, inicio, fim, obs, msg_id, canal_id,
                 segunda_task_user, segunda_task_time, polvora, qtd_galpoes, polvora_por_galpao)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                ON CONFLICT (pid)
                DO UPDATE SET
                galpao=$2,
                autor=$3,
                inicio=$4,
                fim=$5,
                obs=$6,
                msg_id=$7,
                canal_id=$8,
                segunda_task_user=$9,
                segunda_task_time=$10,
                polvora=$11,
                qtd_galpoes=$12,
                polvora_por_galpao=$13
                """,
                pid, dados["galpao"], str(dados["autor"]), inicio_str, fim_str,
                dados.get("obs", ""), str(dados["msg_id"]), str(dados["canal_id"]),
                segunda_user, segunda_time, dados.get("polvora", 400),
                qtd_galpoes, polvora_por_galpao
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar produção {pid}: {e}")

# ---------------------------------------------------------
# ASYNC: deletar_producao
# ---------------------------------------------------------

async def deletar_producao(pid):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM producoes WHERE pid=$1", pid)
    except Exception as e:
        logger.error(f"❌ Erro ao deletar produção {pid}: {e}")

# ---------------------------------------------------------
# ASYNC: carregar_estoque
# ---------------------------------------------------------

async def carregar_estoque():
    pool = await get_pool()
    if not pool:
        return {"PT": 0, "SUB": 0}
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT tipo, quantidade FROM estoque_municoes")
        estoque = {"PT": 0, "SUB": 0}
        for row in rows:
            estoque[row["tipo"]] = row["quantidade"]
        return estoque
    except Exception as e:
        logger.error(f"❌ Erro ao carregar estoque: {e}")
        return {"PT": 0, "SUB": 0}

# ---------------------------------------------------------
# ASYNC: atualizar_estoque
# ---------------------------------------------------------

async def atualizar_estoque(tipo, quantidade, operacao="adicionar"):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            if operacao == "adicionar":
                await conn.execute(
                    "UPDATE estoque_municoes SET quantidade = quantidade + $1, ultima_atualizacao = NOW() WHERE tipo = $2",
                    quantidade, tipo
                )
            else:
                await conn.execute(
                    "UPDATE estoque_municoes SET quantidade = quantidade - $1, ultima_atualizacao = NOW() WHERE tipo = $2 AND quantidade >= $1",
                    quantidade, tipo
                )
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar estoque: {e}")

# ---------------------------------------------------------
# ASYNC: carregar_estoque_insumos
# ---------------------------------------------------------

async def carregar_estoque_insumos():
    pool = await get_pool()
    if not pool:
        return {"capsulas": 0, "embalagens": 0}
    try:
        async with pool.acquire() as conn:
            capsulas_row = await conn.fetchrow("SELECT quantidade FROM estoque_capsulas WHERE id = 1")
            capsulas = capsulas_row["quantidade"] if capsulas_row else 0
            embalagens_row = await conn.fetchrow("SELECT quantidade FROM estoque_embalagens WHERE id = 1")
            embalagens = embalagens_row["quantidade"] if embalagens_row else 0
        return {"capsulas": capsulas, "embalagens": embalagens}
    except Exception as e:
        logger.error(f"❌ Erro ao carregar estoque de insumos: {e}")
        return {"capsulas": 0, "embalagens": 0}

# ---------------------------------------------------------
# ASYNC: atualizar_estoque_capsulas
# ---------------------------------------------------------

async def atualizar_estoque_capsulas(quantidade, operacao="adicionar"):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            if operacao == "adicionar":
                await conn.execute(
                    "UPDATE estoque_capsulas SET quantidade = quantidade + $1, ultima_atualizacao = NOW() WHERE id = 1",
                    quantidade
                )
            else:
                await conn.execute(
                    "UPDATE estoque_capsulas SET quantidade = quantidade - $1, ultima_atualizacao = NOW() WHERE id = 1 AND quantidade >= $1",
                    quantidade
                )
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar estoque de cápsulas: {e}")

# ---------------------------------------------------------
# ASYNC: atualizar_estoque_embalagens
# ---------------------------------------------------------

async def atualizar_estoque_embalagens(quantidade, operacao="adicionar"):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            if operacao == "adicionar":
                await conn.execute(
                    "UPDATE estoque_embalagens SET quantidade = quantidade + $1, ultima_atualizacao = NOW() WHERE id = 1",
                    quantidade
                )
            else:
                await conn.execute(
                    "UPDATE estoque_embalagens SET quantidade = quantidade - $1, ultima_atualizacao = NOW() WHERE id = 1 AND quantidade >= $1",
                    quantidade
                )
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar estoque de embalagens: {e}")

# ---------------------------------------------------------
# ASYNC: registrar_entrada_insumos
# ---------------------------------------------------------

async def registrar_entrada_insumos(tipo, quantidade, registrado_por, obs=""):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO entrada_insumos (tipo, quantidade, registrado_por, obs) VALUES ($1, $2, $3, $4)",
                tipo, quantidade, str(registrado_por), obs
            )
            if tipo == "capsulas":
                await atualizar_estoque_capsulas(quantidade, "adicionar")
            elif tipo == "embalagens":
                await atualizar_estoque_embalagens(quantidade, "adicionar")
    except Exception as e:
        logger.error(f"❌ Erro ao registrar entrada de insumos: {e}")

# ---------------------------------------------------------
# ASYNC: verificar_insumos_producao
# ---------------------------------------------------------

async def verificar_insumos_producao(tipo, pacotes):
    estoque = await carregar_estoque_insumos()
    if tipo == "PT":
        capsulas_necessarias = pacotes * 25
        embalagens_necessarias = pacotes * 5
    else:
        capsulas_necessarias = pacotes * 65
        embalagens_necessarias = pacotes * 10
    return {
        "suficiente": estoque["capsulas"] >= capsulas_necessarias and estoque["embalagens"] >= embalagens_necessarias,
        "capsulas_necessarias": capsulas_necessarias,
        "embalagens_necessarias": embalagens_necessarias,
        "capsulas_disponiveis": estoque["capsulas"],
        "embalagens_disponiveis": estoque["embalagens"]
    }

# ---------------------------------------------------------
# ASYNC: consumir_insumos_producao
# ---------------------------------------------------------

async def consumir_insumos_producao(tipo, pacotes):
    if tipo == "PT":
        capsulas_consumir = pacotes * 25
        embalagens_consumir = pacotes * 5
    else:
        capsulas_consumir = pacotes * 65
        embalagens_consumir = pacotes * 10
    await atualizar_estoque_capsulas(capsulas_consumir, "remover")
    await atualizar_estoque_embalagens(embalagens_consumir, "remover")
    return capsulas_consumir, embalagens_consumir

# ---------------------------------------------------------
# ASYNC: registrar_producao_municao
# ---------------------------------------------------------

async def registrar_producao_municao(tipo, pacotes, produzido_por, obs=""):
    municoes = pacotes * 50
    capsulas_consumidas, embalagens_consumidas = await consumir_insumos_producao(tipo, pacotes)
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO producao_municao (tipo, pacotes, municoes, produzido_por, obs, capsulas_consumidas, embalagens_consumidas) VALUES ($1, $2, $3, $4, $5, $6, $7)",
                tipo, pacotes, municoes, str(produzido_por), obs, capsulas_consumidas, embalagens_consumidas
            )
            await atualizar_estoque(tipo, pacotes, "adicionar")
    except Exception as e:
        logger.error(f"❌ Erro ao registrar produção de munição: {e}")

# ---------------------------------------------------------
# ASYNC: salvar_polvora_db
# ---------------------------------------------------------

async def salvar_polvora_db(user_id, qtd, valor):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            data_str = agora().isoformat()
            await conn.execute(
                "INSERT INTO polvoras (user_id, quantidade, valor, data) VALUES ($1, $2, $3, $4)",
                str(user_id), qtd, valor, data_str
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar pólvora: {e}")

# ---------------------------------------------------------
# ASYNC: carregar_polvoras_db
# ---------------------------------------------------------

async def carregar_polvoras_db():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM polvoras")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar pólvoras: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: limpar_polvoras_db
# ---------------------------------------------------------

async def limpar_polvoras_db():
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM polvoras")
    except Exception as e:
        logger.error(f"❌ Erro ao limpar pólvoras: {e}")

# ---------------------------------------------------------
# ASYNC: salvar_venda_polvora
# ---------------------------------------------------------

async def salvar_venda_polvora(user_id, quantidade):
    pool = await get_pool()
    if not pool:
        return False
    try:
        valor = quantidade * PRECO_POLVORA
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO polvora_vendas (user_id, quantidade, valor, status, data_venda)
                VALUES ($1, $2, $3, 'pendente', NOW())
            """, str(user_id), quantidade, valor)
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao salvar venda de pólvora: {e}")
        return False

# ---------------------------------------------------------
# ASYNC: buscar_polvora_pendente
# ---------------------------------------------------------

async def buscar_polvora_pendente(user_id):
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("""
                SELECT SUM(quantidade) as total_quantidade, SUM(valor) as total_valor
                FROM polvora_vendas
                WHERE user_id = $1 AND status = 'pendente'
            """, str(user_id))
            if row and row["total_quantidade"]:
                return {"quantidade": row["total_quantidade"], "valor": row["total_valor"]}
            return None
    except Exception as e:
        logger.error(f"❌ Erro ao buscar pólvora pendente: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: pagar_polvora
# ---------------------------------------------------------

async def pagar_polvora(user_id):
    pool = await get_pool()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE polvora_vendas
                SET status = 'pago', data_pagamento = NOW()
                WHERE user_id = $1 AND status = 'pendente'
            """, str(user_id))
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao pagar pólvora: {e}")
        return False

# ---------------------------------------------------------
# ASYNC: resetar_polvora_pendente
# ---------------------------------------------------------

async def resetar_polvora_pendente(user_id):
    pool = await get_pool()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                DELETE FROM polvora_vendas
                WHERE user_id = $1 AND status = 'pendente'
            """, str(user_id))
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao resetar pólvora pendente: {e}")
        return False

# ---------------------------------------------------------
# ASYNC: gerar_desc_producao
# ---------------------------------------------------------

async def gerar_desc_producao(prod, pct=None, restante=None):
    try:
        if isinstance(prod["inicio"], str):
            inicio = str_para_datetime_completa(prod["inicio"])
        else:
            inicio = prod["inicio"]
            if isinstance(inicio, datetime) and inicio.tzinfo is None:
                inicio = inicio.replace(tzinfo=BRASIL)
        if isinstance(prod["fim"], str):
            fim = str_para_datetime_completa(prod["fim"])
        else:
            fim = prod["fim"]
            if isinstance(fim, datetime) and fim.tzinfo is None:
                fim = fim.replace(tzinfo=BRASIL)
        if not inicio or not fim:
            return f"**Galpão:** {prod.get('galpao', 'Desconhecido')}\n⏳ **Aguardando dados...**"
        agora_dt = agora()
        if pct is None:
            total = (fim - inicio).total_seconds()
            restante = (fim - agora_dt).total_seconds()
            restante = max(0, restante)
            if total <= 0:
                total = 1
            pct = 1 - (restante / total)
            pct = max(0, min(1, pct))
        else:
            restante = restante or 0
        mins = int(restante // 60)
        segundos = int(restante % 60)
        qtd_galpoes = prod.get('qtd_galpoes', 1)
        polvora_total = prod.get('polvora', 400)
        desc = f"**Galpão:** {prod['galpao']}\n"
        desc += f"**Quantidade de galpões:** {qtd_galpoes}\n"
        desc += f"**Iniciado por:** <@{prod['autor']}>\n"
        if prod.get("obs"):
            desc += f"📝 **Obs:** {prod['obs']}\n"
        desc += f"**Pólvora por galpão:** {prod.get('polvora_por_galpao', 400)}\n"
        desc += f"**Pólvora total:** {polvora_total}\n"
        desc += f"Início: <t:{int(inicio.timestamp())}:t>\n"
        desc += f"Término: <t:{int(fim.timestamp())}:t>\n\n"
        desc += f"⏳ **Restante:** {mins}m {segundos}s\n{barra(pct)}"
        if prod.get("segunda_task_confirmada"):
            uid = prod["segunda_task_confirmada"]["user"]
            desc += f"\n\n✅ **Segunda task concluída por:** <@{uid}>"
        return desc
    except Exception as e:
        logger.error(f"❌ Erro ao gerar descrição: {e}")
        return f"**Galpão:** {prod.get('galpao', 'Desconhecido')}\n⏳ **Erro ao carregar dados...**"

# ---------------------------------------------------------
# ASYNC: acompanhar_producao
# ---------------------------------------------------------

async def acompanhar_producao(pid):
    msg = None
    ultimo_pct = -1
    while True:
        try:
            prod = await carregar_producao(pid)
            if not prod:
                logger.error(f"❌ Produção {pid} não encontrada no banco")
                return
            if isinstance(prod["inicio"], str):
                inicio = str_para_datetime_completa(prod["inicio"])
            else:
                inicio = prod["inicio"]
                if isinstance(inicio, datetime) and inicio.tzinfo is None:
                    inicio = inicio.replace(tzinfo=BRASIL)
            if isinstance(prod["fim"], str):
                fim = str_para_datetime_completa(prod["fim"])
            else:
                fim = prod["fim"]
                if isinstance(fim, datetime) and fim.tzinfo is None:
                    fim = fim.replace(tzinfo=BRASIL)
            if not inicio or not fim:
                await asyncio.sleep(10)
                continue
            agora_dt = agora()
            if agora_dt >= fim:
                canal = bot.get_channel(prod["canal_id"])
                if canal:
                    try:
                        msg = await safe_fetch_message(canal, prod["msg_id"])
                    except:
                        msg = None
                    await finalizar_producao(pid, msg, prod)
                else:
                    await finalizar_producao(pid, None, prod)
                return
            canal = bot.get_channel(prod["canal_id"])
            if not canal:
                await asyncio.sleep(10)
                continue
            if msg is None:
                try:
                    msg = await safe_fetch_message(canal, prod["msg_id"])
                except:
                    desc = await gerar_desc_producao(prod)
                    embed = discord.Embed(title="🏭 Produção", description=desc, color=0x3498db)
                    view = None if prod.get("segunda_task_confirmada") else SegundaTaskView(pid)
                    msg = await safe_request(canal.send, embed=embed, view=view)
                    if msg:
                        prod["msg_id"] = msg.id
                        await salvar_producao(pid, prod)
                    else:
                        await asyncio.sleep(5)
                        continue
            if msg:
                total = (fim - inicio).total_seconds()
                restante = (fim - agora_dt).total_seconds()
                restante = max(0, restante)
                if total <= 0:
                    total = 1
                pct = 1 - (restante / total)
                pct = max(0, min(1, pct))
                pct_int = int(pct * 100)
                if pct_int != ultimo_pct or pct_int % 5 == 0:
                    ultimo_pct = pct_int
                    desc = await gerar_desc_producao(prod, pct, restante)
                    try:
                        await safe_request(msg.edit, embed=discord.Embed(title="🏭 Produção", description=desc, color=0x34495e))
                    except discord.NotFound:
                        msg = None
                        continue
                    except discord.HTTPException as e:
                        if e.status == 429:
                            await asyncio.sleep(5)
        except Exception as e:
            logger.error(f"❌ Erro no acompanhar_producao {pid}: {e}")
        await asyncio.sleep(10)

# ---------------------------------------------------------
# ASYNC: finalizar_producao
# ---------------------------------------------------------

async def finalizar_producao(pid, msg, prod):
    try:
        polvora_total = prod.get("polvora", 400)
        segunda = prod.get("segunda_task_confirmada")
        galpao = prod["galpao"]
        qtd_galpoes = prod.get("qtd_galpoes", 1)
        polvora_por_galpao = prod.get("polvora_por_galpao", polvora_total // qtd_galpoes if qtd_galpoes > 0 else polvora_total)
        if "NORTE" in galpao.upper():
            base_por_galpao = 1777 if segunda else 1688
        elif "SUL" in galpao.upper():
            base_por_galpao = 1618 if segunda else 1608
        else:
            base_por_galpao = 1777 if segunda else 1688
        capsulas_por_galpao = (base_por_galpao * polvora_por_galpao) // 400
        capsulas_total = capsulas_por_galpao * qtd_galpoes
        peso_total = capsulas_total * 0.05
        pool = await get_pool()
        if pool:
            async with pool.acquire() as conn:
                await conn.execute(
                    "INSERT INTO producoes_finalizadas (user_id, capsulas, data, polvora, galpao) VALUES ($1, $2, $3, $4, $5)",
                    str(prod["autor"]), capsulas_total, agora_db(), polvora_total, f"{galpao} ({qtd_galpoes} galpões)"
                )
                await conn.execute(
                    "UPDATE estoque_capsulas SET quantidade = quantidade + $1, ultima_atualizacao = NOW() WHERE id = 1",
                    capsulas_total
                )
                await conn.execute(
                    "INSERT INTO entrada_insumos (tipo, quantidade, registrado_por, obs) VALUES ($1, $2, $3, $4)",
                    "capsulas", capsulas_total, str(prod["autor"]), f"Produção do {galpao} - {qtd_galpoes} galpões - {polvora_total} pólvora"
                )
        if msg:
            try:
                desc = msg.embeds[0].description if msg.embeds and len(msg.embeds) > 0 else ""
                linhas = desc.split("\n")
                novas_linhas = []
                for linha in linhas:
                    if not linha.startswith("⏳ **Restante:**") and not "▓" in linha and not "░" in linha:
                        if linha.strip():
                            novas_linhas.append(linha)
                desc = "\n".join(novas_linhas)
                desc += (f"\n\n🔵 **Produção Finalizada**\n\n🧪 Produziu **{fmt_num(capsulas_total)} cápsulas**\n"
                        f"📦 **Por galpão:** {fmt_num(capsulas_por_galpao)} cápsulas\n"
                        f"🏭 **Quantidade de galpões:** {qtd_galpoes}\n"
                        f"⚖️ Peso total: **{peso_total:.2f} kg**\n"
                        f"💣 Pólvora total utilizada: **{polvora_total}**\n"
                        f"💣 Pólvora por galpão: **{polvora_por_galpao}**\n\n"
                        f"💊 As cápsulas foram adicionadas ao estoque de insumos!")
                await safe_request(msg.edit, embed=discord.Embed(title="🏭 Produção", description=desc, color=0x34495e), view=None)
            except Exception as e:
                logger.error(f"Erro ao editar mensagem final: {e}")
        await deletar_producao(pid)
        if pid in producoes_tasks:
            del producoes_tasks[pid]
        canal_bau = bot.get_channel(CANAL_BAU_GALPAO_ID)
        if canal_bau:
            embed_bau = discord.Embed(title="🏭 PRODUÇÃO DE CÁPSULAS FINALIZADA", color=0x2ecc71, timestamp=agora())
            embed_bau.add_field(name="🏭 Galpão", value=galpao, inline=True)
            embed_bau.add_field(name="🏭 Quantidade", value=f"{qtd_galpoes} galpão(ões)", inline=True)
            embed_bau.add_field(name="💊 Cápsulas produzidas", value=f"**{fmt_num(capsulas_total)}** unidades", inline=True)
            embed_bau.add_field(name="📦 Por galpão", value=f"{fmt_num(capsulas_por_galpao)} cápsulas", inline=True)
            embed_bau.add_field(name="💣 Pólvora total", value=f"**{polvora_total}**", inline=True)
            embed_bau.add_field(name="👤 Produzido por", value=f"<@{prod['autor']}>", inline=True)
            await canal_bau.send(embed=embed_bau)
        await enviar_painel_fabricacao()
    except Exception as e:
        logger.error(f"❌ ERRO ao finalizar produção {pid}: {e}")

# ---------------------------------------------------------
# ASYNC: verificar_heartbeat_producoes
# ---------------------------------------------------------

async def verificar_heartbeat_producoes():
    try:
        pool = await get_pool()
        if not pool:
            return
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT pid, galpao, fim FROM producoes WHERE CAST(fim AS timestamp) > NOW()")
        if not rows:
            return
        agora_br = agora()
        producoes_ativas = {}
        for r in rows:
            pid = r["pid"]
            fim = r["fim"]
            if isinstance(fim, str):
                fim = str_para_datetime(fim)
            if agora_br >= fim:
                prod = await carregar_producao(pid)
                if prod:
                    canal = bot.get_channel(prod["canal_id"])
                    msg = None
                    if canal:
                        try:
                            msg = await safe_fetch_message(canal, prod["msg_id"])
                        except:
                            pass
                    await finalizar_producao(pid, msg, prod)
                continue
            producoes_ativas[pid] = fim
        for pid, fim in producoes_ativas.items():
            if pid not in producoes_tasks or producoes_tasks[pid].done():
                if pid in producoes_tasks:
                    del producoes_tasks[pid]
                task = asyncio.create_task(acompanhar_producao(pid))
                producoes_tasks[pid] = task
            prod = await carregar_producao(pid)
            if prod:
                canal = bot.get_channel(prod["canal_id"])
                if canal:
                    try:
                        await safe_fetch_message(canal, prod["msg_id"])
                    except:
                        desc = await gerar_desc_producao(prod)
                        embed = discord.Embed(title="🏭 Produção", description=desc, color=0x3498db)
                        view = None if prod.get("segunda_task_confirmada") else SegundaTaskView(pid)
                        msg = await safe_request(canal.send, embed=embed, view=view)
                        if msg:
                            prod["msg_id"] = msg.id
                            await salvar_producao(pid, prod)
    except Exception as e:
        logger.error(f"❌ Erro no heartbeat: {e}")

# ---------------------------------------------------------
# ASYNC: enviar_painel_fabricacao
# ---------------------------------------------------------

async def enviar_painel_fabricacao():
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    if not canal:
        logger.error("❌ Canal de fabricação não encontrado")
        return
    estoque_municoes = await carregar_estoque()
    estoque_insumos = await carregar_estoque_insumos()
    alugueis = await carregar_alugueis()
    embed = discord.Embed(
        title="🏭 PAINEL DE FABRICAÇÃO",
        description="**Gerencie a produção e estoque:**",
        color=0x2ecc71
    )
    texto_alugueis = ""
    for galpao, dados in alugueis.items():
        dias = dados["dias"]
        inicio = dados["inicio"]
        if inicio and dias > 0:
            dias_passados = (agora() - inicio.replace(tzinfo=BRASIL)).days
            dias_restantes = max(0, dias - dias_passados)
            if dias_restantes > 0:
                status = f"🟢 {dias_restantes} dias"
            else:
                status = "🔴 EXPIRADO"
        else:
            status = "⚪ NÃO ALUGADO"
        texto_alugueis += f"**{galpao}:** {dias} dias | {status}\n"
    embed.add_field(name="📅 ALUGUEL DE GALPÕES", value=texto_alugueis or "Nenhum aluguel registrado", inline=False)
    embed.add_field(name="📦 ESTOQUE DE MUNIÇÃO", value=f"🔫 **PT:** {fmt_num(estoque_municoes['PT'])} pacotes\n🔫 **SUB:** {fmt_num(estoque_municoes['SUB'])} pacotes", inline=False)
    embed.add_field(name="💊 ESTOQUE DE INSUMOS", value=f"**Cápsulas:** {fmt_num(estoque_insumos['capsulas'])} unidades\n**Embalagens:** {fmt_num(estoque_insumos['embalagens'])} unidades", inline=False)
    embed.add_field(name="🏭 PRODUÇÃO DE CÁPSULAS", value=(
        "• **Galpões Norte:** 65 minutos (3 galpões)\n"
        "• **Galpões Sul:** 130 minutos (3 galpões)\n\n"
        "💡 Ao clicar, informe:\n"
        "   - Quantos galpões (1, 2 ou 3)\n"
        "   - Pólvora por galpão"
    ), inline=False)
    embed.set_footer(text=f"🔄 Atualizado em {agora().strftime('%d/%m/%Y %H:%M:%S')}")
    view = FabricacaoView()
    try:
        async for msg in canal.history(limit=20):
            if msg.author == bot.user and msg.embeds and len(msg.embeds) > 0 and msg.embeds[0].title == "🏭 PAINEL DE FABRICAÇÃO":
                try:
                    await msg.delete()
                except:
                    pass
        await canal.send(embed=embed, view=view)
    except Exception as e:
        logger.error(f"❌ Erro ao enviar painel de fabricação: {e}")

# ---------------------------------------------------------
# ASYNC: enviar_painel_polvoras
# ---------------------------------------------------------

async def enviar_painel_polvoras():
    canal = bot.get_channel(CANAL_CALCULO_POLVORA_ID)
    if not canal:
        logger.error("❌ Canal de pólvora não encontrado")
        return
    embed = discord.Embed(
        title="💣 Registro de Pólvora",
        description=(
            "**Clique no botão abaixo para registrar a compra de pólvora.**\n\n"
            "📌 **Informe apenas a quantidade comprada.**\n"
            f"💰 O valor será calculado automaticamente (R$ {PRECO_POLVORA:.2f} por unidade)."
        ),
        color=0xe67e22
    )
    await enviar_ou_atualizar_painel("painel_polvora", CANAL_CALCULO_POLVORA_ID, embed, PolvoraView())

# ---------------------------------------------------------
# ASYNC: salvar_aluguel
# ---------------------------------------------------------

async def salvar_aluguel(galpao, dias):
    pool = await get_pool()
    if not pool:
        return False
    try:
        dias = safe_int(dias)
        async with pool.acquire() as conn:
            existe = await conn.fetchval("SELECT id FROM alugueis WHERE galpao = $1 AND ativo = true", galpao)
            if existe:
                await conn.execute("""
                    UPDATE alugueis
                    SET dias_alugados = dias_alugados + $1::INTEGER, data_atualizacao = NOW()
                    WHERE galpao = $2 AND ativo = true
                """, dias, galpao)
            else:
                await conn.execute("""
                    INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo)
                    VALUES ($1, $2::INTEGER, NOW(), true)
                """, galpao, dias)
            return True
    except Exception as e:
        logger.error(f"❌ ERRO AO SALVAR ALUGUEL: {e}")
        return False

# ---------------------------------------------------------
# ASYNC: carregar_alugueis
# ---------------------------------------------------------

async def carregar_alugueis():
    pool = await get_pool()
    if not pool:
        return {"GALPÕES NORTE": {"dias": 0, "inicio": None}, "GALPÕES SUL": {"dias": 0, "inicio": None}}
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE alugueis
                SET ativo = false
                WHERE galpao NOT IN ('GALPÕES NORTE', 'GALPÕES SUL')
                  AND ativo = true
            """)
            existe_norte = await conn.fetchval("SELECT 1 FROM alugueis WHERE galpao = 'GALPÕES NORTE'")
            if not existe_norte:
                await conn.execute("""
                    INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo)
                    VALUES ('GALPÕES NORTE', 0, NOW(), true)
                """)
            existe_sul = await conn.fetchval("SELECT 1 FROM alugueis WHERE galpao = 'GALPÕES SUL'")
            if not existe_sul:
                await conn.execute("""
                    INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo)
                    VALUES ('GALPÕES SUL', 0, NOW(), true)
                """)
            rows = await conn.fetch("""
                SELECT galpao, dias_alugados, data_inicio
                FROM alugueis
                WHERE ativo = true
                AND galpao IN ('GALPÕES NORTE', 'GALPÕES SUL')
            """)
            resultado = {}
            for row in rows:
                galpao = row["galpao"]
                resultado[galpao] = {"dias": row["dias_alugados"] or 0, "inicio": row["data_inicio"]}
            if "GALPÕES NORTE" not in resultado:
                resultado["GALPÕES NORTE"] = {"dias": 0, "inicio": None}
            if "GALPÕES SUL" not in resultado:
                resultado["GALPÕES SUL"] = {"dias": 0, "inicio": None}
            return resultado
    except Exception as e:
        logger.error(f"❌ ERRO AO CARREGAR ALUGUEIS: {e}")
        return {"GALPÕES NORTE": {"dias": 0, "inicio": None}, "GALPÕES SUL": {"dias": 0, "inicio": None}}

# ---------------------------------------------------------
# ASYNC: resetar_aluguel
# ---------------------------------------------------------

async def resetar_aluguel(galpao):
    pool = await get_pool()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE alugueis SET ativo = false WHERE galpao = $1 AND ativo = true", galpao)
            await conn.execute("INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo) VALUES ($1, 0, NOW(), true)", galpao)
            return True
    except Exception as e:
        logger.error(f"❌ ERRO AO RESETAR ALUGUEL: {e}")
        return False

# =========================================================
# CLASS: SegundaTaskView
# =========================================================

class SegundaTaskView(discord.ui.View):
    def __init__(self, pid):
        super().__init__(timeout=None)
        self.pid = pid

    @discord.ui.button(label="✅ Confirmar 2ª Task", style=discord.ButtonStyle.success, custom_id="segunda_task_btn")
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)
        try:
            prod = await carregar_producao(self.pid)
            if not prod:
                await interaction.followup.send("❌ Produção não encontrada!", ephemeral=True)
                return
            if prod.get("segunda_task_confirmada"):
                await interaction.followup.send("⚠️ A segunda task já foi confirmada!", ephemeral=True)
                return
            fim = prod["fim"]
            if isinstance(fim, str):
                fim = str_para_datetime(fim)
            if agora() >= fim:
                await interaction.followup.send("⏰ **Produção já terminou!**", ephemeral=True)
                canal = interaction.guild.get_channel(prod["canal_id"])
                msg = None
                if canal:
                    try:
                        msg = await safe_fetch_message(canal, prod["msg_id"])
                    except:
                        pass
                await finalizar_producao(self.pid, msg, prod)
                try:
                    await interaction.message.edit(view=None)
                except:
                    pass
                return
            prod["segunda_task_confirmada"] = {
                "user": interaction.user.id,
                "time": agora().isoformat()
            }
            await salvar_producao(self.pid, prod)
            try:
                await interaction.message.edit(view=None)
            except:
                pass
            await interaction.followup.send("✅ **Segunda task confirmada com sucesso!**", ephemeral=True)
        except Exception as e:
            logger.error(f"Erro segunda task: {e}")
            await interaction.followup.send(f"❌ Erro: {str(e)[:100]}", ephemeral=True)

# =========================================================
# CLASS: ProducaoCompletaModal
# =========================================================

class ProducaoCompletaModal(discord.ui.Modal, title="🏭 Iniciar Produção"):
    qtd_galpoes = discord.ui.TextInput(label="📊 Quantos galpões?", placeholder="Digite 1, 2 ou 3", required=True, max_length=1)
    polvora_por_galpao = discord.ui.TextInput(label="💣 Pólvora por galpão", placeholder="Ex: 400", required=True)
    obs = discord.ui.TextInput(label="📝 Observação (opcional)", style=discord.TextStyle.paragraph, required=False)

    def __init__(self, galpao, tempo_base):
        super().__init__()
        self.galpao = galpao
        self.tempo_base = tempo_base

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            qtd = safe_int(self.qtd_galpoes.value)
            if qtd not in [1, 2, 3]:
                raise ValueError
        except:
            await interaction.followup.send("❌ Quantidade de galpões inválida! Digite 1, 2 ou 3.", ephemeral=True)
            return
        try:
            polvora_por_galpao = safe_int(self.polvora_por_galpao.value)
            if polvora_por_galpao <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ Quantidade de pólvora inválida!", ephemeral=True)
            return
        polvora_total = polvora_por_galpao * qtd
        tempo_real = max(2, int(self.tempo_base * (polvora_por_galpao / 400)))
        pid = f"{self.galpao}_{qtd}g_{interaction.id}_{int(time_module.time())}"
        inicio = agora()
        fim = inicio + timedelta(minutes=tempo_real)
        canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)
        if not canal:
            await interaction.followup.send("❌ Canal de produção não encontrado.", ephemeral=True)
            return
        desc = f"**Galpão:** {self.galpao}\n**Quantidade de galpões:** {qtd}\n**Iniciado por:** {interaction.user.mention}\n"
        if self.obs.value:
            desc += f"📝 **Obs:** {self.obs.value}\n"
        desc += (f"**Pólvora por galpão:** {polvora_por_galpao}\n"
                f"**Pólvora total:** {polvora_total}\n"
                f"Início: <t:{int(inicio.timestamp())}:t>\n"
                f"Término: <t:{int(fim.timestamp())}:t>\n\n"
                f"⏳ **Restante:** {tempo_real} min\n{barra(0)}")
        msg = await safe_request(canal.send,
            embed=discord.Embed(title=f"🏭 Produção - {qtd} Galpão(ões)", description=desc, color=0x3498db),
            view=SegundaTaskView(pid)
        )
        if not msg:
            await interaction.followup.send("❌ Erro ao enviar mensagem de produção!", ephemeral=True)
            return
        dados = {
            "galpao": f"{self.galpao} ({qtd} galpões)",
            "autor": interaction.user.id,
            "inicio": inicio,
            "fim": fim,
            "obs": self.obs.value,
            "polvora": polvora_total,
            "qtd_galpoes": qtd,
            "polvora_por_galpao": polvora_por_galpao,
            "msg_id": msg.id,
            "canal_id": CANAL_REGISTRO_GALPAO_ID
        }
        await salvar_producao(pid, dados)
        if pid not in producoes_tasks:
            task = asyncio.create_task(acompanhar_producao(pid))
            producoes_tasks[pid] = task
        await interaction.followup.send(
            f"✅ **Produção iniciada com sucesso!**\n\n"
            f"🏭 **Galpão:** {self.galpao}\n"
            f"📊 **Quantidade:** {qtd} galpão(ões)\n"
            f"💣 **Pólvora por galpão:** {fmt_num(polvora_por_galpao)}\n"
            f"💣 **Pólvora total:** {fmt_num(polvora_total)}\n"
            f"⏰ **Término previsto:** <t:{int(fim.timestamp())}:t>\n"
            f"⏱️ **Duração:** {tempo_real} minutos",
            ephemeral=True
        )

# =========================================================
# CLASS: ProducaoMunicaoModal
# =========================================================

class ProducaoMunicaoModal(discord.ui.Modal, title="🎯 Produzir Munição"):
    tipo_municao = discord.ui.TextInput(label="Tipo de munição", placeholder="Digite PT ou SUB", required=True, max_length=3)
    quantidade_pacotes = discord.ui.TextInput(label="Quantidade de PACOTES", placeholder="Ex: 100 (cada pacote = 50 munições)", required=True)
    observacao = discord.ui.TextInput(label="Observação (opcional)", style=discord.TextStyle.paragraph, required=False)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        tipo = self.tipo_municao.value.strip().upper()
        if tipo not in ["PT", "SUB"]:
            await interaction.followup.send("❌ **Tipo inválido!** Use `PT` ou `SUB`.", ephemeral=True)
            return
        try:
            pacotes = safe_int(self.quantidade_pacotes.value)
            if pacotes <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ **Quantidade inválida!**", ephemeral=True)
            return
        verificacao = await verificar_insumos_producao(tipo, pacotes)
        if not verificacao["suficiente"]:
            faltando = []
            if verificacao["capsulas_disponiveis"] < verificacao["capsulas_necessarias"]:
                faltando.append(f"🔴 **Cápsulas:** precisa de {fmt_num(verificacao['capsulas_necessarias'])}, tem apenas {fmt_num(verificacao['capsulas_disponiveis'])}")
            if verificacao["embalagens_disponiveis"] < verificacao["embalagens_necessarias"]:
                faltando.append(f"📦 **Embalagens:** precisa de {fmt_num(verificacao['embalagens_necessarias'])}, tem apenas {fmt_num(verificacao['embalagens_disponiveis'])}")
            await interaction.followup.send(f"❌ **INSUMOS INSUFICIENTES!**\n\n" + "\n".join(faltando), ephemeral=True)
            return
        municoes = pacotes * 50
        capsulas_usadas = verificacao["capsulas_necessarias"]
        embalagens_usadas = verificacao["embalagens_necessarias"]
        await registrar_producao_municao(tipo, pacotes, interaction.user.id, self.observacao.value)
        estoque_municoes = await carregar_estoque()
        estoque_insumos = await carregar_estoque_insumos()
        canal_bau = interaction.guild.get_channel(CANAL_BAU_GALPAO_ID)
        if canal_bau:
            embed_bau = discord.Embed(title="🔫 PRODUÇÃO DE MUNIÇÃO REALIZADA", color=0x2ecc71, timestamp=agora())
            embed_bau.add_field(name="🔫 Tipo", value=f"**{tipo}**", inline=True)
            embed_bau.add_field(name="📦 Pacotes", value=f"**{fmt_num(pacotes)}** pacotes", inline=True)
            embed_bau.add_field(name="🔫 Munições", value=f"**{fmt_num(municoes)}** unidades", inline=True)
            embed_bau.add_field(name="👤 Produzido por", value=interaction.user.mention, inline=True)
            embed_bau.add_field(name="📦 INSUMOS CONSUMIDOS", value=f"💊 Cápsulas: **{fmt_num(capsulas_usadas)}**\n📦 Embalagens: **{fmt_num(embalagens_usadas)}**", inline=False)
            if self.observacao.value:
                embed_bau.add_field(name="📝 Observação", value=self.observacao.value, inline=False)
            embed_bau.add_field(name="📊 ESTOQUE APÓS PRODUÇÃO", value=(f"**Munições:**\n🔫 PT: {fmt_num(estoque_municoes['PT'])} pacotes\n🔫 SUB: {fmt_num(estoque_municoes['SUB'])} pacotes\n\n**Insumos restantes:**\n💊 Cápsulas: {fmt_num(estoque_insumos['capsulas'])}\n📦 Embalagens: {fmt_num(estoque_insumos['embalagens'])}"), inline=False)
            await canal_bau.send(embed=embed_bau)
        await interaction.followup.send("✅ **Produção realizada com sucesso!**", ephemeral=True)
        await enviar_painel_fabricacao()

# =========================================================
# CLASS: RegistrarCapsulasModal
# =========================================================

class RegistrarCapsulasModal(discord.ui.Modal, title="📦 Registrar Cápsulas"):
    quantidade = discord.ui.TextInput(label="Quantidade de CÁPSULAS", placeholder="Ex: 1000", required=True)
    observacao = discord.ui.TextInput(label="Observação (opcional)", style=discord.TextStyle.paragraph, required=False)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            quantidade = safe_int(self.quantidade.value)
            if quantidade <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ Quantidade inválida!", ephemeral=True)
            return
        await registrar_entrada_insumos("capsulas", quantidade, interaction.user.id, self.observacao.value)
        estoque = await carregar_estoque_insumos()
        canal_bau = interaction.guild.get_channel(CANAL_BAU_GALPAO_ID)
        if canal_bau:
            embed_bau = discord.Embed(title="📦 ENTRADA DE CÁPSULAS", color=0x3498db, timestamp=agora())
            embed_bau.add_field(name="📦 Quantidade", value=f"**{fmt_num(quantidade)}** cápsulas", inline=True)
            embed_bau.add_field(name="👤 Registrado por", value=interaction.user.mention, inline=True)
            if self.observacao.value:
                embed_bau.add_field(name="📝 Obs", value=self.observacao.value, inline=False)
            embed_bau.set_footer(text=f"Novo estoque: {fmt_num(estoque['capsulas'])} cápsulas")
            await canal_bau.send(embed=embed_bau)
        await interaction.followup.send(f"✅ **{fmt_num(quantidade)} cápsulas adicionadas!**", ephemeral=True)
        await enviar_painel_fabricacao()

# =========================================================
# CLASS: RegistrarEmbalagensModal
# =========================================================

class RegistrarEmbalagensModal(discord.ui.Modal, title="📦 Registrar Embalagens"):
    quantidade = discord.ui.TextInput(label="Quantidade de EMBALAGENS", placeholder="Ex: 500", required=True)
    observacao = discord.ui.TextInput(label="Observação (opcional)", style=discord.TextStyle.paragraph, required=False)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            quantidade = safe_int(self.quantidade.value)
            if quantidade <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ Quantidade inválida!", ephemeral=True)
            return
        await registrar_entrada_insumos("embalagens", quantidade, interaction.user.id, self.observacao.value)
        estoque = await carregar_estoque_insumos()
        canal_bau = interaction.guild.get_channel(CANAL_BAU_GALPAO_ID)
        if canal_bau:
            embed_bau = discord.Embed(title="📦 ENTRADA DE EMBALAGENS", color=0x3498db, timestamp=agora())
            embed_bau.add_field(name="📦 Quantidade", value=f"**{fmt_num(quantidade)}** embalagens", inline=True)
            embed_bau.add_field(name="👤 Registrado por", value=interaction.user.mention, inline=True)
            if self.observacao.value:
                embed_bau.add_field(name="📝 Obs", value=self.observacao.value, inline=False)
            embed_bau.set_footer(text=f"Novo estoque: {fmt_num(estoque['embalagens'])} embalagens")
            await canal_bau.send(embed=embed_bau)
        await interaction.followup.send(f"✅ **{fmt_num(quantidade)} embalagens adicionadas!**", ephemeral=True)
        await enviar_painel_fabricacao()

# =========================================================
# CLASS: EditarEstoqueModal
# =========================================================

class EditarEstoqueModal(discord.ui.Modal, title="📦 EDITAR ESTOQUE DE MUNIÇÃO"):
    pt = discord.ui.TextInput(
        label="🔫 Quantidade de PT (pacotes)",
        placeholder="Digite a quantidade atual de PT",
        required=True,
        max_length=10
    )
    sub = discord.ui.TextInput(
        label="🔫 Quantidade de SUB (pacotes)",
        placeholder="Digite a quantidade atual de SUB",
        required=True,
        max_length=10
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        try:
            nova_pt = int(self.pt.value.replace(".", "").replace(",", ""))
            nova_sub = int(self.sub.value.replace(".", "").replace(",", ""))
            if nova_pt < 0 or nova_sub < 0:
                raise ValueError("Valores não podem ser negativos")
        except ValueError:
            await interaction.followup.send("❌ **Valores inválidos!** Digite números positivos.", ephemeral=True)
            return
        
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        
        try:
            async with pool.acquire() as conn:
                await conn.execute(
                    "UPDATE estoque_municoes SET quantidade = $1, ultima_atualizacao = NOW() WHERE tipo = 'PT'",
                    nova_pt
                )
                await conn.execute(
                    "UPDATE estoque_municoes SET quantidade = $1, ultima_atualizacao = NOW() WHERE tipo = 'SUB'",
                    nova_sub
                )
            
            await enviar_painel_fabricacao()
            
            embed = discord.Embed(
                title="✅ ESTOQUE ATUALIZADO!",
                description=f"🔫 **PT:** {fmt_num(nova_pt)} pacotes\n🔫 **SUB:** {fmt_num(nova_sub)} pacotes",
                color=0x2ecc71,
                timestamp=agora()
            )
            embed.set_footer(text=f"Atualizado por {interaction.user.display_name}")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"❌ Erro ao editar estoque: {e}")
            await interaction.followup.send(f"❌ Erro ao editar estoque: {e}", ephemeral=True)

# =========================================================
# CLASS: FabricacaoView
# =========================================================

class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🏭 Galpões Norte", style=discord.ButtonStyle.primary, custom_id="fabricacao_norte")
    async def norte(self, interaction: discord.Interaction, button: discord.ui.Button):
        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível.", ephemeral=True)
            return
        async with pool.acquire() as conn:
            ativo = await conn.fetchval("SELECT 1 FROM producoes WHERE galpao LIKE 'GALPÕES NORTE%' AND CAST(fim AS timestamp) > NOW()")
        if ativo:
            await interaction.response.send_message("⚠️ Galpões Norte já está em produção.", ephemeral=True)
            return
        await interaction.response.send_modal(ProducaoCompletaModal("GALPÕES NORTE", TEMPO_BASE_NORTE))

    @discord.ui.button(label="🏭 Galpões Sul", style=discord.ButtonStyle.secondary, custom_id="fabricacao_sul")
    async def sul(self, interaction: discord.Interaction, button: discord.ui.Button):
        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível.", ephemeral=True)
            return
        async with pool.acquire() as conn:
            ativo = await conn.fetchval("SELECT 1 FROM producoes WHERE galpao LIKE 'GALPÕES SUL%' AND CAST(fim AS timestamp) > NOW()")
        if ativo:
            await interaction.response.send_message("⚠️ Galpões Sul já está em produção.", ephemeral=True)
            return
        await interaction.response.send_modal(ProducaoCompletaModal("GALPÕES SUL", TEMPO_BASE_SUL))

    @discord.ui.button(label="💊 Registrar Cápsulas", style=discord.ButtonStyle.primary, custom_id="registrar_capsulas", emoji="💊")
    async def registrar_capsulas(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarCapsulasModal())

    @discord.ui.button(label="📦 Registrar Embalagens", style=discord.ButtonStyle.primary, custom_id="registrar_embalagens", emoji="📦")
    async def registrar_embalagens(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarEmbalagensModal())

    @discord.ui.button(label="🔫 Produzir Munição", style=discord.ButtonStyle.success, custom_id="fabricacao_municao", emoji="🎯")
    async def produzir_municao(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ProducaoMunicaoModal())

    @discord.ui.button(label="📊 Estoque", style=discord.ButtonStyle.secondary, custom_id="ver_estoque_completo", emoji="📊")
    async def ver_estoque_completo(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        estoque_municoes = await carregar_estoque()
        estoque_insumos = await carregar_estoque_insumos()
        embed = discord.Embed(title="📊 ESTOQUE COMPLETO", color=0x3498db)
        embed.add_field(name="🔫 MUNIÇÕES", value=f"**PT:** {fmt_num(estoque_municoes['PT'])} pacotes ({fmt_num(estoque_municoes['PT'] * 50)} munições)\n**SUB:** {fmt_num(estoque_municoes['SUB'])} pacotes ({fmt_num(estoque_municoes['SUB'] * 50)} munições)", inline=False)
        embed.add_field(name="💊 INSUMOS", value=f"**Cápsulas:** {fmt_num(estoque_insumos['capsulas'])} unidades\n**Embalagens:** {fmt_num(estoque_insumos['embalagens'])} unidades", inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="📊 Relatório Produção", style=discord.ButtonStyle.secondary, custom_id="fabricacao_relatorio")
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RelatorioProducaoModal())

    @discord.ui.button(label="🔄 Atualizar Painel", style=discord.ButtonStyle.secondary, custom_id="atualizar_painel_btn", emoji="🔄")
    async def atualizar_painel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await enviar_painel_fabricacao()
        await interaction.followup.send("✅ Painel atualizado!", ephemeral=True)

    @discord.ui.button(label="📅 Alugar Galpão", style=discord.ButtonStyle.primary, custom_id="alugar_galpao", emoji="📅")
    async def alugar_galpao(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AlugarGalpaoModal())

    @discord.ui.button(label="📊 Alugueis", style=discord.ButtonStyle.secondary, custom_id="ver_alugueis", emoji="📊")
    async def ver_alugueis(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        alugueis = await carregar_alugueis()
        embed = discord.Embed(title="📅 STATUS DOS ALUGUEIS", color=0x3498db, timestamp=agora())
        for galpao, dados in alugueis.items():
            dias = dados["dias"]
            inicio = dados["inicio"]
            if inicio and dias > 0:
                dias_passados = (agora() - inicio.replace(tzinfo=BRASIL)).days
                dias_restantes = max(0, dias - dias_passados)
                if dias_restantes > 0:
                    status = f"🟢 {dias_restantes} dias restantes"
                else:
                    status = "🔴 EXPIRADO"
            else:
                status = "⚪ NÃO ALUGADO"
            embed.add_field(name=f"🏭 {galpao}", value=f"**Dias alugados:** {dias}\n**Status:** {status}", inline=True)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="✏️ Editar Estoque", style=discord.ButtonStyle.primary, custom_id="editar_estoque_btn", emoji="✏️")
    async def editar_estoque(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ Apenas **Administradores** ou **Gerentes** podem editar o estoque!", ephemeral=True)
            return
        
        estoque = await carregar_estoque()
        modal = EditarEstoqueModal()
        modal.pt.placeholder = f"Atual: {fmt_num(estoque['PT'])} pacotes"
        modal.sub.placeholder = f"Atual: {fmt_num(estoque['SUB'])} pacotes"
        
        await interaction.response.send_modal(modal)

# =========================================================
# CLASS: RelatorioProducaoModal
# =========================================================

class RelatorioProducaoModal(discord.ui.Modal, title="📊 Relatório de Produção"):
    data_inicio = discord.ui.TextInput(label="Data inicial (DD/MM/AAAA)", placeholder="Ex: 01/04/2026")
    data_fim = discord.ui.TextInput(label="Data final (DD/MM/AAAA)", placeholder="Ex: 30/04/2026")

    async def on_submit(self, interaction: discord.Interaction):
        try:
            await interaction.response.defer(ephemeral=True)
            inicio = datetime.strptime(self.data_inicio.value.strip(), "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value.strip(), "%d/%m/%Y")
            inicio_dt = inicio.replace(hour=0, minute=0, second=0)
            fim_dt = fim.replace(hour=23, minute=59, second=59)
            pool = await get_pool()
            if not pool:
                await interaction.followup.send("❌ Banco de dados indisponível.", ephemeral=True)
                return
            async with pool.acquire() as conn:
                rows = await conn.fetch(
                    "SELECT user_id, SUM(capsulas) as total_capsulas, SUM(polvora) as total_polvora FROM producoes_finalizadas WHERE data >= $1 AND data <= $2 GROUP BY user_id ORDER BY total_capsulas DESC",
                    inicio_dt, fim_dt
                )
            if not rows:
                await interaction.followup.send(f"📭 Nenhuma produção no período.", ephemeral=True)
                return
            total_capsulas = sum(r["total_capsulas"] or 0 for r in rows)
            total_polvora = sum(r["total_polvora"] or 0 for r in rows)
            linhas = []
            for r in rows:
                uid = r["user_id"]
                capsulas = int(r["total_capsulas"] or 0)
                polvora = int(r["total_polvora"] or 0)
                try:
                    user = await bot.fetch_user(int(uid))
                    nome = user.display_name if user else str(uid)
                except:
                    nome = str(uid)
                linhas.append(f"**{nome}** — {fmt_num(capsulas)} cápsulas | 💣 {fmt_num(polvora)} pólvora")
            embed = discord.Embed(title="📊 RELATÓRIO DE PRODUÇÃO DE CÁPSULAS", description=f"📅 **Período:** {self.data_inicio.value} até {self.data_fim.value}\n💰 **Total produzido:** `{fmt_num(total_capsulas)}` cápsulas\n💣 **Total pólvora gasto:** `{fmt_num(total_polvora)}`", color=0x2ecc71)
            embed.add_field(name="🏆 RANKING", value="\n".join(linhas) if linhas else "Nenhum", inline=False)
            canal = interaction.guild.get_channel(1422853066541109338)
            if canal:
                await canal.send(embed=embed)
                await interaction.followup.send(f"✅ Relatório enviado!", ephemeral=True)
            else:
                await interaction.followup.send(embed=embed, ephemeral=True)
        except ValueError:
            await interaction.followup.send("❌ **Formato de data inválido!**", ephemeral=True)
        except Exception as e:
            logger.error(f"ERRO RELATORIO: {e}")
            await interaction.followup.send("❌ Erro ao gerar relatório.", ephemeral=True)

# =========================================================
# CLASS: PolvoraModal
# =========================================================

class PolvoraModal(discord.ui.Modal, title="Registro de Compra de Pólvora"):
    quantidade = discord.ui.TextInput(label="Quantidade de Pólvora", placeholder="Digite apenas a quantidade (ex: 100)", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = safe_int(self.quantidade.value)
            if qtd <= 0:
                raise ValueError
        except:
            await interaction.response.send_message("❌ Quantidade inválida!", ephemeral=True)
            return
        valor = qtd * PRECO_POLVORA
        await salvar_polvora_db(interaction.user.id, qtd, valor)
        canal = interaction.guild.get_channel(CANAL_REGISTRO_POLVORA_ID)
        if canal:
            valor_formatado = formatar_dinheiro(valor)
            embed = discord.Embed(title="🧨 Registro de Pólvora", color=0xe67e22, timestamp=agora())
            embed.add_field(name="Registrado por", value=interaction.user.mention, inline=False)
            embed.add_field(name="Quantidade", value=f"{fmt_num(qtd)} unidades", inline=True)
            embed.add_field(name="Valor total", value=f"**{valor_formatado}**", inline=True)
            embed.set_footer(text=f"R$ {PRECO_POLVORA:.2f} por unidade")
            await canal.send(embed=embed)
        await interaction.response.send_message(f"✅ **Registro feito com sucesso!**\n\n📦 Quantidade: {fmt_num(qtd)} unidades\n💰 Valor: {formatar_dinheiro(valor)}", ephemeral=True)

# =========================================================
# CLASS: PolvoraView
# =========================================================

class PolvoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Registrar Compra de Pólvora", style=discord.ButtonStyle.primary, custom_id="polvora_btn")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PolvoraModal())

# =========================================================
# CLASS: ConfirmarPagamentoView
# =========================================================

class ConfirmarPagamentoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Confirmar pagamento", style=discord.ButtonStyle.success, custom_id="confirmar_pagamento")
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.message.edit(content=interaction.message.content + "\n\n✅ **PAGO**", view=None)
        await responder_interacao(interaction, defer=True)

# =========================================================
# CLASS: AlugarGalpaoModal
# =========================================================

class AlugarGalpaoModal(discord.ui.Modal, title="📅 Alugar Galpão"):
    galpao = discord.ui.TextInput(label="🏭 Qual galpão?", placeholder="Digite NORTE ou SUL", required=True, max_length=5)
    dias = discord.ui.TextInput(label="📅 Quantos dias?", placeholder="Digite o número de dias", required=True, max_length=3)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        galpao_input = self.galpao.value.strip().upper()
        if galpao_input == "NORTE":
            galpao = "GALPÕES NORTE"
        elif galpao_input == "SUL":
            galpao = "GALPÕES SUL"
        else:
            await interaction.followup.send("❌ Galpão inválido! Use NORTE ou SUL.", ephemeral=True)
            return
        try:
            dias = safe_int(self.dias.value)
            if dias <= 0:
                raise ValueError
        except ValueError:
            await interaction.followup.send("❌ Número de dias inválido! Digite um número inteiro positivo.", ephemeral=True)
            return
        sucesso = await salvar_aluguel(galpao, dias)
        if not sucesso:
            await interaction.followup.send("❌ Erro ao salvar aluguel. Tente novamente.", ephemeral=True)
            return
        embed = discord.Embed(title="📅 ALUGUEL REGISTRADO", description=f"🏭 **{galpao}**\n📅 **{dias} dias** adicionados", color=0x2ecc71, timestamp=agora())
        alugueis = await carregar_alugueis()
        dados = alugueis.get(galpao, {})
        total_dias = dados.get("dias", 0)
        embed.add_field(name="📊 Total de dias", value=f"{total_dias} dias", inline=True)
        await interaction.followup.send(embed=embed, ephemeral=True)
        await enviar_painel_fabricacao()

# =========================================================
# TASK: relatorio_semanal_polvoras
# =========================================================

@tasks.loop(minutes=1)
async def relatorio_semanal_polvoras():
    agora_br = agora()
    if agora_br.weekday() != 6 or agora_br.hour != 23 or agora_br.minute != 59:
        return
    dados = await carregar_polvoras_db()
    inicio_semana = (agora_br - timedelta(days=6)).replace(hour=0, minute=0, second=0, microsecond=0)
    fim_semana = agora_br.replace(hour=23, minute=59, second=59)
    resumo = {}
    for item in dados:
        data_item = datetime.fromisoformat(item["data"])
        if inicio_semana <= data_item <= fim_semana:
            resumo.setdefault(item["user_id"], 0)
            resumo[item["user_id"]] += item["valor"]
    if not resumo:
        return
    canal = bot.get_channel(CANAL_REGISTRO_POLVORA_ID)
    for user_id, total in resumo.items():
        user = await pegar_usuario(int(user_id))
        await canal.send(
            content=(
                f"🧨 **RELATÓRIO SEMANAL DE PÓLVORA**\n"
                f"📅 Período: {inicio_semana.strftime('%d/%m')} até {fim_semana.strftime('%d/%m')}\n\n"
                f"👤 Comprado por: {user.mention}\n"
                f"💰 Valor a ressarcir: **{formatar_dinheiro(total)}**"
            ),
            view=ConfirmarPagamentoView()
        )

# =========================================================
# ==================== SISTEMA DE VENDAS ==================
# =========================================================

# ---------------------------------------------------------
# 4.1: CONSTANTES DAS VENDAS
# ---------------------------------------------------------

ORGANIZACOES_CONFIG = {
    "VDR": {"emoji": "🕴️", "cor": 0x1e3a8a},
    "POLICIA": {"emoji": "👮", "cor": 0x3498db},
    "MAFIA": {"emoji": "🤵", "cor": 0x8e44ad},
    "BALAS": {"emoji": "🔫", "cor": 0xe67e22},
    "FAMILIA": {"emoji": "👨‍👩‍👧‍👦", "cor": 0x2ecc71}
}

# ---------------------------------------------------------
# ASYNC: proximo_pedido
# ---------------------------------------------------------

async def proximo_pedido():
    pool = await get_pool()
    if not pool:
        return 1
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT ultimo FROM pedidos WHERE id=1")
            if not row:
                await conn.execute("INSERT INTO pedidos (id, ultimo) VALUES (1, 1)")
                return 1
            novo = row["ultimo"] + 1
            await conn.execute("UPDATE pedidos SET ultimo=$1 WHERE id=1", novo)
            return novo
    except Exception as e:
        logger.error(f"❌ Erro ao gerar próximo pedido: {e}")
        return 1

# ---------------------------------------------------------
# ASYNC: salvar_venda_db
# ---------------------------------------------------------

async def salvar_venda_db(vendedor_id, valor, pedido_numero):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO vendas (user_id, valor, data, pedido_numero) VALUES ($1, $2, $3, $4)",
                vendedor_id, valor, agora_db().strftime("%d/%m/%Y"), pedido_numero
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar venda: {e}")

# ---------------------------------------------------------
# ASYNC: atualizar_valor_venda_db
# ---------------------------------------------------------

async def atualizar_valor_venda_db(pedido_numero, valor):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE vendas SET valor=$1 WHERE pedido_numero=$2", valor, pedido_numero)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar venda: {e}")

# ---------------------------------------------------------
# ASYNC: carregar_vendas_db
# ---------------------------------------------------------

async def carregar_vendas_db():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM vendas")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar vendas: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: salvar_entrega_parcelada
# ---------------------------------------------------------

async def salvar_entrega_parcelada(pedido_original, total_entregas, pt_por_entrega, sub_por_entrega, vendedor_id, organizacao, observacoes, canal_id):
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            proxima = agora() + timedelta(days=1)
            proxima = proxima.replace(hour=0, minute=0, second=0, microsecond=0)
            proxima_naive = para_db_naive(proxima)
            return await conn.fetchval(
                """
                INSERT INTO entregas_parceladas (
                    pedido_original, entrega_atual, total_entregas,
                    pt_por_entrega, sub_por_entrega,
                    vendedor_id, organizacao, observacoes,
                    proxima_entrega, canal_id
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                RETURNING id
                """,
                pedido_original, 1, total_entregas,
                pt_por_entrega, sub_por_entrega,
                vendedor_id, organizacao, observacoes,
                proxima_naive, canal_id
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar entrega parcelada: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: buscar_entregas_pendentes
# ---------------------------------------------------------

async def buscar_entregas_pendentes():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT * FROM entregas_parceladas
                WHERE ativo = true
                AND proxima_entrega <= NOW()
                ORDER BY proxima_entrega ASC
                """
            )
    except Exception as e:
        logger.error(f"❌ Erro ao buscar entregas pendentes: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: atualizar_entrega_parcelada
# ---------------------------------------------------------

async def atualizar_entrega_parcelada(entrega_id, entrega_atual, mensagem_id, proxima_entrega=None):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            if proxima_entrega is None:
                proxima_entrega = agora() + timedelta(days=1)
                proxima_entrega = proxima_entrega.replace(hour=0, minute=0, second=0, microsecond=0)
            proxima_naive = para_db_naive(proxima_entrega)
            await conn.execute(
                """
                UPDATE entregas_parceladas
                SET entrega_atual = $1,
                    mensagem_ids = array_append(mensagem_ids, $2),
                    proxima_entrega = $3
                WHERE id = $4
                """,
                entrega_atual, mensagem_id, proxima_naive, entrega_id
            )
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar entrega parcelada: {e}")

# ---------------------------------------------------------
# ASYNC: finalizar_entregas
# ---------------------------------------------------------

async def finalizar_entregas(entrega_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE entregas_parceladas SET ativo = false WHERE id = $1", entrega_id)
    except Exception as e:
        logger.error(f"❌ Erro ao finalizar entregas: {e}")

# ---------------------------------------------------------
# ASYNC: salvar_entrega_detalhes
# ---------------------------------------------------------

async def salvar_entrega_detalhes(entrega_id, entregas_json):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO entregas_detalhes (entrega_id, entregas_json) VALUES ($1, $2) ON CONFLICT (entrega_id) DO UPDATE SET entregas_json = $2",
                entrega_id, entregas_json
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar detalhes da entrega: {e}")

# ---------------------------------------------------------
# ASYNC: registrar_saida_estoque
# ---------------------------------------------------------

async def registrar_saida_estoque(pedido_numero, tipo, pacotes, retirado_por):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO saida_estoque (pedido_numero, tipo, pacotes, retirado_por, data) VALUES ($1, $2, $3, $4, NOW())",
                pedido_numero, tipo, pacotes, str(retirado_por)
            )
            await atualizar_estoque(tipo, pacotes, "remover")
    except Exception as e:
        logger.error(f"❌ Erro ao registrar saída de estoque: {e}")

# ---------------------------------------------------------
# ASYNC: verificar_estoque_suficiente
# ---------------------------------------------------------

async def verificar_estoque_suficiente(tipo, pacotes_necessarios):
    estoque = await carregar_estoque()
    return estoque.get(tipo, 0) >= pacotes_necessarios

# ---------------------------------------------------------
# ASYNC: buscar_grupo_por_organizacao
# ---------------------------------------------------------

async def buscar_grupo_por_organizacao(nome_org):
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            return await conn.fetchrow("SELECT grupo_id FROM grupos WHERE LOWER(nome_org) = LOWER($1) AND ativo = true", nome_org)
    except Exception as e:
        logger.error(f"❌ Erro ao buscar grupo por organização: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: criar_embed_entrega
# ---------------------------------------------------------

async def criar_embed_entrega(interaction, pedido_numero, entrega_atual, total_entregas, pt, sub, org_nome, config, observacoes, entrega_id=None, vendedor_id=None, grupo=None, entregas_lista=None):
    canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
    if not canal:
        await interaction.followup.send("❌ Canal de encomendas não encontrado!", ephemeral=True)
        return

    pacotes_pt = pt // 50
    pacotes_sub = sub // 50

    if total_entregas > 1:
        titulo = f"📦 ENTREGA {entrega_atual}/{total_entregas} • Pedido #{pedido_numero:04d}"
        descricao = f"**🔴 ATENÇÃO! Esta venda tem {total_entregas} entregas no total!**\n📦 **Esta entrega contém:** PT {fmt_num(pt)} + SUB {fmt_num(sub)} munições"
    else:
        titulo = f"📦 NOVA ENCOMENDA • Pedido #{pedido_numero:04d}"
        descricao = "✅ Entrega única"

    embed = discord.Embed(title=titulo, description=descricao, color=config["cor"])

    if total_entregas > 1 and entregas_lista:
        resumo = ""
        for i, e in enumerate(entregas_lista, 1):
            if i < entrega_atual:
                status = "✅"
            elif i == entrega_atual:
                status = "🔴"
            else:
                status = "⏳"
            resumo += f"{status} Entrega {i}/{total_entregas}: PT {fmt_num(e['pt'])} + SUB {fmt_num(e['sub'])} munições\n"
        embed.add_field(name="🚨 RESUMO DAS ENTREGAS", value=resumo, inline=False)

    embed.add_field(name="👤 Vendedor", value=f"<@{vendedor_id or interaction.user.id}>", inline=False)
    embed.add_field(name="🏷 Organização", value=f"{config['emoji']} {org_nome}", inline=False)
    embed.add_field(name="🔫 PT", value=f"{fmt_num(pt)} munições\n📦 {pacotes_pt} pacotes", inline=True)
    embed.add_field(name="🔫 SUB", value=f"{fmt_num(sub)} munições\n📦 {pacotes_sub} pacotes", inline=True)

    valor_entrega = (pt * 50) + (sub * 90)
    embed.add_field(name="💰 Valor (esta entrega)", value=f"**{formatar_dinheiro(valor_entrega)}**", inline=False)

    if total_entregas > 1:
        embed.add_field(name="📋 STATUS DAS ENTREGAS", value=f"**Total de entregas:** {total_entregas}\n**Entrega atual:** {entrega_atual}/{total_entregas}\n**Próxima entrega:** 🔒 Aguardando esta ser ENTREGUE", inline=False)

    embed.add_field(name="📌 Status", value="📦 A Entregar\n⏳ Pagamento pendente", inline=False)

    if observacoes:
        embed.add_field(name="📝 Observações", value=observacoes, inline=False)

    if grupo:
        embed.add_field(name="📊 INTEGRAÇÃO COM GRUPO", value=f"✅ Compra registrada automaticamente no grupo **{org_nome}**", inline=False)

    if entrega_id:
        embed.set_footer(text=f"🛡 Sistema de Encomendas • VDR 442 • Entrega {entrega_atual}/{total_entregas} • ID: {entrega_id}")
    else:
        embed.set_footer(text=f"🛡 Sistema de Encomendas • VDR 442 • Entrega {entrega_atual}/{total_entregas}")

    view = StatusView(
        entrega_id=entrega_id,
        total_entregas=total_entregas,
        entrega_atual=entrega_atual
    )

    msg = await safe_request(canal.send, embed=embed, view=view)

    if msg and entrega_id:
        await atualizar_entrega_parcelada(entrega_id, entrega_atual, str(msg.id), None)

    return msg

# ---------------------------------------------------------
# ASYNC: enviar_painel_vendas
# ---------------------------------------------------------

async def enviar_painel_vendas():
    canal = bot.get_channel(CANAL_VENDAS_ID)
    if not canal:
        logger.error("❌ Canal de vendas não encontrado")
        return
    estoque = await carregar_estoque()
    embed = discord.Embed(
        title="🛒 Painel de Vendas",
        description="Escolha uma opção abaixo.\n\n⚠️ **ATENÇÃO:** Antes de entregar um pedido, verifique se há ESTOQUE disponível!",
        color=0x2ecc71
    )
    embed.add_field(name="📦 ESTOQUE DISPONÍVEL", value=f"🔫 PT: **{fmt_num(estoque['PT'])}** pacotes\n🔫 SUB: **{fmt_num(estoque['SUB'])}** pacotes", inline=False)
    embed.set_footer(text=f"🔄 Atualizado em {agora().strftime('%d/%m/%Y %H:%M:%S')}")
    view = CalculadoraView()
    await enviar_ou_atualizar_painel("painel_vendas", CANAL_VENDAS_ID, embed, view)

# ---------------------------------------------------------
# ASYNC: restaurar_botoes_vendas
# ---------------------------------------------------------

async def restaurar_botoes_vendas():
    try:
        canal = bot.get_channel(CANAL_ENCOMENDAS_ID)
        if not canal:
            logger.error("❌ Canal de encomendas não encontrado para restaurar botões!")
            return

        contador = 0

        async for msg in canal.history(limit=500):
            if msg.author == bot.user and msg.embeds and len(msg.embeds) > 0:
                titulo = msg.embeds[0].title if msg.embeds[0].title else ""
                if "ENTREGA" in titulo.upper() or "ENCOMENDA" in titulo.upper():
                    if not msg.components:
                        entrega_id = None
                        if msg.embeds[0].footer:
                            texto_footer = msg.embeds[0].footer.text
                            if "ID:" in texto_footer:
                                try:
                                    parte_id = texto_footer.split("ID:")[1].strip().split(" ")[0]
                                    entrega_id = safe_int(parte_id)
                                except:
                                    pass

                        total_entregas = 1
                        entrega_atual = 1

                        if msg.embeds[0].description:
                            if "entregas no total" in msg.embeds[0].description:
                                try:
                                    total_entregas = safe_int(msg.embeds[0].description.split("tem")[1].split("entregas")[0].strip())
                                except:
                                    pass
                            if "ENTREGA" in titulo:
                                try:
                                    parte = titulo.split("ENTREGA")[1].strip().split("/")[0].strip()
                                    entrega_atual = safe_int(parte)
                                except:
                                    pass

                        ja_concluida = False
                        for field in msg.embeds[0].fields:
                            if field.name == "📌 Status" and "CONCLUÍDA" in field.value:
                                ja_concluida = True
                                break

                        if ja_concluida:
                            continue

                        view = StatusView(
                            entrega_id=entrega_id,
                            total_entregas=total_entregas,
                            entrega_atual=entrega_atual
                        )

                        await safe_request(msg.edit, view=view)
                        contador += 1
                        await asyncio.sleep(1.0)

        logger.info(f"✅ {contador} mensagens de venda restauradas com botões!")
    except Exception as e:
        logger.error(f"❌ Erro ao restaurar botões de vendas: {e}")

# ---------------------------------------------------------
# ASYNC: recriar_mensagens_vendas
# ---------------------------------------------------------

async def recriar_mensagens_vendas():
    try:
        canal = bot.get_channel(CANAL_ENCOMENDAS_ID)
        if not canal:
            logger.error("❌ Canal de encomendas não encontrado!")
            return
        contador = 0
        async for msg in canal.history(limit=500):
            if msg.author == bot.user and msg.embeds and len(msg.embeds) > 0:
                titulo = msg.embeds[0].title if msg.embeds[0].title else ""
                if "ENTREGA" in titulo.upper() or "ENCOMENDA" in titulo.upper():
                    try:
                        embed = msg.embeds[0]
                        entrega_id = None
                        if embed.footer:
                            texto_footer = embed.footer.text
                            if "ID:" in texto_footer:
                                try:
                                    parte_id = texto_footer.split("ID:")[1].strip().split(" ")[0]
                                    entrega_id = safe_int(parte_id)
                                except:
                                    pass
                        total_entregas = 1
                        if embed.description:
                            if "entregas no total" in embed.description:
                                try:
                                    total_entregas = safe_int(embed.description.split("tem")[1].split("entregas")[0].strip())
                                except:
                                    pass
                        ja_concluida = False
                        for field in embed.fields:
                            if field.name == "📌 Status" and "CONCLUÍDA" in field.value:
                                ja_concluida = True
                                break
                        if ja_concluida:
                            continue
                        view = StatusView(entrega_id=entrega_id, total_entregas=total_entregas)
                        await safe_request(msg.edit, view=view)
                        contador += 1
                        await asyncio.sleep(1.0)
                    except Exception as e:
                        logger.error(f"❌ Erro ao recriar mensagem {msg.id}: {e}")
        logger.info(f"✅ {contador} mensagens de venda recriadas com botões fixos!")
    except Exception as e:
        logger.error(f"❌ Erro ao recriar mensagens de vendas: {e}")

# =========================================================
# CLASS: StatusView
# =========================================================

class StatusView(discord.ui.View):
    def __init__(self, disabled: bool = False, entrega_id: int = None, total_entregas: int = 1, entrega_atual: int = 1, pago_ja_clicado: bool = False, mensagem_original: discord.Message = None):
        super().__init__(timeout=None)
        self.entrega_id = entrega_id
        self.total_entregas = total_entregas
        self.entrega_atual = entrega_atual
        self.entrega_ja_entregue = False
        self.pago_ja_clicado = pago_ja_clicado
        self.mensagem_original = mensagem_original
        self.entrega_criada = False

        is_venda_unica = (total_entregas == 1)

        if is_venda_unica:
            entregue_disabled = False
        else:
            entregue_disabled = False

        self.add_item(discord.ui.Button(
            label="💰 Pago",
            style=discord.ButtonStyle.primary,
            custom_id="status_pago_fixo",
            emoji="💰",
            disabled=self.pago_ja_clicado or disabled
        ))

        self.add_item(discord.ui.Button(
            label="✅ Entregue",
            style=discord.ButtonStyle.success,
            custom_id="status_entregue_fixo",
            emoji="✅",
            disabled=entregue_disabled or disabled
        ))

        self.add_item(discord.ui.Button(
            label="✏️ Editar Venda",
            style=discord.ButtonStyle.primary,
            custom_id="editar_venda_fixo",
            emoji="✏️",
            disabled=disabled
        ))

        self.add_item(discord.ui.Button(
            label="❌ Pedido cancelado",
            style=discord.ButtonStyle.danger,
            custom_id="status_cancelado_fixo",
            emoji="❌",
            disabled=disabled
        ))

        if disabled:
            for item in self.children:
                item.disabled = True

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        custom_id = interaction.data.get("custom_id", "")

        if custom_id == "status_pago_fixo":
            if self.pago_ja_clicado:
                await interaction.response.send_message("⚠️ Este pedido já foi marcado como pago!", ephemeral=True)
                return False
            await interaction.response.defer()
            await self.pago(interaction, None)
            return False
        elif custom_id == "status_entregue_fixo":
            await interaction.response.defer()
            await self.entregue(interaction, None)
            return False
        elif custom_id == "editar_venda_fixo":
            await self.editar_venda(interaction, None)
            return False
        elif custom_id == "status_cancelado_fixo":
            await interaction.response.defer()
            await self.cancelado(interaction, None)
            return False
        return True

    def get_status(self, embed):
        for i, field in enumerate(embed.fields):
            if field.name == "📌 Status":
                return i, field.value.split("\n")
        return None, []

    def set_status(self, embed, idx, linhas):
        if not linhas:
            linhas = ["📦 A entregar"]
        embed.set_field_at(idx, name="📌 Status", value="\n".join(linhas), inline=False)
        return embed

    def pedido_pago(self, linhas):
        return any(l.startswith("💰") for l in linhas)

    def pedido_cancelado(self, linhas):
        return any(l.startswith("❌") for l in linhas)

    def entrega_ja_foi_entregue(self, linhas):
        return any(l.startswith("✅") for l in linhas)

    def extrair_dados_venda(self, embed):
        dados = {
            "pt": 0,
            "sub": 0,
            "organizacao": "Desconhecida",
            "vendedor": "",
            "observacoes": ""
        }
        
        for field in embed.fields:
            if field.name == "🔫 PT":
                try:
                    dados["pt"] = int(field.value.split(" munições")[0].replace(".", "").replace(",", ""))
                except:
                    pass
            if field.name == "🔫 SUB":
                try:
                    dados["sub"] = int(field.value.split(" munições")[0].replace(".", "").replace(",", ""))
                except:
                    pass
            if field.name == "🏷 Organização":
                dados["organizacao"] = field.value.strip()
            if field.name == "👤 Vendedor":
                dados["vendedor"] = field.value.strip()
            if field.name == "📝 Observações":
                dados["observacoes"] = field.value.strip()
        
        return dados

    async def pago(self, interaction: discord.Interaction, button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        if self.pedido_cancelado(linhas):
            await interaction.followup.send("⚠️ Este pedido foi cancelado.", ephemeral=True)
            return

        if self.pedido_pago(linhas):
            await interaction.followup.send("⚠️ Este pedido já foi pago.", ephemeral=True)
            return

        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        user = interaction.user.mention

        linhas = [l for l in linhas if not l.startswith("⏳")]
        linhas = [l for l in linhas if not l.startswith("💰")]
        linhas.append(f"💰 Pago • Recebido por {user} • {agora_str}")

        embed = self.set_status(embed, idx, linhas)

        finalizado = any(l.startswith("💰") for l in linhas) and any(l.startswith("✅") for l in linhas)

        if finalizado:
            embed.color = 0x2ecc71
            embed.title = "🎉 VENDA CONCLUÍDA"
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed.add_field(name="✅ VENDA FINALIZADA COM SUCESSO", value="💰 **Pagamento recebido**\n📦 **Pedido entregue ao cliente**", inline=False)
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="🔥 **Pedido encerrado no sistema**", inline=False)
            await interaction.message.edit(embed=embed, view=StatusView(
                disabled=True,
                entrega_id=self.entrega_id,
                total_entregas=self.total_entregas,
                entrega_atual=self.entrega_atual,
                pago_ja_clicado=True,
                mensagem_original=interaction.message
            ))
        else:
            nova_view = StatusView(
                entrega_id=self.entrega_id,
                total_entregas=self.total_entregas,
                entrega_atual=self.entrega_atual,
                pago_ja_clicado=True,
                mensagem_original=interaction.message
            )
            await interaction.message.edit(embed=embed, view=nova_view)

    async def entregue(self, interaction: discord.Interaction, button):
        if self.entrega_ja_entregue:
            await interaction.followup.send("⚠️ **Esta entrega já foi marcada como entregue!**", ephemeral=True)
            return

        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        if self.pedido_cancelado(linhas):
            await interaction.followup.send("⚠️ Este pedido foi cancelado.", ephemeral=True)
            return

        if self.entrega_ja_foi_entregue(linhas):
            await interaction.followup.send("⚠️ **Esta entrega já foi entregue!**", ephemeral=True)
            return

        pacotes_pt = 0
        pacotes_sub = 0
        for field in embed.fields:
            if field.name == "🔫 PT":
                try:
                    linhas_field = field.value.split("\n")
                    for l in linhas_field:
                        if "📦" in l:
                            pacotes_pt = safe_int(l.replace("📦", "").replace("pacotes", "").strip())
                except:
                    pass
            if field.name == "🔫 SUB":
                try:
                    linhas_field = field.value.split("\n")
                    for l in linhas_field:
                        if "📦" in l:
                            pacotes_sub = safe_int(l.replace("📦", "").replace("pacotes", "").strip())
                except:
                    pass

        if pacotes_pt > 0:
            estoque_suficiente = await verificar_estoque_suficiente("PT", pacotes_pt)
            if not estoque_suficiente:
                estoque_atual = await carregar_estoque()
                await interaction.followup.send(
                    f"❌ **ESTOQUE INSUFICIENTE!**\n\n"
                    f"🔫 PT: {pacotes_pt} pacotes necessários\n"
                    f"📦 Estoque atual: {estoque_atual['PT']} pacotes",
                    ephemeral=True
                )
                return

        if pacotes_sub > 0:
            estoque_suficiente = await verificar_estoque_suficiente("SUB", pacotes_sub)
            if not estoque_suficiente:
                estoque_atual = await carregar_estoque()
                await interaction.followup.send(
                    f"❌ **ESTOQUE INSUFICIENTE!**\n\n"
                    f"🔫 SUB: {pacotes_sub} pacotes necessários\n"
                    f"📦 Estoque atual: {estoque_atual['SUB']} pacotes",
                    ephemeral=True
                )
                return

        self.entrega_ja_entregue = True

        titulo = embed.title
        pedido_numero = safe_int(titulo.split("#")[1]) if "#" in titulo else 0

        if pacotes_pt > 0:
            await registrar_saida_estoque(pedido_numero, "PT", pacotes_pt, interaction.user.id)
        if pacotes_sub > 0:
            await registrar_saida_estoque(pedido_numero, "SUB", pacotes_sub, interaction.user.id)

        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        user = interaction.user

        linhas = [l for l in linhas if not l.startswith("📦")]
        linhas = [l for l in linhas if not l.startswith("✅")]
        linhas.append(f"✅ Entregue por {user.mention} • {agora_str}")

        embed = self.set_status(embed, idx, linhas)

        finalizado = any(l.startswith("💰") for l in linhas) and any(l.startswith("✅") for l in linhas)

        is_ultima_entrega = (self.entrega_atual == self.total_entregas)

        if finalizado:
            embed.color = 0x2ecc71
            embed.title = "🎉 VENDA CONCLUÍDA"
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed.add_field(name="✅ VENDA FINALIZADA COM SUCESSO", value="💰 **Pagamento recebido**\n📦 **Pedido entregue ao cliente**\n📊 **Estoque atualizado**", inline=False)
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="🔥 **Pedido encerrado no sistema**", inline=False)
            await interaction.message.edit(embed=embed, view=StatusView(
                disabled=True,
                entrega_id=self.entrega_id,
                total_entregas=self.total_entregas,
                entrega_atual=self.entrega_atual,
                pago_ja_clicado=True,
                mensagem_original=interaction.message
            ))
        else:
            nova_view = StatusView(
                entrega_id=self.entrega_id,
                total_entregas=self.total_entregas,
                entrega_atual=self.entrega_atual,
                pago_ja_clicado=self.pago_ja_clicado,
                mensagem_original=interaction.message
            )
            for child in nova_view.children:
                if child.custom_id == "status_entregue_fixo":
                    child.disabled = True
                    break
            await interaction.message.edit(embed=embed, view=nova_view)

        if not is_ultima_entrega and not finalizado:
            await self.criar_proxima_entrega(interaction, embed, pedido_numero)

        if pacotes_pt > 0 or pacotes_sub > 0:
            canal_bau = interaction.guild.get_channel(CANAL_BAU_GALPAO_SUL_ID)
            if canal_bau:
                try:
                    texto = f"📦 **Retirada do Baú**\n\n👤 Retirado por: {interaction.user.mention}\n"
                    if pacotes_pt > 0:
                        texto += f"🔫 PT: {pacotes_pt} pacotes\n"
                    if pacotes_sub > 0:
                        texto += f"🔫 SUB: {pacotes_sub} pacotes"
                    await canal_bau.send(texto)
                except Exception as e:
                    logger.error(f"Erro envio baú: {e}")

        await enviar_painel_vendas()
        await enviar_painel_fabricacao()

    async def criar_proxima_entrega(self, interaction: discord.Interaction, embed_anterior, pedido_original):
        try:
            if not self.entrega_id:
                logger.warning("❌ Sem entrega_id para criar próxima")
                return

            if self.entrega_criada:
                return

            pool = await get_pool()
            if not pool:
                logger.error("❌ Banco de dados indisponível")
                return

            async with pool.acquire() as conn:
                entrega = await conn.fetchrow("SELECT * FROM entregas_parceladas WHERE id = $1 AND ativo = true", self.entrega_id)

            if not entrega:
                logger.error(f"❌ Entrega {self.entrega_id} não encontrada")
                return

            total_entregas = entrega["total_entregas"]
            proxima_entrega_num = self.entrega_atual + 1

            if proxima_entrega_num > total_entregas:
                return

            async with pool.acquire() as conn2:
                detalhes = await conn2.fetchrow("SELECT entregas_json FROM entregas_detalhes WHERE entrega_id = $1", self.entrega_id)

            if detalhes and detalhes["entregas_json"]:
                entregas_lista = json.loads(detalhes["entregas_json"])
            else:
                async with pool.acquire() as conn3:
                    primeira = await conn3.fetchrow("SELECT pt_por_entrega, sub_por_entrega FROM entregas_parceladas WHERE pedido_original = $1 ORDER BY id ASC LIMIT 1", pedido_original)

                pt_por_entrega = primeira["pt_por_entrega"] if primeira else entrega["pt_por_entrega"]
                sub_por_entrega = primeira["sub_por_entrega"] if primeira else entrega["sub_por_entrega"]

                entregas_lista = []
                LIMITE_DIARIO = 8000
                pt_total = pt_por_entrega * total_entregas
                sub_total = sub_por_entrega * total_entregas
                pt_restante = pt_total
                sub_restante = sub_total

                for i in range(total_entregas):
                    entrega_num = i + 1
                    if pt_restante > 0:
                        if entrega_num == total_entregas:
                            pt_valor = pt_restante
                        else:
                            pt_valor = min(LIMITE_DIARIO, pt_restante)
                        pt_restante -= pt_valor
                    else:
                        pt_valor = 0

                    if sub_restante > 0:
                        if entrega_num == total_entregas:
                            sub_valor = sub_restante
                        else:
                            sub_valor = min(LIMITE_DIARIO, sub_restante)
                        sub_restante -= sub_valor
                    else:
                        sub_valor = 0

                    entregas_lista.append({"pt": pt_valor, "sub": sub_valor})

            idx = proxima_entrega_num - 1
            if idx >= len(entregas_lista):
                return

            entrega_data = entregas_lista[idx]
            pt_entrega = entrega_data["pt"]
            sub_entrega = entrega_data["sub"]

            if pt_entrega == 0 and sub_entrega == 0:
                return

            vendedor_id = entrega["vendedor_id"]
            organizacao = entrega["organizacao"]
            observacoes = entrega["observacoes"]
            canal_id = int(entrega["canal_id"])

            canal = bot.get_channel(canal_id)
            if not canal:
                logger.error(f"❌ Canal {canal_id} não encontrado")
                return

            config = ORGANIZACOES_CONFIG.get(organizacao, {"emoji": "🏷️", "cor": 0x1e3a8a})
            grupo = await buscar_grupo_por_organizacao(organizacao)

            is_ultima = (proxima_entrega_num == total_entregas)

            titulo_embed = f"📦 ENTREGA {proxima_entrega_num}/{total_entregas} • Pedido #{pedido_original:04d}"
            descricao = f"**🔴 ATENÇÃO! Esta venda tem {total_entregas} entregas no total!**\n📦 **Esta entrega contém:** PT {fmt_num(pt_entrega)} + SUB {fmt_num(sub_entrega)} munições"

            embed_novo = discord.Embed(title=titulo_embed, description=descricao, color=config["cor"])

            resumo = ""
            for i, e in enumerate(entregas_lista, 1):
                if i < proxima_entrega_num:
                    status = "✅"
                elif i == proxima_entrega_num:
                    status = "🔴"
                else:
                    status = "⏳"
                resumo += f"{status} Entrega {i}/{total_entregas}: PT {fmt_num(e['pt'])} + SUB {fmt_num(e['sub'])} munições\n"

            embed_novo.add_field(name="🚨 RESUMO DAS ENTREGAS", value=resumo, inline=False)
            embed_novo.add_field(name="👤 Vendedor", value=f"<@{vendedor_id}>", inline=False)
            embed_novo.add_field(name="🏷 Organização", value=f"{config['emoji']} {organizacao}", inline=False)

            pacotes_pt = pt_entrega // 50
            pacotes_sub = sub_entrega // 50

            embed_novo.add_field(name="🔫 PT", value=f"{fmt_num(pt_entrega)} munições\n📦 {pacotes_pt} pacotes", inline=True)
            embed_novo.add_field(name="🔫 SUB", value=f"{fmt_num(sub_entrega)} munições\n📦 {pacotes_sub} pacotes", inline=True)

            valor_entrega = (pt_entrega * 50) + (sub_entrega * 90)
            embed_novo.add_field(name="💰 Valor (esta entrega)", value=f"**{formatar_dinheiro(valor_entrega)}**", inline=False)

            embed_novo.add_field(name="📋 STATUS DAS ENTREGAS", value=f"**Total de entregas:** {total_entregas}\n**Entrega atual:** {proxima_entrega_num}/{total_entregas}\n**Próxima entrega:** {'🔒 Aguardando esta ser ENTREGUE' if not is_ultima else '✅ Última entrega'}", inline=False)
            embed_novo.add_field(name="📌 Status", value="📦 A Entregar\n⏳ Pagamento pendente", inline=False)

            if observacoes:
                embed_novo.add_field(name="📝 Observações", value=observacoes, inline=False)

            if grupo:
                embed_novo.add_field(name="📊 INTEGRAÇÃO COM GRUPO", value=f"✅ Compra registrada automaticamente no grupo **{organizacao}**", inline=False)

            embed_novo.set_footer(text=f"🛡 Sistema de Encomendas • VDR 442 • Entrega {proxima_entrega_num}/{total_entregas} • ID: {self.entrega_id}")

            view_novo = StatusView(
                entrega_id=self.entrega_id,
                total_entregas=total_entregas,
                entrega_atual=proxima_entrega_num,
                pago_ja_clicado=False,
                mensagem_original=None
            )

            msg = await safe_request(canal.send, embed=embed_novo, view=view_novo)

            if msg:
                await atualizar_entrega_parcelada(self.entrega_id, proxima_entrega_num, str(msg.id), None)

            self.entrega_criada = True
            await interaction.followup.send(f"✅ **Entrega {proxima_entrega_num}/{total_entregas} criada automaticamente!**", ephemeral=True)

            await enviar_painel_vendas()
            await enviar_painel_fabricacao()

        except Exception as e:
            logger.error(f"❌ Erro ao criar próxima entrega automaticamente: {e}")
            await interaction.followup.send(f"❌ **Erro ao criar próxima entrega:** {str(e)}", ephemeral=True)

    async def editar_venda(self, interaction: discord.Interaction, button):
        embed = interaction.message.embeds[0]
        dados = self.extrair_dados_venda(embed)
        
        modal = EditarVendaModal(interaction.message)
        modal.qtd_pt.default = str(dados["pt"])
        modal.qtd_sub.default = str(dados["sub"])
        modal.organizacao.default = dados["organizacao"].replace("🏷️ ", "").strip()
        modal.observacao.default = dados["observacoes"]
        
        await interaction.response.send_modal(modal)

    async def cancelado(self, interaction: discord.Interaction, button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        pacotes_pt = 0
        pacotes_sub = 0
        
        for field in embed.fields:
            if field.name == "🔫 PT":
                try:
                    linhas_field = field.value.split("\n")
                    for l in linhas_field:
                        if "📦" in l:
                            pacotes_pt = safe_int(l.replace("📦", "").replace("pacotes", "").strip())
                except:
                    pass
            if field.name == "🔫 SUB":
                try:
                    linhas_field = field.value.split("\n")
                    for l in linhas_field:
                        if "📦" in l:
                            pacotes_sub = safe_int(l.replace("📦", "").replace("pacotes", "").strip())
                except:
                    pass

        titulo = embed.title
        pedido_numero = safe_int(titulo.split("#")[1]) if "#" in titulo else 0
        status_anterior = ""

        if self.entrega_ja_foi_entregue(linhas) or self.pedido_pago(linhas):
            if pacotes_pt > 0:
                await atualizar_estoque("PT", pacotes_pt, "adicionar")
                logger.info(f"🔄 Estoque PT reabastecido: +{pacotes_pt} pacotes (Pedido #{pedido_numero})")
            
            if pacotes_sub > 0:
                await atualizar_estoque("SUB", pacotes_sub, "adicionar")
                logger.info(f"🔄 Estoque SUB reabastecido: +{pacotes_sub} pacotes (Pedido #{pedido_numero})")

            if self.entrega_ja_foi_entregue(linhas) and self.pedido_pago(linhas):
                status_anterior = "Pago e Entregue"
            elif self.pedido_pago(linhas):
                status_anterior = "Pago"
            elif self.entrega_ja_foi_entregue(linhas):
                status_anterior = "Entregue"

        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        user = interaction.user.mention

        canal_bau = interaction.guild.get_channel(CANAL_BAU_GALPAO_ID)
        if canal_bau:
            try:
                embed_bau = discord.Embed(
                    title="🔄 PEDIDO CANCELADO - REVERSÃO DE ESTOQUE",
                    color=0xe74c3c,
                    timestamp=agora()
                )
                embed_bau.add_field(name="📦 Pedido", value=f"#{pedido_numero:04d}", inline=True)
                embed_bau.add_field(name="👤 Cancelado por", value=interaction.user.mention, inline=True)
                if status_anterior:
                    embed_bau.add_field(name="📌 Status anterior", value=status_anterior, inline=True)
                if pacotes_pt > 0:
                    embed_bau.add_field(name="🔫 PT reabastecido", value=f"+{pacotes_pt} pacotes", inline=True)
                if pacotes_sub > 0:
                    embed_bau.add_field(name="🔫 SUB reabastecido", value=f"+{pacotes_sub} pacotes", inline=True)
                if not pacotes_pt and not pacotes_sub:
                    embed_bau.add_field(name="📌 Observação", value="Nenhum estoque foi retirado ainda.", inline=False)
                embed_bau.set_footer(text=f"Cancelado em {agora_str}")
                await canal_bau.send(embed=embed_bau)
            except Exception as e:
                logger.error(f"Erro envio baú reversão: {e}")

        linhas = [f"❌ Pedido cancelado por {user} • {agora_str}"]
        
        if status_anterior:
            linhas.append(f"🔄 **ESTOQUE REVERTIDO** ({status_anterior})")

        embed = self.set_status(embed, idx, linhas)
        
        await interaction.message.edit(embed=embed, view=StatusView(
            disabled=True,
            entrega_id=self.entrega_id,
            total_entregas=self.total_entregas,
            entrega_atual=self.entrega_atual,
            pago_ja_clicado=self.pago_ja_clicado,
            mensagem_original=interaction.message
        ))

        if self.entrega_id:
            await finalizar_entregas(self.entrega_id)

        await enviar_painel_vendas()
        await enviar_painel_fabricacao()

# =========================================================
# CLASS: VendaModal
# =========================================================

class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(label="🏷️ Organização", placeholder="Digite o nome da organização (ex: VDR, POLICIA)", required=True)
    qtd_pt = discord.ui.TextInput(label="🔫 Quantidade PT", placeholder="Digite a quantidade de munição PT (ex: 24000)", required=True)
    qtd_sub = discord.ui.TextInput(label="🔫 Quantidade SUB", placeholder="Digite a quantidade de munição SUB (ex: 16000)", required=True)
    total_entregas = discord.ui.TextInput(label="📦 Número de entregas", placeholder="Ex: 2, 3, 4... (padrão: 1)", required=False)
    observacoes = discord.ui.TextInput(label="📝 Observações", style=discord.TextStyle.paragraph, required=False)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        try:
            pt = safe_int(self.qtd_pt.value)
            sub = safe_int(self.qtd_sub.value)
            if pt < 0 or sub < 0:
                raise ValueError
            if pt == 0 and sub == 0:
                await interaction.followup.send("❌ Você precisa informar pelo menos PT ou SUB!", ephemeral=True)
                return
        except ValueError:
            await interaction.followup.send("❌ Valores inválidos.", ephemeral=True)
            return

        try:
            total_entregas = safe_int(self.total_entregas.value)
            if total_entregas < 1:
                total_entregas = 1
        except:
            total_entregas = 1

        org_nome = self.organizacao.value.strip().upper()
        config = ORGANIZACOES_CONFIG.get(org_nome, {"emoji": "🏷️", "cor": 0x1e3a8a})
        numero_pedido = await proximo_pedido()

        LIMITE_DIARIO = 8000

        if pt == 0:
            entregas_pt = 0
        else:
            entregas_pt = (pt + LIMITE_DIARIO - 1) // LIMITE_DIARIO

        if sub == 0:
            entregas_sub = 0
        else:
            entregas_sub = (sub + LIMITE_DIARIO - 1) // LIMITE_DIARIO

        num_entregas = max(entregas_pt, entregas_sub)
        if num_entregas == 0:
            num_entregas = 1

        if total_entregas > num_entregas:
            num_entregas = total_entregas

        entregas_lista = []
        pt_restante = pt
        sub_restante = sub

        for i in range(num_entregas):
            entrega_num = i + 1

            if pt_restante > 0:
                if entrega_num == num_entregas:
                    pt_entrega = pt_restante
                else:
                    pt_entrega = min(LIMITE_DIARIO, pt_restante)
                pt_restante -= pt_entrega
            else:
                pt_entrega = 0

            if sub_restante > 0:
                if entrega_num == num_entregas:
                    sub_entrega = sub_restante
                else:
                    sub_entrega = min(LIMITE_DIARIO, sub_restante)
                sub_restante -= sub_entrega
            else:
                sub_entrega = 0

            entregas_lista.append({"pt": pt_entrega, "sub": sub_entrega})

        entregas_json = json.dumps(entregas_lista)

        pacotes_pt_total = pt // 50
        pacotes_sub_total = sub // 50
        total = (pt * 50) + (sub * 90)

        await salvar_venda_db(str(interaction.user.id), total, numero_pedido)

        grupo = await buscar_grupo_por_organizacao(org_nome)
        if grupo:
            if pacotes_pt_total > 0:
                await registrar_compra_grupo_db(grupo["grupo_id"], "PT", pacotes_pt_total, pacotes_pt_total * 50)
            if pacotes_sub_total > 0:
                await registrar_compra_grupo_db(grupo["grupo_id"], "SUB", pacotes_sub_total, pacotes_sub_total * 90)
            await recriar_painel_grupos()

        if num_entregas > 1:
            primeira_entrega = entregas_lista[0]
            entrega_id = await salvar_entrega_parcelada(
                pedido_original=numero_pedido,
                total_entregas=num_entregas,
                pt_por_entrega=primeira_entrega["pt"],
                sub_por_entrega=primeira_entrega["sub"],
                vendedor_id=str(interaction.user.id),
                organizacao=org_nome,
                observacoes=self.observacoes.value,
                canal_id=str(CANAL_ENCOMENDAS_ID)
            )

            if entrega_id:
                await salvar_entrega_detalhes(entrega_id, entregas_json)

                primeira = entregas_lista[0]

                await criar_embed_entrega(
                    interaction=interaction,
                    pedido_numero=numero_pedido,
                    entrega_atual=1,
                    total_entregas=num_entregas,
                    pt=primeira["pt"],
                    sub=primeira["sub"],
                    org_nome=org_nome,
                    config=config,
                    observacoes=self.observacoes.value,
                    entrega_id=entrega_id,
                    vendedor_id=str(interaction.user.id),
                    grupo=grupo,
                    entregas_lista=entregas_lista
                )

            resumo_entregas = ""
            for i, e in enumerate(entregas_lista, 1):
                resumo_entregas += f"• Entrega {i}/{num_entregas}: PT {fmt_num(e['pt'])} + SUB {fmt_num(e['sub'])} munições\n"

            msg_resposta = f"✅ **Venda parcelada registrada!**\n\n📦 **Pedido #{numero_pedido:04d}**\n🏷 **Organização:** {org_nome}\n📦 **Total PT:** {fmt_num(pt)} munições\n📦 **Total SUB:** {fmt_num(sub)} munições\n💰 **Total:** {formatar_dinheiro(total)}\n\n📋 **Entregas ({num_entregas} no total):**\n{resumo_entregas}\n✅ **Entrega 1/{num_entregas} criada!**\n⚠️ **O botão ENTREGUE só será liberado após criar a próxima entrega.**"

            if grupo:
                msg_resposta += f"\n📊 **Grupo integrado:** ✅ {org_nome}"

            await interaction.followup.send(msg_resposta, ephemeral=True)

        else:
            await criar_embed_entrega(
                interaction=interaction,
                pedido_numero=numero_pedido,
                entrega_atual=1,
                total_entregas=1,
                pt=pt,
                sub=sub,
                org_nome=org_nome,
                config=config,
                observacoes=self.observacoes.value,
                entrega_id=None,
                vendedor_id=str(interaction.user.id),
                grupo=grupo,
                entregas_lista=None
            )

            msg_resposta = f"✅ **Venda registrada!**\n\n📦 **Pedido #{numero_pedido:04d}**\n🏷 **Organização:** {org_nome}\n🔫 **PT:** {fmt_num(pt)} munições\n🔫 **SUB:** {fmt_num(sub)} munições\n💰 **Total:** {formatar_dinheiro(total)}"

            if grupo:
                msg_resposta += f"\n📊 **Grupo integrado:** ✅ {org_nome}"

            await interaction.followup.send(msg_resposta, ephemeral=True)

        await enviar_painel_vendas()
        await enviar_painel_fabricacao()

# =========================================================
# CLASS: EditarVendaModal
# =========================================================

class EditarVendaModal(discord.ui.Modal, title="✏️ Editar Venda"):
    def __init__(self, message):
        super().__init__(timeout=300)
        self.message = message
    
    qtd_pt = discord.ui.TextInput(
        label="🔫 Nova Quantidade PT",
        placeholder="Digite a nova quantidade de PT (deixe em branco para manter)",
        required=False,
        max_length=15
    )
    qtd_sub = discord.ui.TextInput(
        label="🔫 Nova Quantidade SUB",
        placeholder="Digite a nova quantidade de SUB (deixe em branco para manter)",
        required=False,
        max_length=15
    )
    organizacao = discord.ui.TextInput(
        label="🏷️ Nova Organização",
        placeholder="Digite a nova organização (deixe em branco para manter)",
        required=False,
        max_length=50
    )
    observacao = discord.ui.TextInput(
        label="📝 Nova Observação",
        placeholder="Digite a nova observação (deixe em branco para manter)",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=500
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        embed = self.message.embeds[0]
        
        pt_atual = 0
        sub_atual = 0
        organizacao_atual = ""
        observacao_atual = ""
        
        for field in embed.fields:
            if field.name == "🔫 PT":
                try:
                    pt_atual = int(field.value.split(" munições")[0].replace(".", "").replace(",", ""))
                except:
                    pass
            if field.name == "🔫 SUB":
                try:
                    sub_atual = int(field.value.split(" munições")[0].replace(".", "").replace(",", ""))
                except:
                    pass
            if field.name == "🏷 Organização":
                organizacao_atual = field.value.replace("🏷️ ", "").strip()
            if field.name == "📝 Observações":
                observacao_atual = field.value.strip()
        
        nova_pt = safe_int(self.qtd_pt.value) if self.qtd_pt.value else pt_atual
        nova_sub = safe_int(self.qtd_sub.value) if self.qtd_sub.value else sub_atual
        nova_organizacao = self.organizacao.value.strip() if self.organizacao.value else organizacao_atual
        nova_observacao = self.observacao.value.strip() if self.observacao.value else observacao_atual
        
        if nova_pt < 0 or nova_sub < 0:
            await interaction.followup.send("❌ Valores não podem ser negativos!", ephemeral=True)
            return
        
        if nova_pt == 0 and nova_sub == 0:
            await interaction.followup.send("❌ Pelo menos PT ou SUB deve ser maior que 0!", ephemeral=True)
            return
        
        pacotes_pt = nova_pt // 50
        pacotes_sub = nova_sub // 50
        total = (nova_pt * 50) + (nova_sub * 90)
        valor_formatado = formatar_dinheiro(total)
        
        for i, field in enumerate(embed.fields):
            if field.name == "🔫 PT":
                embed.set_field_at(i, name="🔫 PT", value=f"{fmt_num(nova_pt)} munições\n📦 {pacotes_pt} pacotes", inline=True)
            elif field.name == "🔫 SUB":
                embed.set_field_at(i, name="🔫 SUB", value=f"{fmt_num(nova_sub)} munições\n📦 {pacotes_sub} pacotes", inline=True)
            elif field.name == "💰 Valor (esta entrega)":
                embed.set_field_at(i, name="💰 Valor (esta entrega)", value=f"**{valor_formatado}**", inline=False)
            elif field.name == "🏷 Organização":
                embed.set_field_at(i, name="🏷 Organização", value=nova_organizacao, inline=False)
            elif field.name == "📝 Observações":
                if nova_observacao:
                    embed.set_field_at(i, name="📝 Observações", value=nova_observacao, inline=False)
                else:
                    embed.remove_field(i)
        
        if nova_observacao and not any(field.name == "📝 Observações" for field in embed.fields):
            embed.add_field(name="📝 Observações", value=nova_observacao, inline=False)
        
        titulo = embed.title
        pedido_numero = safe_int(titulo.split("#")[1]) if "#" in titulo else 0
        if pedido_numero > 0:
            await atualizar_valor_venda_db(pedido_numero, total)
        
        await self.message.edit(embed=embed)
        
        embed_confirmacao = discord.Embed(
            title="✅ VENDA EDITADA!",
            description=f"📦 **Pedido #{pedido_numero:04d}**",
            color=0x2ecc71
        )
        embed_confirmacao.add_field(name="🔫 PT", value=f"{fmt_num(nova_pt)} munições ({pacotes_pt} pacotes)", inline=True)
        embed_confirmacao.add_field(name="🔫 SUB", value=f"{fmt_num(nova_sub)} munições ({pacotes_sub} pacotes)", inline=True)
        embed_confirmacao.add_field(name="💰 Total", value=valor_formatado, inline=True)
        embed_confirmacao.add_field(name="🏷️ Organização", value=nova_organizacao, inline=False)
        if nova_observacao:
            embed_confirmacao.add_field(name="📝 Observação", value=nova_observacao, inline=False)
        
        await interaction.followup.send(embed=embed_confirmacao, ephemeral=True)
        
        await enviar_painel_vendas()
        await enviar_painel_fabricacao()

# =========================================================
# CLASS: CalculadoraView
# =========================================================

class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Registrar Venda", style=discord.ButtonStyle.primary, custom_id="calc_registrar_venda")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(VendaModal())

    @discord.ui.button(label="Relatório", style=discord.ButtonStyle.success, custom_id="calc_relatorio_vendas")
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RelatorioModal())

    @discord.ui.button(label="🔄 Atualizar Estoque", style=discord.ButtonStyle.secondary, custom_id="calc_atualizar_estoque", emoji="🔄")
    async def atualizar_estoque(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await enviar_painel_vendas()
        await interaction.followup.send("✅ Estoque atualizado!", ephemeral=True)

# =========================================================
# CLASS: RelatorioModal
# =========================================================

class RelatorioModal(discord.ui.Modal, title="📊 Relatório de Vendas"):
    data_inicio = discord.ui.TextInput(label="Data inicial", placeholder="Ex: 01/03/2026")
    data_fim = discord.ui.TextInput(label="Data final", placeholder="Ex: 17/03/2026")

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            inicio = datetime.strptime(self.data_inicio.value, "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value, "%d/%m/%Y")
            fim = fim + timedelta(days=1)
        except Exception:
            await interaction.followup.send("Formato inválido.", ephemeral=True)
            return
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("Banco de dados indisponível.", ephemeral=True)
            return
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id, SUM(valor) as total FROM vendas WHERE TO_DATE(data, 'DD/MM/YYYY') BETWEEN $1 AND $2 GROUP BY user_id", inicio, fim)
        if not rows:
            await interaction.followup.send("Nenhuma venda no período.", ephemeral=True)
            return
        total = 0
        linhas = []
        for r in rows:
            valor = r["total"]
            total += valor
            linhas.append(f"👤 <@{r['user_id']}> • {formatar_dinheiro(valor)}")
        embed = discord.Embed(title="📊 Relatório de Vendas", color=0x2ecc71)
        embed.add_field(name="💰 Total Vendido", value=formatar_dinheiro(total), inline=False)
        embed.add_field(name="👥 Por vendedor", value="\n".join(linhas), inline=False)
        canal = interaction.guild.get_channel(1365372467723501723)
        if canal:
            await canal.send(embed=embed)
        await interaction.followup.send("Relatório enviado.", ephemeral=True)

# =========================================================
# ==================== SISTEMA DE AÇÕES ===================
# =========================================================

# ---------------------------------------------------------
# 5.1: CONSTANTES DAS AÇÕES
# ---------------------------------------------------------

ACOES_COMPLEXO = {
    "Joalheria": 5,
    "Banco Fleeca - Rota 68": 4,
    "Banco Fleeca - Chaves": 4,
    "Banco Fleeca - Praia": 4,
    "Banco Fleeca - Shopping": 4,
    "Banco de Paleto": 1,
    "Banco Central Com Refém": 1,
    "Banco Central Sem Refém": 1,
    "Nióbio": 1,
    "Loja de Armas (Ammunation)": None,
    "Loja de Bebidas": None,
    "Loja de Departamento": None,
    "Mergulhador": None,
    "Grapeseed": None,
    "Companhia de Gás": None,
    "Life Invader": None,
    "Aeroporto de Sucata": None,
    "Carro Forte - Açougue": None,
    "Carro Forte - Faculdade": None,
    "Carro Forte - Grove Street": None,
}

ACOES_BAHAMAS = {
    "Banco Bahamas": None,
    "Burgueshot (Bahamas)": None,
    "Refinaria (Bahamas)": None,
    "Lan House - (Bahamas)": None,
    "Lan House - Jersey": None,
    "Lan House - Brooklyn": None,
    "Lan House - Manhattan": None,
}

ACOES_HELICRASH = {
    "🚁 Helicrash (13h)": None,
    "🚁 Helicrash (15h)": None,
    "🚁 Helicrash (22h)": None,
    "🚁 Helicrash (02h)": None,
}

ACOES_SEMANA = {**ACOES_COMPLEXO, **ACOES_BAHAMAS, **ACOES_HELICRASH}

CARGOS_PERMITIDOS_ESCALACAO = [
    CARGO_AGREGADO_ID, CARGO_MEMBRO_ID, CARGO_SOLDADO_ID,
    CARGO_01_ID, CARGO_02_ID, CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID
]

REGRAS_GERAIS_BAHAMAS = """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
📌 **REGRAS GERAIS - BAHAMAS**
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

1️⃣ **Bom senso** é a regra mais importante em ações fechadas ou de rua.

2️⃣ 🚫 **Proibido** uso de drogas ilegais em ações fechadas.

3️⃣ 🚫 **Proibido** uso de capacete em qualquer ação fechada.

4️⃣ 🚫 **Proibido** uso de armas de fogo durante Corridas Clandestinas.

5️⃣ 🚫 **Proibido** usar mais de 1 colete em ações fechadas.

6️⃣ 🚫 **Proibido** movimentação/rotação com qualquer veículo em ações fechadas.

7️⃣ ✅ Liberado o comando `/gg` em ações fechadas e de rua das **00:00 às 12:00**.

8️⃣ 👮 Policiais devem entrar **simultaneamente** no perímetro da ação.

9️⃣ 🚫 **Proibido** uso de gasolina como arma em qualquer ação fechada.

🔟 🚁 Helicóptero policial pode entrar sozinho no perímetro por **2 minutos**.

1️⃣1️⃣ **Disputa de blips:** Apenas 1 pessoa por facção pode puxar a ação.
"""

REGRAS_ACOES = {
    "Loja de Armas (Ammunation)": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 2.",
            "🎯 **Com estande de tiro:** 0 fora.",
            "🎯 **Sem estande de tiro:** 1 fora.",
            "👮 **Máximo de policiais:** 3.",
            "🔫 **Armamento:** Todos de Pistola (exceto Magnum e Ap-Pistol).",
            "🤝 **Negociação:** Obrigatória.",
            "🚫 **Refém:** Proibido."
        ]
    },
    "Loja de Bebidas": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 3.",
            "👮 **Máximo de policiais:** 4.",
            "🔫 **Armamento:** Todos de Pistola (exceto Magnum e Ap-Pistol).",
            "🤝 **Negociação:** Obrigatória.",
            "🚫 **Refém:** Proibido.",
            "📌 **Obs:** Proibido o uso de 2 andares ou mais (teti chão)."
        ]
    },
    "Loja de Departamento": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 4 (máximo de 2 fora).",
            "👮 **Máximo de policiais:** 5.",
            "🚗 **Máximo de veículos:** 1 veículo, 4 rodas ou 2 motos.",
            "🔫 **Armamento:** Todos de Pistola (exceto Glock Rajada).",
            "🤝 **Negociação:** Obrigatória.",
            "👤 **Refém:** Opcional, máximo."
        ]
    },
    "Mergulhador": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 6.",
            "👮 **Máximo de policiais:** 8.",
            "🔫 **Armamento:** Todos de Pistola (exceto Magnum e Ap-Pistol).",
            "⚔️ **Negociação:** Inexistente, ação de confronto direto.",
            "🚫 **Refém:** Proibido.",
            "📌 **Obs:** Proibido o uso de 2 andares ou mais (teti chão)."
        ]
    },
    "Grapeseed": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 6.",
            "👮 **Máximo de policiais:** 7.",
            "🔫 **Armamento:** Todos de Pistola (exceto Magnum e Ap-Pistol).",
            "⚔️ **Negociação:** Inexistente, ação de confronto direto.",
            "🚫 **Refém:** Proibido.",
            "📌 **Obs:** Proibido o uso de 2 andares ou mais (teti chão)."
        ]
    },
    "Companhia de Gás": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 6.",
            "👮 **Máximo de policiais:** 8.",
            "🚗 **Máximo de veículos:** 3.",
            "🔫 **Armamento:** Todos de Pistola (exceto Magnum e Ap-Pistol).",
            "⚔️ **Negociação:** Inexistente, ação de confronto direto.",
            "🚫 **Refém:** Proibido.",
            "📌 1️⃣ Proibido subir em qualquer objeto/lugar durante a ação.",
            "📌 2️⃣ Proibido atirar contra policiais entrando no perímetro.",
            "📌 3️⃣ Todos os participantes devem estar dentro do perímetro para o embate começar."
        ]
    },
    "Life Invader": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 8.",
            "👮 **Máximo de policiais:** 10.",
            "🔫 **Armamento:** Todos de Pistola (exceto Magnum e Ap-Pistol).",
            "⚔️ **Negociação:** Inexistente, ação de confronto direto.",
            "🚫 **Refém:** Proibido.",
            "📌 Proibido subir em qualquer objeto/lugar durante a ação.",
            "📌 Proibido a utilização dos INTERIORES do perímetro (Life Invader, Cozinha/Piscina)."
        ]
    },
    "Aeroporto de Sucata": {
        "regras": [
            "👥 **Máximo de bandidos:** 6.",
            "👮 **Máximo de policiais:** 8.",
            "🔫 **Armamento:** Obrigatório ter 6 pistolas.",
            "⚔️ **Negociação:** Inexistente, ação de confronto direto.",
            "🚫 **Refém:** Proibido."
        ]
    },
    "Carro Forte - Açougue": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 6.",
            "👮 **Máximo de policiais:** 8.",
            "🔫 **Armamento:** Mínimo SMG, obrigatório ter 2 RIFLES.",
            "⚔️ **Negociação:** Inexistente, ação de confronto direto.",
            "🚫 **Refém:** Proibido.",
            "📌 **Obs:** Proibido bugar head-glitch."
        ]
    },
    "Carro Forte - Faculdade": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 6.",
            "👮 **Máximo de policiais:** 8.",
            "🔫 **Armamento:** Mínimo SMG, obrigatório ter 2 RIFLES.",
            "⚔️ **Negociação:** Inexistente, ação de confronto direto.",
            "🚫 **Refém:** Proibido.",
            "📌 **Obs:** Proibido bugar head-glitch."
        ]
    },
    "Carro Forte - Grove Street": {
        "regras": [
            "👮 **Máximo de policiais:** 8.",
            "🔫 **Armamento:** Mínimo SMG, obrigatório ter 2 RIFLES.",
            "⚔️ **Negociação:** Inexistente, ação de confronto direto.",
            "🚫 **Refém:** Proibido.",
            "📌 **Obs:** Proibido o uso de 2 andares ou mais (teti chão).",
            "📌 **Obs:** Helicóptero somente para visual, sem atirador."
        ]
    },
    "Joalheria": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 7 (máximo de 3 fora e 4 dentro).",
            "👮 **Máximo de policiais:** 9.",
            "🚗 **Máximo de veículos:** 3 (em caso de fuga).",
            "🔫 **Armamento:** No mínimo Submetralhadora.",
            "🤝 **Negociação:** Obrigatória.",
            "👤 **Refém:** Opcional, no máximo 3.",
            "📌 Proibido a utilização dos INTERIORES do perímetro (Prefeitura)."
        ]
    },
    "Banco Fleeca - Rota 68": {
        "regras": [
            "👥 **Mínimo de bandidos:** 6 (mínimo de 3 dentro).",
            "👥 **Máximo de bandidos:** 8 (mínimo de 3 dentro).",
            "🚗 **Máximo de veículos:** 3.",
            "👮 **Máximo de policiais:** 9.",
            "🔫 **Armamento:** Mínimo submetralhadora, obrigatório ter 4 Rifles.",
            "🤝 **Negociação:** Obrigatória.",
            "👤 **Refém:** Opcional, no máximo 3.",
            "📌 Na fuga, só é permitido fazer o Fleeca Chaves."
        ]
    },
    "Banco Fleeca - Chaves": {
        "regras": [
            "👥 **Mínimo de bandidos:** 6.",
            "👥 **Máximo de bandidos:** 8.",
            "🚗 **Máximo de veículos:** 3.",
            "👮 **Máximo de policiais:** 9.",
            "🔫 **Armamento:** Mínimo Submetralhadora.",
            "🤝 **Negociação:** Obrigatória.",
            "👤 **Refém:** Opcional, no máximo 3.",
            "📌 Regras de posicionamento: até 3 integrantes em locais altos/acessíveis no prédio e até 3 no interior do resort."
        ]
    },
    "Banco Fleeca - Praia": {
        "regras": [
            "🔫 **Armamento:** Somente Submetralhadora.",
            "📌 Heli drone + teti chão.",
            "📌 Proibido interior da lojinha (cofre).",
            "📌 Proibido veículo dentro do perímetro.",
            "📌 Na casa de madeira fica limitado 3 bandidos.",
            "📌 Polícia não pode marcar saída.",
            "📌 Proibida a fuga."
        ]
    },
    "Banco Fleeca - Shopping": {
        "regras": [
            "🔫 **Armamento:** Mínimo submetralhadora, obrigatório ter 4 Rifles.",
            "📌 Com atirador: máximo 4 bandidos em prédios.",
            "📌 Sem atirador: uso do interior do prédio proibido.",
            "📌 Limite máximo de pessoas no metrô: 3.",
            "📌 Proibida a fuga."
        ]
    },
    "Banco de Paleto": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 10.",
            "👮 **Máximo de policiais:** 12.",
            "🔫 **Armamento:** Todos de Rifle.",
            "⚔️ **Negociação:** Inexistente, ação de confronto direto.",
            "🚫 **Refém:** Proibido.",
            "📌 Os bandidos devem esperar o início da ação.",
            "📌 Ação inicia quando a polícia entrar no perímetro.",
            "📌 Helicóptero só poderá ter o piloto.",
            "📌 Máximo de 6 pessoas dentro do GALINHEIRO."
        ]
    },
    "Banco Central Com Refém": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 10.",
            "👥 **Bandidos fora:** Máximo 3 em prédios ou 5 no chão.",
            "🚗 **Máximo de veículos:** 4.",
            "👮 **Máximo de policiais:** 12.",
            "🔫 **Armamento:** Obrigatório RIFLE.",
            "🤝 **Negociação:** Obrigatória.",
            "👤 **Refém:** Permitido, máximo 4.",
            "📌 Reféns podem ser usados para tirar atiradores ou proibir reposicionamento com helicóptero.",
            "📌 Não pode ser os dois ao mesmo tempo.",
            "📌 Proibido o uso do interior do apartamento em frente ao POSTAL.",
            "📌 Obs: Proibido ter bandidos fora se a ação for na fuga."
        ]
    },
    "Banco Central Sem Refém": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 10.",
            "👥 **Bandidos fora:** Máximo 3 em prédios ou 5 no chão.",
            "🚗 **Máximo de veículos:** 3.",
            "👮 **Máximo de policiais:** 12.",
            "🔫 **Armamento:** Obrigatório RIFLE.",
            "🤝 **Negociação:** Obrigatória.",
            "🚫 **Refém:** Proibido.",
            "📌 Proibido o uso do interior do apartamento em frente ao POSTAL.",
            "📌 Obs: Proibido ter bandidos fora se a ação for na fuga."
        ]
    },
    "Nióbio": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 12 (sem limites fora).",
            "👮 **Máximo de policiais:** 18.",
            "🔫 **Armamento:** Obrigatório RIFLE.",
            "⚔️ **Negociação:** Inexistente.",
            "🚫 **Refém:** Proibido.",
            "📌 Proibido marcar a porta que dá acesso a água.",
            "📌 A parte da água só poderá ser acessada para entrar ou sair do túnel do NIÓBIO.",
            "📌 Limite de 4 bandidos entre o corredor que dá acesso a água e o quadrado do quebrado.",
            "📌 Máximo de 4 bandidos no fundo do nióbio."
        ]
    },
    "🚁 Helicrash (13h)": {
        "regras": [
            "👥 **Máximo de participantes por facção/grupo:** 10.",
            "🚗 **Máximo de veículos por facção/grupo:** 2.",
            "🚫 **Proibido** o roubo de veículos durante o evento.",
            "👕 Todos os membros deverão OBRIGATORIAMENTE utilizar a roupa completa da sua facção/grupo.",
            "👥 Jogadores membros (setados) só poderão participar junto da sua própria facção/grupo.",
            "👤 Jogadores sem set podem formar grupos entre si, mas deverão usar uma roupa igual.",
            "💉 A reanimação é permitida somente após o término completo da ação.",
            "🚫 Proibido a utilização de GRANADEIRA."
        ]
    },
    "🚁 Helicrash (15h)": {
        "regras": [
            "👥 **Máximo de participantes por facção/grupo:** 10.",
            "🚗 **Máximo de veículos por facção/grupo:** 2.",
            "🚫 **Proibido** o roubo de veículos durante o evento.",
            "👕 Todos os membros deverão OBRIGATORIAMENTE utilizar a roupa completa da sua facção/grupo.",
            "👥 Jogadores membros (setados) só poderão participar junto da sua própria facção/grupo.",
            "👤 Jogadores sem set podem formar grupos entre si, mas deverão usar uma roupa igual.",
            "💉 A reanimação é permitida somente após o término completo da ação.",
            "🚫 Proibido a utilização de GRANADEIRA."
        ]
    },
    "🚁 Helicrash (22h)": {
        "regras": [
            "👥 **Máximo de participantes por facção/grupo:** 10.",
            "🚗 **Máximo de veículos por facção/grupo:** 2.",
            "🚫 **Proibido** o roubo de veículos durante o evento.",
            "👕 Todos os membros deverão OBRIGATORIAMENTE utilizar a roupa completa da sua facção/grupo.",
            "👥 Jogadores membros (setados) só poderão participar junto da sua própria facção/grupo.",
            "👤 Jogadores sem set podem formar grupos entre si, mas deverão usar uma roupa igual.",
            "💉 A reanimação é permitida somente após o término completo da ação.",
            "🚫 Proibido a utilização de GRANADEIRA."
        ]
    },
    "🚁 Helicrash (02h)": {
        "regras": [
            "👥 **Máximo de participantes por facção/grupo:** 10.",
            "🚗 **Máximo de veículos por facção/grupo:** 2.",
            "🚫 **Proibido** o roubo de veículos durante o evento.",
            "👕 Todos os membros deverão OBRIGATORIAMENTE utilizar a roupa completa da sua facção/grupo.",
            "👥 Jogadores membros (setados) só poderão participar junto da sua própria facção/grupo.",
            "👤 Jogadores sem set podem formar grupos entre si, mas deverão usar uma roupa igual.",
            "💉 A reanimação é permitida somente após o término completo da ação.",
            "🚫 Proibido a utilização de GRANADEIRA."
        ]
    },
    "Banco Bahamas": {
        "regras": [
            "👥 **Máximo de Bandidos:** 10.",
            "👮 **Máximo de Policiais:** 14.",
            "🔫 **Armamento:** Obrigatório RIFLE.",
            "🤝 **Negociação:** Obrigatória.",
            "👤 **Refém:** Opcional.",
            "📌 Proibido a utilização das estações de METRO (Subterraneo).",
            "📌 Limite de 6 pessoas no Salão.",
            "📌 Máximo de 4 bandidos na parte de baixo do Banco."
        ],
        "is_bahamas": True
    },
    "Burgueshot (Bahamas)": {
        "regras": [
            "👥 **Mínimo de Bandidos:** 3.",
            "👥 **Máximo de Bandidos:** 5.",
            "👮 **Máximo de Policiais:** 5.",
            "🔫 **Armamento:** Mínimo pistola.",
            "🤝 **Negociação:** Obrigatória.",
            "👤 **Refém:** Opcional."
        ],
        "is_bahamas": True
    },
    "Refinaria (Bahamas)": {
        "regras": [
            "👥 **Bandidos:** Obrigatório 6.",
            "👮 **Máximo de policiais:** 7.",
            "🔫 **Armamento:** Mínimo SMG.",
            "⚔️ **Negociação:** Inexistente, ação de confronto direto.",
            "🚫 **Refém:** Proibido.",
            "📌 Fica proibido o uso de atirador."
        ],
        "is_bahamas": True
    },
    "Lan House - (Bahamas)": {
        "regras": [
            "👥 **Mínimo de Bandidos:** 6.",
            "👥 **Máximo de Bandidos:** 8.",
            "👮 **Máximo de Policiais:** 10.",
            "🔫 **Armamento:** Mínimo SMG.",
            "🤝 **Negociação:** Obrigatória.",
            "👤 **Refém:** Opcional.",
            "📌 Limite de 4 pessoas dentro da Lan House."
        ],
        "is_bahamas": True
    },
    "Lan House - Jersey": {
        "regras": [
            "👥 **Mínimo de Bandidos:** 6.",
            "👥 **Máximo de Bandidos:** 8.",
            "👮 **Máximo de Policiais:** 10.",
            "🔫 **Armamento:** Mínimo SMG.",
            "🤝 **Negociação:** Obrigatória.",
            "👤 **Refém:** Opcional.",
            "📌 Limite de 4 pessoas dentro da Lan House."
        ],
        "is_bahamas": True
    },
    "Lan House - Brooklyn": {
        "regras": [
            "👥 **Mínimo de Bandidos:** 6.",
            "👥 **Máximo de Bandidos:** 8.",
            "👮 **Máximo de Policiais:** 10.",
            "🔫 **Armamento:** Mínimo SMG.",
            "🤝 **Negociação:** Obrigatória.",
            "👤 **Refém:** Opcional.",
            "📌 Limite de 4 pessoas dentro da Lan House."
        ],
        "is_bahamas": True
    },
    "Lan House - Manhattan": {
        "regras": [
            "👥 **Mínimo de Bandidos:** 6.",
            "👥 **Máximo de Bandidos:** 8.",
            "👮 **Máximo de Policiais:** 10.",
            "🔫 **Armamento:** Mínimo SMG.",
            "🤝 **Negociação:** Obrigatória.",
            "👤 **Refém:** Opcional.",
            "📌 Limite de 4 pessoas dentro da Lan House."
        ],
        "is_bahamas": True
    }
}

# ---------------------------------------------------------
# ASYNC: salvar_acao_db
# ---------------------------------------------------------

async def salvar_acao_db(tipo, autor):
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            return await conn.fetchval(
                "INSERT INTO acoes_semana (tipo, data, autor, status) VALUES ($1, $2, $3, 'aberta') RETURNING id",
                tipo, agora_db(), str(autor)
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar ação: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: buscar_acoes_semana
# ---------------------------------------------------------

async def buscar_acoes_semana():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("""
                SELECT tipo, COUNT(*) as qtd
                FROM acoes_semana
                WHERE status = 'concluida' AND (resultado = 'ganhou' OR resultado = 'perdeu' OR resultado = 'concluida')
                GROUP BY tipo
            """)
    except Exception as e:
        logger.error(f"❌ Erro ao buscar ações: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: participar_acao_db
# ---------------------------------------------------------

async def participar_acao_db(acao_id, user_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO participantes_acoes (acao_id, user_id) VALUES ($1, $2)", acao_id, str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao participar ação: {e}")

# ---------------------------------------------------------
# ASYNC: remover_participante_db
# ---------------------------------------------------------

async def remover_participante_db(acao_id, user_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM participantes_acoes WHERE acao_id = $1 AND user_id = $2", acao_id, str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao remover participante: {e}")

# ---------------------------------------------------------
# ASYNC: buscar_participantes_db
# ---------------------------------------------------------

async def buscar_participantes_db(acao_id):
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id = $1", acao_id)
    except Exception as e:
        logger.error(f"❌ Erro ao buscar participantes: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: buscar_acao_db
# ---------------------------------------------------------

async def buscar_acao_db(acao_id):
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM acoes_semana WHERE id = $1", acao_id)
    except Exception as e:
        logger.error(f"❌ Erro ao buscar ação: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: cancelar_acao_db
# ---------------------------------------------------------

async def cancelar_acao_db(acao_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE acoes_semana SET status='cancelada' WHERE id = $1", acao_id)
    except Exception as e:
        logger.error(f"❌ Erro ao cancelar ação: {e}")

# ---------------------------------------------------------
# ASYNC: concluir_acao_db
# ---------------------------------------------------------

async def concluir_acao_db(acao_id, resultado, valor=0):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE acoes_semana SET status='concluida', resultado=$1, valor=$2 WHERE id=$3",
                resultado, valor, acao_id
            )
    except Exception as e:
        logger.error(f"❌ Erro ao concluir ação: {e}")

# ---------------------------------------------------------
# ASYNC: restaurar_acoes
# ---------------------------------------------------------

async def restaurar_acoes():
    try:
        canal = bot.get_channel(CANAL_ESCALACOES_ID)
        if not canal:
            logger.error("❌ Canal de escalações não encontrado!")
            return
        contador = 0
        async for msg in canal.history(limit=500):
            if msg.author == bot.user and msg.embeds and len(msg.embeds) > 0:
                embed = msg.embeds[0]
                if embed.footer and "ID:" in embed.footer.text:
                    try:
                        acao_id = safe_int(embed.footer.text.split("ID:")[1].strip().split(" ")[0])
                        acao = await buscar_acao_db(acao_id)
                        if not acao or acao["status"] != "aberta":
                            continue
                        criador_id = int(acao["autor"])
                        if not msg.components:
                            view = AcaoViewRestaurada(acao_id, criador_id)
                            await msg.edit(view=view)
                            contador += 1
                            await asyncio.sleep(1.0)
                    except:
                        pass
        logger.info(f"✅ {contador} ações restauradas com botões!")
    except Exception as e:
        logger.error(f"❌ Erro ao restaurar ações: {e}")

# ---------------------------------------------------------
# ASYNC: enviar_painel_acoes
# ---------------------------------------------------------

async def enviar_painel_acoes(guild):
    canal = guild.get_channel(CANAL_ESCALACOES_ID)
    if not canal:
        logger.error("❌ Canal ações não encontrado")
        return
    
    rows = await buscar_acoes_semana()
    feitas = {r["tipo"]: r["qtd"] for r in rows}
    
    def calcular_progresso(acoes_dict):
        linhas = []
        total_feitas = 0
        total_meta = 0
        for nome, limite in acoes_dict.items():
            qtd = feitas.get(nome, 0)
            total_feitas += qtd
            if limite is None:
                linhas.append(f"• {nome}: {qtd}")
            else:
                restante = max(limite - qtd, 0)
                if qtd >= limite:
                    linhas.append(f"• {nome}: ✅ {qtd}/{limite} (COMPLETO)")
                else:
                    linhas.append(f"• {nome}: {qtd}/{limite} (restam {restante})")
                total_meta += limite
        return linhas, total_feitas, total_meta
    
    linhas_complexo, total_feitas_complexo, total_meta_complexo = calcular_progresso(ACOES_COMPLEXO)
    linhas_bahamas, total_feitas_bahamas, total_meta_bahamas = calcular_progresso(ACOES_BAHAMAS)
    linhas_helicrash, total_feitas_helicrash, total_meta_helicrash = calcular_progresso(ACOES_HELICRASH)
    
    total_geral_feitas = total_feitas_complexo + total_feitas_bahamas + total_feitas_helicrash
    total_geral_meta = total_meta_complexo + total_meta_bahamas + total_meta_helicrash
    
    embed = discord.Embed(
        title="📊 AÇÕES DA SEMANA",
        description="**Controle de ações realizadas no período**\n\n"
                    "**🏙️ COMPLEXO**\n" + "\n".join(linhas_complexo) + "\n\n"
                    "**🏝️ BAHAMAS**\n" + "\n".join(linhas_bahamas) + "\n\n"
                    "**🚁 HELICRASH**\n" + "\n".join(linhas_helicrash),
        color=0x2ecc71
    )
    
    if total_geral_meta > 0:
        porcentagem = int((total_geral_feitas / total_geral_meta) * 100) if total_geral_meta > 0 else 0
        barra_progresso = "▓" * (porcentagem // 5) + "░" * (20 - (porcentagem // 5))
        embed.add_field(
            name="📊 PROGRESSO GERAL",
            value=f"{porcentagem}% {barra_progresso}\n{total_geral_feitas}/{total_geral_meta} ações realizadas",
            inline=False
        )
    else:
        embed.add_field(
            name="📊 PROGRESSO GERAL",
            value=f"{total_geral_feitas} ações realizadas (sem limite)",
            inline=False
        )
    
    embed.set_footer(text=f"Atualizado em {agora().strftime('%d/%m/%Y %H:%M')}")
    
    await enviar_ou_atualizar_painel("painel_acoes", CANAL_ESCALACOES_ID, embed, PainelAcoesView())

# =========================================================
# CLASS: SelecionarAcaoView
# =========================================================

class SelecionarAcaoView(discord.ui.View):
    def __init__(self, acoes, titulo, emoji):
        super().__init__(timeout=60)
        options = []
        for nome, limite in acoes.items():
            emoji_acao = "🚁" if "Helicrash" in nome else "🏪"
            if "Bahamas" in nome:
                emoji_acao = "🏝️"
            if "Banco" in nome or "Joalheria" in nome:
                emoji_acao = "🏦"
            if "Carro Forte" in nome:
                emoji_acao = "🚚"
            if limite is not None:
                options.append(discord.SelectOption(label=nome, description=f"Limite: {limite}/semana", emoji=emoji_acao))
            else:
                options.append(discord.SelectOption(label=nome, description="Ilimitado", emoji=emoji_acao))
        self.select = discord.ui.Select(placeholder=f"📋 {titulo}", options=options, max_values=1)
        self.select.callback = self.select_callback
        self.add_item(self.select)
        self.add_item(FecharButton())

    async def select_callback(self, interaction: discord.Interaction):
        acao_tipo = interaction.data["values"][0]
        await interaction.response.defer(ephemeral=True)
        
        limite = ACOES_SEMANA.get(acao_tipo)
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        
        if limite and limite is not None:
            async with pool.acquire() as conn:
                qtd = await conn.fetchval("SELECT COUNT(*) FROM acoes_semana WHERE tipo=$1 AND status='concluida' AND (resultado='ganhou' OR resultado='perdeu')", acao_tipo)
                if qtd >= limite:
                    await interaction.followup.send(f"❌ Ação **{acao_tipo}** já atingiu o limite semanal de **{limite}** vez(es)!", ephemeral=True)
                    return
        
        acao_id = await salvar_acao_db(acao_tipo, interaction.user.id)
        
        regras_data = REGRAS_ACOES.get(acao_tipo, {"regras": ["📌 Regras não definidas para esta ação."]})
        regras = regras_data.get("regras", [])
        is_bahamas = regras_data.get("is_bahamas", False)
        
        cor = 0xe67e22 if "Helicrash" in acao_tipo else 0x3498db
        emoji = "🚁" if "Helicrash" in acao_tipo else "🎯"
        if "Bahamas" in acao_tipo:
            emoji = "🏝️"
            cor = 0x1abc9c
        if "Banco" in acao_tipo:
            emoji = "🏦"
            cor = 0xe74c3c
        if "Carro Forte" in acao_tipo:
            emoji = "🚚"
            cor = 0xf39c12
        
        embed = discord.Embed(
            title=f"{emoji} ESCALAÇÃO - {acao_tipo}",
            color=cor,
            timestamp=agora()
        )
        
        embed.add_field(
            name="📌 REGRAS DA AÇÃO",
            value="\n".join(regras),
            inline=False
        )
        
        if is_bahamas:
            embed.add_field(
                name="🏝️ REGRAS GERAIS - BAHAMAS",
                value=REGRAS_GERAIS_BAHAMAS,
                inline=False
            )
        
        if "Helicrash" in acao_tipo:
            horario = acao_tipo.split("(")[1].replace(")", "")
            embed.add_field(
                name="⏰ HORÁRIO",
                value=f"{horario} (horário de Brasília)",
                inline=False
            )
        
        if limite and limite is not None:
            async with pool.acquire() as conn:
                qtd_feita = await conn.fetchval("SELECT COUNT(*) FROM acoes_semana WHERE tipo=$1 AND status='concluida' AND (resultado='ganhou' OR resultado='perdeu')", acao_tipo)
                embed.add_field(
                    name="📊 LIMITE SEMANAL",
                    value=f"{qtd_feita}/{limite} ações realizadas",
                    inline=False
                )
        
        embed.add_field(
            name="👥 PARTICIPANTES (0)",
            value="Nenhum participante ainda.\nClique no botão ✅ PARTICIPAR para se inscrever!",
            inline=False
        )
        
        embed.add_field(
            name="👤 CRIADO POR",
            value=interaction.user.mention,
            inline=True
        )
        embed.add_field(
            name="📅 DATA",
            value=agora().strftime('%d/%m/%Y %H:%M'),
            inline=True
        )
        
        embed.add_field(
            name="📝 COMO PARTICIPAR",
            value="✅ Clique em **'Participar'** para se inscrever na ação.\n📤 Quando a escalação estiver completa, o criador clica em **'Concluir'**.",
            inline=False
        )
        
        embed.set_footer(text=f"ID: {acao_id}")
        
        canal = interaction.guild.get_channel(CANAL_ESCALACOES_ID)
        if canal:
            view = AcaoView(acao_id, interaction.user.id)
            await canal.send(embed=embed, view=view)
            acoes_ativas[acao_id] = {"embed": embed, "criador_id": interaction.user.id}
            await interaction.followup.send(f"✅ Ação **{acao_tipo}** criada com sucesso!", ephemeral=True)
            try:
                await interaction.message.delete()
            except:
                pass
        else:
            await interaction.followup.send("❌ Canal de escalações não encontrado!", ephemeral=True)

# =========================================================
# CLASS: FecharButton
# =========================================================

class FecharButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="❌ Fechar", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction):
        await interaction.message.delete()

# =========================================================
# CLASS: AcaoView
# =========================================================

class AcaoView(discord.ui.View):
    def __init__(self, acao_id, criador_id):
        super().__init__(timeout=None)
        self.acao_id = acao_id
        self.criador_id = criador_id

    @discord.ui.button(label="✅ Participar", style=discord.ButtonStyle.success, custom_id="acao_participar", emoji="✅")
    async def participar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not any(role.id in CARGOS_PERMITIDOS_ESCALACAO for role in interaction.user.roles):
            await interaction.response.send_message("❌ Você não tem permissão para participar de ações!", ephemeral=True)
            return

        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
            return

        async with pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.response.send_message("❌ Esta ação já foi concluída ou cancelada!", ephemeral=True)
                return

            ja_participa = await conn.fetchval("SELECT 1 FROM participantes_acoes WHERE acao_id=$1 AND user_id=$2", self.acao_id, str(interaction.user.id))
            if ja_participa:
                await interaction.response.send_message("⚠️ Você já está participando!", ephemeral=True)
                return

            await conn.execute("INSERT INTO participantes_acoes (acao_id, user_id) VALUES ($1, $2)", self.acao_id, str(interaction.user.id))
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)

        await self.atualizar_embed(interaction, participantes, acao)
        await interaction.response.send_message(f"✅ Você se inscreveu na ação **{acao['tipo']}**!", ephemeral=True)

    @discord.ui.button(label="❌ Sair", style=discord.ButtonStyle.danger, custom_id="acao_sair", emoji="❌")
    async def sair(self, interaction: discord.Interaction, button: discord.ui.Button):
        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
            return

        async with pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.response.send_message("❌ Esta ação já foi concluída ou cancelada!", ephemeral=True)
                return

            participa = await conn.fetchval("SELECT 1 FROM participantes_acoes WHERE acao_id=$1 AND user_id=$2", self.acao_id, str(interaction.user.id))
            if not participa:
                await interaction.response.send_message("⚠️ Você não está participando desta ação!", ephemeral=True)
                return

            await conn.execute("DELETE FROM participantes_acoes WHERE acao_id = $1 AND user_id = $2", self.acao_id, str(interaction.user.id))
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)

        await self.atualizar_embed(interaction, participantes, acao)
        await interaction.response.send_message(f"✅ Você saiu da ação **{acao['tipo']}**!", ephemeral=True)

    @discord.ui.button(label="🚫 Cancelar Ação", style=discord.ButtonStyle.danger, custom_id="acao_cancelar", emoji="🚫")
    async def cancelar_acao(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_criador = interaction.user.id == self.criador_id
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)

        if not is_criador and not is_gerente:
            await interaction.response.send_message("❌ Apenas o criador ou gerentes podem cancelar a ação!", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return

        async with pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.followup.send("❌ Esta ação já foi concluída ou cancelada!", ephemeral=True)
                return

            await conn.execute("UPDATE acoes_semana SET status='cancelada' WHERE id=$1", self.acao_id)
            acao = await conn.fetchrow("SELECT tipo FROM acoes_semana WHERE id=$1", self.acao_id)

        await interaction.message.delete()
        await interaction.followup.send(f"✅ Ação **{acao['tipo']}** cancelada e removida!", ephemeral=True)
        await enviar_painel_acoes(interaction.guild)

    @discord.ui.button(label="📤 Concluir Escalação", style=discord.ButtonStyle.primary, custom_id="acao_concluir", emoji="📤")
    async def concluir(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_criador = interaction.user.id == self.criador_id
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)

        if not is_criador and not is_gerente:
            await interaction.response.send_message("❌ Apenas o criador ou gerentes podem concluir!", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return

        async with pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.followup.send("❌ Esta ação já foi concluída ou cancelada!", ephemeral=True)
                return

            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            is_helicrash = "Helicrash" in acao["tipo"]

            if not participantes:
                await interaction.followup.send("⚠️ Nenhum participante! Ação cancelada.", ephemeral=True)
                await interaction.message.delete()
                return

            await conn.execute("UPDATE acoes_semana SET status='concluida' WHERE id=$1", self.acao_id)
            if is_helicrash:
                await conn.execute("UPDATE acoes_semana SET resultado='concluida', valor=0 WHERE id=$1", self.acao_id)

        lista_participantes = "\n".join([f"<@{p['user_id']}>" for p in participantes])

        if is_helicrash:
            embed_relatorio = discord.Embed(
                title="🚁 RELATÓRIO DE HELICRASH",
                description=f"**{acao['tipo']}**\n\n✅ Evento registrado com sucesso!",
                color=0xe67e22
            )
            embed_relatorio.add_field(name="🏦 Evento", value=acao["tipo"], inline=False)
            embed_relatorio.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
            embed_relatorio.add_field(name="📅 Data", value=agora().strftime('%d/%m/%Y %H:%M'), inline=False)
            embed_relatorio.set_footer(text=f"ID: {self.acao_id} • Criada por: <@{acao['autor']}>")

            canal_relatorio = interaction.guild.get_channel(CANAL_RELATORIO_ACOES_ID)
            if canal_relatorio:
                await canal_relatorio.send(embed=embed_relatorio)
                await interaction.message.delete()
                await interaction.followup.send(f"✅ Helicrash **{acao['tipo']}** registrado!", ephemeral=True)
            else:
                await interaction.followup.send("❌ Canal de relatório não encontrado!", ephemeral=True)

            await enviar_painel_acoes(interaction.guild)
            return

        embed_relatorio = discord.Embed(title="🚨 RELATÓRIO DE AÇÃO", color=0xe74c3c)
        embed_relatorio.add_field(name="🏦 Ação", value=acao["tipo"], inline=False)
        embed_relatorio.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
        embed_relatorio.add_field(name="🎯 Resultado", value="⏳ Aguardando finalização...", inline=False)
        embed_relatorio.set_footer(text=f"ID: {self.acao_id} • Criada por: <@{acao['autor']}>")

        canal_relatorio = interaction.guild.get_channel(CANAL_RELATORIO_ACOES_ID)
        if canal_relatorio:
            msg = await canal_relatorio.send(embed=embed_relatorio, view=None)
            await msg.edit(view=ResultadoAcaoView(self.acao_id, msg))
            await interaction.message.delete()
            await interaction.followup.send(f"✅ Escalação concluída!", ephemeral=True)
            await enviar_painel_acoes(interaction.guild)
        else:
            await interaction.followup.send("❌ Canal de relatório não encontrado!", ephemeral=True)

    async def atualizar_embed(self, interaction, participantes, acao):
        try:
            mensagem = interaction.message
            
            if not mensagem or not mensagem.embeds:
                canal = interaction.channel
                async for msg in canal.history(limit=10):
                    if msg.author == bot.user and msg.embeds:
                        for field in msg.embeds[0].fields:
                            if field.name.startswith("👥 Participantes"):
                                mensagem = msg
                                break
                        if mensagem:
                            break
            
            if not mensagem or not mensagem.embeds:
                logger.error("❌ Não foi possível encontrar a mensagem do embed para atualizar")
                return
            
            embed = mensagem.embeds[0]
            
            if participantes and len(participantes) > 0:
                lista_participantes = "\n".join([f"<@{p['user_id']}>" for p in participantes])
            else:
                lista_participantes = "Nenhum participante ainda.\nClique no botão ✅ PARTICIPAR para se inscrever!"
            
            campo_atualizado = False
            for i, field in enumerate(embed.fields):
                if field.name.startswith("👥 Participantes"):
                    embed.set_field_at(
                        i,
                        name=f"👥 Participantes ({len(participantes) if participantes else 0})",
                        value=lista_participantes,
                        inline=False
                    )
                    campo_atualizado = True
                    break
            
            if not campo_atualizado:
                embed.add_field(
                    name=f"👥 Participantes ({len(participantes) if participantes else 0})",
                    value=lista_participantes,
                    inline=False
                )
            
            await mensagem.edit(embed=embed)
            
        except Exception as e:
            logger.error(f"❌ Erro ao atualizar embed: {e}")

# =========================================================
# CLASS: AcaoViewRestaurada
# =========================================================

class AcaoViewRestaurada(discord.ui.View):
    def __init__(self, acao_id, criador_id):
        super().__init__(timeout=None)
        self.acao_id = acao_id
        self.criador_id = criador_id
        self.add_item(discord.ui.Button(label="✅ Participar", style=discord.ButtonStyle.success, custom_id=f"acao_participar_{acao_id}", emoji="✅"))
        self.add_item(discord.ui.Button(label="❌ Sair", style=discord.ButtonStyle.danger, custom_id=f"acao_sair_{acao_id}", emoji="❌"))
        self.add_item(discord.ui.Button(label="🚫 Cancelar Ação", style=discord.ButtonStyle.danger, custom_id=f"acao_cancelar_{acao_id}", emoji="🚫"))
        self.add_item(discord.ui.Button(label="📤 Concluir Escalação", style=discord.ButtonStyle.primary, custom_id=f"acao_concluir_{acao_id}", emoji="📤"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        custom_id = interaction.data.get("custom_id", "")
        if custom_id == f"acao_participar_{self.acao_id}":
            await self.participar(interaction, None)
            return False
        elif custom_id == f"acao_sair_{self.acao_id}":
            await self.sair(interaction, None)
            return False
        elif custom_id == f"acao_cancelar_{self.acao_id}":
            await self.cancelar_acao(interaction, None)
            return False
        elif custom_id == f"acao_concluir_{self.acao_id}":
            await self.concluir(interaction, None)
            return False
        return True

    async def participar(self, interaction: discord.Interaction, button):
        if not any(role.id in CARGOS_PERMITIDOS_ESCALACAO for role in interaction.user.roles):
            await interaction.response.send_message("❌ Você não tem permissão!", ephemeral=True)
            return
        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.response.send_message("❌ Esta ação já foi concluída ou cancelada!", ephemeral=True)
                return
            ja_participa = await conn.fetchval("SELECT 1 FROM participantes_acoes WHERE acao_id=$1 AND user_id=$2", self.acao_id, str(interaction.user.id))
            if ja_participa:
                await interaction.response.send_message("⚠️ Você já está participando!", ephemeral=True)
                return
            await conn.execute("INSERT INTO participantes_acoes (acao_id, user_id) VALUES ($1, $2)", self.acao_id, str(interaction.user.id))
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)
        await self.atualizar_embed(interaction, participantes, acao)
        await interaction.response.send_message(f"✅ Você se inscreveu na ação!", ephemeral=True)

    async def sair(self, interaction: discord.Interaction, button):
        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.response.send_message("❌ Esta ação já foi concluída ou cancelada!", ephemeral=True)
                return
            participa = await conn.fetchval("SELECT 1 FROM participantes_acoes WHERE acao_id=$1 AND user_id=$2", self.acao_id, str(interaction.user.id))
            if not participa:
                await interaction.response.send_message("⚠️ Você não está participando!", ephemeral=True)
                return
            await conn.execute("DELETE FROM participantes_acoes WHERE acao_id = $1 AND user_id = $2", self.acao_id, str(interaction.user.id))
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)
        await self.atualizar_embed(interaction, participantes, acao)
        await interaction.response.send_message(f"✅ Você saiu da ação!", ephemeral=True)

    async def cancelar_acao(self, interaction: discord.Interaction, button):
        is_criador = interaction.user.id == self.criador_id
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_criador and not is_gerente:
            await interaction.response.send_message("❌ Apenas o criador ou gerentes!", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.followup.send("❌ Esta ação já foi concluída!", ephemeral=True)
                return
            await conn.execute("UPDATE acoes_semana SET status='cancelada' WHERE id=$1", self.acao_id)
            acao = await conn.fetchrow("SELECT tipo FROM acoes_semana WHERE id=$1", self.acao_id)
        await interaction.message.delete()
        await interaction.followup.send(f"✅ Ação cancelada e removida!", ephemeral=True)
        await enviar_painel_acoes(interaction.guild)

    async def concluir(self, interaction: discord.Interaction, button):
        is_criador = interaction.user.id == self.criador_id
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_criador and not is_gerente:
            await interaction.response.send_message("❌ Apenas o criador ou gerentes!", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.followup.send("❌ Esta ação já foi concluída!", ephemeral=True)
                return
            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            is_helicrash = "Helicrash" in acao["tipo"]
            if not participantes:
                await interaction.followup.send("⚠️ Nenhum participante!", ephemeral=True)
                await interaction.message.delete()
                return
            await conn.execute("UPDATE acoes_semana SET status='concluida' WHERE id=$1", self.acao_id)
            if is_helicrash:
                await conn.execute("UPDATE acoes_semana SET resultado='concluida', valor=0 WHERE id=$1", self.acao_id)
        lista_participantes = "\n".join([f"<@{p['user_id']}>" for p in participantes])
        if is_helicrash:
            embed_relatorio = discord.Embed(
                title="🚁 RELATÓRIO DE HELICRASH",
                description=f"**{acao['tipo']}**\n\n✅ Evento registrado!",
                color=0xe67e22
            )
            embed_relatorio.add_field(name="🏦 Evento", value=acao["tipo"], inline=False)
            embed_relatorio.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
            embed_relatorio.set_footer(text=f"ID: {self.acao_id}")
            canal_relatorio = interaction.guild.get_channel(CANAL_RELATORIO_ACOES_ID)
            if canal_relatorio:
                await canal_relatorio.send(embed=embed_relatorio)
                await interaction.message.delete()
                await interaction.followup.send(f"✅ Helicrash registrado!", ephemeral=True)
            else:
                await interaction.followup.send("❌ Canal não encontrado!", ephemeral=True)
            await enviar_painel_acoes(interaction.guild)
            return
        embed_relatorio = discord.Embed(title="🚨 RELATÓRIO DE AÇÃO", color=0xe74c3c)
        embed_relatorio.add_field(name="🏦 Ação", value=acao["tipo"], inline=False)
        embed_relatorio.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
        embed_relatorio.add_field(name="🎯 Resultado", value="⏳ Aguardando...", inline=False)
        embed_relatorio.set_footer(text=f"ID: {self.acao_id}")
        canal_relatorio = interaction.guild.get_channel(CANAL_RELATORIO_ACOES_ID)
        if canal_relatorio:
            msg = await canal_relatorio.send(embed=embed_relatorio, view=None)
            await msg.edit(view=ResultadoAcaoView(self.acao_id, msg))
            await interaction.message.delete()
            await interaction.followup.send(f"✅ Escalação concluída!", ephemeral=True)
            await enviar_painel_acoes(interaction.guild)
        else:
            await interaction.followup.send("❌ Canal não encontrado!", ephemeral=True)

    async def atualizar_embed(self, interaction, participantes, acao):
        embed = interaction.message.embeds[0]
        lista_participantes = "\n".join([f"<@{p['user_id']}>" for p in participantes]) if participantes else "Nenhum participante."
        for i, field in enumerate(embed.fields):
            if field.name.startswith("👥 Participantes"):
                embed.set_field_at(
                    i,
                    name=f"👥 Participantes ({len(participantes)})",
                    value=lista_participantes,
                    inline=False
                )
                break
        await interaction.message.edit(embed=embed)

# =========================================================
# CLASS: ResultadoAcaoView
# =========================================================

class ResultadoAcaoView(discord.ui.View):
    def __init__(self, acao_id, mensagem_original):
        super().__init__(timeout=None)
        self.acao_id = acao_id
        self.mensagem_original = mensagem_original

    @discord.ui.button(label="🏆 Ganhou", style=discord.ButtonStyle.success, custom_id="resultado_ganhou")
    async def ganhou(self, interaction: discord.Interaction, button):
        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            acao = await conn.fetchrow("SELECT autor FROM acoes_semana WHERE id=$1", self.acao_id)
        is_autor = str(interaction.user.id) == acao["autor"]
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_autor and not is_gerente:
            await interaction.response.send_message("❌ Sem permissão!", ephemeral=True)
            return
        await interaction.response.send_modal(ResultadoGanhouModal(self.acao_id, self.mensagem_original))

    @discord.ui.button(label="💀 Perdeu", style=discord.ButtonStyle.danger, custom_id="resultado_perdeu")
    async def perdeu(self, interaction: discord.Interaction, button):
        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            acao = await conn.fetchrow("SELECT autor FROM acoes_semana WHERE id=$1", self.acao_id)
        is_autor = str(interaction.user.id) == acao["autor"]
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_autor and not is_gerente:
            await interaction.response.send_message("❌ Sem permissão!", ephemeral=True)
            return
        await interaction.response.send_modal(ResultadoPerdeuModal(self.acao_id, self.mensagem_original))

# =========================================================
# CLASS: ResultadoGanhouModal
# =========================================================

class ResultadoGanhouModal(discord.ui.Modal, title="🎉 Resultado - GANHOU"):
    dinheiro = discord.ui.TextInput(label="Valor total ganho (em reais)", placeholder="Ex: 50000", required=True)

    def __init__(self, acao_id, mensagem_original):
        super().__init__()
        self.acao_id = acao_id
        self.mensagem_original = mensagem_original

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            valor_total = safe_int(self.dinheiro.value)
            if valor_total <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ Valor inválido!", ephemeral=True)
            return
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            acao = await conn.fetchrow("SELECT tipo FROM acoes_semana WHERE id=$1", self.acao_id)
            limite = ACOES_SEMANA.get(acao["tipo"])
            if limite and limite is not None:
                qtd_feita = await conn.fetchval("SELECT COUNT(*) FROM acoes_semana WHERE tipo=$1 AND resultado='ganhou' AND id != $2", acao["tipo"], self.acao_id)
                if qtd_feita >= limite:
                    await interaction.followup.send(f"❌ Ação **{acao['tipo']}** já atingiu o limite semanal de **{limite}** vitória(s)!", ephemeral=True)
                    return
            await conn.execute("UPDATE acoes_semana SET valor=$1, resultado='ganhou' WHERE id=$2", valor_total, self.acao_id)
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
        ids_participantes = [str(p["user_id"]) for p in participantes]
        qtd = len(ids_participantes)
        if qtd == 0:
            await interaction.followup.send("⚠️ Nenhum participante!", ephemeral=True)
            return
        valor_por_pessoa = valor_total // qtd
        resto = valor_total % qtd
        depositos_ok = 0
        for uid in ids_participantes:
            sucesso = await depositar_na_meta(int(uid), valor_por_pessoa, f"Ação: {acao['tipo']}")
            if sucesso:
                depositos_ok += 1
        if resto > 0 and ids_participantes:
            await depositar_na_meta(int(ids_participantes[0]), resto, f"Ação: {acao['tipo']} (Restante)")
        lista_participantes = "\n".join([f"<@{uid}>" for uid in ids_participantes])
        embed = discord.Embed(title="🎉 RESULTADO DA AÇÃO - GANHOU!", color=0x2ecc71)
        embed.add_field(name="🎯 Ação", value=acao["tipo"], inline=False)
        embed.add_field(name="💰 Total Ganho", value=formatar_dinheiro(valor_total), inline=False)
        embed.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
        embed.add_field(name="💸 Valor por pessoa", value=formatar_dinheiro(valor_por_pessoa), inline=True)
        embed.add_field(name="✅ Depósitos", value=f"{depositos_ok}/{qtd} realizados", inline=True)
        if resto > 0:
            embed.add_field(name="📦 Restante", value=formatar_dinheiro(resto), inline=True)
        await self.mensagem_original.edit(embed=embed, view=None)
        await enviar_painel_acoes(interaction.guild)
        await interaction.followup.send(f"✅ {depositos_ok} depósitos realizados!", ephemeral=True)

# =========================================================
# CLASS: ResultadoPerdeuModal
# =========================================================

class ResultadoPerdeuModal(discord.ui.Modal, title="💀 Resultado - PERDEU"):
    confirmacao = discord.ui.TextInput(label="Digite CONFIRMAR para registrar a perda", required=True)

    def __init__(self, acao_id, mensagem_original):
        super().__init__()
        self.acao_id = acao_id
        self.mensagem_original = mensagem_original

    async def on_submit(self, interaction: discord.Interaction):
        if self.confirmacao.value.strip().upper() != "CONFIRMAR":
            await interaction.response.send_message("❌ Confirmação incorreta!", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            await conn.execute("UPDATE acoes_semana SET valor=0, resultado='perdeu' WHERE id=$1", self.acao_id)
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            acao = await conn.fetchrow("SELECT tipo FROM acoes_semana WHERE id=$1", self.acao_id)
        ids_participantes = [str(p["user_id"]) for p in participantes]
        lista_participantes = "\n".join([f"<@{uid}>" for uid in ids_participantes]) if ids_participantes else "Ninguém"
        embed = discord.Embed(
            title="💀 RESULTADO DA AÇÃO - PERDEU!",
            description="A ação foi perdida, nenhum valor foi distribuído.",
            color=0xe74c3c
        )
        embed.add_field(name="🎯 Ação", value=acao["tipo"], inline=False)
        embed.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
        embed.add_field(name="💰 Total", value="R$ 0,00", inline=True)
        embed.add_field(name="📝 Status", value="❌ AÇÃO PERDIDA", inline=True)
        await self.mensagem_original.edit(embed=embed, view=None)
        await enviar_painel_acoes(interaction.guild)
        await interaction.followup.send(f"✅ Ação registrada como PERDIDA!", ephemeral=True)

# =========================================================
# CLASS: PainelAcoesView
# =========================================================

class PainelAcoesView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🏙️ Complexo", style=discord.ButtonStyle.primary, custom_id="acoes_complexo", emoji="🏙️")
    async def acoes_complexo(self, interaction: discord.Interaction, button):
        await interaction.response.defer(ephemeral=True)
        view = SelecionarAcaoView(ACOES_COMPLEXO, "ESCOLHA A AÇÃO DO COMPLEXO", "🏙️")
        await interaction.followup.send("**🏙️ Selecione a ação do Complexo:**", view=view, ephemeral=True)

    @discord.ui.button(label="🏝️ Bahamas", style=discord.ButtonStyle.primary, custom_id="acoes_bahamas", emoji="🏝️")
    async def acoes_bahamas(self, interaction: discord.Interaction, button):
        await interaction.response.defer(ephemeral=True)
        view = SelecionarAcaoView(ACOES_BAHAMAS, "ESCOLHA A AÇÃO DE BAHAMAS", "🏝️")
        await interaction.followup.send("**🏝️ Selecione a ação de Bahamas:**", view=view, ephemeral=True)

    @discord.ui.button(label="🚁 Helicrash", style=discord.ButtonStyle.primary, custom_id="acoes_helicrash", emoji="🚁")
    async def acoes_helicrash(self, interaction: discord.Interaction, button):
        await interaction.response.defer(ephemeral=True)
        view = SelecionarAcaoView(ACOES_HELICRASH, "ESCOLHA O HELICRASH", "🚁")
        await interaction.followup.send("**🚁 Selecione o Helicrash:**", view=view, ephemeral=True)

    @discord.ui.button(label="📊 Ver Relatório", style=discord.ButtonStyle.secondary, custom_id="acoes_relatorio", emoji="📊")
    async def relatorio(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(RelatorioPeriodoModal())

    @discord.ui.button(label="♻️ Resetar Ações", style=discord.ButtonStyle.danger, custom_id="acoes_reset", emoji="♻️")
    async def reset(self, interaction: discord.Interaction, button):
        await interaction.response.defer(ephemeral=True)
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_gerente and not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("❌ Apenas gerentes podem resetar as ações!", ephemeral=True)
            return
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM acoes_semana")
            await conn.execute("DELETE FROM participantes_acoes")
        await enviar_painel_acoes(interaction.guild)
        await interaction.followup.send("✅ Todas as ações foram resetadas!", ephemeral=True)

# =========================================================
# CLASS: RelatorioPeriodoModal
# =========================================================

class RelatorioPeriodoModal(discord.ui.Modal, title="📊 Gerar Relatório"):
    data_inicio = discord.ui.TextInput(label="Data início (DD/MM/AAAA)")
    data_fim = discord.ui.TextInput(label="Data fim (DD/MM/AAAA)")

    async def on_submit(self, interaction: discord.Interaction):
        try:
            inicio = datetime.strptime(self.data_inicio.value, "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value, "%d/%m/%Y") + timedelta(days=1)
        except:
            await interaction.response.send_message("❌ Data inválida.", ephemeral=True)
            return
        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível.", ephemeral=True)
            return
        async with pool.acquire() as conn:
            total = await conn.fetchval("SELECT COALESCE(SUM(valor), 0) FROM acoes_semana WHERE DATE(data) BETWEEN DATE($1) AND DATE($2) AND resultado = 'ganhou'", inicio, fim)
            rows = await conn.fetch("SELECT p.user_id, COUNT(*) as qtd FROM participantes_acoes p JOIN acoes_semana a ON a.id = p.acao_id WHERE DATE(a.data) BETWEEN DATE($1) AND DATE($2) GROUP BY p.user_id ORDER BY qtd DESC", inicio, fim)
        linhas = [f"<@{r['user_id']}> • {r['qtd']} participações" for r in rows]
        embed = discord.Embed(title="📊 Relatório de Ações", color=0x3498db)
        embed.add_field(name="📅 Período", value=f"{self.data_inicio.value} até {self.data_fim.value}", inline=False)
        embed.add_field(name="💰 Total Movimentado (vitórias)", value=formatar_dinheiro(total), inline=False)
        embed.add_field(name="👥 Participações", value="\n".join(linhas) if linhas else "Nenhuma", inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

# =========================================================
# ==================== SISTEMA DE GRUPOS ==================
# =========================================================

# ---------------------------------------------------------
# 6.1: CONSTANTES DOS GRUPOS
# ---------------------------------------------------------

TIPOS_ORGANIZACAO = {
    "PISTA SEM PAINEL": {
        "nome": "📋 PISTA SEM PAINEL",
        "descricao": "APENAS PT",
        "pode_pt": True,
        "pode_sub": False,
        "emoji": "📋",
        "produtos": ["PT"]
    },
    "PISTA COM PAINEL": {
        "nome": "📱 PISTA COM PAINEL",
        "descricao": "PT E SUB",
        "pode_pt": True,
        "pode_sub": True,
        "emoji": "📱",
        "produtos": ["PT", "SUB"]
    },
    "MAFIAS": {
        "nome": "🤵 MAFIAS",
        "descricao": "PT E SUB",
        "pode_pt": True,
        "pode_sub": True,
        "emoji": "🤵",
        "produtos": ["MUNIÇÃO FUZIL", "MUNIÇÃO PISTOLA", "SUB", "ARMAS", "LAVAGEM", "CONTRABANDO", "KIT REPARO"]
    },
    "FAVELAS": {
        "nome": "🏚️ FAVELAS",
        "descricao": "PT E SUB",
        "pode_pt": True,
        "pode_sub": True,
        "emoji": "🏚️",
        "produtos": ["HAXIXE", "AQUABLITS", "LEAN", "MD", "COCA", "LANÇA", "BALÃO", "K9", "KETAMINA"]
    },
    "MECÂNICA ILEGAL": {
        "nome": "🔧 MECÂNICA ILEGAL",
        "descricao": "PT E SUB",
        "pode_pt": True,
        "pode_sub": True,
        "emoji": "🔧",
        "produtos": ["TUNNING DE VEÍCULOS", "PEÇAS ILEGAIS", "PLACA FALSA", "NITRO"]
    }
}

# ---------------------------------------------------------
# ASYNC: salvar_grupo_db
# ---------------------------------------------------------

async def salvar_grupo_db(grupo_id, nome_org, lider_nome, lider_telefone, braco_nome, braco_telefone, produto, tipo_org="PISTA SEM PAINEL", observacoes=""):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO grupos (
                    grupo_id, nome_org, lider_nome, lider_telefone,
                    braco_nome, braco_telefone, produto, tipo_org, observacoes,
                    data_criacao, ativo
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, true)
                """,
                grupo_id, nome_org.upper(), lider_nome.upper(), lider_telefone.upper(),
                braco_nome.upper() if braco_nome else None,
                braco_telefone.upper() if braco_telefone else None,
                produto.upper(), tipo_org, observacoes.upper() if observacoes else "", agora_db()
            )
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")

# ---------------------------------------------------------
# ASYNC: carregar_grupo_db
# ---------------------------------------------------------

async def carregar_grupo_db(grupo_id):
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM grupos WHERE grupo_id = $1 AND ativo = true", grupo_id)
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: carregar_grupos_db
# ---------------------------------------------------------

async def carregar_grupos_db():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM grupos WHERE ativo = true ORDER BY nome_org ASC")
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: atualizar_grupo_db
# ---------------------------------------------------------

async def atualizar_grupo_db(grupo_id, nome_org, lider_nome, lider_telefone, braco_nome, braco_telefone, produto, tipo_org=None, observacoes=None):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            query = """
                UPDATE grupos SET
                    nome_org = $2, lider_nome = $3, lider_telefone = $4,
                    braco_nome = $5, braco_telefone = $6, produto = $7,
                    data_atualizacao = $8
            """
            params = [grupo_id, nome_org.upper(), lider_nome.upper(), lider_telefone.upper(),
                     braco_nome.upper() if braco_nome else None,
                     braco_telefone.upper() if braco_telefone else None,
                     produto.upper(), agora_db()]
            if tipo_org is not None:
                query += ", tipo_org = $9"
                params.append(tipo_org)
            if observacoes is not None:
                query += ", observacoes = $" + str(len(params) + 1)
                params.append(observacoes.upper() if observacoes else "")
            query += " WHERE grupo_id = $1 AND ativo = true"
            await conn.execute(query, *params)
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")

# ---------------------------------------------------------
# ASYNC: desativar_grupo_db
# ---------------------------------------------------------

async def desativar_grupo_db(grupo_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE grupos SET ativo = false, data_exclusao = $1 WHERE grupo_id = $2", agora_db(), grupo_id)
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")

# ---------------------------------------------------------
# ASYNC: registrar_compra_grupo_db
# ---------------------------------------------------------

async def registrar_compra_grupo_db(grupo_id, tipo, quantidade, valor):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO compras_grupo (grupo_id, tipo, quantidade, valor, data) VALUES ($1, $2, $3, $4, $5)",
                grupo_id, tipo.upper(), quantidade, valor, agora_db()
            )
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")

# ---------------------------------------------------------
# ASYNC: carregar_compras_grupo_db
# ---------------------------------------------------------

async def carregar_compras_grupo_db(grupo_id):
    pool = await get_pool()
    if not pool:
        return {"PT": {"quantidade": 0, "valor": 0}, "SUB": {"quantidade": 0, "valor": 0}}
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT tipo, SUM(quantidade) as total_quantidade, SUM(valor) as total_valor
                FROM compras_grupo
                WHERE grupo_id = $1
                GROUP BY tipo
                """,
                grupo_id
            )
            compras = {"PT": {"quantidade": 0, "valor": 0}, "SUB": {"quantidade": 0, "valor": 0}}
            for row in rows:
                tipo = row["tipo"]
                compras[tipo] = {"quantidade": row["total_quantidade"] or 0, "valor": row["total_valor"] or 0}
            return compras
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")
        return {"PT": {"quantidade": 0, "valor": 0}, "SUB": {"quantidade": 0, "valor": 0}}

# ---------------------------------------------------------
# ASYNC: recriar_painel_grupos
# ---------------------------------------------------------

async def recriar_painel_grupos():
    canal = bot.get_channel(CANAL_GRUPOS_ID)
    if not canal:
        logger.error(f"❌ CANAL NÃO ENCONTRADO: {CANAL_GRUPOS_ID}")
        return False
    try:
        deletadas = 0
        async for msg in canal.history(limit=500):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    deletadas += 1
                    await asyncio.sleep(0.3)
                except:
                    pass
        await asyncio.sleep(2)
        await enviar_painel_grupos()
        return True
    except Exception as e:
        logger.error(f"❌ ERRO AO RECRIAR PAINEL: {e}")
        return False

# ---------------------------------------------------------
# ASYNC: enviar_painel_grupos
# ---------------------------------------------------------

async def enviar_painel_grupos():
    canal = bot.get_channel(CANAL_GRUPOS_ID)
    if not canal:
        logger.error(f"❌ CANAL NÃO ENCONTRADO")
        return
    try:
        grupos = await carregar_grupos_db()
        embed = discord.Embed(
            title="📋 GERENCIAMENTO DE GRUPOS",
            description="**SELECIONE UM GRUPO NO MENU ABAIXO:**\n\n📌 **TIPOS:**\n• 📋 PISTA SEM PAINEL - APENAS PT\n• 📱 PISTA COM PAINEL - PT E SUB\n• 🤵 MAFIAS - PT E SUB\n• 🏚️ FAVELAS - PT E SUB\n• 🔧 MECÂNICA ILEGAL - PT E SUB",
            color=0x2ecc71, timestamp=agora()
        )
        if grupos:
            total_pt = 0
            total_sub = 0
            for grupo in grupos:
                try:
                    compras = await carregar_compras_grupo_db(grupo["grupo_id"])
                    total_pt += compras.get("PT", {}).get("quantidade", 0)
                    total_sub += compras.get("SUB", {}).get("quantidade", 0)
                except:
                    pass
            embed.add_field(name="📊 RESUMO", value=f"**{len(grupos)} GRUPOS** | PT: {fmt_num(total_pt)} | SUB: {fmt_num(total_sub)}", inline=False)
        else:
            embed.add_field(name="📭 NENHUM GRUPO", value="CLIQUE EM **➕ NOVO GRUPO** PARA CADASTRAR.", inline=False)
        embed.set_footer(text="👇 SELECIONE UM GRUPO NO DROPDOWN")
        view = PainelGruposView(grupos)
        await canal.send(embed=embed, view=view)
    except Exception as e:
        logger.error(f"❌ ERRO AO ENVIAR PAINEL: {e}")

# =========================================================
# CLASS: PainelGruposView
# =========================================================

class PainelGruposView(discord.ui.View):
    def __init__(self, grupos):
        super().__init__(timeout=None)
        self.grupos = grupos
        import time
        self.uid = str(int(time.time()))[-6:]
        
        if grupos and len(grupos) > 0:
            options = []
            for grupo in grupos[:25]:
                nome = grupo['nome_org'][:45]
                tipo = grupo.get('tipo_org', 'PISTA SEM PAINEL')
                emoji = TIPOS_ORGANIZACAO.get(tipo, {}).get('emoji', '🏷️')
                options.append(
                    discord.SelectOption(
                        label=nome,
                        description=f"{emoji} {grupo['lider_nome'][:20]}",
                        value=grupo['grupo_id'],
                        emoji="🏷️"
                    )
                )
            if options:
                select = discord.ui.Select(
                    placeholder="📋 SELECIONE UM GRUPO...",
                    options=options,
                    min_values=1,
                    max_values=1,
                    custom_id=f"select_{self.uid}"
                )
                select.callback = self.select_callback
                self.add_item(select)

    async def select_callback(self, interaction: discord.Interaction):
        try:
            grupo_id = interaction.data["values"][0]
            await interaction.response.defer(ephemeral=True)
            
            dados = await carregar_grupo_db(grupo_id)
            if not dados:
                await interaction.followup.send("❌ GRUPO NÃO ENCONTRADO!", ephemeral=True)
                return
            
            compras = await carregar_compras_grupo_db(grupo_id)
            tipo_org = dados.get('tipo_org', 'PISTA SEM PAINEL')
            info_tipo = TIPOS_ORGANIZACAO.get(tipo_org, TIPOS_ORGANIZACAO['PISTA SEM PAINEL'])
            
            embed = discord.Embed(
                title=f"{info_tipo['emoji']} {dados['nome_org']}",
                color=0x3498db,
                timestamp=agora()
            )
            
            info = f"**👤 LÍDER:** {dados['lider_nome']}\n"
            info += f"**📱 TELEFONE:** {dados['lider_telefone']}\n"
            if dados.get('braco_nome'):
                info += f"**👤 BRAÇO:** {dados['braco_nome']}\n"
            if dados.get('braco_telefone'):
                info += f"**📱 TELEFONE BRAÇO:** {dados['braco_telefone']}\n"
            info += f"\n**🔫 PRODUTO:** {dados['produto']}\n"
            info += f"\n**📌 TIPO:** {info_tipo['nome']}\n"
            info += f"**📝 {info_tipo['descricao']}**"
            
            embed.add_field(name="📋 INFORMAÇÕES", value=info, inline=False)
            
            pt = compras.get("PT", {})
            sub = compras.get("SUB", {})
            compras_texto = ""
            if pt.get("quantidade", 0) > 0 or sub.get("quantidade", 0) > 0:
                if pt.get("quantidade", 0) > 0:
                    compras_texto += f"**🔫 PT:** {fmt_num(pt['quantidade'])} PACOTES\n💰 {formatar_dinheiro(pt['valor'])}\n"
                if sub.get("quantidade", 0) > 0:
                    compras_texto += f"**🔫 SUB:** {fmt_num(sub['quantidade'])} PACOTES\n💰 {formatar_dinheiro(sub['valor'])}\n"
                compras_texto += f"\n**📦 TOTAL:** {fmt_num(pt.get('quantidade', 0) + sub.get('quantidade', 0))} PACOTES"
            else:
                compras_texto = "📭 NENHUMA COMPRA"
            embed.add_field(name="📦 COMPRAS", value=compras_texto, inline=False)
            
            if dados.get('observacoes'):
                embed.add_field(name="📝 OBS", value=dados['observacoes'], inline=False)
            
            view = GrupoView(grupo_id, dados['nome_org'])
            await interaction.followup.send(embed=embed, view=view, ephemeral=True)
            
        except Exception as e:
            logger.error(f"❌ ERRO: {e}")
            await interaction.followup.send(f"❌ ERRO: {str(e)[:100]}", ephemeral=True)

    @discord.ui.button(label="➕ NOVO GRUPO", style=discord.ButtonStyle.success, custom_id="novo_padrao", emoji="➕")
    async def novo_grupo(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ APENAS ADM OU GERENTES!", ephemeral=True)
            return
        
        view = EscolherTipoView("registrar")
        embed = discord.Embed(
            title="📌 SELECIONE O TIPO DE ORGANIZAÇÃO",
            description=(
                "**CLIQUE NO BOTÃO CORRESPONDENTE AO TIPO:**\n\n"
                "📋 **PISTA SEM PAINEL** - APENAS PT\n"
                "📱 **PISTA COM PAINEL** - PT E SUB\n"
                "🤵 **MAFIAS** - PT E SUB\n"
                "🏚️ **FAVELAS** - PT E SUB\n"
                "🔧 **MECÂNICA ILEGAL** - PT E SUB"
            ),
            color=0x3498db
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    @discord.ui.button(label="🔄 ATUALIZAR", style=discord.ButtonStyle.secondary, custom_id="atualizar_padrao", emoji="🔄")
    async def atualizar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await recriar_painel_grupos()
        await interaction.followup.send("✅ PAINEL ATUALIZADO!", ephemeral=True)

# =========================================================
# CLASS: GrupoView
# =========================================================

class GrupoView(discord.ui.View):
    def __init__(self, grupo_id, nome_org):
        super().__init__(timeout=300)
        self.grupo_id = grupo_id
        self.nome_org = nome_org
        import time
        self.uid = str(int(time.time()))[-6:]
        self.add_item(discord.ui.Button(label="✏️ EDITAR", style=discord.ButtonStyle.primary, custom_id=f"editar_{self.uid}", emoji="✏️"))
        self.add_item(discord.ui.Button(label="🗑️ EXCLUIR", style=discord.ButtonStyle.danger, custom_id=f"excluir_{self.uid}", emoji="🗑️"))
        self.add_item(discord.ui.Button(label="📊 COMPRAS", style=discord.ButtonStyle.success, custom_id=f"compras_{self.uid}", emoji="📦"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        custom_id = interaction.data.get("custom_id", "")
        if custom_id.startswith(f"editar_{self.uid}"):
            await self.editar(interaction)
            return False
        elif custom_id.startswith(f"excluir_{self.uid}"):
            await self.excluir(interaction)
            return False
        elif custom_id.startswith(f"compras_{self.uid}"):
            await self.compras(interaction)
            return False
        return True

    async def editar(self, interaction: discord.Interaction):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ APENAS ADM OU GERENTES!", ephemeral=True)
            return
        dados = await carregar_grupo_db(self.grupo_id)
        if not dados:
            await interaction.response.send_message("❌ GRUPO NÃO ENCONTRADO!", ephemeral=True)
            return
        view = EscolherTipoView("editar", {"grupo_id": self.grupo_id, "dados": dados})
        embed = discord.Embed(
            title="📌 SELECIONE O NOVO TIPO DE ORGANIZAÇÃO",
            description=(
                "**CLIQUE NO BOTÃO CORRESPONDENTE AO NOVO TIPO:**\n\n"
                "📋 **PISTA SEM PAINEL** - APENAS PT\n"
                "📱 **PISTA COM PAINEL** - PT E SUB\n"
                "🤵 **MAFIAS** - PT E SUB\n"
                "🏚️ **FAVELAS** - PT E SUB\n"
                "🔧 **MECÂNICA ILEGAL** - PT E SUB\n\n"
                f"📌 **TIPO ATUAL:** {dados.get('tipo_org', 'PISTA SEM PAINEL')}"
            ),
            color=0x3498db
        )
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def excluir(self, interaction: discord.Interaction):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ APENAS ADM OU GERENTES!", ephemeral=True)
            return
        view = ConfirmarExcluirView(self.grupo_id, self.nome_org)
        await interaction.response.send_message(f"⚠️ **EXCLUIR {self.nome_org}?**", view=view, ephemeral=True)

    async def compras(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        compras = await carregar_compras_grupo_db(self.grupo_id)
        pt = compras.get("PT", {})
        sub = compras.get("SUB", {})
        embed = discord.Embed(title=f"📦 COMPRAS - {self.nome_org}", color=0x2ecc71)
        if pt.get("quantidade", 0) > 0 or sub.get("quantidade", 0) > 0:
            if pt.get("quantidade", 0) > 0:
                embed.add_field(name="🔫 PT", value=f"**{fmt_num(pt['quantidade'])}** PACOTES\n💰 {formatar_dinheiro(pt['valor'])}", inline=True)
            if sub.get("quantidade", 0) > 0:
                embed.add_field(name="🔫 SUB", value=f"**{fmt_num(sub['quantidade'])}** PACOTES\n💰 {formatar_dinheiro(sub['valor'])}", inline=True)
            total = pt.get("quantidade", 0) + sub.get("quantidade", 0)
            total_valor = pt.get("valor", 0) + sub.get("valor", 0)
            embed.add_field(name="📦 TOTAL", value=f"**{fmt_num(total)}** PACOTES\n💰 {formatar_dinheiro(total_valor)}", inline=False)
        else:
            embed.add_field(name="📭 NENHUMA COMPRA", value="ESTE GRUPO AINDA NÃO REALIZOU COMPRAS.", inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

# =========================================================
# CLASS: RegistrarGrupoModal
# =========================================================

class RegistrarGrupoModal(discord.ui.Modal, title="📋 REGISTRAR NOVO GRUPO"):
    def __init__(self, tipo_escolhido, produtos_texto):
        super().__init__(timeout=300)
        self.tipo_escolhido = tipo_escolhido
        self.nome_org = discord.ui.TextInput(label="🏷️ NOME DA ORGANIZAÇÃO", placeholder="EX: VDR, POLÍCIA, MAFIA", required=True, max_length=50)
        self.lider = discord.ui.TextInput(label="👤 LÍDER (NOME - TELEFONE)", placeholder="EX: JOÃO SILVA - (11) 99999-9999", required=True, max_length=100)
        self.braco = discord.ui.TextInput(label="👤 BRAÇO (NOME - TELEFONE - OPCIONAL)", placeholder="EX: JOSÉ SANTOS - (11) 88888-8888", required=False, max_length=100)
        self.produto = discord.ui.TextInput(label=f"🔫 PRODUTO QUE FORNECE ({self.tipo_escolhido})", placeholder=f"OPÇÕES: {produtos_texto}", required=True, max_length=50)
        self.tipo_org = discord.ui.TextInput(label="📌 TIPO DE ORGANIZAÇÃO (DEFINIDO)", default=self.tipo_escolhido, required=True, max_length=30)
        self.add_item(self.nome_org)
        self.add_item(self.lider)
        self.add_item(self.braco)
        self.add_item(self.produto)
        self.add_item(self.tipo_org)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        lider_parts = self.lider.value.strip().split(" - ")
        lider_nome = lider_parts[0] if lider_parts else self.lider.value
        lider_telefone = lider_parts[1] if len(lider_parts) > 1 else "NÃO INFORMADO"
        braco_nome = None
        braco_telefone = None
        if self.braco.value:
            braco_parts = self.braco.value.strip().split(" - ")
            braco_nome = braco_parts[0] if braco_parts else self.braco.value
            braco_telefone = braco_parts[1] if len(braco_parts) > 1 else "NÃO INFORMADO"
        tipo_org = self.tipo_org.value.strip().upper()
        import time
        grupo_id = f"GRUPO_{int(time.time())}_{interaction.user.id}"
        await salvar_grupo_db(grupo_id, self.nome_org.value.strip().upper(), lider_nome.upper(), lider_telefone.upper(), braco_nome.upper() if braco_nome else None, braco_telefone.upper() if braco_telefone else None, self.produto.value.strip().upper(), tipo_org, "")
        await recriar_painel_grupos()
        await interaction.followup.send(f"✅ **GRUPO {self.nome_org.value.upper()} REGISTRADO!**", ephemeral=True)
        await asyncio.sleep(5)
        try:
            await interaction.delete_original_response()
        except:
            pass

# =========================================================
# CLASS: EditarGrupoModal
# =========================================================

class EditarGrupoModal(discord.ui.Modal, title="✏️ EDITAR GRUPO"):
    def __init__(self, grupo_id, dados, tipo_escolhido, produtos_texto):
        super().__init__(timeout=300)
        self.grupo_id = grupo_id
        self.nome_org = discord.ui.TextInput(label="🏷️ NOME DA ORGANIZAÇÃO", default=dados.get('nome_org', '').upper(), required=True, max_length=50)
        lider_texto = f"{dados.get('lider_nome', '').upper()} - {dados.get('lider_telefone', '').upper()}"
        self.lider = discord.ui.TextInput(label="👤 LÍDER (NOME - TELEFONE)", default=lider_texto, required=True, max_length=100)
        if dados.get('braco_nome') and dados.get('braco_telefone'):
            braco_default = f"{dados.get('braco_nome', '').upper()} - {dados.get('braco_telefone', '').upper()}"
        elif dados.get('braco_nome'):
            braco_default = dados.get('braco_nome', '').upper()
        else:
            braco_default = ""
        self.braco = discord.ui.TextInput(label="👤 BRAÇO (NOME - TELEFONE - OPCIONAL)", default=braco_default, required=False, max_length=100)
        self.produto = discord.ui.TextInput(label=f"🔫 PRODUTO QUE FORNECE ({tipo_escolhido})", default=dados.get('produto', '').upper(), placeholder=f"OPÇÕES: {produtos_texto}", required=True, max_length=50)
        self.tipo_org = discord.ui.TextInput(label="📌 TIPO DE ORGANIZAÇÃO (DEFINIDO)", default=tipo_escolhido, required=True, max_length=30)
        self.add_item(self.nome_org)
        self.add_item(self.lider)
        self.add_item(self.braco)
        self.add_item(self.produto)
        self.add_item(self.tipo_org)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        lider_parts = self.lider.value.strip().split(" - ")
        lider_nome = lider_parts[0] if lider_parts else self.lider.value
        lider_telefone = lider_parts[1] if len(lider_parts) > 1 else "NÃO INFORMADO"
        braco_nome = None
        braco_telefone = None
        if self.braco.value:
            braco_parts = self.braco.value.strip().split(" - ")
            braco_nome = braco_parts[0] if braco_parts else self.braco.value
            braco_telefone = braco_parts[1] if len(braco_parts) > 1 else "NÃO INFORMADO"
        tipo_org = self.tipo_org.value.strip().upper()
        await atualizar_grupo_db(self.grupo_id, self.nome_org.value.strip().upper(), lider_nome.upper(), lider_telefone.upper(), braco_nome.upper() if braco_nome else None, braco_telefone.upper() if braco_telefone else None, self.produto.value.strip().upper(), tipo_org, "")
        await recriar_painel_grupos()
        await interaction.followup.send(f"✅ **GRUPO {self.nome_org.value.upper()} ATUALIZADO!**", ephemeral=True)
        await asyncio.sleep(5)
        try:
            await interaction.delete_original_response()
        except:
            pass

# =========================================================
# CLASS: ConfirmarExcluirView
# =========================================================

class ConfirmarExcluirView(discord.ui.View):
    def __init__(self, grupo_id, nome_org):
        super().__init__(timeout=60)
        self.grupo_id = grupo_id
        self.nome_org = nome_org
        import time
        self.uid = str(int(time.time()))[-6:]
        self.add_item(discord.ui.Button(label="✅ SIM", style=discord.ButtonStyle.danger, custom_id=f"conf_{self.uid}", emoji="✅"))
        self.add_item(discord.ui.Button(label="❌ CANCELAR", style=discord.ButtonStyle.secondary, custom_id=f"cancel_{self.uid}", emoji="❌"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        custom_id = interaction.data.get("custom_id", "")
        if custom_id.startswith("conf_"):
            await self.confirmar(interaction)
            return False
        elif custom_id.startswith("cancel_"):
            await self.cancelar(interaction)
            return False
        return True

    async def confirmar(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        await desativar_grupo_db(self.grupo_id)
        await recriar_painel_grupos()
        await interaction.followup.send(f"✅ **GRUPO {self.nome_org} EXCLUÍDO!**", ephemeral=True)

    async def cancelar(self, interaction: discord.Interaction):
        await interaction.response.send_message("❌ CANCELADO.", ephemeral=True)

# =========================================================
# CLASS: EscolherTipoView
# =========================================================

class EscolherTipoView(discord.ui.View):
    def __init__(self, acao, dados=None):
        super().__init__(timeout=120)
        self.acao = acao
        self.dados = dados
        self.add_item(discord.ui.Button(label="📋 PISTA SEM PAINEL", style=discord.ButtonStyle.secondary, custom_id="tipo_pista_sem", emoji="📋"))
        self.add_item(discord.ui.Button(label="📱 PISTA COM PAINEL", style=discord.ButtonStyle.primary, custom_id="tipo_pista_com", emoji="📱"))
        self.add_item(discord.ui.Button(label="🤵 MAFIAS", style=discord.ButtonStyle.primary, custom_id="tipo_mafias", emoji="🤵"))
        self.add_item(discord.ui.Button(label="🏚️ FAVELAS", style=discord.ButtonStyle.primary, custom_id="tipo_favelas", emoji="🏚️"))
        self.add_item(discord.ui.Button(label="🔧 MECÂNICA ILEGAL", style=discord.ButtonStyle.primary, custom_id="tipo_mecanica", emoji="🔧"))
        self.add_item(discord.ui.Button(label="❌ CANCELAR", style=discord.ButtonStyle.danger, custom_id="cancelar_tipo", emoji="❌"))

    async def interaction_check(self, interaction: discord.Interaction):
        custom_id = interaction.data.get("custom_id", "")
        if custom_id == "cancelar_tipo":
            await interaction.response.send_message("❌ OPERAÇÃO CANCELADA.", ephemeral=True)
            try:
                await interaction.message.delete()
            except:
                pass
            return False
        tipos = {
            "tipo_pista_sem": "PISTA SEM PAINEL",
            "tipo_pista_com": "PISTA COM PAINEL",
            "tipo_mafias": "MAFIAS",
            "tipo_favelas": "FAVELAS",
            "tipo_mecanica": "MECÂNICA ILEGAL"
        }
        tipo_escolhido = tipos.get(custom_id)
        if tipo_escolhido:
            info_tipo = TIPOS_ORGANIZACAO.get(tipo_escolhido, {})
            produtos = info_tipo.get("produtos", [])
            produtos_texto = ", ".join(produtos) if produtos else "NENHUM"
            try:
                await interaction.message.delete()
            except:
                pass
            if self.acao == "registrar":
                modal = RegistrarGrupoModal(tipo_escolhido, produtos_texto)
                await interaction.response.send_modal(modal)
            else:
                modal = EditarGrupoModal(self.dados["grupo_id"], self.dados["dados"], tipo_escolhido, produtos_texto)
                await interaction.response.send_modal(modal)
            return False
        return True

# =========================================================
# ==================== SISTEMA DE LIVES ===================
# =========================================================

# ---------------------------------------------------------
# ASYNC: carregar_lives_db
# ---------------------------------------------------------

async def carregar_lives_db():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM lives")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar lives: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: salvar_live_db
# ---------------------------------------------------------

async def salvar_live_db(user_id, link):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO lives (user_id, link, divulgado) VALUES ($1, $2, false)", str(user_id), link)
    except Exception as e:
        logger.error(f"❌ Erro ao salvar live: {e}")

# ---------------------------------------------------------
# ASYNC: atualizar_divulgado_db
# ---------------------------------------------------------

async def atualizar_divulgado_db(link, valor):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE lives SET divulgado=$1 WHERE link=$2", valor, link)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar divulgado: {e}")

# ---------------------------------------------------------
# ASYNC: remover_live_db
# ---------------------------------------------------------

async def remover_live_db(user_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM lives WHERE user_id=$1", str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao remover live: {e}")

# ---------------------------------------------------------
# ASYNC: salvar_live_manual
# ---------------------------------------------------------

async def salvar_live_manual(user_id, user_name, plataforma, link, titulo, categoria):
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE lives_manual SET ativo = false WHERE user_id = $1 AND ativo = true", str(user_id))
            return await conn.fetchval("INSERT INTO lives_manual (user_id, user_name, plataforma, link, titulo, categoria) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id", str(user_id), user_name, plataforma, link, titulo, categoria)
    except Exception as e:
        logger.error(f"❌ Erro ao salvar live manual: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: buscar_lives_ativas
# ---------------------------------------------------------

async def buscar_lives_ativas():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM lives_manual WHERE ativo = true ORDER BY data_cadastro DESC")
    except Exception as e:
        logger.error(f"❌ Erro ao buscar lives ativas: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: desativar_live_manual
# ---------------------------------------------------------

async def desativar_live_manual(live_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE lives_manual SET ativo = false WHERE id = $1", live_id)
    except Exception as e:
        logger.error(f"❌ Erro ao desativar live manual: {e}")

# ---------------------------------------------------------
# ASYNC: obter_token_twitch
# ---------------------------------------------------------

async def obter_token_twitch():
    global twitch_token, twitch_token_expira
    agora_ts = time_module.time()
    if twitch_token and agora_ts < twitch_token_expira:
        return twitch_token
    url = "https://id.twitch.tv/oauth2/token"
    params = {"client_id": TWITCH_CLIENT_ID, "client_secret": TWITCH_CLIENT_SECRET, "grant_type": "client_credentials"}
    try:
        async with http_session.post(url, params=params) as r:
            data = await r.json()
            if "access_token" not in data:
                logger.error(f"Erro Twitch API: {data}")
                return None
            twitch_token = data["access_token"]
            twitch_token_expira = agora_ts + data["expires_in"] - 100
            return twitch_token
    except Exception as e:
        logger.error(f"❌ Erro ao obter token Twitch: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: checar_twitch
# ---------------------------------------------------------

async def checar_twitch(canal):
    try:
        token = await obter_token_twitch()
        if not token:
            return False, None, None, None
        headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"}
        url = f"https://api.twitch.tv/helix/streams?user_login={canal}"
        async with http_session.get(url, headers=headers, timeout=10) as r:
            if r.status != 200:
                return False, None, None, None
            data = await r.json()
            if data.get("data"):
                info = data["data"][0]
                thumbnail = info["thumbnail_url"].replace("{width}", "1280").replace("{height}", "720")
                return True, info.get("title"), info.get("game_name"), thumbnail
        return False, None, None, None
    except Exception as e:
        logger.error(f"Erro Twitch API para {canal}: {e}")
        return False, None, None, None

# ---------------------------------------------------------
# ASYNC: divulgar_live
# ---------------------------------------------------------

async def divulgar_live(user_id, link, titulo, jogo, thumbnail, plataforma=None):
    try:
        canal = bot.get_channel(CANAL_DIVULGACAO_LIVE_ID)
        if not canal:
            return False
        user = await pegar_usuario(int(user_id))
        if not user:
            return False
        if not plataforma:
            plataforma = detectar_plataforma(link) or "desconhecida"
        cores = {"twitch": 0x9146FF, "kick": 0x53FC18, "tiktok": 0x000000, "youtube": 0xFF0000, "desconhecida": 0x808080}
        nomes = {"twitch": "Twitch", "kick": "Kick", "tiktok": "TikTok", "youtube": "YouTube", "desconhecida": "Desconhecida"}
        icones = {"twitch": "🟣", "kick": "🟢", "tiktok": "📱", "youtube": "▶️", "desconhecida": "🔴"}
        thumbnails = {"twitch": "https://www.twitch.tv/favicon.ico", "kick": "https://kick.com/favicon.ico", "tiktok": "https://www.tiktok.com/favicon.ico", "youtube": "https://www.youtube.com/favicon.ico"}
        plataforma_nome = nomes.get(plataforma, plataforma.upper())
        icone = icones.get(plataforma, "🔴")
        cor = cores.get(plataforma, 0x808080)
        thumb = thumbnails.get(plataforma)
        embed = discord.Embed(title=f"{icone} LIVE AO VIVO!", color=cor, timestamp=agora())
        descricao = f"👤 **Streamer:** {user.mention}\n📺 **Plataforma:** {plataforma_nome}\n"
        if jogo and jogo != "TikTok" and jogo != "None" and jogo.strip():
            descricao += f"🎮 **Jogo:** {jogo}\n"
        descricao += f"📝 **Título:** {titulo or 'Sem título'}\n\n🔗 **Assistir:** {link}"
        embed.description = descricao
        if thumbnail and thumbnail != "None" and thumbnail.startswith("http"):
            embed.set_image(url=thumbnail)
        elif thumb:
            embed.set_thumbnail(url=thumb)
        embed.set_footer(text=f"Live detectada • {agora().strftime('%d/%m/%Y %H:%M:%S')}")
        await safe_request(canal.send, content="@everyone 🔴 **LIVE INICIADA!**", embed=embed, allowed_mentions=discord.AllowedMentions(everyone=True))
        return True
    except Exception as e:
        logger.error(f"❌ ERRO ao divulgar live: {e}")
        return False

# ---------------------------------------------------------
# ASYNC: enviar_painel_lives
# ---------------------------------------------------------

async def enviar_painel_lives():
    canal = bot.get_channel(CANAL_CADASTRO_LIVE_ID)
    if not canal:
        logger.error("❌ Canal cadastro live não encontrado")
        return
    embed = discord.Embed(
        title="🎥 SISTEMA DE LIVES",
        description="**Gerencie suas lives de forma simples e rápida!**\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🟣 **TWITCH - AUTOMÁTICO**\n• Cadastre sua live **uma única vez**\n• Quando entrar ao vivo, o bot **anuncia automaticamente**\n• Você não precisa fazer mais nada!\n\n🟢 **KICK / TIKTOK / YOUTUBE - MANUAL**\n• **Toda vez** que for começar a live, publique manualmente\n• Preencha as informações e clique em 'Publicar Live'\n• O anúncio vai imediatamente para o canal de divulgação\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📢 **Todas as lives vão para:** <#1243325102917943335>\n⚠️ **Importante:** O link deve ser válido e acessível!",
        color=0x9146FF, timestamp=agora()
    )
    embed.set_thumbnail(url="https://www.twitch.tv/favicon.ico")
    embed.set_footer(text="Vida Rasa • Sistema de Lives")
    try:
        async for msg in canal.history(limit=30):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    await asyncio.sleep(0.3)
                except:
                    pass
        await canal.send(embed=embed, view=PainelLivesUnicoView())
    except Exception as e:
        logger.error(f"❌ Erro ao enviar painel de lives: {e}")

# =========================================================
# CLASS: CadastrarLiveModal
# =========================================================

class CadastrarLiveModal(discord.ui.Modal, title="🎥 Cadastrar Live"):
    link = discord.ui.TextInput(label="Cole o link da sua live", placeholder="https://kick.com/seucanal ou https://twitch.tv/seucanal")

    async def on_submit(self, interaction: discord.Interaction):
        lives = await carregar_lives_db()
        novo_link = self.link.value.strip().lower()
        novo_link = novo_link.split("?")[0].rstrip("/")
        plataforma = detectar_plataforma(novo_link)
        novo_canal = extrair_canal(novo_link)
        if not plataforma or not novo_canal:
            await interaction.response.send_message("❌ Link inválido.", ephemeral=True)
            return
        for row in lives:
            if str(row["user_id"]) != str(interaction.user.id):
                continue
            link_existente = row["link"]
            if extrair_canal(link_existente) == novo_canal and detectar_plataforma(link_existente) == plataforma:
                await interaction.response.send_message(f"❌ Você já cadastrou o canal **{novo_canal}** na plataforma **{plataforma}**!", ephemeral=True)
                return
        await salvar_live_db(interaction.user.id, novo_link)
        embed = discord.Embed(title="✅ Live cadastrada!", description=f"{interaction.user.mention}\n📺 **{plataforma.upper()}** - {novo_link}", color=0x2ecc71)
        await interaction.response.send_message(embed=embed, ephemeral=True)

# =========================================================
# CLASS: CadastrarLiveView
# =========================================================

class CadastrarLiveView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎥 Cadastrar minha Live", style=discord.ButtonStyle.primary, custom_id="cadastrar_live_btn")
    async def cadastrar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(CadastrarLiveModal())

# =========================================================
# CLASS: PainelLivesUnicoView
# =========================================================

class PainelLivesUnicoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎥 Cadastrar Twitch", style=discord.ButtonStyle.primary, custom_id="cadastrar_twitch", emoji="🎥")
    async def cadastrar_twitch(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(CadastrarLiveModal())

    @discord.ui.button(label="📢 Publicar Live", style=discord.ButtonStyle.success, custom_id="publicar_live", emoji="📢")
    async def publicar_live(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PublicarLiveManualModal(interaction.user.id, interaction.user.display_name))

    @discord.ui.button(label="⚙️ Gerenciar", style=discord.ButtonStyle.secondary, custom_id="gerenciar_lives_adm", emoji="⚙️")
    async def gerenciar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != ADM_ID:
            await interaction.response.send_message("❌ Apenas ADM podem usar este recurso!", ephemeral=True)
            return
        view = GerenciarLivesView()
        await interaction.response.send_message("**⚙️ PAINEL DE GERENCIAMENTO DE LIVES**\n\n📋 **Ver Lives** - Lista todas as lives cadastradas\n🗑️ **Remover Live** - Remove um usuário e todas as suas lives", view=view, ephemeral=True)

# =========================================================
# CLASS: PublicarLiveManualModal
# =========================================================

class PublicarLiveManualModal(discord.ui.Modal, title="📢 PUBLICAR LIVE"):
    def __init__(self, user_id, user_name):
        super().__init__()
        self.user_id = user_id
        self.user_name = user_name
    plataforma = discord.ui.TextInput(label="📺 PLATAFORMA", placeholder="EX: KICK, TIKTOK, YOUTUBE", required=True)
    link = discord.ui.TextInput(label="🔗 LINK DA LIVE", placeholder="https://kick.com/seu_canal", required=True)
    titulo = discord.ui.TextInput(label="📝 TÍTULO DA LIVE", placeholder="EX: MUITA AÇÃO NA VDR!", required=True)
    jogo = discord.ui.TextInput(label="🎮 JOGO/CATEGORIA", placeholder="EX: GTA RP, MINECRAFT", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        plataforma = self.plataforma.value.strip().upper()
        link = self.link.value.strip()
        titulo = self.titulo.value.strip()
        jogo = self.jogo.value.strip()
        if not link.startswith("http://") and not link.startswith("https://"):
            link = f"https://{link}"
        resultado = await divulgar_live(user_id=self.user_id, link=link, titulo=titulo, jogo=jogo, thumbnail=None, plataforma=plataforma.lower())
        if resultado:
            await interaction.response.send_message(f"✅ **LIVE PUBLICADA COM SUCESSO!**\n\n📺 **Plataforma:** {plataforma}\n🔗 **Link:** {link}\n📝 **Título:** {titulo}\n🎮 **Jogo:** {jogo}", ephemeral=True)
        else:
            await interaction.response.send_message("❌ **ERRO AO PUBLICAR LIVE!**", ephemeral=True)

# =========================================================
# CLASS: RemoverLiveSelect
# =========================================================

class RemoverLiveSelect(discord.ui.Select):
    def __init__(self, lives):
        options = []
        usuarios_vistos = set()
        for row in lives:
            uid = row["user_id"]
            if uid in usuarios_vistos:
                continue
            usuarios_vistos.add(uid)
            user = bot.get_user(int(uid))
            nome = user.display_name if user else f"ID: {uid}"
            options.append(discord.SelectOption(label=nome, value=uid, emoji="🎥"))
        if not options:
            options = [discord.SelectOption(label="Nenhuma live", value="none", emoji="📭")]
        super().__init__(placeholder="Selecione o usuário", options=options)
        self.lives = lives

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != ADM_ID:
            await interaction.response.send_message("❌ Apenas ADM.", ephemeral=True)
            return
        user_id = self.values[0]
        if user_id == "none":
            await interaction.response.send_message("📭 Nenhuma live cadastrada.", ephemeral=True)
            return
        user = bot.get_user(int(user_id))
        nome = user.display_name if user else user_id
        original_message = interaction.message
        view = ConfirmarRemoverView(user_id, nome, original_message)
        await interaction.response.edit_message(content=f"⚠️ **Remover todas as lives de {nome}?**\nEsta ação é irreversível!", view=view)

# =========================================================
# CLASS: ConfirmarRemoverView
# =========================================================

class ConfirmarRemoverView(discord.ui.View):
    def __init__(self, user_id, nome, message):
        super().__init__(timeout=30)
        self.user_id = user_id
        self.nome = nome
        self.message = message

    @discord.ui.button(label="✅ Sim, remover", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirmar(self, interaction: discord.Interaction, button):
        if interaction.user.id != ADM_ID:
            await interaction.response.send_message("❌ Apenas ADM.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await remover_live_db(self.user_id)
        await interaction.followup.send(f"✅ **Lives removidas com sucesso!**\nUsuário: {self.nome}", ephemeral=True)
        try:
            await self.message.delete()
        except:
            pass
        await enviar_painel_lives()

    @discord.ui.button(label="❌ Cancelar", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancelar(self, interaction: discord.Interaction, button):
        if interaction.user.id != ADM_ID:
            await interaction.response.send_message("❌ Apenas ADM.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send("❌ Operação cancelada.", ephemeral=True)
        try:
            await self.message.delete()
        except:
            pass

# =========================================================
# CLASS: GerenciarLivesView
# =========================================================

class GerenciarLivesView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)

    @discord.ui.button(label="📋 Ver Lives", style=discord.ButtonStyle.secondary, emoji="📋")
    async def ver(self, interaction: discord.Interaction, button):
        if interaction.user.id != ADM_ID:
            await interaction.response.send_message("❌ Apenas ADM.", ephemeral=True)
            return
        await interaction.response.defer()
        lives = await carregar_lives_db()
        if not lives:
            await interaction.followup.send("📭 Nenhuma live cadastrada.", ephemeral=True)
            return
        texto = "**📡 LIVES CADASTRADAS:**\n\n"
        grouped = {}
        for row in lives:
            uid = row["user_id"]
            if uid not in grouped:
                grouped[uid] = []
            grouped[uid].append(row)
        for uid, lista in grouped.items():
            user = bot.get_user(int(uid))
            nome = user.display_name if user else uid
            texto += f"👤 **{nome}** (ID: {uid})\n"
            for live in lista:
                link = live["link"]
                divulgado = "✅ Divulgado" if live["divulgado"] else "⏳ Pendente"
                plataforma = detectar_plataforma(link)
                texto += f"   📺 {plataforma.upper()}: {link} - {divulgado}\n"
            texto += "\n"
        if len(texto) > 2000:
            partes = [texto[i:i+1900] for i in range(0, len(texto), 1900)]
            for parte in partes:
                await interaction.followup.send(parte, ephemeral=True)
        else:
            await interaction.followup.send(texto, ephemeral=True)

    @discord.ui.button(label="🗑️ Remover Live", style=discord.ButtonStyle.danger, emoji="🗑️")
    async def remover(self, interaction: discord.Interaction, button):
        if interaction.user.id != ADM_ID:
            await interaction.response.send_message("❌ Apenas ADM.", ephemeral=True)
            return
        lives = await carregar_lives_db()
        if not lives:
            await interaction.response.send_message("📭 Nenhuma live cadastrada para remover.", ephemeral=True)
            return
        view = discord.ui.View(timeout=60)
        view.add_item(RemoverLiveSelect(lives))
        view.add_item(FecharButtonRemover())
        await interaction.response.send_message("📋 **Selecione o usuário para remover as lives:**", view=view, ephemeral=True)

# =========================================================
# CLASS: FecharButtonRemover
# =========================================================

class FecharButtonRemover(discord.ui.Button):
    def __init__(self):
        super().__init__(label="❌ Fechar", style=discord.ButtonStyle.danger, emoji="❌")

    async def callback(self, interaction: discord.Interaction):
        try:
            await interaction.message.delete()
        except:
            pass

# =========================================================
# CLASS: CadastrarLiveManualModal
# =========================================================

class CadastrarLiveManualModal(discord.ui.Modal, title="🎥 CADASTRAR LIVE"):
    def __init__(self, user_id, user_name):
        super().__init__()
        self.user_id = user_id
        self.user_name = user_name
    plataforma = discord.ui.TextInput(label="📺 PLATAFORMA", placeholder="EX: KICK, TIKTOK, YOUTUBE, ETC", required=True)
    link = discord.ui.TextInput(label="🔗 LINK DA LIVE", placeholder="https://kick.com/seu_canal", required=True)
    titulo = discord.ui.TextInput(label="📝 TÍTULO DA LIVE (OPCIONAL)", placeholder="EX: MUITA AÇÃO NA VDR!", required=False)
    categoria = discord.ui.TextInput(label="🎮 CATEGORIA/JOGO (OPCIONAL)", placeholder="EX: GTA RP, MINECRAFT, ETC", required=False)

    async def on_submit(self, interaction: discord.Interaction):
        plataforma = self.plataforma.value.strip().upper()
        link = self.link.value.strip()
        titulo = self.titulo.value.strip() if self.titulo.value else None
        categoria = self.categoria.value.strip() if self.categoria.value else None
        if not link.startswith("http://") and not link.startswith("https://"):
            link = f"https://{link}"
        await salvar_live_manual(self.user_id, self.user_name, plataforma, link, titulo, categoria)
        embed = discord.Embed(title="✅ LIVE CADASTRADA COM SUCESSO!", description=f"📺 **Plataforma:** {plataforma}\n🔗 **Link:** {link}\n📝 **Título:** {titulo or 'Não informado'}\n🎮 **Categoria:** {categoria or 'Não informado'}\n\n📢 **Quando for começar a live, clique no botão 'ANUNCIAR LIVE'**", color=0x2ecc71)
        embed.set_footer(text="Sistema de Lives • VDR")
        await interaction.response.send_message(embed=embed, ephemeral=True)

# =========================================================
# CLASS: GerenciarLiveView
# =========================================================

class GerenciarLiveView(discord.ui.View):
    def __init__(self, user_id, user_name):
        super().__init__(timeout=None)
        self.user_id = user_id
        self.user_name = user_name

    @discord.ui.button(label="📝 Cadastrar/Atualizar Live", style=discord.ButtonStyle.primary, custom_id="cadastrar_live_manual", emoji="📝")
    async def cadastrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != str(self.user_id):
            await interaction.response.send_message("❌ Apenas o dono desta live pode cadastrar/atualizar!", ephemeral=True)
            return
        await interaction.response.send_modal(CadastrarLiveManualModal(self.user_id, self.user_name))

    @discord.ui.button(label="📢 ANUNCIAR LIVE", style=discord.ButtonStyle.success, custom_id="anunciar_live_manual", emoji="📢")
    async def anunciar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != str(self.user_id):
            await interaction.response.send_message("❌ Apenas o dono desta live pode anunciar!", ephemeral=True)
            return
        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            live = await conn.fetchrow("SELECT * FROM lives_manual WHERE user_id = $1 AND ativo = true", str(self.user_id))
        if not live:
            await interaction.response.send_message("❌ **Você não tem uma live cadastrada!**\nClique em 'Cadastrar/Atualizar Live' primeiro.", ephemeral=True)
            return
        plataforma = live["plataforma"].upper()
        link = live["link"]
        titulo = live["titulo"] or "Live ao vivo!"
        categoria = live["categoria"] or "GTA RP"
        cores = {"KICK": 0x53FC18, "TIKTOK": 0x000000, "YOUTUBE": 0xFF0000, "TWITCH": 0x9146FF}
        icones = {"KICK": "🟢", "TIKTOK": "📱", "YOUTUBE": "▶️", "TWITCH": "🟣"}
        color = cores.get(plataforma, 0x2ecc71)
        icone = icones.get(plataforma, "🔴")
        embed = discord.Embed(title=f"{icone} LIVE AO VIVO!", description=f"👤 **Streamer:** {interaction.user.mention}\n📺 **Plataforma:** {plataforma}\n🎮 **Jogo:** {categoria}\n📝 **Título:** {titulo}\n\n🔗 **Assistir:** {link}", color=color, timestamp=agora())
        if plataforma == "KICK":
            embed.set_thumbnail(url="https://kick.com/favicon.ico")
        elif plataforma == "TWITCH":
            embed.set_thumbnail(url="https://www.twitch.tv/favicon.ico")
        elif plataforma == "TIKTOK":
            embed.set_thumbnail(url="https://www.tiktok.com/favicon.ico")
        embed.set_footer(text=f"Live iniciada • {agora().strftime('%d/%m/%Y %H:%M')}")
        canal_divulgacao = interaction.guild.get_channel(CANAL_DIVULGACAO_LIVE_ID)
        if not canal_divulgacao:
            await interaction.response.send_message("❌ Canal de divulgação não encontrado!", ephemeral=True)
            return
        await safe_request(canal_divulgacao.send, content=f"@everyone 🔴 **LIVE INICIADA!**", embed=embed, allowed_mentions=discord.AllowedMentions(everyone=True))
        await desativar_live_manual(live["id"])
        await interaction.response.send_message(f"✅ **LIVE ANUNCIADA COM SUCESSO!**\n📢 Anúncio enviado para <#{CANAL_DIVULGACAO_LIVE_ID}>", ephemeral=True)

    @discord.ui.button(label="❌ Cancelar Live", style=discord.ButtonStyle.danger, custom_id="cancelar_live_manual", emoji="❌")
    async def cancelar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != str(self.user_id):
            await interaction.response.send_message("❌ Apenas o dono desta live pode cancelar!", ephemeral=True)
            return
        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            live = await conn.fetchrow("SELECT id FROM lives_manual WHERE user_id = $1 AND ativo = true", str(self.user_id))
        if not live:
            await interaction.response.send_message("❌ Você não tem uma live ativa para cancelar!", ephemeral=True)
            return
        await desativar_live_manual(live["id"])
        await interaction.response.send_message("✅ **Live cancelada com sucesso!**\nVocê pode cadastrar uma nova live quando quiser.", ephemeral=True)

# =========================================================
# CLASS: PainelLivesManualView
# =========================================================

class PainelLivesManualView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎥 Minha Live", style=discord.ButtonStyle.primary, custom_id="minha_live_manual", emoji="🎥")
    async def minha_live(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = GerenciarLiveView(interaction.user.id, interaction.user.display_name)
        embed = discord.Embed(title="🎥 GERENCIAR MINHA LIVE", description="**📌 Como funciona:**\n\n1. Clique em **'Cadastrar/Atualizar Live'**\n2. Informe a plataforma (Kick, TikTok, etc)\n3. Cole o link da sua live\n4. Quando começar, clique em **'ANUNCIAR LIVE'**\n\n✅ **Plataformas suportadas:**\n• 🟢 Kick\n• 📱 TikTok\n• ▶️ YouTube\n• E qualquer outra!", color=0x3498db)
        embed.set_footer(text="Sistema de Lives • VDR")
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# =========================================================
# TASK: verificar_lives
# =========================================================

@tasks.loop(minutes=2)
async def verificar_lives():
    try:
        lives = await carregar_lives_db()
        if not lives:
            return
        for row in lives:
            user_id = row["user_id"]
            link = row["link"]
            divulgado = row["divulgado"]
            if not link:
                continue
            plataforma = detectar_plataforma(link)
            canal_name = extrair_canal(link)
            if not plataforma or not canal_name:
                continue
            if plataforma != "twitch":
                continue
            ao_vivo = False
            titulo = None
            jogo = None
            thumbnail = None
            try:
                ao_vivo, titulo, jogo, thumbnail = await checar_twitch(canal_name)
            except Exception as e:
                logger.error(f"❌ Erro ao verificar Twitch/{canal_name}: {e}")
                continue
            if not ao_vivo and divulgado:
                await atualizar_divulgado_db(link, False)
            if ao_vivo and not divulgado:
                resultado = await divulgar_live(user_id, link, titulo, jogo, thumbnail)
                if resultado:
                    await atualizar_divulgado_db(link, True)
    except Exception as e:
        logger.error(f"❌ Erro no loop de lives: {e}")

# =========================================================
# TASK: limpar_cache_lives
# =========================================================

@tasks.loop(minutes=10)
async def limpar_cache_lives():
    global cache_lives
    agora_ts = time_module.time()
    keys_to_remove = []
    for key, (_, timestamp) in cache_lives.items():
        if agora_ts - timestamp > CACHE_LIVES_TTL:
            keys_to_remove.append(key)
    for key in keys_to_remove:
        del cache_lives[key]
    if keys_to_remove:
        logger.info(f"🧹 Cache de lives limpo: {len(keys_to_remove)} entradas removidas")

# =========================================================
# ==================== SISTEMA DE AUSÊNCIA ================
# =========================================================

# ---------------------------------------------------------
# ASYNC: salvar_ausencia_db
# ---------------------------------------------------------

async def salvar_ausencia_db(user_id, nome, motivo, data_inicio, data_fim):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO ausencias (user_id, nome, motivo, data_inicio, data_fim, ativo) VALUES ($1, $2, $3, $4, $5, true)",
                str(user_id), nome, motivo, data_inicio, data_fim
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar ausência: {e}")

# ---------------------------------------------------------
# ASYNC: buscar_ausencias_ativas_db
# ---------------------------------------------------------

async def buscar_ausencias_ativas_db():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM ausencias WHERE ativo = true AND data_fim > NOW() ORDER BY data_fim ASC")
    except Exception as e:
        logger.error(f"❌ Erro ao buscar ausências ativas: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: buscar_ausencia_por_user
# ---------------------------------------------------------

async def buscar_ausencia_por_user(user_id):
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM ausencias WHERE user_id = $1 AND ativo = true AND data_fim > NOW()", str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao buscar ausência por usuário: {e}")
        return None

# ---------------------------------------------------------
# ASYNC: desativar_ausencia
# ---------------------------------------------------------

async def desativar_ausencia(user_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE ausencias SET ativo = false WHERE user_id = $1 AND ativo = true", str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao desativar ausência: {e}")

# ---------------------------------------------------------
# ASYNC: remover_ausencias_expiradas
# ---------------------------------------------------------

async def remover_ausencias_expiradas():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM ausencias WHERE ativo = true AND data_fim <= NOW()")
            for row in rows:
                await conn.execute("UPDATE ausencias SET ativo = false WHERE user_id = $1", row["user_id"])
            return [row["user_id"] for row in rows]
    except Exception as e:
        logger.error(f"❌ Erro ao remover ausências expiradas: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: enviar_painel_botao_ausencia
# ---------------------------------------------------------

async def enviar_painel_botao_ausencia():
    canal = bot.get_channel(CANAL_BOTAO_AUSENCIA_ID)
    if not canal:
        logger.error(f"❌ Canal do botão NÃO ENCONTRADO! ID: {CANAL_BOTAO_AUSENCIA_ID}")
        return
    embed = discord.Embed(
        title="📋 Solicitar Ausência",
        description="Clique no botão abaixo para solicitar sua ausência.\n\n📌 **Como usar:**\n• Digite seu nome completo\n• Informe a **data de INÍCIO** (ex: `10/04/2026`)\n• Informe a **data de RETORNO** (ex: `15/04/2026`)\n• Digite o motivo\n\n✅ Você receberá o cargo **Ausente**\n✅ Quando o período acabar, o cargo será removido\n\n⚠️ **Ausências de 15 dias ou mais** serão notificadas à gerência",
        color=0xe67e22
    )
    embed.add_field(name="📅 Exemplo", value="• Data INÍCIO: `10/04/2026`\n• Data RETORNO: `15/04/2026`\n(contando todos os dias entre 10 e 15)", inline=False)
    await enviar_ou_atualizar_painel("painel_botao_ausencia", CANAL_BOTAO_AUSENCIA_ID, embed, AusenciaBotaoView())

# ---------------------------------------------------------
# ASYNC: enviar_painel_remover_ausencia
# ---------------------------------------------------------

async def enviar_painel_remover_ausencia():
    canal = bot.get_channel(CANAL_BOTAO_AUSENCIA_ID)
    if not canal:
        logger.error(f"❌ Canal do botão NÃO ENCONTRADO! ID: {CANAL_BOTAO_AUSENCIA_ID}")
        return
    try:
        async for msg in canal.history(limit=30):
            if msg.author == bot.user and msg.embeds and len(msg.embeds) > 0 and msg.embeds[0].title == "🔄 Remover Ausência (Retorno Antecipado)":
                return
        embed = discord.Embed(
            title="🔄 Remover Ausência (Retorno Antecipado)",
            description="Clique no botão abaixo caso um membro tenha **retornado antes do previsto**.\n\n⚠️ **Apenas para:** Gerente, Cargo 01, Cargo 02 e Gerente Geral",
            color=0x3498db
        )
        embed.add_field(name="📌 Como usar", value="1. Clique no botão\n2. Selecione o membro na lista\n3. Confirme a remoção\n\nO cargo **Ausente** será removido imediatamente.", inline=False)
        await enviar_ou_atualizar_painel("painel_remover_ausencia", CANAL_BOTAO_AUSENCIA_ID, embed, BotaoRemoverAusenciaView())
    except Exception as e:
        logger.error(f"❌ Erro ao enviar painel remover ausência: {e}")

# =========================================================
# CLASS: AusenciaModal
# =========================================================

class AusenciaModal(discord.ui.Modal, title="📝 Solicitar Ausência"):
    nome = discord.ui.TextInput(label="Seu nome completo", placeholder="Digite seu nome", required=True)
    data_inicio = discord.ui.TextInput(label="Data de INÍCIO da ausência", placeholder="Ex: 10/04/2026", required=True)
    data_fim = discord.ui.TextInput(label="Data de RETORNO", placeholder="Ex: 15/04/2026", required=True)
    motivo = discord.ui.TextInput(label="Motivo da ausência", placeholder="Ex: Viagem, Problemas de saúde, etc", style=discord.TextStyle.paragraph, required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            data_inicio_dt = datetime.strptime(self.data_inicio.value.strip(), "%d/%m/%Y")
            data_fim_dt = datetime.strptime(self.data_fim.value.strip(), "%d/%m/%Y")
            data_inicio_naive = data_inicio_dt.replace(hour=0, minute=0, second=0)
            data_fim_naive = data_fim_dt.replace(hour=23, minute=59, second=59)
        except ValueError:
            await interaction.followup.send("❌ Formato de data inválido!", ephemeral=True)
            return
        if data_fim_naive <= data_inicio_naive:
            await interaction.followup.send("❌ A data de RETORNO deve ser **depois** da data de INÍCIO!", ephemeral=True)
            return
        ausencia_existente = await buscar_ausencia_por_user(interaction.user.id)
        if ausencia_existente:
            await interaction.followup.send("❌ Você já possui uma ausência ativa!", ephemeral=True)
            return
        dias_ausencia = (data_fim_naive - data_inicio_naive).days + 1
        if dias_ausencia >= 15:
            canal_gerencia = interaction.guild.get_channel(CANAL_GERENCIA_ID)
            if canal_gerencia:
                embed_alerta = discord.Embed(title="⚠️ AUSÊNCIA PROLONGADA", description=f"{interaction.user.mention} solicitou ausência de **{dias_ausencia} dias**!", color=0xe74c3c)
                embed_alerta.add_field(name="👤 Nome", value=self.nome.value, inline=True)
                embed_alerta.add_field(name="📅 Período", value=f"{self.data_inicio.value} a {self.data_fim.value}", inline=True)
                embed_alerta.add_field(name="📝 Motivo", value=self.motivo.value[:100], inline=False)
                embed_alerta.add_field(name="⚠️ Ação necessária", value="Este membro deve ser **removido do tablet** durante o período de ausência.", inline=False)
                embed_alerta.set_footer(text="Gerência, tomem as providências necessárias.")
                await canal_gerencia.send(embed=embed_alerta)
        await salvar_ausencia_db(interaction.user.id, self.nome.value, self.motivo.value, data_inicio_naive, data_fim_naive)
        cargo = interaction.guild.get_role(CARGO_AUSENTE_ID)
        if cargo:
            await interaction.user.add_roles(cargo)
        canal_registro = interaction.guild.get_channel(CANAL_REGISTRO_AUSENCIA_ID)
        if canal_registro:
            embed_ausencia = discord.Embed(title="📋 AUSÊNCIA REGISTRADA", description=f"{interaction.user.mention} está ausente!", color=0xe67e22)
            embed_ausencia.add_field(name="👤 Nome", value=self.nome.value, inline=True)
            embed_ausencia.add_field(name="📅 Período", value=f"{self.data_inicio.value} a {self.data_fim.value}", inline=True)
            embed_ausencia.add_field(name="⏳ Total de dias", value=f"{dias_ausencia} dia(s)", inline=True)
            embed_ausencia.add_field(name="📝 Motivo", value=self.motivo.value, inline=False)
            if dias_ausencia >= 15:
                embed_ausencia.add_field(name="⚠️ Atenção", value="Ausência prolongada! Gerência notificada.", inline=False)
            embed_ausencia.set_footer(text=f"Solicitado em {agora().strftime('%d/%m/%Y às %H:%M')}")
            await canal_registro.send(embed=embed_ausencia)
        embed_privado = discord.Embed(title="✅ Ausência Registrada!", color=0x2ecc71)
        embed_privado.add_field(name="👤 Nome", value=self.nome.value, inline=True)
        embed_privado.add_field(name="📅 Período", value=f"{self.data_inicio.value} a {self.data_fim.value}", inline=True)
        embed_privado.add_field(name="📝 Motivo", value=self.motivo.value[:100], inline=False)
        if dias_ausencia >= 15:
            embed_privado.add_field(name="⚠️ Observação", value="Por ser uma ausência prolongada (+15 dias), a gerência foi notificada.", inline=False)
        embed_privado.set_footer(text="Quando retornar, seu cargo será removido automaticamente!")
        await interaction.followup.send(embed=embed_privado, ephemeral=True)

# =========================================================
# CLASS: AusenciaBotaoView
# =========================================================

class AusenciaBotaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📝 Solicitar Ausência", style=discord.ButtonStyle.primary, custom_id="ausencia_solicitar_botao")
    async def solicitar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(AusenciaModal())

# =========================================================
# CLASS: RemoverAusenciaSelect
# =========================================================

class RemoverAusenciaSelect(discord.ui.Select):
    def __init__(self, ausencias):
        options = []
        for ausencia in ausencias:
            nome = ausencia['nome'][:50]
            periodo = f"{ausencia['data_inicio'].strftime('%d/%m')} a {ausencia['data_fim'].strftime('%d/%m')}"
            options.append(discord.SelectOption(label=nome, description=f"Período: {periodo}", value=str(ausencia['user_id'])))
        super().__init__(placeholder="Selecione a ausência para remover (volta antecipada)", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        user_id = int(self.values[0])
        member = interaction.guild.get_member(user_id)
        ausencia = await buscar_ausencia_por_user(user_id)
        await desativar_ausencia(user_id)
        cargo = interaction.guild.get_role(CARGO_AUSENTE_ID)
        if cargo and member and cargo in member.roles:
            await member.remove_roles(cargo)
        dias_antecipados = 0
        if ausencia:
            data_fim = ausencia["data_fim"]
            if data_fim.tzinfo is None:
                data_fim = data_fim.replace(tzinfo=BRASIL)
            dias_antecipados = (data_fim - agora()).days + 1
            if dias_antecipados < 0:
                dias_antecipados = 0
        embed = discord.Embed(title="✅ AUSÊNCIA REMOVIDA (RETORNO ANTECIPADO)", description=f"A ausência de {member.mention if member else f'<@{user_id}>'} foi encerrada!", color=0x2ecc71)
        embed.add_field(name="👤 Usuário", value=member.mention if member else f"ID: {user_id}", inline=True)
        if dias_antecipados > 0:
            embed.add_field(name="📅 Dias antecipados", value=f"{dias_antecipados} dia(s) antes do previsto", inline=True)
        embed.add_field(name="📝 Status", value="Cargo ausente removido. Usuário pode solicitar nova ausência.", inline=False)
        await interaction.response.edit_message(content=None, embed=embed, view=None)
        canal_registro = interaction.guild.get_channel(CANAL_REGISTRO_AUSENCIA_ID)
        if canal_registro:
            await canal_registro.send(embed=embed)

# =========================================================
# CLASS: RemoverAusenciaView
# =========================================================

class RemoverAusenciaView(discord.ui.View):
    def __init__(self, ausencias):
        super().__init__(timeout=60)
        self.add_item(RemoverAusenciaSelect(ausencias))

# =========================================================
# CLASS: BotaoRemoverAusenciaView
# =========================================================

class BotaoRemoverAusenciaView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🔄 Remover Ausência (Retorno Antecipado)", style=discord.ButtonStyle.primary, custom_id="remover_ausencia_botao", emoji="🔄")
    async def remover(self, interaction: discord.Interaction, button):
        if not pode_remover_ausencia(interaction.user):
            await interaction.response.send_message("❌ Você não tem permissão para remover ausências!\nApenas **Gerente, Cargo 01, Cargo 02 e Gerente Geral** podem usar este recurso.", ephemeral=True)
            return
        ausencias = await buscar_ausencias_ativas_db()
        if not ausencias:
            await interaction.response.send_message("📭 Nenhuma ausência ativa no momento.", ephemeral=True)
            return
        view = RemoverAusenciaView(ausencias)
        await interaction.response.send_message("📋 Selecione o membro que **retornou antes do previsto**:\n(O cargo ausente será removido imediatamente)", view=view, ephemeral=True)

# =========================================================
# TASK: verificar_ausencias_expiradas
# =========================================================

@tasks.loop(minutes=60)
async def verificar_ausencias_expiradas():
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return
    cargo_ausente = guild.get_role(CARGO_AUSENTE_ID)
    if not cargo_ausente:
        return
    users_para_remover = await remover_ausencias_expiradas()
    for user_id in users_para_remover:
        member = guild.get_member(int(user_id))
        if member and cargo_ausente in member.roles:
            await member.remove_roles(cargo_ausente)
            canal_registro = guild.get_channel(CANAL_REGISTRO_AUSENCIA_ID)
            if canal_registro:
                embed_retorno = discord.Embed(title="🎉 RETORNO REGISTRADO", description=f"{member.mention} retornou! O cargo ausente foi removido automaticamente.", color=0x2ecc71)
                await canal_registro.send(embed=embed_retorno)

# =========================================================
# ==================== SISTEMA DE LAVAGEM =================
# =========================================================

# ---------------------------------------------------------
# ASYNC: salvar_lavagem_db
# ---------------------------------------------------------

async def salvar_lavagem_db(user_id, valor_sujo, taxa, valor_retorno):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO lavagens (user_id, valor, taxa, liquido, data) VALUES ($1,$2,$3,$4,$5)",
                str(user_id), valor_sujo, taxa, valor_retorno, agora_db()
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar lavagem: {e}")

# ---------------------------------------------------------
# ASYNC: carregar_lavagens_db
# ---------------------------------------------------------

async def carregar_lavagens_db():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM lavagens")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar lavagens: {e}")
        return []

# ---------------------------------------------------------
# ASYNC: limpar_lavagens_db
# ---------------------------------------------------------

async def limpar_lavagens_db():
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM lavagens")
    except Exception as e:
        logger.error(f"❌ Erro ao limpar lavagens: {e}")

# ---------------------------------------------------------
# FUNÇÃO: pode_gerenciar_lavagem
# ---------------------------------------------------------

def pode_gerenciar_lavagem(member: discord.Member):
    cargos_permitidos = [CARGO_GERENTE_ID, CARGO_01_ID, CARGO_02_ID, CARGO_GERENTE_GERAL_ID]
    return any(role.id in cargos_permitidos for role in member.roles)

# ---------------------------------------------------------
# ASYNC: enviar_painel_lavagem
# ---------------------------------------------------------

async def enviar_painel_lavagem():
    canal = bot.get_channel(CANAL_INICIAR_LAVAGEM_ID)
    if not canal:
        logger.error("❌ Canal de lavagem não encontrado")
        return
    embed = discord.Embed(title="🧼 Lavagem de Dinheiro", description="Clique para iniciar lavagem.", color=0x27ae60)
    await enviar_ou_atualizar_painel("painel_lavagem", CANAL_INICIAR_LAVAGEM_ID, embed, LavagemView())

# =========================================================
# CLASS: LavagemModal
# =========================================================

class LavagemModal(discord.ui.Modal, title="Iniciar Lavagem"):
    valor = discord.ui.TextInput(label="Valor do dinheiro sujo")

    async def on_submit(self, interaction: discord.Interaction):
        await responder_interacao(interaction, defer=True)
        try:
            valor_sujo = safe_int(self.valor.value)
        except:
            await interaction.followup.send("Valor inválido.", ephemeral=True)
            return
        taxa = 20
        valor_retorno = int(valor_sujo * 0.8)
        msg_info = await interaction.channel.send(f"{interaction.user.mention} envie agora o PRINT da tela.")
        lavagens_pendentes[interaction.user.id] = {"sujo": valor_sujo, "retorno": valor_retorno, "taxa": taxa, "msg_info": msg_info}

# =========================================================
# CLASS: LavagemView
# =========================================================

class LavagemView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Iniciar Lavagem", style=discord.ButtonStyle.primary, custom_id="lavagem_iniciar")
    async def iniciar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(LavagemModal())

    @discord.ui.button(label="🧹 Limpar Sala", style=discord.ButtonStyle.danger, custom_id="lavagem_limpar")
    async def limpar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message("Você não tem permissão.", ephemeral=True)
            return
        canal = interaction.guild.get_channel(CANAL_LAVAGEM_MEMBROS_ID)
        async for msg in canal.history(limit=200):
            try:
                await msg.delete()
            except:
                pass
        await limpar_lavagens_db()
        await interaction.response.send_message("Sala limpa!", ephemeral=True)

    @discord.ui.button(label="📊 Gerar Relatório", style=discord.ButtonStyle.success, custom_id="lavagem_relatorio")
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message("Você não tem permissão.", ephemeral=True)
            return
        dados = await carregar_lavagens_db()
        canal = interaction.guild.get_channel(CANAL_RELATORIO_LAVAGEM_ID)
        for item in dados:
            user = await bot.fetch_user(int(item["user_id"]))
            await canal.send(f"{user.mention} - Valor a repassar: {formatar_dinheiro(item['liquido'])} - Valor sujo: {formatar_dinheiro(item['valor'])}")
        await interaction.response.send_message("Relatório enviado!", ephemeral=True)

    @discord.ui.button(label="📩 Avisar TODOS no DM", style=discord.ButtonStyle.primary, custom_id="lavagem_dm")
    async def avisar_todos(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message("Você não tem permissão.", ephemeral=True)
            return
        dados = await carregar_lavagens_db()
        enviados = 0
        falhas = 0
        for item in dados:
            try:
                user = await bot.fetch_user(int(item["user_id"]))
                await user.send(f"🧼 **Seu dinheiro foi lavado com sucesso!**\n\n💵 Dinheiro informado: {formatar_dinheiro(item['valor'])}\n💰 Valor repassado: {formatar_dinheiro(item['liquido'])}")
                enviados += 1
            except:
                falhas += 1
        await interaction.response.send_message(f"DM enviada para {enviados} membros.\nFalhas: {falhas}", ephemeral=True)

# =========================================================
# ASYNC: on_message_lavagem
# =========================================================

async def on_message_lavagem(message: discord.Message):
    if message.author.bot:
        return
    if message.channel.id == CANAL_INICIAR_LAVAGEM_ID:
        if message.attachments and message.author.id in lavagens_pendentes:
            dados_temp = lavagens_pendentes.pop(message.author.id)
            valor_sujo = dados_temp["sujo"]
            valor_retorno = dados_temp["retorno"]
            taxa = dados_temp["taxa"]
            canal_destino = bot.get_channel(CANAL_LAVAGEM_MEMBROS_ID)
            arquivo = await message.attachments[0].to_file()
            try:
                await message.delete()
            except:
                pass
            try:
                await dados_temp["msg_info"].delete()
            except:
                pass
            await salvar_lavagem_db(message.author.id, valor_sujo, taxa, valor_retorno)
            embed = discord.Embed(title="🧼 Nova Lavagem", color=0x1abc9c)
            embed.add_field(name="Membro", value=message.author.mention, inline=False)
            embed.add_field(name="Valor sujo", value=formatar_dinheiro(valor_sujo), inline=True)
            embed.add_field(name="Valor a repassar (80%)", value=formatar_dinheiro(valor_retorno), inline=True)
            embed.set_image(url=f"attachment://{arquivo.filename}")
            await canal_destino.send(embed=embed, file=arquivo)

# =========================================================
# TASK: limpar_lavagens_pendentes
# =========================================================

@tasks.loop(minutes=15)
async def limpar_lavagens_pendentes():
    lavagens_pendentes.clear()

# =========================================================
# ==================== SISTEMA FINANCEIRO =================
# =========================================================

# ---------------------------------------------------------
# ASYNC: salvar_compra_db
# ---------------------------------------------------------

async def salvar_compra_db(produto, valor, comprado_por):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO compras (produto, valor, comprado_por, data) VALUES ($1, $2, $3, $4)",
                produto, valor, str(comprado_por), agora_db()
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar compra: {e}")

# ---------------------------------------------------------
# ASYNC: enviar_painel_registrar_compra
# ---------------------------------------------------------

async def enviar_painel_registrar_compra():
    canal = bot.get_channel(CANAL_REGISTRAR_COMPRA_ID)
    if not canal:
        logger.error(f"❌ Canal de registrar compra não encontrado: {CANAL_REGISTRAR_COMPRA_ID}")
        return
    embed = discord.Embed(
        title="💰 REGISTRAR COMPRA",
        description="Clique no botão abaixo para registrar uma nova compra.\n\n📋 **Informações necessárias:**\n• 📦 Nome do produto\n• 💰 Valor da compra\n\nApós registrar, a compra aparecerá automaticamente no canal de registros.",
        color=0x3498db
    )
    embed.add_field(name="📌 EXEMPLO", value="**Produto:** Pólvora\n**Valor:** 50000", inline=False)
    embed.set_footer(text="Todas as compras ficam salvas no banco de dados para relatórios futuros")
    try:
        async for msg in canal.history(limit=10):
            if msg.author == bot.user and msg.embeds and len(msg.embeds) > 0 and msg.embeds[0].title == "💰 REGISTRAR COMPRA":
                try:
                    await msg.delete()
                except:
                    pass
        await canal.send(embed=embed, view=RegistrarCompraView())
    except Exception as e:
        logger.error(f"❌ Erro ao enviar painel registrar compra: {e}")

# ---------------------------------------------------------
# ASYNC: enviar_painel_relatorio_financeiro
# ---------------------------------------------------------

async def enviar_painel_relatorio_financeiro():
    canal = bot.get_channel(CANAL_RELATORIO_FINANCEIRO_ID)
    if not canal:
        logger.error("❌ Canal de relatório financeiro não encontrado")
        return
    embed = discord.Embed(
        title="💰 RELATÓRIO FINANCEIRO",
        description="Clique no botão abaixo para gerar um relatório financeiro completo.\n\n📋 **O relatório inclui:**\n• 💣 Pólvora utilizada na produção\n• 💰 Gasto total com pólvora\n• 🛒 Total de vendas no período\n• 📦 Gasto com embalagens (opcional)\n• 📦 Outras compras registradas\n• 📊 Saldo final (vendas - gastos)\n\n📅 **Você pode escolher:**\n• Data inicial e final\n• Incluir ou não outras compras (SIM/NAO)",
        color=0x1abc9c
    )
    embed.add_field(name="📌 EXEMPLO DE PREENCHIMENTO", value="**Data inicial:** `01/04/2026`\n**Data final:** `30/04/2026`\n**Incluir compras:** `SIM` (ou `NAO`)", inline=False)
    embed.set_footer(text="Os valores são calculados automaticamente com base no banco de dados")
    await enviar_ou_atualizar_painel("painel_relatorio_financeiro", CANAL_RELATORIO_FINANCEIRO_ID, embed, RelatorioFinanceiroView())

# =========================================================
# CLASS: RegistrarCompraModal
# =========================================================

class RegistrarCompraModal(discord.ui.Modal, title="📝 Registrar Compra"):
    produto = discord.ui.TextInput(label="📦 Nome do produto", placeholder="Ex: Pólvora, Embalagens, Munição, etc", required=True, max_length=100)
    valor = discord.ui.TextInput(label="💰 Valor da compra", placeholder="Ex: 50000", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        produto = self.produto.value.strip()
        if not produto:
            await interaction.followup.send("❌ **Produto inválido!**", ephemeral=True)
            return
        try:
            valor_compra = safe_int(self.valor.value)
            if valor_compra <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ **Valor inválido!**", ephemeral=True)
            return
        await salvar_compra_db(produto, valor_compra, interaction.user.id)
        data_atual = agora()
        embed = discord.Embed(title="📦 NOVA COMPRA REGISTRADA", color=0x3498db, timestamp=data_atual)
        embed.add_field(name="📦 Produto", value=produto, inline=True)
        embed.add_field(name="💰 Valor", value=formatar_dinheiro(valor_compra), inline=True)
        embed.add_field(name="👤 Comprado por", value=interaction.user.mention, inline=True)
        embed.add_field(name="📅 Data da compra", value=f"<t:{int(data_atual.timestamp())}:F>", inline=False)
        embed.set_footer(text=f"Compra registrada no sistema")
        canal_destino = interaction.guild.get_channel(CANAL_COMPRAS_REGISTRADAS_ID)
        if canal_destino:
            await canal_destino.send(embed=embed)
            await interaction.followup.send(f"✅ **Compra registrada com sucesso!**\n📦 Produto: {produto}\n💰 Valor: {formatar_dinheiro(valor_compra)}", ephemeral=True)
        else:
            await interaction.followup.send(f"✅ **Compra registrada com sucesso!**\n📦 Produto: {produto}\n💰 Valor: {formatar_dinheiro(valor_compra)}", ephemeral=True)

# =========================================================
# CLASS: RegistrarCompraView
# =========================================================

class RegistrarCompraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📝 Registrar Nova Compra", style=discord.ButtonStyle.success, custom_id="registrar_compra_btn", emoji="💰")
    async def registrar_compra(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarCompraModal())

# =========================================================
# CLASS: RelatorioFinanceiroModal
# =========================================================

class RelatorioFinanceiroModal(discord.ui.Modal, title="📊 RELATÓRIO FINANCEIRO"):
    data_inicio = discord.ui.TextInput(label="📅 Data INÍCIO", placeholder="Ex: 01/04/2026", required=True)
    data_fim = discord.ui.TextInput(label="📅 Data FIM", placeholder="Ex: 30/04/2026", required=True)
    incluir_compras = discord.ui.TextInput(label="📦 Incluir compras registradas?", placeholder="Digite SIM ou NAO (padrão é SIM)", required=False)
    embalagens = discord.ui.TextInput(label="📦 Embalagens compradas (opcional)", placeholder="Ex: 25000 (deixe em branco se não quiser)", required=False)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            inicio = datetime.strptime(self.data_inicio.value.strip(), "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value.strip(), "%d/%m/%Y")
            inicio_dt = inicio.replace(hour=0, minute=0, second=0)
            fim_dt = fim.replace(hour=23, minute=59, second=59)
            incluir_compras = self.incluir_compras.value.strip().upper() != "NAO"
            total_embalagens = 0
            total_gasto_embalagens = 0
            if self.embalagens.value and self.embalagens.value.strip():
                try:
                    total_embalagens = safe_int(self.embalagens.value)
                    total_gasto_embalagens = int(total_embalagens * PRECO_EMBALAGEM_POR_UNIDADE)
                except:
                    pass
            pool = await get_pool()
            if not pool:
                await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
                return
            async with pool.acquire() as conn:
                polvora_row = await conn.fetchrow("SELECT COALESCE(SUM(polvora), 0) as total_polvora FROM producoes_finalizadas WHERE data >= $1 AND data <= $2", inicio_dt, fim_dt)
                vendas_row = await conn.fetchrow("SELECT COALESCE(SUM(valor), 0) as total_vendas FROM vendas WHERE TO_DATE(data, 'DD/MM/YYYY') BETWEEN $1 AND $2", inicio.date(), fim.date())
                polvora_comprada_row = await conn.fetchrow("SELECT COALESCE(SUM(quantidade), 0) as total_quantidade, COALESCE(SUM(valor), 0) as total_valor FROM polvoras WHERE data::date BETWEEN $1::date AND $2::date", inicio, fim)
                compras_row = None
                total_gasto_compras = 0
                lista_compras = []
                if incluir_compras:
                    compras_row = await conn.fetch("SELECT produto, valor, comprado_por, data FROM compras WHERE data >= $1 AND data <= $2 ORDER BY data DESC", inicio_dt, fim_dt)
                    for compra in compras_row:
                        total_gasto_compras += compra["valor"] or 0
                        lista_compras.append(compra)
            total_polvora_gasta = polvora_row["total_polvora"] or 0
            total_gasto_polvora = total_polvora_gasta * PRECO_POLVORA
            total_vendas = vendas_row["total_vendas"] or 0
            total_polvora_comprada = polvora_comprada_row["total_quantidade"] or 0
            total_gasto_polvora_comprada = polvora_comprada_row["total_valor"] or 0
            total_gastos = total_gasto_polvora + total_gasto_embalagens + total_gasto_compras
            saldo = total_vendas - total_gastos
            embed = discord.Embed(title="📊 RELATÓRIO FINANCEIRO", description=f"📅 **Período:** {self.data_inicio.value} até {self.data_fim.value}", color=0x1abc9c)
            embed.add_field(name="💣 PÓLVORA", value=f"**Utilizada na produção:** {fmt_num(total_polvora_gasta)} unidades\n**💰 Gasto com pólvora:** {formatar_dinheiro(total_gasto_polvora)}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n**Comprada no período:** {fmt_num(total_polvora_comprada)} unidades\n**💰 Gasto na compra:** {formatar_dinheiro(total_gasto_polvora_comprada)}", inline=False)
            if total_embalagens > 0:
                embed.add_field(name="📦 EMBALAGENS", value=f"**Quantidade comprada:** {fmt_num(total_embalagens)} unidades\n**💰 Gasto com embalagens:** {formatar_dinheiro(total_gasto_embalagens)}", inline=False)
            if incluir_compras and lista_compras:
                compras_texto = ""
                for compra in lista_compras[:10]:
                    data = compra["data"]
                    if data.tzinfo is None:
                        data = data.replace(tzinfo=BRASIL)
                    compras_texto += f"• {compra['produto']} - {formatar_dinheiro(compra['valor'])} - {data.strftime('%d/%m')}\n"
                if len(lista_compras) > 10:
                    compras_texto += f"\n*... e mais {len(lista_compras) - 10} compras*"
                embed.add_field(name="📦 OUTRAS COMPRAS", value=f"**Total gasto em outras compras:** {formatar_dinheiro(total_gasto_compras)}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{compras_texto}", inline=False)
            elif incluir_compras:
                embed.add_field(name="📦 OUTRAS COMPRAS", value="Nenhuma compra registrada no período.", inline=False)
            embed.add_field(name="🛒 VENDAS", value=f"**💰 Total de vendas:** {formatar_dinheiro(total_vendas)}", inline=False)
            cor_saldo = 0x2ecc71 if saldo >= 0 else 0xe74c3c
            emoji_saldo = "🟢" if saldo >= 0 else "🔴"
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            detalhe_gastos = f"• Pólvora: {formatar_dinheiro(total_gasto_polvora)}"
            if total_gasto_embalagens > 0:
                detalhe_gastos += f"\n• Embalagens: {formatar_dinheiro(total_gasto_embalagens)}"
            if incluir_compras and total_gasto_compras > 0:
                detalhe_gastos += f"\n• Outras compras: {formatar_dinheiro(total_gasto_compras)}"
            detalhe_gastos += f"\n• **TOTAL:** {formatar_dinheiro(total_gastos)}"
            embed.add_field(name="📊 RESUMO FINANCEIRO", value=f"**💰 Total de Vendas:** {formatar_dinheiro(total_vendas)}\n**💸 Total de Gastos:** {formatar_dinheiro(total_gastos)}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{emoji_saldo} **SALDO:** {formatar_dinheiro(saldo)}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n**📋 DETALHAMENTO DOS GASTOS:**\n{detalhe_gastos}", inline=False)
            embed.set_footer(text=f"Relatório gerado em {agora().strftime('%d/%m/%Y às %H:%M')}")
            canal = interaction.guild.get_channel(CANAL_RELATORIO_FINANCEIRO_ID)
            if canal:
                await canal.send(embed=embed)
                await interaction.followup.send(f"✅ Relatório financeiro enviado!", ephemeral=True)
            else:
                await interaction.followup.send(embed=embed, ephemeral=True)
        except ValueError:
            await interaction.followup.send("❌ **Formato de data inválido!**", ephemeral=True)
        except Exception as e:
            logger.error(f"ERRO RELATORIO FINANCEIRO: {e}")
            await interaction.followup.send(f"❌ Erro ao gerar relatório: {str(e)}", ephemeral=True)

# =========================================================
# CLASS: RelatorioFinanceiroView
# =========================================================

class RelatorioFinanceiroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📊 Gerar Relatório Financeiro", style=discord.ButtonStyle.success, custom_id="relatorio_financeiro_btn", emoji="💰")
    async def gerar_relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RelatorioFinanceiroModal())

# =========================================================
# ==================== SISTEMA DE MENSAGENS ===============
# =========================================================

# ---------------------------------------------------------
# ASYNC: limpar_mensagem_andamento
# ---------------------------------------------------------

async def limpar_mensagem_andamento(user_id):
    if user_id in mensagens_em_andamento:
        mensagens_em_andamento.remove(user_id)
    if user_id in mensagens_timers:
        del mensagens_timers[user_id]

# ---------------------------------------------------------
# ASYNC: limpar_timer_mensagem
# ---------------------------------------------------------

async def limpar_timer_mensagem(user_id, tempo_segundos):
    await asyncio.sleep(tempo_segundos)
    await limpar_mensagem_andamento(user_id)

# ---------------------------------------------------------
# ASYNC: enviar_painel_mensagens
# ---------------------------------------------------------

async def enviar_painel_mensagens():
    canal = bot.get_channel(CANAL_TEXTOS_VENDAS_ID)
    if not canal:
        logger.error("❌ Canal de textos vendas não encontrado")
        return
    embed = discord.Embed(
        title="📝 GERADOR DE MENSAGENS DE VENDA",
        description="**Clique no botão abaixo para abrir o menu de mensagens.**\n\n📌 **Mensagens disponíveis:**\n• 📦 Pedido Pronto\n• ❌ Pedido Cancelado\n• ✅ Pedido Finalizado\n• 💰 Pendência de Pagamento\n• ⚠️ Pagamento Pendente\n\n🔹 **Como funciona:**\n1. Selecione o tipo de mensagem\n2. Preencha os campos solicitados\n3. A mensagem será gerada automaticamente\n4. Copie e cole no canal desejado",
        color=0x3498db
    )
    embed.add_field(name="📌 DICA", value="• O passaporte é extraído automaticamente do seu apelido no servidor\n• Certifique-se de ter seu apelido no formato: `PASSAPORTE - NOME`\n• Se fechar a janela, espere 5 minutos ou clique em 'Fechar' para liberar", inline=False)
    embed.set_footer(text="Sistema de Mensagens • VDR 442")
    view = MenuMensagensView()
    try:
        async for msg in canal.history(limit=20):
            if msg.author == bot.user and msg.embeds and len(msg.embeds) > 0 and msg.embeds[0].title == "📝 GERADOR DE MENSAGENS DE VENDA":
                try:
                    await msg.edit(embed=embed, view=view)
                    return
                except:
                    pass
        await canal.send(embed=embed, view=view)
    except Exception as e:
        logger.error(f"❌ Erro ao enviar painel de mensagens: {e}")

# =========================================================
# CLASS: MenuMensagensView
# =========================================================

class MenuMensagensView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📝 Gerar Mensagem de Venda", style=discord.ButtonStyle.primary, custom_id="gerar_mensagem_venda", emoji="📝")
    async def gerar_mensagem(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="📝 MENU DE MENSAGENS DE VENDA",
            description="**Selecione o tipo de mensagem que deseja gerar:**\n\n📌 **Opções disponíveis:**\n• 📦 Pedido Pronto\n• ❌ Pedido Cancelado\n• ✅ Pedido Finalizado\n• 💰 Pendência de Pagamento\n• ⚠️ Pagamento Pendente\n\n🔹 **Você precisará informar:**\n• O valor (quando aplicável)\n• Seu passaporte e nome (para a chave PIX)",
            color=0x3498db
        )
        embed.set_footer(text="Clique no botão correspondente à mensagem que deseja gerar")
        view = SelecionarMensagemView()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# =========================================================
# CLASS: SelecionarMensagemView
# =========================================================

class SelecionarMensagemView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)
        self.add_item(discord.ui.Button(label="📦 Pedido Pronto", style=discord.ButtonStyle.success, custom_id="msg_pedido_pronto", emoji="📦", row=0))
        self.add_item(discord.ui.Button(label="❌ Pedido Cancelado", style=discord.ButtonStyle.danger, custom_id="msg_pedido_cancelado", emoji="❌", row=0))
        self.add_item(discord.ui.Button(label="✅ Pedido Finalizado", style=discord.ButtonStyle.success, custom_id="msg_pedido_finalizado", emoji="✅", row=0))
        self.add_item(discord.ui.Button(label="💰 Pendência de Pagamento", style=discord.ButtonStyle.primary, custom_id="msg_pendencia_pagamento", emoji="💰", row=1))
        self.add_item(discord.ui.Button(label="⚠️ Pagamento Pendente", style=discord.ButtonStyle.primary, custom_id="msg_pagamento_pendente", emoji="⚠️", row=1))
        self.add_item(discord.ui.Button(label="❌ Fechar", style=discord.ButtonStyle.secondary, custom_id="fechar_mensagens", emoji="❌", row=1))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        custom_id = interaction.data.get("custom_id", "")
        if custom_id == "fechar_mensagens":
            await limpar_mensagem_andamento(interaction.user.id)
            try:
                await interaction.message.delete()
            except:
                pass
            return False
        handlers = {
            "msg_pedido_pronto": self.handle_pedido_pronto,
            "msg_pedido_cancelado": self.handle_pedido_cancelado,
            "msg_pedido_finalizado": self.handle_pedido_finalizado,
            "msg_pendencia_pagamento": self.handle_pendencia_pagamento,
            "msg_pagamento_pendente": self.handle_pagamento_pendente,
        }
        handler = handlers.get(custom_id)
        if handler:
            await handler(interaction)
            return False
        return True

    async def handle_pedido_pronto(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(limpar_timer_mensagem(interaction.user.id, 300))
        modal = MensagemPedidoProntoModal(interaction.user)
        await interaction.response.send_modal(modal)

    async def handle_pedido_cancelado(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(limpar_timer_mensagem(interaction.user.id, 300))
        modal = MensagemPedidoCanceladoModal()
        await interaction.response.send_modal(modal)

    async def handle_pedido_finalizado(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(limpar_timer_mensagem(interaction.user.id, 300))
        modal = MensagemPedidoFinalizadoModal()
        await interaction.response.send_modal(modal)

    async def handle_pendencia_pagamento(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(limpar_timer_mensagem(interaction.user.id, 300))
        modal = MensagemPendenciaPagamentoModal()
        await interaction.response.send_modal(modal)

    async def handle_pagamento_pendente(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(limpar_timer_mensagem(interaction.user.id, 300))
        modal = MensagemPagamentoPendenteModal(interaction.user)
        await interaction.response.send_modal(modal)

# =========================================================
# CLASS: MensagemPedidoProntoModal
# =========================================================

class MensagemPedidoProntoModal(discord.ui.Modal, title="📦 Pedido Pronto"):
    def __init__(self, usuario):
        super().__init__(timeout=300)
        self.usuario = usuario
    valor = discord.ui.TextInput(label="💰 Valor da encomenda (opcional)", placeholder="Ex: 50000 ou deixe em branco", required=False, max_length=50)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        valor_texto = ""
        if self.valor.value and self.valor.value.strip():
            try:
                valor = safe_int(self.valor.value)
                valor_texto = formatar_dinheiro(valor)
            except:
                valor_texto = self.valor.value
        nome_display = interaction.user.display_name
        passaporte = "SEM PASSAPORTE"
        if " - " in nome_display:
            partes = nome_display.split(" - ", 1)
            passaporte = partes[0]
            nome = partes[1] if len(partes) > 1 else nome_display
        else:
            nome = nome_display
        mensagem = f"""📝 PEDIDO PRONTO!

🚚 Sua encomenda está pronta e será entregue assim que você estiver disponível para receber.

⚠️ Caso não haja ninguém para receber em até 24 horas, o pedido será cancelado automaticamente.

📞 Entre em contato antes do prazo para confirmar o recebimento e evitar o cancelamento.

{passaporte} - {nome} — {agora().strftime('%d/%m/%Y %H:%M')}"""
        if valor_texto:
            mensagem += f"\n💰 Valor: {valor_texto}"
        embed = discord.Embed(title="📋 MENSAGEM GERADA - PEDIDO PRONTO", description="**Copie a mensagem abaixo e cole no canal desejado:**", color=0x2ecc71)
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await limpar_mensagem_andamento(interaction.user.id)
        logger.error(f"Erro no modal: {error}")

# =========================================================
# CLASS: MensagemPedidoCanceladoModal
# =========================================================

class MensagemPedidoCanceladoModal(discord.ui.Modal, title="❌ Pedido Cancelado"):
    def __init__(self):
        super().__init__(timeout=300)
    valor = discord.ui.TextInput(label="💰 Valor da encomenda (opcional)", placeholder="Ex: 50000 ou deixe em branco", required=False, max_length=50)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        valor_texto = ""
        if self.valor.value and self.valor.value.strip():
            try:
                valor = safe_int(self.valor.value)
                valor_texto = f"\n💰 Valor: {formatar_dinheiro(valor)}"
            except:
                valor_texto = f"\n💰 Valor: {self.valor.value}"
        nome_display = interaction.user.display_name
        passaporte = "SEM PASSAPORTE"
        if " - " in nome_display:
            partes = nome_display.split(" - ", 1)
            passaporte = partes[0]
            nome = partes[1] if len(partes) > 1 else nome_display
        else:
            nome = nome_display
        mensagem = f"""❌ PEDIDO CANCELADO

Sua encomenda foi cancelada por não haver ninguém disponível para receber dentro do prazo de 24 horas.

Caso ainda tenha interesse, será necessário realizar um novo pedido.

{passaporte} - {nome} — {agora().strftime('%d/%m/%Y %H:%M')}
{valor_texto}"""
        embed = discord.Embed(title="📋 MENSAGEM GERADA - PEDIDO CANCELADO", description="**Copie a mensagem abaixo e cole no canal desejado:**", color=0xe74c3c)
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await limpar_mensagem_andamento(interaction.user.id)
        logger.error(f"Erro no modal: {error}")

# =========================================================
# CLASS: MensagemPedidoFinalizadoModal
# =========================================================

class MensagemPedidoFinalizadoModal(discord.ui.Modal, title="✅ Pedido Finalizado"):
    def __init__(self):
        super().__init__(timeout=300)
    valor = discord.ui.TextInput(label="💰 Valor da encomenda (opcional)", placeholder="Ex: 50000 ou deixe em branco", required=False, max_length=50)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        valor_texto = ""
        if self.valor.value and self.valor.value.strip():
            try:
                valor = safe_int(self.valor.value)
                valor_texto = f"\n💰 Valor: {formatar_dinheiro(valor)}"
            except:
                valor_texto = f"\n💰 Valor: {self.valor.value}"
        mensagem = f"""✅ PEDIDO FINALIZADO

Sua encomenda foi entregue e o pagamento foi confirmado.

Agradecemos pela preferência!

{interaction.user.display_name} — {agora().strftime('%d/%m/%Y %H:%M')}
{valor_texto}"""
        embed = discord.Embed(title="📋 MENSAGEM GERADA - PEDIDO FINALIZADO", description="**Copie a mensagem abaixo e cole no canal desejado:**", color=0x2ecc71)
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await limpar_mensagem_andamento(interaction.user.id)
        logger.error(f"Erro no modal: {error}")

# =========================================================
# CLASS: MensagemPendenciaPagamentoModal
# =========================================================

class MensagemPendenciaPagamentoModal(discord.ui.Modal, title="💰 Pendência de Pagamento"):
    def __init__(self):
        super().__init__(timeout=300)
    valor = discord.ui.TextInput(label="💰 Valor pendente", placeholder="Ex: 50000", required=True, max_length=50)
    chave_pix = discord.ui.TextInput(label="📱 Chave PIX (passaporte e nome)", placeholder="Ex: 820 - Leon", required=True, max_length=100)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        try:
            valor = safe_int(self.valor.value)
            valor_texto = formatar_dinheiro(valor)
        except:
            valor_texto = self.valor.value
        chave_pix = self.chave_pix.value.strip()
        mensagem = f"""🔔 ATENÇÃO – PENDÊNCIA DE PAGAMENTO

Consta uma pendência referente à sua última encomenda.

💰 Valor pendente: R$ {valor_texto}
📱 Chave PIX: {chave_pix}

Pedimos que o pagamento seja realizado o quanto antes.

Obrigado!"""
        embed = discord.Embed(title="📋 MENSAGEM GERADA - PENDÊNCIA DE PAGAMENTO", description="**Copie a mensagem abaixo e cole no canal desejado:**", color=0xf1c40f)
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n💰 Valor: R$ {valor_texto}\n📱 Chave PIX: {chave_pix}\n📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await limpar_mensagem_andamento(interaction.user.id)
        logger.error(f"Erro no modal: {error}")

# =========================================================
# CLASS: MensagemPagamentoPendenteModal
# =========================================================

class MensagemPagamentoPendenteModal(discord.ui.Modal, title="⚠️ Pagamento Pendente"):
    def __init__(self, usuario):
        super().__init__(timeout=300)
        self.usuario = usuario
    valor = discord.ui.TextInput(label="💰 Valor pendente", placeholder="Ex: 50000", required=True, max_length=50)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        try:
            valor = safe_int(self.valor.value)
            valor_texto = formatar_dinheiro(valor)
        except:
            valor_texto = self.valor.value
        nome_display = interaction.user.display_name
        if " - " in nome_display:
            chave_pix = nome_display
        else:
            chave_pix = f"SEM PASSAPORTE - {nome_display}"
        mensagem = f"""🔔 ATENÇÃO!

✅ Sua encomenda foi entregue.

💰 Pagamento pendente: R$ {valor_texto}
📱 Chave PIX: {chave_pix}

{chave_pix} — {agora().strftime('%H:%M')}"""
        embed = discord.Embed(title="📋 MENSAGEM GERADA - PAGAMENTO PENDENTE", description="**Copie a mensagem abaixo e cole no canal desejado:**", color=0xe67e22)
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n💰 Valor: R$ {valor_texto}\n📱 Chave PIX: {chave_pix}\n📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await limpar_mensagem_andamento(interaction.user.id)
        logger.error(f"Erro no modal: {error}")

# =========================================================
# CLASS: CopiarMensagemView
# =========================================================

class CopiarMensagemView(discord.ui.View):
    def __init__(self, mensagem):
        super().__init__(timeout=120)
        self.mensagem = mensagem

    @discord.ui.button(label="📋 Copiar Mensagem", style=discord.ButtonStyle.success, custom_id="copiar_mensagem", emoji="📋")
    async def copiar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(f"✅ **Mensagem copiada!**\n\nUse `Ctrl+C` para copiar a mensagem abaixo:\n\n```\n{self.mensagem}\n```", ephemeral=True)

    @discord.ui.button(label="❌ Fechar", style=discord.ButtonStyle.secondary, custom_id="fechar_copiar", emoji="❌")
    async def fechar(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.message.delete()
        except:
            pass

# =========================================================
# ==================== SISTEMA DE CLIPES ==================
# =========================================================

# ---------------------------------------------------------
# EVENTO: on_reaction_add - CLIPES
# ---------------------------------------------------------

@bot.event
async def on_reaction_add(reaction, user):
    try:
        if user.bot:
            return
        message = reaction.message
        if message.channel.id != CANAL_CLIPES_ID:
            return
        if str(reaction.emoji) != EMOJI_APROVACAO:
            return
        if message.id in clips_postados:
            return
        tem_video = False
        tem_link = False
        if message.attachments:
            att = message.attachments[0]
            if att.filename.endswith((".mp4", ".mov")):
                tem_video = True
        if message.content and "http" in message.content:
            tem_link = True
        if not tem_video and not tem_link:
            await message.reply("❌ Precisa ter vídeo ou link.")
            return
        clips_postados.add(message.id)
        await fila_clipes.put(message)
        await message.reply("🚀 Vai pro X!")
    except Exception as e:
        logger.error(f"Erro reação clip: {e}")

# ---------------------------------------------------------
# ASYNC: worker_clipes
# ---------------------------------------------------------

async def worker_clipes():
    global fila_clipes
    while True:
        message = await fila_clipes.get()
        try:
            canal = bot.get_channel(CANAL_POSTAGEM_X)
            if not canal:
                await message.reply("❌ Canal de postagem não encontrado.")
                fila_clipes.task_done()
                continue
            link = message.content if message.content else "Sem link"
            texto = f"🚀 **CLIPE APROVADO**\n\n👤 Autor: {message.author.mention}\n🔗 Link: {link}\n\n━━━━━━━━━━━━━━━━━━━━━━\n📝 **COPIAR E POSTAR NO X:**\n\n🔥 Olha esse clipe!\n\n{link}\n\n#fivem #clips #gaming\n━━━━━━━━━━━━━━━━━━━━━━"
            await canal.send(texto)
            await message.reply("📤 Enviado para canal de postagem!")
        except Exception as e:
            logger.error(f"ERRO CLIP: {e}")
            await message.reply("❌ Erro ao enviar.")
        await asyncio.sleep(5)
        fila_clipes.task_done()

# =========================================================
# ==================== EVENTOS DO BOT =====================
# =========================================================

# ---------------------------------------------------------
# EVENTO: on_member_join
# ---------------------------------------------------------

@bot.event
async def on_member_join(member):
    if member.bot:
        return
    try:
        cargo_em_registro = member.guild.get_role(EM_REGISTRO_ROLE_ID)
        if cargo_em_registro:
            await member.add_roles(cargo_em_registro)
    except Exception as e:
        logger.error(f"❌ Erro ao adicionar cargo 'Em Registro' para {member.name}: {e}")

# ---------------------------------------------------------
# EVENTO: on_member_update
# ---------------------------------------------------------

@bot.event
async def on_member_update(before, after):
    if after.bot:
        return
    
    # Verificar se ganhou o cargo RESP_METAS
    tinha_resp = any(r.id == CARGO_RESP_METAS_ID for r in before.roles)
    tem_resp = any(r.id == CARGO_RESP_METAS_ID for r in after.roles)
    
    if not tinha_resp and tem_resp:
        await atualizar_acesso_responsaveis()
    
    # Verificar se ganhou cargo de Agregado (criar sala)
    tinha_agregado = any(r.id == AGREGADO_ROLE_ID for r in before.roles)
    tem_agregado = any(r.id == AGREGADO_ROLE_ID for r in after.roles)
    
    if not tinha_agregado and tem_agregado:
        await asyncio.sleep(2)
        pool = await get_pool()
        if pool:
            async with pool.acquire() as conn:
                meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(after.id))
        else:
            meta = None
        
        if not meta:
            sala = await criar_sala_meta(after)
            if sala:
                cargo_resp = after.guild.get_role(CARGO_RESP_METAS_ID)
                if cargo_resp and sala:
                    for resp_member in after.guild.members:
                        if cargo_resp in resp_member.roles:
                            try:
                                await sala.set_permissions(resp_member, view_channel=True, send_messages=True)
                            except Exception as e:
                                logger.error(f"❌ Erro ao dar acesso a {resp_member.display_name}: {e}")
        else:
            canal = after.guild.get_channel(meta["canal_id"])
            if not canal:
                sala = await criar_sala_meta(after)
                if sala:
                    cargo_resp = after.guild.get_role(CARGO_RESP_METAS_ID)
                    if cargo_resp and sala:
                        for resp_member in after.guild.members:
                            if cargo_resp in resp_member.roles:
                                try:
                                    await sala.set_permissions(resp_member, view_channel=True, send_messages=True)
                                except Exception as e:
                                    logger.error(f"❌ Erro ao dar acesso a {resp_member.display_name}: {e}")
            else:
                await atualizar_embed_meta(after.id)
        return
    
    # Atualizar categoria da meta se o cargo mudou
    if str(after.id) in metas_cache:
        await atualizar_categoria_meta(after)

# ---------------------------------------------------------
# EVENTO: on_guild_channel_delete
# ---------------------------------------------------------

@bot.event
async def on_guild_channel_delete(channel):
    for uid, dados in list(metas_cache.items()):
        if dados["canal_id"] == channel.id:
            metas_cache.pop(uid)
            try:
                pool = await get_pool()
                if pool:
                    async with pool.acquire() as conn:
                        await conn.execute("DELETE FROM metas WHERE user_id = $1", uid)
            except Exception as e:
                logger.error(f"❌ Erro ao remover meta do banco: {e}")
            break

# ---------------------------------------------------------
# EVENTO: on_message
# ---------------------------------------------------------

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    canal = message.channel
    if isinstance(canal, discord.TextChannel):
        for uid, dados in list(metas_cache.items()):
            if dados["canal_id"] == canal.id:
                try:
                    await asyncio.sleep(2)
                    await fixar_painel_meta_no_final(int(uid))
                except Exception as e:
                    logger.error(f"Erro ao fixar painel: {e}")
                break
    await on_message_lavagem(message)
    await bot.process_commands(message)

# ---------------------------------------------------------
# EVENTO: on_member_remove
# ---------------------------------------------------------

@bot.event
async def on_member_remove(member):
    if member.bot:
        return
    await asyncio.sleep(2)
    nome_servidor = member.display_name
    nome_usuario = member.name
    nome_global = member.global_name or nome_usuario
    status_apelido = "✅ **Diferente do nome de usuário**" if nome_servidor != nome_usuario and nome_servidor != nome_global else "ℹ️ **Mesmo nome de usuário**"
    status_dm = ""
    dm_sucesso = False
    try:
        embed_msg = discord.Embed(
            title="📤 NOTIFICAÇÃO DE SAÍDA",
            description=(
                f"Olá **{member.display_name}**, tudo bom?\n\n"
                "Devido à sua saída do servidor **Vida Rasa**, "
                "pedimos que procure algum **gerente in game** "
                "para tomar seu **PD da facção**.\n\n"
                "⚠️ **Caso já tenha tomado seu PD, ignore este aviso.**\n\n"
                "——————————————————\n"
                "_Se saiu por engano, você pode voltar a qualquer momento._"
            ),
            color=0xe74c3c
        )
        if member.display_avatar:
            embed_msg.set_thumbnail(url=member.display_avatar.url)
        embed_msg.set_footer(text=f"Vida Rasa • Sistema Automático • ID: {member.id}")
        await member.send(embed=embed_msg)
        status_dm = "✅ **MENSAGEM ENVIADA COM SUCESSO**"
        dm_sucesso = True
        cor_log = 0xe74c3c
    except discord.Forbidden:
        status_dm = "❌ **MENSAGEM NÃO ENVIADA**\nMotivo: Usuário bloqueou o bot ou tem DM fechada"
        dm_sucesso = False
        cor_log = 0xf1c40f
    except discord.HTTPException as e:
        status_dm = f"❌ **MENSAGEM NÃO ENVIADA**\nMotivo: Erro HTTP - {e}"
        dm_sucesso = False
        cor_log = 0xf1c40f
    except Exception as e:
        status_dm = f"❌ **MENSAGEM NÃO ENVIADA**\nMotivo: Erro inesperado - {str(e)[:100]}"
        dm_sucesso = False
        cor_log = 0xf1c40f
    
    canal_gerencia = bot.get_channel(CANAL_GERENCIA_ID)
    if canal_gerencia:
        tempo_permanencia = "Desconhecido"
        if member.joined_at:
            dias = (agora() - member.joined_at.replace(tzinfo=BRASIL)).days
            tempo_permanencia = f"{dias} dia(s)" if dias > 0 else f"{(agora() - member.joined_at.replace(tzinfo=BRASIL)).seconds // 3600} hora(s)"
        embed_log = discord.Embed(title="📤 USUÁRIO SAIU DO SERVIDOR", color=cor_log, timestamp=agora())
        embed_log.add_field(name="👤 INFORMAÇÕES DO USUÁRIO", value=f"```\nMencão: {member.mention}\nID: {member.id}\nNome de usuário: {member.name}\nAlias global: {member.global_name or 'Nenhum'}\n```", inline=False)
        embed_log.add_field(name="🏷️ APELIDO NO SERVIDOR", value=f"```\nApelido: {nome_servidor}\nStatus: {status_apelido}\n```", inline=False)
        embed_log.add_field(name="⏱️ TEMPO NO SERVIDOR", value=f"```\nEntrou em: {member.joined_at.strftime('%d/%m/%Y %H:%M') if member.joined_at else 'Desconhecido'}\nPermanência: {tempo_permanencia}\nConta criada: {member.created_at.strftime('%d/%m/%Y')}\n```", inline=False)
        embed_log.add_field(name=f"{'✅' if dm_sucesso else '❌'} STATUS DA MENSAGEM", value=status_dm, inline=False)
        if member.display_avatar:
            embed_log.set_thumbnail(url=member.display_avatar.url)
        embed_log.set_footer(text=f"Sistema Automático • Saída em {agora().strftime('%d/%m/%Y às %H:%M:%S')}")
        await canal_gerencia.send(embed=embed_log)

# =========================================================
# ==================== COMANDOS DO BOT ====================
# =========================================================

# ---------------------------------------------------------
# COMANDO: !estoque
# ---------------------------------------------------------

@bot.command(name="estoque")
async def cmd_ver_estoque(ctx):
    estoque_municoes = await carregar_estoque()
    estoque_insumos = await carregar_estoque_insumos()
    embed = discord.Embed(title="📦 ESTOQUE COMPLETO", color=0x3498db)
    embed.add_field(name="🔫 MUNIÇÕES", value=f"**PT:** {fmt_num(estoque_municoes['PT'])} pacotes ({fmt_num(estoque_municoes['PT'] * 50)} munições)\n**SUB:** {fmt_num(estoque_municoes['SUB'])} pacotes ({fmt_num(estoque_municoes['SUB'] * 50)} munições)", inline=False)
    embed.add_field(name="💊 INSUMOS", value=f"**Cápsulas:** {fmt_num(estoque_insumos['capsulas'])} unidades\n**Embalagens:** {fmt_num(estoque_insumos['embalagens'])} unidades", inline=False)
    await ctx.send(embed=embed)

# ---------------------------------------------------------
# COMANDO: !historico_producao
# ---------------------------------------------------------

@bot.command(name="historico_producao")
async def cmd_historico_producao(ctx, limite: int = 10):
    pool = await get_pool()
    if not pool:
        await ctx.send("❌ Banco de dados indisponível!")
        return
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM producao_municao ORDER BY data DESC LIMIT $1", limite)
    if not rows:
        await ctx.send("📭 Nenhuma produção registrada ainda.")
        return
    embed = discord.Embed(title="📋 HISTÓRICO DE PRODUÇÃO DE MUNIÇÃO", color=0x2ecc71)
    for row in rows:
        data = row["data"]
        if data.tzinfo is None:
            data = data.replace(tzinfo=BRASIL)
        embed.add_field(name=f"{data.strftime('%d/%m/%Y %H:%M')}", value=f"🔫 **{row['tipo']}** • {fmt_num(row['pacotes'])} pacotes ({fmt_num(row['municoes'])} munições)\n💊 Consumiu: {fmt_num(row['capsulas_consumidas'])} cápsulas + {fmt_num(row['embalagens_consumidas'])} embalagens\n👤 <@{row['produzido_por']}>", inline=False)
    await ctx.send(embed=embed)

# ---------------------------------------------------------
# COMANDO: !historico_vendas_estoque
# ---------------------------------------------------------

@bot.command(name="historico_vendas_estoque")
async def cmd_historico_vendas_estoque(ctx, limite: int = 10):
    pool = await get_pool()
    if not pool:
        await ctx.send("❌ Banco de dados indisponível!")
        return
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM saida_estoque ORDER BY data DESC LIMIT $1", limite)
    if not rows:
        await ctx.send("📭 Nenhuma venda registrada ainda.")
        return
    embed = discord.Embed(title="📋 HISTÓRICO DE VENDAS (ESTOQUE)", color=0xe74c3c)
    for row in rows:
        data = row["data"]
        if data.tzinfo is None:
            data = data.replace(tzinfo=BRASIL)
        embed.add_field(name=f"Pedido #{row['pedido_numero']} - {data.strftime('%d/%m/%Y %H:%M')}", value=f"🔫 **{row['tipo']}** • {fmt_num(row['pacotes'])} pacotes\n👤 Retirado por: <@{row['retirado_por']}>", inline=False)
    await ctx.send(embed=embed)

# ---------------------------------------------------------
# COMANDO: !ausentes
# ---------------------------------------------------------

@bot.command(name="ausentes")
@commands.has_permissions(administrator=True)
async def listar_ausentes(ctx):
    ausencias = await buscar_ausencias_ativas_db()
    if not ausencias:
        await ctx.send("📭 Nenhum membro ausente.")
        return
    embed = discord.Embed(title="📋 Membros Ausentes", color=0xe67e22)
    for ausencia in ausencias:
        embed.add_field(name=f"👤 {ausencia['nome']}", value=f"📅 {ausencia['data_inicio'].strftime('%d/%m/%Y')} a {ausencia['data_fim'].strftime('%d/%m/%Y')}\n📝 {ausencia['motivo'][:50]}", inline=False)
    await ctx.send(embed=embed)

# ---------------------------------------------------------
# COMANDO: !remover_ausencia
# ---------------------------------------------------------

@bot.command(name="remover_ausencia")
async def remover_ausencia_cmd(ctx, member: discord.Member):
    if not pode_remover_ausencia(ctx.author):
        await ctx.send("❌ Você não tem permissão para remover ausências!\nApenas **Gerente, Cargo 01, Cargo 02 e Gerente Geral** podem usar este comando.")
        return
    ausencia = await buscar_ausencia_por_user(member.id)
    if not ausencia:
        await ctx.send(f"❌ {member.mention} não está ausente.")
        return
    await desativar_ausencia(member.id)
    cargo = ctx.guild.get_role(CARGO_AUSENTE_ID)
    if cargo and cargo in member.roles:
        await member.remove_roles(cargo)
    embed = discord.Embed(title="✅ Ausência Removida (Retorno Antecipado)", description=f"A ausência de {member.mention} foi encerrada!", color=0x2ecc71)
    await ctx.send(embed=embed)

# ---------------------------------------------------------
# COMANDO: !testar_live
# ---------------------------------------------------------

@bot.command(name="testar_live")
async def testar_live_cmd(ctx, plataforma: str = None, canal: str = None):
    if not plataforma or not canal:
        await ctx.send("❌ Uso correto:\n`!testar_live twitch NOME`\n`!testar_live kick NOME`\n`!testar_live tiktok NOME`")
        return
    plataforma = plataforma.lower()
    await ctx.send(f"🔍 Testando live na **{plataforma.upper()}**: `{canal}`")
    ao_vivo = False
    titulo = None
    jogo = None
    thumbnail = None
    if plataforma == "twitch":
        ao_vivo, titulo, jogo, thumbnail = await checar_twitch(canal)
    else:
        await ctx.send("❌ Plataforma inválida! Use: `twitch` apenas para testes automáticos")
        return
    if ao_vivo:
        embed = discord.Embed(title=f"✅ ESTÁ AO VIVO! ({plataforma.upper()})", color=0x2ecc71)
        embed.add_field(name="Canal", value=canal, inline=True)
        embed.add_field(name="Título", value=titulo[:100] if titulo else "Sem título", inline=False)
        if jogo:
            embed.add_field(name="Jogo", value=jogo, inline=True)
        if thumbnail:
            embed.set_image(url=thumbnail)
        await ctx.send(embed=embed)
    else:
        await ctx.send(f"❌ O canal **{canal}** NÃO está ao vivo no momento na {plataforma.upper()}.")

# ---------------------------------------------------------
# COMANDO: !listar_lives
# ---------------------------------------------------------

@bot.command(name="listar_lives")
async def listar_lives_cmd(ctx):
    lives = await carregar_lives_db()
    if not lives:
        await ctx.send("📭 Nenhuma live cadastrada.")
        return
    embed = discord.Embed(title="📡 LIVES CADASTRADAS", color=0x9146FF)
    grouped = {}
    for row in lives:
        uid = row["user_id"]
        if uid not in grouped:
            grouped[uid] = []
        grouped[uid].append(row)
    for uid, lista in grouped.items():
        user = await pegar_usuario(int(uid))
        nome = user.display_name if user else f"ID: {uid}"
        for live in lista:
            link = live["link"]
            divulgado = "✅ Divulgado" if live["divulgado"] else "⏳ Aguardando"
            plataforma = detectar_plataforma(link)
            embed.add_field(name=f"👤 {nome}", value=f"📺 {plataforma.upper()}\n🔗 {link}\n📌 {divulgado}", inline=False)
    await ctx.send(embed=embed)

# ---------------------------------------------------------
# COMANDO: !atualizar_paineis_metas
# ---------------------------------------------------------

@bot.command(name="atualizar_paineis_metas")
@commands.has_permissions(administrator=True)
async def cmd_atualizar_paineis_metas(ctx):
    await ctx.send("🔄 Atualizando painéis de metas...")
    try:
        await carregar_metas_cache()
        guild = ctx.guild
        contador = 0
        for uid, dados in metas_cache.items():
            canal = guild.get_channel(dados["canal_id"])
            if canal:
                await atualizar_embed_meta(int(uid))
                contador += 1
                await asyncio.sleep(0.3)
        await enviar_painel_solicitar_sala()
        await enviar_painel_relatorio_metas()
        await ctx.send(f"✅ **{contador} painéis de metas atualizados!**")
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar painéis de metas: {e}")
        await ctx.send(f"❌ Erro ao atualizar painéis: {e}")

# ---------------------------------------------------------
# COMANDO: !atualizar_metas
# ---------------------------------------------------------

@bot.command(name="atualizar_metas")
@commands.has_permissions(administrator=True)
async def cmd_atualizar_metas(ctx):
    await ctx.send("🔄 Atualizando salas de metas...")
    try:
        await carregar_metas_cache()
        contador = 0
        for uid in list(metas_cache.keys()):
            await atualizar_embed_meta(int(uid))
            contador += 1
            await asyncio.sleep(0.2)
        await ctx.send(f"✅ **{contador} salas de metas atualizadas!**")
    except Exception as e:
        logger.error(f"❌ Erro: {e}")
        await ctx.send(f"❌ Erro: {e}")

# ---------------------------------------------------------
# COMANDO: !recriar_metas
# ---------------------------------------------------------

@bot.command(name="recriar_metas")
@commands.has_permissions(administrator=True)
async def cmd_recriar_metas(ctx):
    await ctx.send("🔄 **RECRIANDO TODOS OS PAINÉIS DE METAS...**\n⏳ Isso pode levar alguns segundos.")
    try:
        await carregar_metas_cache()
        contador = 0
        for uid in list(metas_cache.keys()):
            try:
                await atualizar_embed_meta(int(uid))
                contador += 1
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"❌ Erro ao recriar meta {uid}: {e}")
        await ctx.send(f"✅ **{contador} painéis de metas recriados com sucesso!**")
    except Exception as e:
        logger.error(f"❌ Erro ao recriar metas: {e}")
        await ctx.send(f"❌ Erro ao recriar metas: {e}")

# ---------------------------------------------------------
# COMANDO: !recriar_meta
# ---------------------------------------------------------

@bot.command(name="recriar_meta")
@commands.has_permissions(administrator=True)
async def cmd_recriar_meta(ctx, member: discord.Member):
    await ctx.send(f"🔄 Recriando painel de meta de {member.mention}...")
    try:
        await atualizar_embed_meta(member.id)
        await ctx.send(f"✅ Painel de meta de {member.mention} recriado com sucesso!")
    except Exception as e:
        logger.error(f"❌ Erro ao recriar meta de {member.id}: {e}")
        await ctx.send(f"❌ Erro ao recriar meta: {e}")

# ---------------------------------------------------------
# COMANDO: !atualizar_acesso_resp
# ---------------------------------------------------------

@bot.command(name="atualizar_acesso_resp")
@commands.has_permissions(administrator=True)
async def cmd_atualizar_acesso_resp(ctx):
    await ctx.send("🔄 Atualizando acesso dos responsáveis...")
    await atualizar_acesso_responsaveis()
    await ctx.send("✅ Acesso dos responsáveis atualizado!")

# ---------------------------------------------------------
# COMANDO: !testar_aviso_quarta
# ---------------------------------------------------------

@bot.command(name="testar_aviso_quarta")
@commands.has_permissions(administrator=True)
async def cmd_testar_aviso_quarta(ctx):
    await ctx.send("🔄 Testando aviso de quarta-feira...")
    resultado = await verificar_avisos_quarta_forcado()
    if resultado:
        await ctx.send("✅ Avisos enviados com sucesso!")
    else:
        await ctx.send("❌ Erro ao enviar avisos. Verifique os logs.")

# ---------------------------------------------------------
# COMANDO: !limpar_sala
# ---------------------------------------------------------

@bot.command(name="limpar_sala")
@commands.has_permissions(administrator=True)
async def cmd_limpar_sala(ctx):
    canal = ctx.channel
    
    await ctx.send("🔄 **LIMPANDO A SALA...**\n⏳ Mantendo apenas a última mensagem do bot.")
    
    try:
        ultima_msg_bot = None
        async for msg in canal.history(limit=100):
            if msg.author == bot.user:
                ultima_msg_bot = msg
                break
        
        deletadas = 0
        async for msg in canal.history(limit=1000):
            if ultima_msg_bot and msg.id == ultima_msg_bot.id:
                continue
            try:
                await msg.delete()
                deletadas += 1
                if deletadas % 50 == 0:
                    await asyncio.sleep(0.5)
            except:
                pass
        
        embed = discord.Embed(
            title="🧹 SALA LIMPA!",
            description=f"✅ **{deletadas} mensagens deletadas!**\n📌 A última mensagem do bot foi mantida.",
            color=0x2ecc71,
            timestamp=agora()
        )
        embed.set_footer(text=f"Comando executado por {ctx.author.display_name}")
        
        if ultima_msg_bot:
            if ultima_msg_bot.embeds:
                embed_original = ultima_msg_bot.embeds[0]
                novo_embed = discord.Embed(
                    title=embed_original.title,
                    description=embed_original.description,
                    color=embed_original.color,
                    timestamp=agora()
                )
                for field in embed_original.fields:
                    novo_embed.add_field(name=field.name, value=field.value, inline=field.inline)
                novo_embed.add_field(
                    name="🧹 LIMPEZA REALIZADA",
                    value=f"✅ {deletadas} mensagens deletadas por {ctx.author.mention}",
                    inline=False
                )
                novo_embed.set_footer(text=f"Última limpeza: {agora().strftime('%d/%m/%Y %H:%M:%S')}")
                await ultima_msg_bot.edit(embed=novo_embed)
            else:
                await canal.send(embed=embed)
        else:
            await canal.send(embed=embed)
            
    except Exception as e:
        logger.error(f"Erro ao limpar sala: {e}")
        await ctx.send(f"❌ **Erro ao limpar a sala:** {e}")

# ---------------------------------------------------------
# COMANDO: !recriar_vendas
# ---------------------------------------------------------

@bot.command(name="recriar_vendas")
@commands.has_permissions(administrator=True)
async def cmd_recriar_vendas(ctx):
    await ctx.send("🔄 Recriando mensagens de vendas...")
    await recriar_mensagens_vendas()
    await ctx.send("✅ Mensagens de vendas recriadas!")

# ---------------------------------------------------------
# COMANDO: !diagnostico
# ---------------------------------------------------------

@bot.command(name="diagnostico")
@commands.has_permissions(administrator=True)
async def cmd_diagnostico(ctx):
    pool = get_db()
    embed = discord.Embed(title="🔍 DIAGNÓSTICO DO BOT", color=0x3498db, timestamp=agora())
    db_status = "✅ Conectado" if pool and not pool._closed else "❌ Desconectado"
    embed.add_field(name="📊 Banco de Dados", value=db_status, inline=True)
    try:
        process = psutil.Process()
        memory_usage = process.memory_info().rss / 1024 / 1024
        embed.add_field(name="💾 Memória", value=f"{memory_usage:.2f} MB", inline=True)
    except:
        embed.add_field(name="💾 Memória", value="N/A", inline=True)
    tasks = len([t for t in asyncio.all_tasks() if not t.done()])
    embed.add_field(name="🔄 Tarefas Ativas", value=str(tasks), inline=True)
    cache_size = cache.size()
    embed.add_field(name="📦 Cache", value=f"{cache_size} itens", inline=True)
    embed.add_field(name="🏭 Produções", value=str(len(producoes_tasks)), inline=True)
    embed.add_field(name="📊 Metas", value=str(len(metas_cache)), inline=True)
    embed.add_field(name="📝 Comandos", value=str(metricas.comandos_executados), inline=True)
    embed.add_field(name="❌ Erros", value=str(metricas.erros), inline=True)
    embed.add_field(name="⏱️ Uptime", value=f"{int(metricas.get_uptime() // 3600)}h {(int(metricas.get_uptime()) % 3600) // 60}m", inline=True)
    embed.set_footer(text=f"Versão 3.3.1 • {agora().strftime('%d/%m/%Y %H:%M:%S')}")
    await ctx.send(embed=embed)

# ---------------------------------------------------------
# COMANDO: !stats
# ---------------------------------------------------------

@bot.command(name="stats")
async def cmd_stats(ctx):
    embed = discord.Embed(title="📊 ESTATÍSTICAS DO BOT", color=0x3498db)
    embed.add_field(name="⏱️ Uptime", value=f"{int(metricas.get_uptime() // 3600)}h {(int(metricas.get_uptime()) % 3600) // 60}m", inline=True)
    embed.add_field(name="📝 Comandos", value=str(metricas.comandos_executados), inline=True)
    embed.add_field(name="❌ Erros", value=str(metricas.erros), inline=True)
    embed.add_field(name="📊 Metas", value=str(len(metas_cache)), inline=True)
    embed.add_field(name="🏭 Produções", value=str(len(producoes_tasks)), inline=True)
    await ctx.send(embed=embed)

# ---------------------------------------------------------
# COMANDO: !help_vdr
# ---------------------------------------------------------

@bot.command(name="help_vdr")
async def cmd_help_vdr(ctx):
    embed = discord.Embed(
        title="📋 LISTA DE COMANDOS - VDR BOT",
        description="**Comandos disponíveis para todos os membros:**",
        color=0x3498db
    )
    embed.add_field(
        name="📊 ESTOQUE E PRODUÇÃO",
        value="`!estoque` - Ver estoque completo\n`!historico_producao` - Histórico de produção\n`!historico_vendas_estoque` - Histórico de vendas",
        inline=False
    )
    embed.add_field(
        name="🎥 LIVES",
        value="`!listar_lives` - Lista lives cadastradas\n`!testar_live twitch NOME` - Testa se está ao vivo",
        inline=False
    )
    embed.add_field(
        name="📊 ESTATÍSTICAS",
        value="`!stats` - Estatísticas do bot",
        inline=False
    )
    embed.add_field(
        name="👑 COMANDOS DE ADM",
        value="`!ausentes` - Lista ausentes\n`!remover_ausencia @membro` - Remove ausência\n`!limpar_sala` - Limpa o canal\n`!atualizar_metas` - Atualiza metas\n`!recriar_metas` - Recria todos os painéis\n`!recriar_meta @membro` - Recria painel de um membro\n`!diagnostico` - Diagnóstico do bot",
        inline=False
    )
    embed.set_footer(text="Sistema VDR • v3.3.1")
    await ctx.send(embed=embed)

# =========================================================
# ==================== TASKS BACKGROUND ===================
# =========================================================

# ---------------------------------------------------------
# ASYNC: iniciar_tarefas_background
# ---------------------------------------------------------

async def iniciar_tarefas_background():
    try:
        if not verificar_lives.is_running():
            verificar_lives.start()
    except Exception as e:
        logger.error(f"Erro loop lives: {e}")
    try:
        if not relatorio_semanal_polvoras.is_running():
            relatorio_semanal_polvoras.start()
    except Exception as e:
        logger.error(f"Erro loop polvora: {e}")
    try:
        if not verificar_ausencias_expiradas.is_running():
            verificar_ausencias_expiradas.start()
    except Exception as e:
        logger.error(f"Erro loop ausência: {e}")
    try:
        if not limpar_lavagens_pendentes.is_running():
            limpar_lavagens_pendentes.start()
    except Exception as e:
        logger.error(f"Erro loop limpeza lavagens: {e}")
    try:
        if not verificar_avisos_meta.is_running():
            verificar_avisos_meta.start()
    except Exception as e:
        logger.error(f"Erro loop avisos: {e}")
    try:
        if not limpar_cache_lives.is_running():
            limpar_cache_lives.start()
    except Exception as e:
        logger.error(f"Erro loop cache lives: {e}")

# ---------------------------------------------------------
# ASYNC: limpeza_cache_periodica
# ---------------------------------------------------------

async def limpeza_cache_periodica():
    while True:
        try:
            await asyncio.sleep(3600)
            removidos = await cache.clean_expired()
            if removidos > 0:
                logger.info(f"🧹 Cache limpo: {removidos} entradas removidas")
        except Exception as e:
            logger.error(f"Erro na limpeza de cache: {e}")

# ---------------------------------------------------------
# ASYNC: health_check
# ---------------------------------------------------------

async def health_check():
    while True:
        try:
            await asyncio.sleep(60)
            if bot.is_closed():
                logger.warning("⚠️ Bot está fechado! Tentando reconectar...")
                await bot.close()
                await bot.start(TOKEN)
                continue
            pool = get_db()
            if not pool:
                logger.warning("⚠️ Pool do banco vazio! Reconectando...")
                await conectar_db()
                continue
            if hasattr(pool, '_closed') and pool._closed:
                logger.warning("⚠️ Pool do banco fechado! Reconectando...")
                await conectar_db()
                continue
            if not verificar_lives.is_running():
                logger.warning("⚠️ Loop de lives parado! Reiniciando...")
                verificar_lives.start()
                continue
        except Exception as e:
            logger.error(f"Erro no health check: {e}")
            await asyncio.sleep(10)

# =========================================================
# ==================== ON_READY ===========================
# =========================================================

@bot.event
async def on_ready():
    global http_session, fila_clipes
    
    if hasattr(bot, "ja_iniciado"):
        return
    
    bot.ja_iniciado = True
    
    logger.info("🔄 Iniciando configuração do bot...")
    logger.info(f"✅ Logado como {bot.user}")
    
    # Inicializar HTTP session
    if not http_session:
        http_session = aiohttp.ClientSession()
    
    # Conectar ao banco de dados
    db_pool = await conectar_db()
    if not db_pool:
        logger.critical("❌ Não foi possível conectar ao banco de dados!")
        return
    
    # Carregar guild e membros
    guild = bot.get_guild(GUILD_ID)
    if guild:
        try:
            await guild.chunk()
        except Exception as e:
            logger.error(f"Erro ao carregar membros: {e}")
    
    logger.info(f"🕒 Horário Brasília: {agora().strftime('%d/%m/%Y %H:%M:%S')}")
    
    # Iniciar worker de edição
    if not hasattr(bot, "edit_worker_started"):
        bot.loop.create_task(edit_worker())
        bot.edit_worker_started = True
    
    # Iniciar fila de clipes
    fila_clipes = asyncio.Queue()
    bot.loop.create_task(worker_clipes())
    
    # Iniciar tarefas de background
    await iniciar_tarefas_background()
    
    # Iniciar limpeza de cache
    bot.loop.create_task(limpeza_cache_periodica())
    
    # Iniciar health check
    bot.loop.create_task(health_check())
    
    # Carregar dados iniciais
    await carregar_dados_iniciais()
    
    # Enviar painéis
    await enviar_paineis_iniciais(guild)
    await recriar_painel_grupos()
    await recriar_mensagens_vendas()
    await restaurar_botoes_vendas()
    await restaurar_acoes()
    
    # Restaurar botões das metas
    await restaurar_botoes_metas()
    
    # Garantir acesso dos responsáveis
    await atualizar_acesso_responsaveis()
    
    # Status do bot
    await setup_status()
    
    # Limpeza de memória
    gc.collect()
    logger.info("=" * 50)
    logger.info("✅ BOT ONLINE 100% ESTÁVEL - v3.3.1")
    logger.info("=" * 50)

# ---------------------------------------------------------
# ASYNC: setup_status
# ---------------------------------------------------------

async def setup_status():
    @tasks.loop(minutes=5)
    async def atualizar_status():
        try:
            guild = bot.get_guild(GUILD_ID)
            if guild:
                membros = len([m for m in guild.members if not m.bot])
                await bot.change_presence(
                    activity=discord.Activity(
                        type=discord.ActivityType.watching,
                        name=f"{membros} membros • v3.3.1"
                    )
                )
        except Exception as e:
            logger.error(f"Erro ao atualizar status: {e}")
    if not atualizar_status.is_running():
        atualizar_status.start()

# ---------------------------------------------------------
# ASYNC: carregar_dados_iniciais
# ---------------------------------------------------------

async def carregar_dados_iniciais():
    try:
        rows = await carregar_metas_db()
        for r in rows:
            metas_cache[str(r["user_id"])] = {
                "canal_id": int(r["canal_id"]),
                "dinheiro": r["dinheiro"],
                "polvora": r["polvora"],
                "acao": r["acao"],
                "dinheiro_acoes": r.get("dinheiro_acoes") or 0
            }
    except Exception as e:
        logger.error(f"Erro ao carregar metas: {e}")
    await restaurar_producoes()

# ---------------------------------------------------------
# ASYNC: enviar_paineis_iniciais
# ---------------------------------------------------------

async def enviar_paineis_iniciais(guild):
    try:
        paineis = [
            ("Registro", enviar_painel_registro),
            ("Fabricação", enviar_painel_fabricacao),
            ("Lives", enviar_painel_lives),
            ("Pólvora", enviar_painel_polvoras),
            ("Lavagem", enviar_painel_lavagem),
            ("Vendas", enviar_painel_vendas),
            ("Remover Ausência", enviar_painel_remover_ausencia),
            ("Relatório Financeiro", enviar_painel_relatorio_financeiro),
            ("Registrar Compra", enviar_painel_registrar_compra),
            ("Solicitar Sala", enviar_painel_solicitar_sala),
            ("Botão Ausência", enviar_painel_botao_ausencia),
            ("Painel Grupos", enviar_painel_grupos),
            ("Relatório Metas", enviar_painel_relatorio_metas),
            ("Mensagens", enviar_painel_mensagens),
        ]
        for i, (nome, func) in enumerate(paineis):
            try:
                await func()
                if i < len(paineis) - 1:
                    await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"❌ Erro ao enviar painel {nome}: {e}")
                await asyncio.sleep(3)
        if guild:
            try:
                await enviar_painel_acoes(guild)
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"❌ Erro ao enviar painel de ações: {e}")
        try:
            await recriar_painel_grupos()
        except Exception as e:
            logger.error(f"❌ Erro ao forçar atualização grupos: {e}")
    except Exception as e:
        logger.error(f"❌ Erro geral ao enviar painéis: {e}")

# ---------------------------------------------------------
# ASYNC: restaurar_producoes
# ---------------------------------------------------------

async def restaurar_producoes():
    try:
        pool = await get_pool()
        if not pool:
            return
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT pid FROM producoes WHERE CAST(fim AS timestamp) > NOW()")
        for row in rows:
            pid = row["pid"]
            if pid not in producoes_tasks or producoes_tasks[pid].done():
                if pid in producoes_tasks:
                    del producoes_tasks[pid]
                task = asyncio.create_task(acompanhar_producao(pid))
                producoes_tasks[pid] = task
    except Exception as e:
        logger.error(f"❌ Erro ao restaurar produções: {e}")

# ---------------------------------------------------------
# ASYNC: restaurar_botoes_metas
# ---------------------------------------------------------

async def restaurar_botoes_metas():
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            logger.error("❌ Guild não encontrada!")
            return 0
        
        await carregar_metas_cache()
        
        contador = 0
        for uid, dados in list(metas_cache.items()):
            canal = guild.get_channel(dados["canal_id"])
            if not canal:
                continue
            
            try:
                mensagem_encontrada = False
                async for msg in canal.history(limit=30):
                    if msg.author == bot.user and msg.embeds:
                        if msg.embeds[0].title and "META DE" in msg.embeds[0].title.upper():
                            mensagem_encontrada = True
                            if not msg.components:
                                await atualizar_embed_meta(int(uid))
                                contador += 1
                                await asyncio.sleep(0.5)
                            else:
                                tem_fixo = False
                                for component in msg.components:
                                    for item in component.children:
                                        if item.custom_id and "fixo" in item.custom_id:
                                            tem_fixo = True
                                            break
                                if not tem_fixo:
                                    await atualizar_embed_meta(int(uid))
                                    contador += 1
                                    await asyncio.sleep(0.5)
                            break
                
                if not mensagem_encontrada:
                    await atualizar_embed_meta(int(uid))
                    contador += 1
                    await asyncio.sleep(0.5)
                    
            except Exception as e:
                logger.error(f"❌ Erro ao restaurar meta {uid}: {e}")
        
        logger.info(f"✅ {contador} painéis de metas restaurados com botões!")
        return contador
        
    except Exception as e:
        logger.error(f"❌ Erro ao restaurar botões das metas: {e}")
        return 0

# =========================================================
# ==================== SHUTDOWN E START ===================
# =========================================================

# ---------------------------------------------------------
# ASYNC: shutdown
# ---------------------------------------------------------

async def shutdown():
    logger.info("🔄 Iniciando shutdown gracioso...")
    global http_session
    if http_session:
        await http_session.close()
    pool = get_db()
    if pool:
        await pool.close()
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    await bot.close()

# ---------------------------------------------------------
# ASYNC: verificar_avisos_quarta_forcado (para testes)
# ---------------------------------------------------------

async def verificar_avisos_quarta_forcado():
    logger.info("📨 TESTE FORÇADO: Verificando avisos de quarta-feira...")
    pool = await get_pool()
    if not pool:
        logger.error("❌ Banco de dados indisponível!")
        return False
    try:
        hoje = agora()
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            logger.error("❌ Guild não encontrada!")
            return False
        cargos_obrigados = [
            CARGO_AGREGADO_ID, CARGO_MEMBRO_ID, CARGO_SOLDADO_ID,
            CARGO_01_ID, CARGO_02_ID, CARGO_RESP_P1_ID, CARGO_RESP_METAS_ID,
            CARGO_RESP_ACAO_ID, CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID
        ]
        async with pool.acquire() as conn:
            avisos_enviados = 0
            for member in guild.members:
                if member.bot:
                    continue
                tem_cargo = any(r.id in cargos_obrigados for r in member.roles)
                if not tem_cargo:
                    continue
                user_id = str(member.id)
                meta = await conn.fetchrow("SELECT dinheiro, dinheiro_acoes FROM metas WHERE user_id = $1", user_id)
                if not meta:
                    continue
                dinheiro = meta["dinheiro"] or 0
                dinheiro_acoes = meta.get("dinheiro_acoes") or 0
                total = dinheiro + dinheiro_acoes
                if total == 0:
                    canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", user_id)
                    if canal_id:
                        canal = bot.get_channel(int(canal_id))
                        if canal:
                            embed = discord.Embed(
                                title="⚠️ [TESTE] AVISO DE META SEMANAL",
                                description=f"{member.mention} **atenção!**",
                                color=0xe74c3c
                            )
                            embed.add_field(
                                name="📌 Você ainda NÃO fez nenhum depósito na sua meta esta semana!",
                                value=(
                                    "⏰ **Você tem até domingo para completar sua meta!**\n\n"
                                    "⚠️ **Consequências:**\n"
                                    "• Se NÃO fechar a meta: **REBAIXAMENTO** na facção\n"
                                    "• Se atrasar 2 vezes: **REMOÇÃO** da facção\n\n"
                                    "💪 **Corra atrás do prejuízo!**\n\n"
                                    "🔴 **ESTE É UM TESTE**"
                                ),
                                inline=False
                            )
                            embed.set_footer(text="Meta semanal • Vida Rasa • TESTE")
                            await canal.send(embed=embed)
                            avisos_enviados += 1
        logger.info(f"✅ [TESTE] Avisos enviados: {avisos_enviados} membros")
        return True
    except Exception as e:
        logger.error(f"❌ Erro no teste de aviso: {e}")
        return False

# =========================================================
# ==================== STARTER ============================
# =========================================================

# Registrar sinais de shutdown
try:
    import signal
    for sig in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_event_loop().add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))
except:
    pass

if __name__ == "__main__":
    logger.info("🚀 Iniciando bot v3.3.1...")
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    try:
        bot.run(TOKEN, reconnect=True)
    except discord.LoginFailure:
        logger.critical("❌ Falha no login! TOKEN inválido?")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"❌ Erro fatal: {e}")
        sys.exit(1)

# =========================================================
# ==================== FIM DO CÓDIGO ======================
# =========================================================
