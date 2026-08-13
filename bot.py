# =========================================================
# ======================== BOT VDR ========================
# =========================================================
# Versão: 3.2 - Correção de inicialização - COMPLETO
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
from discord.ext import commands, tasks
from discord.utils import escape_markdown
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

# =========================================================
# ==================== CONFIGURAÇÃO DE LOG ================
# =========================================================

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('VDR_BOT')

# =========================================================
# ==================== SEÇÃO 0: BOT =======================
# =========================================================

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.guilds = True
intents.presences = True
intents.reactions = True

bot = commands.Bot(command_prefix="!", intents=intents)

# =========================================================
# ==================== SEÇÃO GLOBAL: CONFIGURAÇÕES ========
# =========================================================

# --- TOKENS E CREDENCIAIS ---
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    logger.error("❌ TOKEN não encontrado nas variáveis de ambiente!")
    sys.exit(1)

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    logger.error("❌ DATABASE_URL não encontrada nas variáveis de ambiente!")
    sys.exit(1)

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")
API_KEY = os.environ.get("API_KEY")
API_SECRET = os.environ.get("API_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_SECRET = os.environ.get("ACCESS_SECRET")

# --- FUSO HORÁRIO ---
BRASIL = ZoneInfo("America/Sao_Paulo")

# --- GUILD ---
GUILD_ID = 1229526644193099880

# --- PATHS ---
BASE_PATH = "/mnt/data"

# --- EMOJIS GLOBAIS ---
EMOJI_APROVACAO = "✅"

# =========================================================
# ==================== SEÇÃO GLOBAL: CONSTANTES ===========
# =========================================================

# --- CONSTANTES DA PRODUÇÃO ---
PRECO_POLVORA = 80
PRECO_EMBALAGEM_POR_UNIDADE = 2000000 / 25000
TEMPO_BASE_NORTE = 65
TEMPO_BASE_SUL = 130

# --- ITENS E ALIASES DO CONTROLE ---
ITENS_DISPONIVEIS = [
    "🔫 Fuzil",
    "🔫 M4",
    "🔫 SIG Sauer",
    "🔫 AK47",
    "🔫 Glock",
    "🔫 Shotgun",
    "🔫 Sniper",
    "🎯 Kit Reparos Comum",
    "🎯 Kit Reparos Raro",
    "🎯 Kit Reparos Épico",
    "🎯 Kit Reparos Lendário",
    "🛡️ Colete Leve",
    "🛡️ Colete Médio",
    "🛡️ Colete Pesado",
    "📦 Municao PT",
    "📦 Municao SUB",
    "🧨 Explosivo",
    "💊 Kit Médico",
    "🔑 Chave Mestra",
    "📡 Rádio",
    "🔦 Lanterna"
]

ALIASES = {
    "fuzil": "Fuzil",
    "m4": "M4",
    "sig": "SIG Sauer",
    "ak": "AK47",
    "ak47": "AK47",
    "glock": "Glock",
    "shotgun": "Shotgun",
    "sniper": "Sniper",
    "kit comum": "Kit Reparos Comum",
    "kit raro": "Kit Reparos Raro",
    "kit epico": "Kit Reparos Épico",
    "kit lendario": "Kit Reparos Lendário",
    "colete leve": "Colete Leve",
    "colete medio": "Colete Médio",
    "colete pesado": "Colete Pesado",
    "municao pt": "Municao PT",
    "municao sub": "Municao SUB",
    "explosivo": "Explosivo",
    "kit medico": "Kit Médico",
    "chave mestra": "Chave Mestra",
    "radio": "Rádio",
    "lanterna": "Lanterna",
    "pt": "Municao PT",
    "sub": "Municao SUB"
}

ITENS_COM_OPCOES = {
    "Kit Reparos": ["Kit Reparos Comum", "Kit Reparos Raro", "Kit Reparos Épico", "Kit Reparos Lendário"],
    "Colete": ["Colete Leve", "Colete Médio", "Colete Pesado"],
    "Municao": ["Municao PT", "Municao SUB"]
}

# --- IDs DAS METAS ---
CANAL_SOLICITAR_SALA_ID = 1337374500366450741
RESULTADOS_METAS_ID = 1341403574483288125
CARGO_01_ID = 1258753233355014144
CARGO_02_ID = 1258753479082512394
CARGO_GERENTE_ID = 1324499473296134154
CARGO_GERENTE_GERAL_ID = 1462804425163935796
CARGO_RESP_METAS_ID = 1337407399656423485
CARGO_RESP_ACAO_ID = 1337379517274259509
CARGO_RESP_VENDAS_ID = 1337379530586980352
CARGO_RESP_PRODUCAO_ID = 1337379524949573662
CARGO_SOLDADO_ID = 1422845498863259700
CARGO_MEMBRO_ID = 1422847198789369926
CARGO_AGREGADO_ID = 1422847202937536532
CATEGORIA_META_GERENTE_ID = 1337374002422743122
CATEGORIA_META_RESPONSAVEIS_ID = 1462810826992783422
CATEGORIA_META_SOLDADO_ID = 1461335635519475894
CATEGORIA_META_MEMBRO_ID = 1461335697209163900
CATEGORIA_META_AGREGADO_ID = 1461335748870541323
AGREGADO_ROLE_ID = 1422847202937536532

# --- CARGOS PERMITIDOS PARA REMOVER AUSÊNCIA ---
CARGOS_PERMITIDOS_REMOVER = [
    CARGO_GERENTE_ID,
    CARGO_GERENTE_GERAL_ID,
    CARGO_01_ID,
    CARGO_02_ID
]

# =========================================================
# ==================== SEÇÃO GLOBAL: FUNÇÕES AUXILIARES ===
# =========================================================

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

def formatar_dinheiro(valor):
    try:
        valor = float(valor)
    except:
        valor = 0
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_num(valor):
    return f"{valor:,.0f}".replace(",", ".")

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

def calcular_semana_anterior():
    hoje = agora()
    dia_semana = hoje.weekday()
    dias_para_domingo_anterior = dia_semana + 1
    domingo_anterior = hoje - timedelta(days=dias_para_domingo_anterior)
    segunda_anterior = domingo_anterior - timedelta(days=6)
    segunda_anterior = segunda_anterior.replace(hour=0, minute=0, second=0, microsecond=0)
    domingo_anterior = domingo_anterior.replace(hour=23, minute=59, second=59, microsecond=0)
    return segunda_anterior, domingo_anterior

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
    if any(r in roles for r in [CARGO_RESP_METAS_ID, CARGO_RESP_ACAO_ID, CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID]):
        return CATEGORIA_META_RESPONSAVEIS_ID
    if CARGO_SOLDADO_ID in roles:
        return CATEGORIA_META_SOLDADO_ID
    if CARGO_MEMBRO_ID in roles:
        return CATEGORIA_META_MEMBRO_ID
    if AGREGADO_ROLE_ID in roles:
        return CATEGORIA_META_AGREGADO_ID
    return None

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

# =========================================================
# ==================== SEÇÃO GLOBAL: BANCO DE DADOS =======
# =========================================================

db = None
db_lock = asyncio.Lock()
db_reconnect_attempts = 0
MAX_DB_RECONNECT_ATTEMPTS = 10

async def conectar_db():
    global db, db_reconnect_attempts
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL não encontrada!")
        return None
    
    async with db_lock:
        if db and not db._closed:
            return db
        
        try:
            db = await asyncpg.create_pool(
                DATABASE_URL,
                min_size=1,
                max_size=5,
                command_timeout=30,
                max_inactive_connection_lifetime=300
            )
            db_reconnect_attempts = 0
            logger.info("🟢 Pool PostgreSQL conectado com sucesso!")
            
            # Inicializar tabelas se necessário
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

def get_db():
    return db

async def inicializar_tabelas(pool):
    """Cria todas as tabelas necessárias se não existirem."""
    async with pool.acquire() as conn:
        # Tabelas principais
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
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS metas_avisos (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                tipo VARCHAR(20),
                data TIMESTAMP
            )
        """)
        
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
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS vendas (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                valor INTEGER,
                data VARCHAR(20),
                pedido_numero INTEGER
            )
        """)
        
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
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS entregas_detalhes (
                entrega_id INTEGER PRIMARY KEY,
                entregas_json TEXT,
                data_criacao TIMESTAMP DEFAULT NOW()
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS polvoras (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                quantidade INTEGER,
                valor INTEGER,
                data TEXT
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS lives (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                link TEXT,
                divulgado BOOLEAN DEFAULT false,
                data_cadastro TIMESTAMP DEFAULT NOW()
            )
        """)
        
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
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS participantes_acoes (
                id SERIAL PRIMARY KEY,
                acao_id INTEGER,
                user_id VARCHAR(30)
            )
        """)
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS paineis (
                nome VARCHAR(50) PRIMARY KEY,
                canal_id VARCHAR(30),
                mensagem_id VARCHAR(30),
                data_atualizacao TIMESTAMP DEFAULT NOW()
            )
        """)
        
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
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS compras (
                id SERIAL PRIMARY KEY,
                produto TEXT,
                valor INTEGER,
                comprado_por VARCHAR(30),
                data TIMESTAMP DEFAULT NOW()
            )
        """)
        
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
        
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bau_estoque (
                id SERIAL PRIMARY KEY,
                item_nome VARCHAR(100) UNIQUE NOT NULL,
                quantidade INT DEFAULT 0,
                ultima_atualizacao TIMESTAMP DEFAULT NOW()
            )
        """)
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

async def criar_tabela_alugueis():
    """Cria tabela de aluguel de galpões."""
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            # Criar tabela se não existir
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
            
            # Verificar se a coluna dias_alugados existe
            coluna_existe = await conn.fetchval("""
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'alugueis' AND column_name = 'dias_alugados'
            """)
            
            if not coluna_existe:
                await conn.execute("""
                    ALTER TABLE alugueis ADD COLUMN dias_alugados INTEGER DEFAULT 0
                """)
                logger.info("✅ Coluna dias_alugados adicionada")
            
            # DESATIVAR registros antigos que não são os principais
            await conn.execute("""
                UPDATE alugueis 
                SET ativo = false 
                WHERE galpao NOT IN ('GALPÕES NORTE', 'GALPÕES SUL')
                  AND ativo = true
            """)
            
            # Inserir registros padrão se não existirem
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
            
            logger.info("✅ TABELA ALUGUEIS CRIADA/VERIFICADA")
    except Exception as e:
        logger.error(f"❌ ERRO AO CRIAR TABELA ALUGUEIS: {e}")

# =========================================================
# ==================== SEÇÃO GLOBAL: CACHE =================
# =========================================================

class CacheManager:
    """Gerenciador de cache com TTL automático."""
    
    def __init__(self, default_ttl=300):
        self._cache = {}
        self._default_ttl = default_ttl
        self._lock = asyncio.Lock()
    
    async def get(self, key):
        """Obtém um valor do cache."""
        async with self._lock:
            if key in self._cache:
                value, timestamp = self._cache[key]
                if time_module.time() - timestamp < value.get('ttl', self._default_ttl):
                    return value.get('data')
                else:
                    del self._cache[key]
            return None
    
    async def set(self, key, data, ttl=None):
        """Armazena um valor no cache."""
        async with self._lock:
            self._cache[key] = {
                'data': data,
                'ttl': ttl or self._default_ttl,
                'timestamp': time_module.time()
            }
    
    async def delete(self, key):
        """Remove um valor do cache."""
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
    
    async def clear(self):
        """Limpa todo o cache."""
        async with self._lock:
            self._cache.clear()
    
    async def clean_expired(self):
        """Remove entradas expiradas."""
        async with self._lock:
            now = time_module.time()
            expired = []
            for key, value in self._cache.items():
                if now - value['timestamp'] > value.get('ttl', self._default_ttl):
                    expired.append(key)
            for key in expired:
                del self._cache[key]
            return len(expired)

# Cache global
cache = CacheManager(default_ttl=300)

# =========================================================
# ==================== SEÇÃO GLOBAL: VARIÁVEIS GLOBAIS ====
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

# =========================================================
# ==================== SEÇÃO GLOBAL: EDIT WORKER ==========
# =========================================================

async def edit_worker():
    """Worker para edições de mensagem com rate limiting."""
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
# ==================== SEÇÃO GLOBAL: PEAR USUÁRIO ========
# =========================================================

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
# ==================== SEÇÃO GLOBAL: ENVIAR PAINEL =======
# =========================================================

async def enviar_ou_atualizar_painel(nome, canal_id, embed, view):
    canal = bot.get_channel(canal_id)
    if not canal:
        logger.error(f"❌ Canal não encontrado para painel: {nome}")
        return
    
    pool = get_db()
    if not pool:
        logger.error(f"❌ Banco de dados não disponível para painel: {nome}")
        return
    
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT mensagem_id, canal_id FROM paineis WHERE nome=$1", nome)
            if row:
                try:
                    canal_salvo = bot.get_channel(int(row["canal_id"])) or canal
                    msg = await canal_salvo.fetch_message(int(row["mensagem_id"]))
                    await msg.edit(embed=embed, view=view)
                    return
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao atualizar painel {nome}: {e}")
            
            msg = await canal.send(embed=embed, view=view)
            await conn.execute(
                "INSERT INTO paineis (nome, canal_id, mensagem_id) VALUES ($1,$2,$3) ON CONFLICT (nome) DO UPDATE SET canal_id=$2, mensagem_id=$3",
                nome, str(canal_id), str(msg.id)
            )
    except Exception as e:
        logger.error(f"❌ Erro crítico ao enviar painel {nome}: {e}")

# =========================================================
# ==================== SEÇÃO 1: REGISTRO ==================
# =========================================================

# --- IDs DO REGISTRO ---
CANAL_REGISTRO_ID = 1229556030397218878
CANAL_LOG_REGISTRO_ID = 1462457604939841851
CONVIDADO_ROLE_ID = 1337382961456353342
EM_REGISTRO_ROLE_ID = 1337382961456353342

# --- FUNÇÕES AUXILIARES DO REGISTRO ---
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

# --- QUERIES DO REGISTRO ---
async def salvar_registro_historico(user_id, user_name, passaporte, nome, vulgo, telefone, indicado, tipo):
    pool = get_db()
    if not pool:
        logger.error("❌ Banco de dados não disponível para salvar registro")
        return
    
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO registros_historico (
                    user_id, user_name, passaporte, nome, vulgo, 
                    telefone, indicado, tipo, data_registro
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, 
                str(user_id),
                user_name,
                passaporte,
                nome,
                vulgo,
                telefone,
                indicado,
                tipo,
                agora_db()
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar registro histórico: {e}")

async def verificar_registro_existente(user_id):
    pool = get_db()
    if not pool:
        return False
    
    try:
        async with pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT 1 FROM registros_historico WHERE user_id = $1",
                str(user_id)
            )
    except Exception as e:
        logger.error(f"❌ Erro ao verificar registro: {e}")
        return False

# --- MODAIS E VIEWS DO REGISTRO ---
class RegistroModal(discord.ui.Modal, title="📋 Registro de Entrada"):
    passaporte = discord.ui.TextInput(
        label="📋 Passaporte",
        placeholder="Digite seu passaporte",
        required=True
    )
    nome = discord.ui.TextInput(
        label="👤 Nome (igual está na cidade)",
        placeholder="Ex: Rodrigo Santos",
        required=True
    )
    vulgo = discord.ui.TextInput(
        label="🏷️ Vulgo (opcional)",
        placeholder="Ex: Ruivo, Juca, Dreck, etc",
        required=False
    )
    telefone = discord.ui.TextInput(
        label="📱 Telefone In Game",
        placeholder="Ex: (11) 99999-9999",
        required=True
    )
    indicado = discord.ui.TextInput(
        label="👤 Indicado por (opcional)",
        placeholder="Nome de quem te indicou",
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        membro = interaction.user
        guild = interaction.guild
        
        nome_formatado = capitalizar_nome(self.nome.value)
        vulgo_formatado = capitalizar_nome(self.vulgo.value) if self.vulgo.value else None
        
        if vulgo_formatado:
            nick = f"{self.passaporte.value} - {nome_formatado} | {vulgo_formatado}"
        else:
            nick = f"{self.passaporte.value} - {nome_formatado}"
        
        try:
            await membro.edit(nick=nick)
        except Exception as e:
            logger.error(f"❌ Erro ao editar nick: {e}")
        
        view = TipoRegistroView(
            nome=nome_formatado,
            passaporte=self.passaporte.value,
            vulgo=vulgo_formatado,
            telefone=self.telefone.value,
            indicado=self.indicado.value if self.indicado.value else None
        )
        
        await interaction.response.send_message(
            "**Selecione o tipo de entrada:**\n\n"
            f"📋 **Passaporte:** {self.passaporte.value}\n"
            f"👤 **Nome:** {nome_formatado}\n"
            f"🏷️ **Vulgo:** {vulgo_formatado or 'Não informado'}\n"
            f"📱 **Telefone:** {self.telefone.value}\n"
            f"👤 **Indicado por:** {self.indicado.value or 'Não informado'}",
            view=view,
            ephemeral=True
        )

class TipoRegistroSelect(discord.ui.Select):
    def __init__(self, nome, passaporte, vulgo, telefone, indicado):
        self.nome = nome
        self.passaporte = passaporte
        self.vulgo = vulgo
        self.telefone = telefone
        self.indicado = indicado
        options = [
            discord.SelectOption(label="Agregado", description="Se tornar membro da facção", emoji="🕴️"),
            discord.SelectOption(label="Amigo", description="Apenas para resenha ou reunião", emoji="🤝")
        ]
        super().__init__(placeholder="Escolha o tipo de acesso", min_values=1, max_values=1, options=options)
    
    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        membro = interaction.user
        agregado = guild.get_role(AGREGADO_ROLE_ID)
        amigos = guild.get_role(1309121290241704046)
        convidado = guild.get_role(CONVIDADO_ROLE_ID)
        em_registro = guild.get_role(EM_REGISTRO_ROLE_ID)
        escolha = self.values[0]
        
        if em_registro:
            try:
                await membro.remove_roles(em_registro)
            except Exception as e:
                logger.error(f"❌ Erro ao remover cargo 'Em Registro': {e}")
        
        if escolha == "Agregado":
            if agregado:
                try:
                    await membro.add_roles(agregado)
                except Exception as e:
                    logger.error(f"❌ Erro ao adicionar cargo 'Agregado': {e}")
        elif escolha == "Amigo":
            if amigos:
                try:
                    await membro.add_roles(amigos)
                except Exception as e:
                    logger.error(f"❌ Erro ao adicionar cargo 'Amigos': {e}")
        
        if convidado:
            try:
                await membro.remove_roles(convidado)
            except:
                pass
        
        await salvar_registro_historico(
            membro.id,
            membro.name,
            self.passaporte,
            self.nome,
            self.vulgo,
            self.telefone,
            self.indicado,
            escolha
        )
        
        canal_log = interaction.guild.get_channel(CANAL_LOG_REGISTRO_ID)
        if canal_log:
            embed = discord.Embed(
                title="🎉 NOVO MEMBRO REGISTRADO!",
                description=f"**{membro.mention}** acabou de se registrar na **Vida Rasa**!",
                color=0x2ecc71,
                timestamp=agora()
            )
            if membro.display_avatar:
                embed.set_thumbnail(url=membro.display_avatar.url)
            
            informacoes = (
                f"**📋 Passaporte:** `{self.passaporte}`\n"
                f"**👤 Nome:** {self.nome}\n"
                f"**🏷️ Vulgo:** {self.vulgo or '❌ Não informado'}\n"
                f"**📱 Telefone:** {self.telefone}\n"
                f"**👤 Indicado por:** {self.indicado or '❌ Não informado'}\n"
                f"**🎯 Tipo:** {escolha}"
            )
            embed.add_field(name="📋 INFORMAÇÕES DO MEMBRO", value=informacoes, inline=False)
            embed.add_field(name="📌 STATUS", value=f"✅ **Registro concluído**\n🔹 Cargo atribuído: **{escolha}**\n🆔 ID: `{membro.id}`", inline=False)
            embed.add_field(name="🎯 CARGO ATRIBUÍDO", value=f"{'🕴️' if escolha == 'Agregado' else '🤝'} **{escolha}**", inline=False)
            embed.set_footer(text=f"Registro realizado com sucesso • Sistema Automático")
            
            try:
                await canal_log.send(embed=embed)
                await interaction.response.send_message(
                    f"✅ **Registro concluído com sucesso!**\n\n"
                    f"📋 Você foi registrado como: **{escolha}**\n"
                    f"👤 Nome: {self.nome}\n"
                    f"📨 Seu registro foi enviado para o histórico!",
                    ephemeral=True
                )
            except:
                await interaction.response.send_message(
                    f"✅ **Registro concluído com sucesso!**\n\n"
                    f"📋 Você foi registrado como: **{escolha}**\n"
                    f"⚠️ **Mas houve um erro ao enviar para o histórico!**",
                    ephemeral=True
                )

class TipoRegistroView(discord.ui.View):
    def __init__(self, nome, passaporte, vulgo, telefone, indicado):
        super().__init__(timeout=300)
        self.add_item(TipoRegistroSelect(nome, passaporte, vulgo, telefone, indicado))

class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📋 Fazer Registro", style=discord.ButtonStyle.success, custom_id="registro_fazer")
    async def registro(self, interaction: discord.Interaction, button: discord.ui.Button):
        existe = await verificar_registro_existente(interaction.user.id)
        if existe:
            await interaction.response.send_message(
                "❌ **Você já está registrado!**\nCaso precise atualizar seus dados, procure um administrador.",
                ephemeral=True
            )
            return
        await interaction.response.send_modal(RegistroModal())

# --- PAINEL DO REGISTRO ---
async def enviar_painel_registro():
    canal = bot.get_channel(CANAL_REGISTRO_ID)
    if not canal:
        logger.error("❌ Canal registro não encontrado")
        return
    embed = discord.Embed(
        title="📋 Registro",
        description="Clique no botão abaixo para realizar seu registro.",
        color=0x2ecc71
    )
    await enviar_ou_atualizar_painel("painel_registro", CANAL_REGISTRO_ID, embed, RegistroView())
    logger.info("✅ Painel de registro criado")

# =========================================================
# ==================== SEÇÃO 2: SAÍDA DE MEMBROS ==========
# =========================================================

MENSAGEM_SAIDA = {
    "titulo": "📤 NOTIFICAÇÃO DE SAÍDA",
    "mensagem": (
        "Olá **{nome}**, tudo bom?\n\n"
        "Devido à sua saída do servidor **Vida Rasa**, "
        "pedimos que procure algum **gerente in game** "
        "para tomar seu **PD da facção**.\n\n"
        "⚠️ **Caso já tenha tomado seu PD, ignore este aviso.**\n\n"
        "——————————————————\n"
        "_Se saiu por engano, você pode voltar a qualquer momento._"
    ),
    "cor": 0xe74c3c,
    "footer": "Vida Rasa • Sistema Automático"
}

# ID do canal de gerência
CANAL_GERENCIA_ID = 1237393478414241854

@bot.event
async def on_member_remove(member):
    if member.bot:
        return
    
    await asyncio.sleep(2)
    
    nome_servidor = member.display_name
    nome_usuario = member.name
    nome_global = member.global_name or nome_usuario
    
    status_apelido = "✅ **Diferente do nome de usuário**" if nome_servidor != nome_usuario and nome_servidor != nome_global else "ℹ️ **Mesmo nome de usuário**"
    apelido_detalhe = f"**Apelido no servidor:** {nome_servidor}\n**Nome de usuário:** {nome_usuario}" if status_apelido.startswith("✅") else f"**Nome usado:** {nome_servidor}"
    
    status_dm = ""
    dm_sucesso = False
    
    try:
        embed_msg = discord.Embed(
            title=MENSAGEM_SAIDA["titulo"],
            description=MENSAGEM_SAIDA["mensagem"].format(nome=member.display_name),
            color=MENSAGEM_SAIDA["cor"]
        )
        if member.display_avatar:
            embed_msg.set_thumbnail(url=member.display_avatar.url)
        embed_msg.set_footer(text=f"{MENSAGEM_SAIDA['footer']} • ID: {member.id}")
        
        await member.send(embed=embed_msg)
        status_dm = "✅ **MENSAGEM ENVIADA COM SUCESSO**"
        dm_sucesso = True
        cor_log = 0xe74c3c
        logger.info(f"✅ [SAÍDA] DM enviada para {member.name} (ID: {member.id})")
        
    except discord.Forbidden:
        status_dm = "❌ **MENSAGEM NÃO ENVIADA**\nMotivo: Usuário bloqueou o bot ou tem DM fechada"
        dm_sucesso = False
        cor_log = 0xf1c40f
        logger.info(f"❌ [SAÍDA] DM bloqueada para {member.name}")
    except discord.HTTPException as e:
        status_dm = f"❌ **MENSAGEM NÃO ENVIADA**\nMotivo: Erro HTTP - {e}"
        dm_sucesso = False
        cor_log = 0xf1c40f
        logger.info(f"❌ [SAÍDA] Erro HTTP para {member.name}: {e}")
    except Exception as e:
        status_dm = f"❌ **MENSAGEM NÃO ENVIADA**\nMotivo: Erro inesperado - {str(e)[:100]}"
        dm_sucesso = False
        cor_log = 0xf1c40f
        logger.info(f"❌ [SAÍDA] Erro para {member.name}: {e}")
    
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
# ==================== SEÇÃO 3: PRODUÇÃO (GALPÕES) =======
# =========================================================

# --- IDs DA PRODUÇÃO ---
CANAL_FABRICACAO_ID = 1466421612566810634
CANAL_REGISTRO_GALPAO_ID = 1356174712337862819
CANAL_BAU_GALPAO_SUL_ID = 1356174937764794521
CANAL_BAU_GALPAO_ID = 1448561598384963747
CANAL_CALCULO_POLVORA_ID = 1462834441968943157
CANAL_REGISTRO_POLVORA_ID = 1448570795101261846

# --- QUERIES DA PRODUÇÃO ---
async def carregar_producao(pid):
    try:
        pool = get_db()
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
    
    pool = get_db()
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
                pid,
                dados["galpao"],
                str(dados["autor"]),
                inicio_str,
                fim_str,
                dados.get("obs", ""),
                str(dados["msg_id"]),
                str(dados["canal_id"]),
                segunda_user,
                segunda_time,
                dados.get("polvora", 400),
                qtd_galpoes,
                polvora_por_galpao
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar produção {pid}: {e}")

async def deletar_producao(pid):
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM producoes WHERE pid=$1", pid)
    except Exception as e:
        logger.error(f"❌ Erro ao deletar produção {pid}: {e}")

# --- ESTOQUE QUERIES ---
async def carregar_estoque():
    pool = get_db()
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

async def atualizar_estoque(tipo, quantidade, operacao="adicionar"):
    pool = get_db()
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

async def carregar_estoque_insumos():
    pool = get_db()
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

async def atualizar_estoque_capsulas(quantidade, operacao="adicionar"):
    pool = get_db()
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

async def atualizar_estoque_embalagens(quantidade, operacao="adicionar"):
    pool = get_db()
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

async def registrar_entrada_insumos(tipo, quantidade, registrado_por, obs=""):
    pool = get_db()
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

async def registrar_producao_municao(tipo, pacotes, produzido_por, obs=""):
    municoes = pacotes * 50
    capsulas_consumidas, embalagens_consumidas = await consumir_insumos_producao(tipo, pacotes)
    pool = get_db()
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

# --- PÓLVORA QUERIES ---
async def salvar_polvora_db(user_id, qtd, valor):
    pool = get_db()
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

async def carregar_polvoras_db():
    pool = get_db()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM polvoras")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar pólvoras: {e}")
        return []

async def limpar_polvoras_db():
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM polvoras")
    except Exception as e:
        logger.error(f"❌ Erro ao limpar pólvoras: {e}")

# --- FUNÇÕES DE PÓLVORA VENDIDA ---
async def salvar_venda_polvora(user_id, quantidade):
    """Salva uma venda de pólvora pendente."""
    pool = get_db()
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

async def buscar_polvora_pendente(user_id):
    """Busca a pólvora pendente de um membro."""
    pool = get_db()
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

async def pagar_polvora(user_id):
    """Marca todas as vendas de pólvora como pagas."""
    pool = get_db()
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

async def resetar_polvora_pendente(user_id):
    """Reseta a pólvora pendente (usado quando cancela)."""
    pool = get_db()
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

class VenderPolvoraModal(discord.ui.Modal, title="💣 Vender Pólvora"):
    def __init__(self, user_id):
        super().__init__(timeout=300)
        self.user_id = user_id
    
    quantidade = discord.ui.TextInput(
        label="📦 Quantidade de Pólvora",
        placeholder="Digite a quantidade (ex: 100)",
        required=True,
        max_length=10
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        try:
            qtd = int(self.quantidade.value.strip())
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
        
        # Apenas confirma e atualiza o embed da meta
        await interaction.followup.send(
            f"✅ **{fmt_num(qtd)} unidades de pólvora registradas para venda!**\n"
            f"💰 Valor a receber: {formatar_dinheiro(valor)}\n"
            f"📌 Aguarde o pagamento no painel da sua meta.",
            ephemeral=True
        )
        
        # Atualizar o embed da meta (mostra a pólvora pendente)
        await atualizar_embed_meta(self.user_id)
        
        embed = discord.Embed(
            title="💣 VENDA DE PÓLVORA REGISTRADA",
            description=f"👤 <@{self.user_id}>",
            color=0xe67e22,
            timestamp=agora()
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

    
class ConfirmarPagamentoPolvoraView(discord.ui.View):
    def __init__(self, user_id, pendente):
        super().__init__(timeout=120)
        self.user_id = user_id
        self.pendente = pendente
    
    @discord.ui.button(label="✅ Confirmar Pagamento", style=discord.ButtonStyle.success, custom_id="confirmar_pagamento_polvora", emoji="✅")
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        # Marcar como pago
        sucesso = await pagar_polvora(self.user_id)
        if not sucesso:
            await interaction.followup.send("❌ Erro ao marcar pólvora como paga!", ephemeral=True)
            return
        
        # 🔹 Buscar o canal do membro para notificar
        canal_membro = None
        pool = get_db()
        if pool:
            async with pool.acquire() as conn:
                canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(self.user_id))
                if canal_id:
                    canal_membro = interaction.guild.get_channel(int(canal_id))
        
        # 🔹 Se não achou o canal, tenta encontrar pelo nome do membro
        if not canal_membro:
            member = interaction.guild.get_member(int(self.user_id))
            if member:
                for canal in interaction.guild.text_channels:
                    if member.display_name.lower() in canal.name.lower() and "📁" in canal.name:
                        canal_membro = canal
                        break
        
        # 🔹 Notificar o membro na sala dele (com embed)
        if canal_membro:
            embed_notificacao = discord.Embed(
                title="✅ PÓLVORA PAGA!",
                description=f"👤 <@{self.user_id}> sua pólvora foi paga!",
                color=0x2ecc71,
                timestamp=agora()
            )
            embed_notificacao.add_field(
                name="📦 Quantidade",
                value=f"{fmt_num(self.pendente['quantidade'])} unidades",
                inline=True
            )
            embed_notificacao.add_field(
                name="💰 Valor recebido",
                value=formatar_dinheiro(self.pendente['valor']),
                inline=True
            )
            embed_notificacao.add_field(
                name="💵 Preço por unidade",
                value=f"R$ {PRECO_POLVORA:.2f}",
                inline=True
            )
            embed_notificacao.set_footer(text="Pólvora paga com sucesso! ✅")
            
            await canal_membro.send(embed=embed_notificacao)
        else:
            # 🔹 Se não achou o canal, tenta enviar por DM
            try:
                member = interaction.guild.get_member(int(self.user_id))
                if member:
                    embed_dm = discord.Embed(
                        title="✅ PÓLVORA PAGA!",
                        description=f"Sua pólvora foi paga!",
                        color=0x2ecc71,
                        timestamp=agora()
                    )
                    embed_dm.add_field(
                        name="📦 Quantidade",
                        value=f"{fmt_num(self.pendente['quantidade'])} unidades",
                        inline=True
                    )
                    embed_dm.add_field(
                        name="💰 Valor recebido",
                        value=formatar_dinheiro(self.pendente['valor']),
                        inline=True
                    )
                    embed_dm.add_field(
                        name="💵 Preço por unidade",
                        value=f"R$ {PRECO_POLVORA:.2f}",
                        inline=True
                    )
                    await member.send(embed=embed_dm)
            except:
                pass
        
        # Embed para o gerente (confirmando que foi pago)
        embed = discord.Embed(
            title="✅ PÓLVORA PAGA COM SUCESSO!",
            description=f"👤 <@{self.user_id}>",
            color=0x2ecc71,
            timestamp=agora()
        )
        embed.add_field(
            name="📦 Quantidade",
            value=f"{fmt_num(self.pendente['quantidade'])} unidades",
            inline=True
        )
        embed.add_field(
            name="💰 Valor pago",
            value=formatar_dinheiro(self.pendente['valor']),
            inline=True
        )
        embed.add_field(
            name="💵 Preço por unidade",
            value=f"R$ {PRECO_POLVORA:.2f}",
            inline=True
        )
        embed.set_footer(text="Pólvora paga! ✅")
        
        await interaction.followup.send(embed=embed, ephemeral=True)
        
        # Atualizar o embed da meta (remove a pólvora pendente)
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
            
# --- FUNÇÕES AUXILIARES DA PRODUÇÃO ---
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

# --- LOOP DA PRODUÇÃO ---
async def acompanhar_producao(pid):
    logger.info(f"▶ Produção iniciada/restaurada: {pid}")
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
                logger.info(f"⏰ Produção {pid} expirou, finalizando...")
                canal = bot.get_channel(prod["canal_id"])
                if canal:
                    try:
                        msg = await canal.fetch_message(prod["msg_id"])
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
                    msg = await canal.fetch_message(prod["msg_id"])
                except discord.NotFound:
                    desc = await gerar_desc_producao(prod)
                    embed = discord.Embed(title="🏭 Produção", description=desc, color=0x3498db)
                    view = None if prod.get("segunda_task_confirmada") else SegundaTaskView(pid)
                    msg = await canal.send(embed=embed, view=view)
                    prod["msg_id"] = msg.id
                    await salvar_producao(pid, prod)
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao buscar mensagem {pid}: {e}")
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
                        await msg.edit(embed=discord.Embed(title="🏭 Produção", description=desc, color=0x34495e))
                    except discord.NotFound:
                        msg = None
                        continue
                    except discord.HTTPException as e:
                        if e.status == 429:
                            await asyncio.sleep(5)
        
        except Exception as e:
            logger.error(f"❌ Erro no acompanhar_producao {pid}: {e}")
        
        await asyncio.sleep(10)

async def finalizar_producao(pid, msg, prod):
    logger.info(f"🔵 FINALIZANDO produção {pid}")
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
        
        pool = get_db()
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
                desc = msg.embeds[0].description if msg.embeds else ""
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
                await msg.edit(embed=discord.Embed(title="🏭 Produção", description=desc, color=0x34495e), view=None)
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
        logger.info(f"✅ Produção {pid} finalizada com {capsulas_total} cápsulas ({qtd_galpoes} galpões)")
    except Exception as e:
        logger.error(f"❌ ERRO ao finalizar produção {pid}: {e}")

# --- LOOP HEARTBEAT ---
async def verificar_heartbeat_producoes():
    try:
        pool = get_db()
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
                            msg = await canal.fetch_message(prod["msg_id"])
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
                        await canal.fetch_message(prod["msg_id"])
                    except discord.NotFound:
                        desc = await gerar_desc_producao(prod)
                        embed = discord.Embed(title="🏭 Produção", description=desc, color=0x3498db)
                        view = None if prod.get("segunda_task_confirmada") else SegundaTaskView(pid)
                        msg = await canal.send(embed=embed, view=view)
                        prod["msg_id"] = msg.id
                        await salvar_producao(pid, prod)
        logger.info(f"💚 HEARTBEAT: {len(producoes_ativas)} produções ativas verificadas")
    except Exception as e:
        logger.error(f"❌ Erro no heartbeat: {e}")

# --- VIEWS E MODAIS DA PRODUÇÃO ---
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
                        msg = await canal.fetch_message(prod["msg_id"])
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
            qtd = int(self.qtd_galpoes.value)
            if qtd not in [1, 2, 3]:
                raise ValueError
        except:
            await interaction.followup.send("❌ Quantidade de galpões inválida! Digite 1, 2 ou 3.", ephemeral=True)
            return
        try:
            polvora_por_galpao = int(self.polvora_por_galpao.value)
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
        msg = await canal.send(
            embed=discord.Embed(title=f"🏭 Produção - {qtd} Galpão(ões)", description=desc, color=0x3498db),
            view=SegundaTaskView(pid)
        )
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
            pacotes = int(self.quantidade_pacotes.value.replace(".", "").replace(",", ""))
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

class RegistrarCapsulasModal(discord.ui.Modal, title="📦 Registrar Cápsulas"):
    quantidade = discord.ui.TextInput(label="Quantidade de CÁPSULAS", placeholder="Ex: 1000", required=True)
    observacao = discord.ui.TextInput(label="Observação (opcional)", style=discord.TextStyle.paragraph, required=False)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            quantidade = int(self.quantidade.value.replace(".", "").replace(",", ""))
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

class RegistrarEmbalagensModal(discord.ui.Modal, title="📦 Registrar Embalagens"):
    quantidade = discord.ui.TextInput(label="Quantidade de EMBALAGENS", placeholder="Ex: 500", required=True)
    observacao = discord.ui.TextInput(label="Observação (opcional)", style=discord.TextStyle.paragraph, required=False)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            quantidade = int(self.quantidade.value.replace(".", "").replace(",", ""))
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

class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🏭 Galpões Norte", style=discord.ButtonStyle.primary, custom_id="fabricacao_norte")
    async def norte(self, interaction: discord.Interaction, button: discord.ui.Button):
        pool = get_db()
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
        pool = get_db()
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
        
        embed = discord.Embed(
            title="📅 STATUS DOS ALUGUEIS",
            color=0x3498db,
            timestamp=agora()
        )
        
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
            
            embed.add_field(
                name=f"🏭 {galpao}",
                value=f"**Dias alugados:** {dias}\n**Status:** {status}",
                inline=True
            )
        
        await interaction.followup.send(embed=embed, ephemeral=True)

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
            pool = get_db()
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

# --- PAINEL DA PRODUÇÃO ---
async def enviar_painel_fabricacao():
    """Envia o painel de fabricação com informações de aluguel."""
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
    
    # --- ALUGUEL DE GALPÕES ---
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
    
    embed.add_field(
        name="📅 ALUGUEL DE GALPÕES",
        value=texto_alugueis or "Nenhum aluguel registrado",
        inline=False
    )
    
    # --- ESTOQUES ---
    embed.add_field(
        name="📦 ESTOQUE DE MUNIÇÃO",
        value=f"🔫 **PT:** {fmt_num(estoque_municoes['PT'])} pacotes\n🔫 **SUB:** {fmt_num(estoque_municoes['SUB'])} pacotes",
        inline=False
    )
    
    embed.add_field(
        name="💊 ESTOQUE DE INSUMOS",
        value=f"**Cápsulas:** {fmt_num(estoque_insumos['capsulas'])} unidades\n**Embalagens:** {fmt_num(estoque_insumos['embalagens'])} unidades",
        inline=False
    )
    
    embed.add_field(
        name="🏭 PRODUÇÃO DE CÁPSULAS",
        value=(
            "• **Galpões Norte:** 65 minutos (3 galpões)\n"
            "• **Galpões Sul:** 130 minutos (3 galpões)\n\n"
            "💡 Ao clicar, informe:\n"
            "   - Quantos galpões (1, 2 ou 3)\n"
            "   - Pólvora por galpão"
        ),
        inline=False
    )
    
    embed.set_footer(text=f"🔄 Atualizado em {agora().strftime('%d/%m/%Y %H:%M:%S')}")
    
    view = FabricacaoView()
    
    try:
        async for msg in canal.history(limit=20):
            if msg.author == bot.user and msg.embeds and msg.embeds[0].title == "🏭 PAINEL DE FABRICAÇÃO":
                try:
                    await msg.delete()
                except:
                    pass
        await canal.send(embed=embed, view=view)
        logger.info("🏭 Painel de fabricação atualizado")
    except Exception as e:
        logger.error(f"❌ Erro ao enviar painel de fabricação: {e}")

# --- PAINEL DE PÓLVORA ---
class PolvoraModal(discord.ui.Modal, title="Registro de Compra de Pólvora"):
    quantidade = discord.ui.TextInput(label="Quantidade de Pólvora", placeholder="Digite apenas a quantidade (ex: 100)", required=True)
    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value.strip())
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

class ConfirmarPagamentoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="Confirmar pagamento", style=discord.ButtonStyle.success, custom_id="confirmar_pagamento")
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.message.edit(content=interaction.message.content + "\n\n✅ **PAGO**", view=None)
        await responder_interacao(interaction, defer=True)

class PolvoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="Registrar Compra de Pólvora", style=discord.ButtonStyle.primary, custom_id="polvora_btn")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PolvoraModal())

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
    logger.info("💣 Painel de pólvora verificado/atualizado")

# =========================================================
# ==================== FUNÇÕES DE ALUGUEL =================
# =========================================================

async def criar_tabela_alugueis():
    """Cria tabela de aluguel de galpões."""
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            # DROP e RECREATE para garantir a estrutura correta
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
            
            # Verificar se a coluna existe, se não, adicionar
            coluna_existe = await conn.fetchval("""
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'alugueis' AND column_name = 'dias_alugados'
            """)
            
            if not coluna_existe:
                await conn.execute("""
                    ALTER TABLE alugueis ADD COLUMN dias_alugados INTEGER DEFAULT 0
                """)
                logger.info("✅ Coluna dias_alugados adicionada")
            
            # Verificar se já existem registros
            existe_norte = await conn.fetchval("SELECT 1 FROM alugueis WHERE galpao = 'NORTE'")
            if not existe_norte:
                await conn.execute("""
                    INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo)
                    VALUES ('NORTE', 0, NOW(), true)
                """)
            
            existe_sul = await conn.fetchval("SELECT 1 FROM alugueis WHERE galpao = 'SUL'")
            if not existe_sul:
                await conn.execute("""
                    INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo)
                    VALUES ('SUL', 0, NOW(), true)
                """)
            
            logger.info("✅ TABELA ALUGUEIS CRIADA/VERIFICADA")
    except Exception as e:
        logger.error(f"❌ ERRO AO CRIAR TABELA ALUGUEIS: {e}")

async def salvar_aluguel(galpao, dias):
    """Salva ou atualiza o aluguel de um galpão."""
    pool = get_db()
    if not pool:
        return False
    try:
        # Garantir que dias é inteiro
        dias = int(dias)
        
        async with pool.acquire() as conn:
            # Verificar se já existe registro ativo
            existe = await conn.fetchval(
                "SELECT id FROM alugueis WHERE galpao = $1 AND ativo = true",
                galpao
            )
            
            if existe:
                # Atualizar somando dias com CAST explícito
                await conn.execute("""
                    UPDATE alugueis 
                    SET dias_alugados = dias_alugados + $1::INTEGER,
                        data_atualizacao = NOW()
                    WHERE galpao = $2 AND ativo = true
                """, dias, galpao)
            else:
                # Criar novo registro com CAST explícito
                await conn.execute("""
                    INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo)
                    VALUES ($1, $2::INTEGER, NOW(), true)
                """, galpao, dias)
            
            return True
    except Exception as e:
        logger.error(f"❌ ERRO AO SALVAR ALUGUEL: {e}")
        return False

async def carregar_alugueis():
    """Carrega os dados de aluguel dos galpões."""
    pool = get_db()
    if not pool:
        return {"GALPÕES NORTE": {"dias": 0, "inicio": None}, "GALPÕES SUL": {"dias": 0, "inicio": None}}
    try:
        async with pool.acquire() as conn:
            # Primeiro, limpar registros antigos que não são os principais
            await conn.execute("""
                UPDATE alugueis 
                SET ativo = false 
                WHERE galpao NOT IN ('GALPÕES NORTE', 'GALPÕES SUL')
                  AND ativo = true
            """)
            
            # Garantir que os registros principais existem
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
            
            # Carregar apenas os registros ativos principais
            rows = await conn.fetch("""
                SELECT galpao, dias_alugados, data_inicio 
                FROM alugueis 
                WHERE ativo = true 
                AND galpao IN ('GALPÕES NORTE', 'GALPÕES SUL')
            """)
            
            resultado = {}
            for row in rows:
                galpao = row["galpao"]
                resultado[galpao] = {
                    "dias": row["dias_alugados"] or 0,
                    "inicio": row["data_inicio"]
                }
            
            # Garantir que ambos existem no resultado
            if "GALPÕES NORTE" not in resultado:
                resultado["GALPÕES NORTE"] = {"dias": 0, "inicio": None}
            if "GALPÕES SUL" not in resultado:
                resultado["GALPÕES SUL"] = {"dias": 0, "inicio": None}
            
            return resultado
    except Exception as e:
        logger.error(f"❌ ERRO AO CARREGAR ALUGUEIS: {e}")
        return {"GALPÕES NORTE": {"dias": 0, "inicio": None}, "GALPÕES SUL": {"dias": 0, "inicio": None}}
        
async def resetar_aluguel(galpao):
    """Reseta o aluguel de um galpão."""
    pool = get_db()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            # Desativar registro antigo
            await conn.execute("""
                UPDATE alugueis 
                SET ativo = false 
                WHERE galpao = $1 AND ativo = true
            """, galpao)
            
            # Criar novo registro com 0 dias
            await conn.execute("""
                INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo)
                VALUES ($1, 0, NOW(), true)
            """, galpao)
            return True
    except Exception as e:
        logger.error(f"❌ ERRO AO RESETAR ALUGUEL: {e}")
        return False

# =========================================================
# ==================== MODAL DE ALUGUEL ===================
# =========================================================

class AlugarGalpaoModal(discord.ui.Modal, title="📅 Alugar Galpão"):
    galpao = discord.ui.TextInput(
        label="🏭 Qual galpão?",
        placeholder="Digite NORTE ou SUL",
        required=True,
        max_length=5
    )
    dias = discord.ui.TextInput(
        label="📅 Quantos dias?",
        placeholder="Digite o número de dias",
        required=True,
        max_length=3
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        galpao_input = self.galpao.value.strip().upper()
        
        # Converter para o nome correto
        if galpao_input == "NORTE":
            galpao = "GALPÕES NORTE"
        elif galpao_input == "SUL":
            galpao = "GALPÕES SUL"
        else:
            await interaction.followup.send("❌ Galpão inválido! Use NORTE ou SUL.", ephemeral=True)
            return
        
        try:
            dias = int(self.dias.value.strip())
            if dias <= 0:
                raise ValueError
        except ValueError:
            await interaction.followup.send("❌ Número de dias inválido! Digite um número inteiro positivo.", ephemeral=True)
            return
        
        # Salvar com o valor inteiro
        sucesso = await salvar_aluguel(galpao, dias)
        
        if not sucesso:
            await interaction.followup.send("❌ Erro ao salvar aluguel. Tente novamente.", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="📅 ALUGUEL REGISTRADO",
            description=f"🏭 **{galpao}**\n📅 **{dias} dias** adicionados",
            color=0x2ecc71,
            timestamp=agora()
        )
        
        alugueis = await carregar_alugueis()
        dados = alugueis.get(galpao, {})
        total_dias = dados.get("dias", 0)
        embed.add_field(name="📊 Total de dias", value=f"{total_dias} dias", inline=True)
        
        await interaction.followup.send(embed=embed, ephemeral=True)
        await enviar_painel_fabricacao()
# =========================================================
# ==================== SEÇÃO 4: VENDAS ====================
# =========================================================

# --- IDs DAS VENDAS ---
CANAL_CALCULADORA_ID = 1460984821458272347
CANAL_ENCOMENDAS_ID = 1460980984811098294
CANAL_VENDAS_ID = CANAL_CALCULADORA_ID
CANAL_TEXTOS_VENDAS_ID = 1499045083994001500

# --- VARIÁVEIS GLOBAIS DAS VENDAS ---
mensagens_em_andamento = set()

# --- ORGANIZAÇÕES CONFIG ---
ORGANIZACOES_CONFIG = {
    "VDR": {"emoji": "🕴️", "cor": 0x1e3a8a},
    "POLICIA": {"emoji": "👮", "cor": 0x3498db},
    "MAFIA": {"emoji": "🤵", "cor": 0x8e44ad},
    "BALAS": {"emoji": "🔫", "cor": 0xe67e22},
    "FAMILIA": {"emoji": "👨‍👩‍👧‍👦", "cor": 0x2ecc71}
}

# --- QUERIES DAS VENDAS ---
async def proximo_pedido():
    pool = get_db()
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

async def salvar_venda_db(vendedor_id, valor, pedido_numero):
    pool = get_db()
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

async def atualizar_valor_venda_db(pedido_numero, valor):
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE vendas SET valor=$1 WHERE pedido_numero=$2", valor, pedido_numero)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar venda: {e}")

async def carregar_vendas_db():
    pool = get_db()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM vendas")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar vendas: {e}")
        return []

async def salvar_entrega_parcelada(pedido_original, total_entregas, pt_por_entrega, sub_por_entrega, vendedor_id, organizacao, observacoes, canal_id):
    pool = get_db()
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

async def buscar_entregas_pendentes():
    pool = get_db()
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

async def atualizar_entrega_parcelada(entrega_id, entrega_atual, mensagem_id, proxima_entrega=None):
    pool = get_db()
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

async def finalizar_entregas(entrega_id):
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE entregas_parceladas SET ativo = false WHERE id = $1", entrega_id)
    except Exception as e:
        logger.error(f"❌ Erro ao finalizar entregas: {e}")

async def salvar_entrega_detalhes(entrega_id, entregas_json):
    pool = get_db()
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

async def registrar_saida_estoque(pedido_numero, tipo, pacotes, retirado_por):
    pool = get_db()
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

async def verificar_estoque_suficiente(tipo, pacotes_necessarios):
    estoque = await carregar_estoque()
    return estoque.get(tipo, 0) >= pacotes_necessarios

# --- FUNÇÕES AUXILIARES DAS VENDAS ---
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
    msg = await canal.send(embed=embed, view=StatusView(entrega_id=entrega_id))
    if entrega_id:
        await atualizar_entrega_parcelada(entrega_id, entrega_atual, str(msg.id), None)
    return msg

async def buscar_grupo_por_organizacao(nome_org):
    pool = get_db()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            return await conn.fetchrow("SELECT grupo_id FROM grupos WHERE LOWER(nome_org) = LOWER($1) AND ativo = true", nome_org)
    except Exception as e:
        logger.error(f"❌ Erro ao buscar grupo por organização: {e}")
        return None

# --- VIEWS E MODAIS DAS VENDAS ---
class StatusView(discord.ui.View):
    def __init__(self, disabled: bool = False, entrega_id: int = None, total_entregas: int = 1):
        super().__init__(timeout=None)
        self.entrega_id = entrega_id
        self.total_entregas = total_entregas
        self.entrega_ja_entregue = False
        self.proxima_criada = False
        if total_entregas > 1:
            for child in self.children:
                if child.custom_id == "criar_proxima_entrega":
                    child.disabled = False
                    child.label = f"📦 Criar Próxima Entrega (2/{total_entregas})"
        if disabled:
            for item in self.children:
                item.disabled = True

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

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="status_pago")
    async def pago(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)
        if self.pedido_cancelado(linhas):
            await interaction.response.send_message("⚠️ Este pedido foi cancelado.", ephemeral=True)
            return
        if self.pedido_pago(linhas):
            await interaction.response.send_message("⚠️ Este pedido já foi pago.", ephemeral=True)
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
            await interaction.message.edit(embed=embed, view=StatusView(disabled=True))
        else:
            await interaction.message.edit(embed=embed, view=self)
        await responder_interacao(interaction, defer=True)

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="status_entregue")
    async def entregue(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.entrega_ja_entregue:
            await interaction.response.send_message("⚠️ **Esta entrega já foi marcada como entregue!**", ephemeral=True)
            return
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)
        if self.pedido_cancelado(linhas):
            await interaction.response.send_message("⚠️ Este pedido foi cancelado.", ephemeral=True)
            return
        if self.entrega_ja_foi_entregue(linhas):
            await interaction.response.send_message("⚠️ **Esta entrega já foi entregue!**", ephemeral=True)
            return
        pacotes_pt = 0
        pacotes_sub = 0
        for field in embed.fields:
            if field.name == "🔫 PT":
                try:
                    linhas_field = field.value.split("\n")
                    for l in linhas_field:
                        if "📦" in l:
                            pacotes_pt = int(l.replace("📦", "").replace("pacotes", "").strip())
                except:
                    pass
            if field.name == "🔫 SUB":
                try:
                    linhas_field = field.value.split("\n")
                    for l in linhas_field:
                        if "📦" in l:
                            pacotes_sub = int(l.replace("📦", "").replace("pacotes", "").strip())
                except:
                    pass
        if pacotes_pt > 0:
            estoque_suficiente = await verificar_estoque_suficiente("PT", pacotes_pt)
            if not estoque_suficiente:
                estoque_atual = await carregar_estoque()
                await interaction.response.send_message(f"❌ **ESTOQUE INSUFICIENTE!**\n\n🔫 PT: {pacotes_pt} pacotes necessários\n📦 Estoque atual: {estoque_atual['PT']} pacotes", ephemeral=True)
                return
        if pacotes_sub > 0:
            estoque_suficiente = await verificar_estoque_suficiente("SUB", pacotes_sub)
            if not estoque_suficiente:
                estoque_atual = await carregar_estoque()
                await interaction.response.send_message(f"❌ **ESTOQUE INSUFICIENTE!**\n\n🔫 SUB: {pacotes_sub} pacotes necessários\n📦 Estoque atual: {estoque_atual['SUB']} pacotes", ephemeral=True)
                return
        self.entrega_ja_entregue = True
        for child in self.children:
            if child.custom_id == "status_entregue":
                child.disabled = True
                child.label = "✅ Entregue (Concluído)"
        titulo = embed.title
        pedido_numero = int(titulo.split("#")[1]) if "#" in titulo else 0
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
        if finalizado:
            embed.color = 0x2ecc71
            embed.title = "🎉 VENDA CONCLUÍDA"
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed.add_field(name="✅ VENDA FINALIZADA COM SUCESSO", value="💰 **Pagamento recebido**\n📦 **Pedido entregue ao cliente**\n📊 **Estoque atualizado**", inline=False)
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="🔥 **Pedido encerrado no sistema**", inline=False)
            await interaction.message.edit(embed=embed, view=StatusView(disabled=True))
        else:
            await interaction.message.edit(embed=embed, view=self)
        await responder_interacao(interaction, defer=True)
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

    @discord.ui.button(label="📦 Criar Próxima Entrega", style=discord.ButtonStyle.primary, custom_id="criar_proxima_entrega", disabled=False)
    async def criar_proxima(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not self.entrega_id:
            await interaction.response.send_message("❌ Esta venda não tem entregas parceladas.", ephemeral=True)
            return
        if self.proxima_criada:
            await interaction.response.send_message("⚠️ **A próxima entrega já foi criada!**", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        try:
            pool = get_db()
            if not pool:
                await interaction.followup.send("❌ Banco de dados indisponível.", ephemeral=True)
                return
            async with pool.acquire() as conn:
                entrega = await conn.fetchrow("SELECT * FROM entregas_parceladas WHERE id = $1 AND ativo = true", self.entrega_id)
            if not entrega:
                await interaction.followup.send("❌ **Entrega não encontrada!**", ephemeral=True)
                return
            entrega_atual = entrega["entrega_atual"]
            total_entregas = entrega["total_entregas"]
            if entrega_atual >= total_entregas:
                await interaction.followup.send(f"✅ **Todas as {total_entregas} entregas já foram concluídas!**", ephemeral=True)
                self.proxima_criada = True
                for child in self.children:
                    if child.custom_id == "criar_proxima_entrega":
                        child.disabled = True
                        child.label = "✅ Todas criadas"
                await interaction.message.edit(view=self)
                return
            proxima_entrega_num = entrega_atual + 1
            pedido_original = entrega["pedido_original"]
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
                await interaction.followup.send(f"❌ **Erro: Entrega {proxima_entrega_num} não encontrada!**", ephemeral=True)
                return
            entrega_data = entregas_lista[idx]
            pt_entrega = entrega_data["pt"]
            sub_entrega = entrega_data["sub"]
            if pt_entrega == 0 and sub_entrega == 0:
                await interaction.followup.send(f"✅ **Todas as entregas foram concluídas!**", ephemeral=True)
                self.proxima_criada = True
                for child in self.children:
                    if child.custom_id == "criar_proxima_entrega":
                        child.disabled = True
                        child.label = "✅ Todas criadas"
                await interaction.message.edit(view=self)
                return
            vendedor_id = entrega["vendedor_id"]
            organizacao = entrega["organizacao"]
            observacoes = entrega["observacoes"]
            canal_id = int(entrega["canal_id"])
            canal = bot.get_channel(canal_id)
            if not canal:
                await interaction.followup.send(f"❌ **Canal {canal_id} não encontrado!**", ephemeral=True)
                return
            config = ORGANIZACOES_CONFIG.get(organizacao, {"emoji": "🏷️", "cor": 0x1e3a8a})
            grupo = await buscar_grupo_por_organizacao(organizacao)
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
            embed_novo.add_field(name="📋 STATUS DAS ENTREGAS", value=f"**Total de entregas:** {total_entregas}\n**Entrega atual:** {proxima_entrega_num}/{total_entregas}\n**Próxima entrega:** 🔒 Aguardando esta ser ENTREGUE", inline=False)
            embed_novo.add_field(name="📌 Status", value="📦 A Entregar\n⏳ Pagamento pendente", inline=False)
            if observacoes:
                embed_novo.add_field(name="📝 Observações", value=observacoes, inline=False)
            if grupo:
                embed_novo.add_field(name="📊 INTEGRAÇÃO COM GRUPO", value=f"✅ Compra registrada automaticamente no grupo **{organizacao}**", inline=False)
            embed_novo.set_footer(text=f"🛡 Sistema de Encomendas • VDR 442 • Entrega {proxima_entrega_num}/{total_entregas} • ID: {self.entrega_id}")
            msg = await canal.send(embed=embed_novo, view=StatusView(entrega_id=self.entrega_id, total_entregas=total_entregas))
            await atualizar_entrega_parcelada(self.entrega_id, proxima_entrega_num, str(msg.id), None)
            self.proxima_criada = True
            for child in self.children:
                if child.custom_id == "criar_proxima_entrega":
                    child.disabled = True
                    child.label = f"✅ Próxima criada ({proxima_entrega_num}/{total_entregas})"
            await interaction.message.edit(view=self)
            await interaction.followup.send(f"✅ **Entrega {proxima_entrega_num}/{total_entregas} criada com sucesso!**", ephemeral=True)
            await enviar_painel_vendas()
            await enviar_painel_fabricacao()
        except Exception as e:
            logger.error(f"❌ Erro ao criar próxima entrega: {e}")
            await interaction.followup.send(f"❌ **Erro ao criar próxima entrega:** {str(e)}", ephemeral=True)

    @discord.ui.button(label="❌ Pedido cancelado", style=discord.ButtonStyle.danger, custom_id="status_cancelado")
    async def cancelado(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)
        if self.pedido_pago(linhas):
            await interaction.response.send_message("⚠️ Este pedido já foi pago e não pode ser cancelado.", ephemeral=True)
            return
        if self.pedido_cancelado(linhas):
            await interaction.response.send_message("⚠️ Este pedido já foi cancelado.", ephemeral=True)
            return
        if self.entrega_ja_foi_entregue(linhas):
            pacotes_pt = 0
            pacotes_sub = 0
            for field in embed.fields:
                if field.name == "🔫 PT":
                    try:
                        linhas_field = field.value.split("\n")
                        for l in linhas_field:
                            if "📦" in l:
                                pacotes_pt = int(l.replace("📦", "").replace("pacotes", "").strip())
                    except:
                        pass
                if field.name == "🔫 SUB":
                    try:
                        linhas_field = field.value.split("\n")
                        for l in linhas_field:
                            if "📦" in l:
                                pacotes_sub = int(l.replace("📦", "").replace("pacotes", "").strip())
                    except:
                        pass
            if pacotes_pt > 0 or pacotes_sub > 0:
                titulo = embed.title
                pedido_numero = int(titulo.split("#")[1]) if "#" in titulo else 0
                if pacotes_pt > 0:
                    await atualizar_estoque("PT", pacotes_pt, "adicionar")
                if pacotes_sub > 0:
                    await atualizar_estoque("SUB", pacotes_sub, "adicionar")
                canal_bau = interaction.guild.get_channel(CANAL_BAU_GALPAO_SUL_ID)
                if canal_bau:
                    try:
                        texto = f"🔄 **REVERSÃO DE ESTOQUE - PEDIDO CANCELADO**\n\n"
                        texto += f"👤 Cancelado por: {interaction.user.mention}\n"
                        texto += f"📦 Pedido #{pedido_numero}\n"
                        if pacotes_pt > 0:
                            texto += f"🔫 PT: +{pacotes_pt} pacotes (reabastecido)\n"
                        if pacotes_sub > 0:
                            texto += f"🔫 SUB: +{pacotes_sub} pacotes (reabastecido)"
                        await canal_bau.send(texto)
                    except Exception as e:
                        logger.error(f"Erro envio baú reversão: {e}")
        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        user = interaction.user.mention
        if self.entrega_ja_foi_entregue(linhas):
            linhas = [f"❌ Pedido cancelado por {user} • {agora_str}\n🔄 **ESTOQUE REVERTIDO**"]
        else:
            linhas = [f"❌ Pedido cancelado por {user} • {agora_str}"]
        embed = self.set_status(embed, idx, linhas)
        await interaction.message.edit(embed=embed, view=StatusView(disabled=True))
        await responder_interacao(interaction, defer=True)
        if self.entrega_id:
            await finalizar_entregas(self.entrega_id)
        await enviar_painel_vendas()
        await enviar_painel_fabricacao()

class EditarVendaModal(discord.ui.Modal, title="✏️ Editar Venda"):
    qtd_pt = discord.ui.TextInput(label="Nova Quantidade PT", placeholder="Digite a nova quantidade de PT")
    qtd_sub = discord.ui.TextInput(label="Nova Quantidade SUB", placeholder="Digite a nova quantidade de SUB")
    organizacao = discord.ui.TextInput(label="Nova Organização (opcional)", placeholder="Digite o novo nome da organização", required=False)
    observacao = discord.ui.TextInput(label="Nova Observação (opcional)", style=discord.TextStyle.paragraph, required=False)
    def __init__(self, message):
        super().__init__()
        self.message = message
    async def on_submit(self, interaction: discord.Interaction):
        try:
            pt = int(self.qtd_pt.value.strip())
            sub = int(self.qtd_sub.value.strip())
        except:
            await interaction.response.send_message("❌ Valores inválidos.", ephemeral=True)
            return
        pacotes_pt = pt // 50
        pacotes_sub = sub // 50
        total = (pt * 50) + (sub * 90)
        valor_formatado = formatar_dinheiro(total)
        embed = self.message.embeds[0]
        pt_antigo = 0
        sub_antigo = 0
        valor_antigo = 0
        organizacao_antiga = "Desconhecida"
        for field in embed.fields:
            if field.name == "🔫 PT":
                try:
                    pt_antigo = int(field.value.split(" munições")[0])
                except:
                    pass
            if field.name == "🔫 SUB":
                try:
                    sub_antigo = int(field.value.split(" munições")[0])
                except:
                    pass
            if field.name == "💰 Total":
                try:
                    valor_antigo = float(field.value.replace("**R$ ", "").replace("**", "").replace(".", "").replace(",", "."))
                except:
                    pass
            if field.name == "🏷 Organização":
                organizacao_antiga = field.value
        pacotes_pt_antigo = pt_antigo // 50
        pacotes_sub_antigo = sub_antigo // 50
        for i, field in enumerate(embed.fields):
            if field.name == "🔫 PT":
                embed.set_field_at(i, name="🔫 PT", value=f"{pt} munições\n📦 {pacotes_pt} pacotes", inline=True)
            if field.name == "🔫 SUB":
                embed.set_field_at(i, name="🔫 SUB", value=f"{sub} munições\n📦 {pacotes_sub} pacotes", inline=True)
            if field.name == "💰 Total":
                embed.set_field_at(i, name="💰 Total", value=f"**{valor_formatado}**", inline=False)
            if field.name == "🏷 Organização" and self.organizacao.value:
                embed.set_field_at(i, name="🏷 Organização", value=self.organizacao.value.strip(), inline=False)
            if field.name == "📝 Observações" and self.observacao.value:
                embed.set_field_at(i, name="📝 Observações", value=self.observacao.value.strip(), inline=False)
        titulo = embed.title
        pedido_numero = int(titulo.split("#")[1])
        await atualizar_valor_venda_db(pedido_numero, total)
        await self.message.edit(embed=embed)
        alteracoes = []
        if pt_antigo != pt:
            alteracoes.append(f"PT: {pt_antigo} → {pt}")
        if sub_antigo != sub:
            alteracoes.append(f"SUB: {sub_antigo} → {sub}")
        if valor_antigo != total:
            alteracoes.append(f"Valor: {formatar_dinheiro(valor_antigo)} → {valor_formatado}")
        if self.organizacao.value:
            alteracoes.append("Organização alterada")
        if self.observacao.value:
            alteracoes.append("Observação alterada")
        org_nome_final = self.organizacao.value.strip().upper() if self.organizacao.value else organizacao_antiga
        if org_nome_final:
            grupo = await buscar_grupo_por_organizacao(org_nome_final)
            if grupo:
                diff_pt = pacotes_pt - pacotes_pt_antigo
                diff_sub = pacotes_sub - pacotes_sub_antigo
                if diff_pt > 0:
                    await registrar_compra_grupo_db(grupo["grupo_id"], "PT", diff_pt, diff_pt * 50)
                elif diff_pt < 0:
                    await registrar_compra_grupo_db(grupo["grupo_id"], "PT", diff_pt, diff_pt * 50)
                if diff_sub > 0:
                    await registrar_compra_grupo_db(grupo["grupo_id"], "SUB", diff_sub, diff_sub * 90)
                elif diff_sub < 0:
                    await registrar_compra_grupo_db(grupo["grupo_id"], "SUB", diff_sub, diff_sub * 90)
                await recriar_painel_grupos()
        canal_log = interaction.guild.get_channel(1478381934026424391)
        if canal_log:
            embed_log = discord.Embed(title="✏️ Venda Editada", color=0xf1c40f)
            embed_log.add_field(name="👤 Editado por", value=interaction.user.mention, inline=False)
            embed_log.add_field(name="🧾 Pedido", value=embed.title, inline=False)
            embed_log.add_field(name="🔧 Alterações", value="\n".join(alteracoes) if alteracoes else "Nenhuma", inline=False)
            await canal_log.send(embed=embed_log)
        msg_resposta = f"✅ **Venda editada com sucesso!**\n\n📦 **Pedido #{pedido_numero:04d}**\n🔫 **PT:** {pt} munições ({pacotes_pt} pacotes)\n🔫 **SUB:** {sub} munições ({pacotes_sub} pacotes)\n💰 **Total:** {valor_formatado}"
        await interaction.response.send_message(msg_resposta, ephemeral=True)

class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(
        label="🏷️ Organização", 
        placeholder="Digite o nome da organização (ex: VDR, POLICIA)", 
        required=True
    )
    qtd_pt = discord.ui.TextInput(
        label="🔫 Quantidade PT", 
        placeholder="Digite a quantidade de munição PT (ex: 24000)", 
        required=True
    )
    qtd_sub = discord.ui.TextInput(
        label="🔫 Quantidade SUB", 
        placeholder="Digite a quantidade de munição SUB (ex: 16000)", 
        required=True
    )
    total_entregas = discord.ui.TextInput(
        label="📦 Número de entregas", 
        placeholder="Ex: 2, 3, 4... (padrão: 1)", 
        required=False
    )
    observacoes = discord.ui.TextInput(
        label="📝 Observações", 
        style=discord.TextStyle.paragraph, 
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        # PRIMEIRO: Deferir a resposta para não expirar
        await interaction.response.defer(ephemeral=True)
        
        try:
            pt = int(self.qtd_pt.value.strip()) if self.qtd_pt.value.strip() else 0
            sub = int(self.qtd_sub.value.strip()) if self.qtd_sub.value.strip() else 0
            if pt < 0 or sub < 0:
                raise ValueError
            if pt == 0 and sub == 0:
                await interaction.followup.send("❌ Você precisa informar pelo menos PT ou SUB!", ephemeral=True)
                return
        except ValueError:
            await interaction.followup.send("❌ Valores inválidos.", ephemeral=True)
            return
        
        try:
            total_entregas = int(self.total_entregas.value.strip()) if self.total_entregas.value else 1
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
        
        # Buscar grupo e registrar compras
        grupo = await buscar_grupo_por_organizacao(org_nome)
        if grupo:
            if pacotes_pt_total > 0:
                await registrar_compra_grupo_db(grupo["grupo_id"], "PT", pacotes_pt_total, pacotes_pt_total * 50)
            if pacotes_sub_total > 0:
                await registrar_compra_grupo_db(grupo["grupo_id"], "SUB", pacotes_sub_total, pacotes_sub_total * 90)
            await recriar_painel_grupos()
        
        # Criar embeds de entrega
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
            
            msg_resposta = f"✅ **Venda parcelada registrada!**\n\n📦 **Pedido #{numero_pedido:04d}**\n🏷 **Organização:** {org_nome}\n📦 **Total PT:** {fmt_num(pt)} munições\n📦 **Total SUB:** {fmt_num(sub)} munições\n💰 **Total:** {formatar_dinheiro(total)}\n\n📋 **Entregas ({num_entregas} no total):**\n{resumo_entregas}\n✅ **Entrega 1/{num_entregas} criada!**"
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
        pool = get_db()
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

# --- PAINEL DE VENDAS ---
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
    logger.info("🛒 Painel de vendas atualizado")

# =========================================================
# ==================== SEÇÃO 5: METAS =====================
# =========================================================

# --- IDs DAS METAS (já definidos globalmente) ---

# --- VARIÁVEIS GLOBAIS DAS METAS ---
# metas_cache já definida globalmente

# --- QUERIES DAS METAS ---
async def carregar_metas_db():
    pool = get_db()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM metas")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar metas: {e}")
        return []

async def salvar_meta_db(user_id, canal_id, dinheiro, polvora, acao):
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            if acao is not None:
                acao = str(acao)
            await conn.execute(
                """
                INSERT INTO metas (user_id, canal_id, dinheiro, polvora, acao)
                VALUES ($1,$2,$3,$4,$5)
                ON CONFLICT (user_id)
                DO UPDATE SET canal_id=$2, dinheiro=$3, polvora=$4, acao=$5
                """,
                str(user_id), str(canal_id), dinheiro, polvora, acao
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar meta: {e}")

async def depositar_na_meta_db(user_id, valor):
    pool = get_db()
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

async def adicionar_polvora_meta(user_id, quantidade):
    pool = get_db()
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

async def adicionar_dinheiro_meta(user_id, valor):
    pool = get_db()
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
        logger.error(f"❌ Erro ao adicionar dinheiro: {e}")
        return False

async def fechar_meta(user_id, data_inicio, data_fim):
    pool = get_db()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(user_id))
            if not meta:
                return None
            acao = meta.get("acao")
            if acao is None:
                acao = "N/A"
            else:
                acao = str(acao)
            await conn.execute(
                """
                INSERT INTO metas_historico (user_id, dinheiro, polvora, acao, dinheiro_acoes, data_inicio, data_fim, data_fechamento)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                str(user_id), meta["dinheiro"], meta["polvora"], acao,
                meta.get("dinheiro_acoes") or 0, data_inicio, data_fim, agora_db()
            )
            await conn.execute("UPDATE metas SET dinheiro = 0, polvora = 0, dinheiro_acoes = 0 WHERE user_id = $1", str(user_id))
            return {"dinheiro": meta["dinheiro"], "polvora": meta["polvora"], "acao": acao, "dinheiro_acoes": meta.get("dinheiro_acoes") or 0}
    except Exception as e:
        logger.error(f"❌ Erro ao fechar meta: {e}")
        return None

async def buscar_historico_metas(data_inicio, data_fim):
    pool = get_db()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT * FROM metas_historico 
                WHERE data_fechamento BETWEEN $1 AND $2
                ORDER BY data_fechamento DESC
                """,
                data_inicio, data_fim
            )
    except Exception as e:
        logger.error(f"❌ Erro ao buscar histórico: {e}")
        return []

async def fechar_todas_metas(data_inicio, data_fim):
    pool = get_db()
    if not pool:
        return None, []
    try:
        async with pool.acquire() as conn:
            metas = await conn.fetch("SELECT * FROM metas")
            if not metas:
                return None, []
            
            relatorio = []
            guild = bot.get_guild(GUILD_ID)
            
            # Processar cada meta
            for meta in metas:
                user_id = meta["user_id"]
                member = guild.get_member(int(user_id)) if guild else None
                
                # Verificar status do membro
                status = membro_deve_ter_meta(member) if member else None
                
                # Se não tem cargo relevante, pular
                if status is None:
                    continue
                
                dinheiro = meta["dinheiro"] or 0
                polvora = meta["polvora"] or 0
                acao = meta["acao"] or "N/A"
                dinheiro_acoes = meta.get("dinheiro_acoes") or 0
                total = dinheiro + dinheiro_acoes
                
                # Se for isento, registrar com status especial
                if status == "isento":
                    # Salvar no histórico como isento
                    await conn.execute(
                        """
                        INSERT INTO metas_historico (user_id, dinheiro, polvora, acao, dinheiro_acoes, data_inicio, data_fim, data_fechamento)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        """,
                        user_id, dinheiro, polvora, acao, dinheiro_acoes, data_inicio, data_fim, agora_db()
                    )
                    relatorio.append({
                        "user_id": user_id, 
                        "dinheiro": dinheiro, 
                        "polvora": polvora, 
                        "acao": acao, 
                        "dinheiro_acoes": dinheiro_acoes, 
                        "total": total,
                        "status": "isento"
                    })
                else:
                    # Salvar no histórico normalmente
                    await conn.execute(
                        """
                        INSERT INTO metas_historico (user_id, dinheiro, polvora, acao, dinheiro_acoes, data_inicio, data_fim, data_fechamento)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                        """,
                        user_id, dinheiro, polvora, acao, dinheiro_acoes, data_inicio, data_fim, agora_db()
                    )
                    relatorio.append({
                        "user_id": user_id, 
                        "dinheiro": dinheiro, 
                        "polvora": polvora, 
                        "acao": acao, 
                        "dinheiro_acoes": dinheiro_acoes, 
                        "total": total,
                        "status": "obrigado"
                    })
                
                # Resetar meta
                await conn.execute("UPDATE metas SET dinheiro = 0, polvora = 0, dinheiro_acoes = 0 WHERE user_id = $1", user_id)
            
            # Buscar membros SEM META (apenas quem tem cargo obrigatório)
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

async def zerar_todas_metas():
    pool = get_db()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE metas SET dinheiro = 0, dinheiro_acoes = 0, polvora = 0")
            rows = await conn.fetch("SELECT user_id, canal_id FROM metas")
            return rows
    except Exception as e:
        logger.error(f"❌ Erro ao zerar metas: {e}")
        return []

async def verificar_meta_concluida(user_id, valor_total):
    if valor_total >= 300000:
        pool = get_db()
        if not pool:
            return False
        try:
            async with pool.acquire() as conn:
                ja_avisado = await conn.fetchval("SELECT 1 FROM metas_avisos WHERE user_id = $1 AND tipo = 'concluida' AND data > NOW() - INTERVAL '1 day'", str(user_id))
                if not ja_avisado:
                    await conn.execute("INSERT INTO metas_avisos (user_id, tipo, data) VALUES ($1, 'concluida', $2)", str(user_id), agora_db())
                    canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(user_id))
                    if canal_id:
                        canal = bot.get_channel(int(canal_id))
                        if canal:
                            user = await pegar_usuario(user_id)
                            embed = discord.Embed(title="🎉 META SEMANAL CONCLUÍDA!", description=f"{user.mention} **parabéns!** Sua meta semanal de **R$ 300.000,00** foi atingida! 🎉", color=0x2ecc71)
                            embed.add_field(name="💰 Total atingido", value=formatar_dinheiro(valor_total), inline=True)
                            embed.add_field(name="📅 Data", value=agora().strftime('%d/%m/%Y %H:%M'), inline=True)
                            await canal.send(embed=embed)
                            return True
            return False
        except Exception as e:
            logger.error(f"❌ Erro ao verificar meta concluída: {e}")
            return False

async def verificar_avisos_quarta():
    """Verifica e envia aviso na quarta-feira para quem não fez depósito."""
    hoje = agora()
    # Só executa na quarta-feira (weekday = 2)
    if hoje.weekday() != 2:
        logger.info("📅 Hoje não é quarta-feira. Avisos não enviados.")
        return
    
    logger.info("📨 Verificando avisos de quarta-feira...")
    
    pool = get_db()
    if not pool:
        logger.error("❌ Banco de dados indisponível!")
        return
    
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            logger.error("❌ Guild não encontrada!")
            return
        
        # Cargos que DEVEM ter meta (obrigados)
        cargos_obrigados = [
            CARGO_AGREGADO_ID,
            CARGO_MEMBRO_ID,
            CARGO_SOLDADO_ID,
            CARGO_01_ID,
            CARGO_02_ID,
            CARGO_RESP_METAS_ID,
            CARGO_RESP_ACAO_ID,
            CARGO_RESP_VENDAS_ID,
            CARGO_RESP_PRODUCAO_ID
        ]
        
        async with pool.acquire() as conn:
            avisos_enviados = 0
            for member in guild.members:
                if member.bot:
                    continue
                
                # Verificar se o membro tem cargo obrigatório
                tem_cargo = any(r.id in cargos_obrigados for r in member.roles)
                if not tem_cargo:
                    continue
                
                user_id = str(member.id)
                
                # Buscar meta do membro
                meta = await conn.fetchrow("SELECT dinheiro, dinheiro_acoes FROM metas WHERE user_id = $1", user_id)
                
                # Se não tem meta, criar uma
                if not meta:
                    logger.info(f"📝 Criando meta para {member.display_name} (não tinha)")
                    
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
                
                # SÓ AVISA QUEM NÃO FEZ NENHUM DEPÓSITO
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
                                logger.info(f"📨 Aviso de quarta enviado para {member.display_name}")
        
        logger.info(f"✅ Avisos de quarta-feira enviados: {avisos_enviados} membros")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro ao verificar avisos de quarta: {e}")
        return False

# --- FUNÇÕES AUXILIARES DAS METAS ---
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
                "dinheiro_acoes": r.get("dinheiro_acoes") or 0
            }
        logger.info(f"📊 Cache de metas recarregado: {len(metas_cache)} metas")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao recarregar cache de metas: {e}")
        return False

async def criar_sala_meta(member: discord.Member):
    guild = member.guild
    pool = get_db()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            meta_existente = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(member.id))
        if meta_existente:
            canal_id = int(meta_existente["canal_id"])
            canal_existe = guild.get_channel(canal_id)
            if canal_existe:
                metas_cache[str(member.id)] = {"canal_id": canal_id, "dinheiro": meta_existente["dinheiro"], "polvora": meta_existente["polvora"], "acao": meta_existente["acao"], "dinheiro_acoes": meta_existente.get("dinheiro_acoes") or 0}
                await atualizar_embed_meta(member.id)
                return canal_existe
            else:
                await conn.execute("DELETE FROM metas WHERE user_id = $1", str(member.id))
                if str(member.id) in metas_cache:
                    del metas_cache[str(member.id)]
        for canal in guild.text_channels:
            if member.display_name.lower() in canal.name.lower() and "📁" in canal.name:
                await salvar_meta_db(member.id, canal.id, 0, 0, 0)
                metas_cache[str(member.id)] = {"canal_id": canal.id, "dinheiro": 0, "polvora": 0, "acao": None, "dinheiro_acoes": 0}
                await atualizar_embed_meta(member.id)
                return canal
        categoria_id = obter_categoria_meta(member)
        if not categoria_id:
            return None
        categoria = guild.get_channel(categoria_id)
        if not categoria:
            return None
        nome_canal = f"📁・{member.display_name.lower().replace(' ', '-')}"
        overwrites = {guild.default_role: discord.PermissionOverwrite(view_channel=False), member: discord.PermissionOverwrite(view_channel=True, send_messages=True)}
        gerente = guild.get_role(CARGO_GERENTE_ID)
        if gerente:
            overwrites[gerente] = discord.PermissionOverwrite(view_channel=True)
        gerente_geral = guild.get_role(CARGO_GERENTE_GERAL_ID)
        if gerente_geral:
            overwrites[gerente_geral] = discord.PermissionOverwrite(view_channel=True)
        canal = await guild.create_text_channel(nome_canal, category=categoria, overwrites=overwrites)
        await salvar_meta_db(member.id, canal.id, 0, 0, 0)
        metas_cache[str(member.id)] = {"canal_id": canal.id, "dinheiro": 0, "polvora": 0, "acao": None, "dinheiro_acoes": 0}
        await asyncio.sleep(1)
        await atualizar_embed_meta(member.id)
        return canal
    except Exception as e:
        logger.error(f"❌ Erro ao criar sala meta: {e}")
        return None

async def atualizar_embed_meta(user_id):
    """Atualiza o embed da meta de um usuário."""
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
            pool = get_db()
            if pool:
                async with pool.acquire() as conn:
                    await conn.execute("DELETE FROM metas WHERE user_id = $1", str(user_id))
            return
        
        pool = get_db()
        if not pool:
            return
        
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(user_id))
        
        if not meta:
            await salvar_meta_db(user_id, canal.id, 0, 0, 0)
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(user_id))
            if not meta:
                return
            metas_cache[str(user_id)] = {"canal_id": canal.id, "dinheiro": 0, "polvora": 0, "acao": None, "dinheiro_acoes": 0}
        
        # Buscar pólvora pendente
        pendente = await buscar_polvora_pendente(user_id)
        
        # Buscar o apelido do membro
        guild = bot.get_guild(GUILD_ID)
        member = guild.get_member(int(user_id))
        if member:
            nome = member.display_name
        else:
            user = await pegar_usuario(user_id)
            nome = user.display_name if user else str(user_id)
        
        dinheiro_meta = meta["dinheiro"] or 0
        polvora = meta["polvora"] or 0
        
        embed = discord.Embed(
            title=f"📊 META DE {nome.upper()}",
            color=0x3498db,
            timestamp=agora()
        )
        
        # DINHEIRO SUJO
        embed.add_field(
            name="💰 DINHEIRO SUJO (Meta)",
            value=formatar_dinheiro(dinheiro_meta),
            inline=False
        )
        
        # Pólvora da meta
        if pendente and pendente["quantidade"] > 0:
            embed.add_field(
                name="💣 PÓLVORA",
                value=f"**Na meta:** {fmt_num(polvora)} unidades\n**Vendida (pendente):** {fmt_num(pendente['quantidade'])} unidades (R$ {formatar_dinheiro(pendente['valor'])})",
                inline=False
            )
        else:
            embed.add_field(
                name="💣 PÓLVORA",
                value=f"{fmt_num(polvora)} unidades" if polvora > 0 else "0 unidades",
                inline=False
            )
        
        # BARRA DE PROGRESSO DA META (R$ 300.000)
        meta_total = 300000
        progresso = min(dinheiro_meta / meta_total, 1.0)
        barra = "▓" * int(progresso * 20) + "░" * (20 - int(progresso * 20))
        porcentagem = int(progresso * 100)
        
        if progresso >= 1:
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
            value=f"`{barra}` **{porcentagem}%**\n**{status_meta}**\n💰 {formatar_dinheiro(dinheiro_meta)} / {formatar_dinheiro(meta_total)}",
            inline=False
        )
        
        embed.add_field(
            name="📌 COMO USAR",
            value="**💣 Vender Pólvora** - Venda pólvora para a facção\n**💰 Adicionar Dinheiro Sujo** - Registre dinheiro da meta\n**💰 Pólvora Paga** - Gerente paga a pólvora pendente",
            inline=False
        )
        embed.set_footer(text=f"ID: {user_id}")
        
        # Deletar mensagens antigas
        mensagens_deletadas = 0
        async for msg in canal.history(limit=30):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    mensagens_deletadas += 1
                    await asyncio.sleep(0.3)
                except:
                    pass
        
        await canal.send(embed=embed, view=MetaView(user_id))
        await verificar_meta_concluida(user_id, dinheiro_meta)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar embed da meta: {e}")
        
def membro_deve_ter_meta(member):
    """Verifica se o membro deve ter meta baseado nos cargos."""
    if not member:
        return None
    
    # Cargos que DEVEM ter meta
    cargos_com_meta = [
        CARGO_AGREGADO_ID,
        CARGO_MEMBRO_ID,
        CARGO_SOLDADO_ID,
        CARGO_01_ID,
        CARGO_02_ID,
        CARGO_RESP_METAS_ID,
        CARGO_RESP_ACAO_ID,
        CARGO_RESP_VENDAS_ID,
        CARGO_RESP_PRODUCAO_ID
    ]
    
    # Cargos que são ISENTOS (não pagam mas aparecem no relatório)
    cargos_isentos = [
        CARGO_GERENTE_ID,
        CARGO_GERENTE_GERAL_ID
    ]
    
    roles = [r.id for r in member.roles]
    
    # Se tem cargo de gerente, é isento
    if any(r in roles for r in cargos_isentos):
        return "isento"
    
    # Se tem cargo que deve ter meta
    if any(r in roles for r in cargos_com_meta):
        return "obrigado"
    
    # Não tem cargo relevante
    return None

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
            if msg.author == bot.user and msg.embeds:
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

async def depositar_na_meta(user_id, valor, motivo):
    pool = get_db()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT dinheiro, dinheiro_acoes FROM metas WHERE user_id = $1", str(user_id))
            if meta:
                if "Ação" in motivo:
                    novo_acoes = (meta["dinheiro_acoes"] or 0) + valor
                    await conn.execute("UPDATE metas SET dinheiro_acoes = $1 WHERE user_id = $2", novo_acoes, str(user_id))
                else:
                    novo_valor = meta["dinheiro"] + valor
                    await conn.execute("UPDATE metas SET dinheiro = $1 WHERE user_id = $2", novo_valor, str(user_id))
                canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(user_id))
                if canal_id:
                    canal = bot.get_channel(int(canal_id))
                    if canal:
                        await canal.send(f"💰 **Depósito recebido!**\n📝 Motivo: {motivo}\n💵 Valor: {formatar_dinheiro(valor)}\n✨ **Saldo atualizado na sua meta!**")
                return True
            else:
                guild = bot.get_guild(GUILD_ID)
                member = guild.get_member(int(user_id))
                if member:
                    canal = await criar_sala_meta(member)
                    if canal:
                        if "Ação" in motivo:
                            await conn.execute("UPDATE metas SET dinheiro_acoes = $1 WHERE user_id = $2", valor, str(user_id))
                        else:
                            await conn.execute("UPDATE metas SET dinheiro = $1 WHERE user_id = $2", valor, str(user_id))
                        return True
                return False
    except Exception as e:
        logger.error(f"❌ Erro ao depositar na meta: {e}")
        return False

# --- VIEWS E MODAIS DAS METAS ---
class AdicionarPolvoraModal(discord.ui.Modal, title="💣 Adicionar Pólvora"):
    quantidade = discord.ui.TextInput(label="Quantidade de Pólvora", placeholder="Digite a quantidade (ex: 100)", required=True)
    def __init__(self, user_id):
        super().__init__()
        self.user_id = user_id
    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value.strip())
            if qtd <= 0:
                raise ValueError
        except:
            await interaction.response.send_message("❌ Quantidade inválida!", ephemeral=True)
            return
        pool = get_db()
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
        sucesso = await adicionar_polvora_meta(self.user_id, qtd)
        if not sucesso:
            await interaction.response.send_message("❌ Erro ao adicionar pólvora!", ephemeral=True)
            return
        await carregar_metas_cache()
        await atualizar_embed_meta(self.user_id)
        await interaction.response.send_message(f"✅ **{fmt_num(qtd)} pólvora(s) adicionada(s) à meta!**", ephemeral=True)

class VenderPolvoraModal(discord.ui.Modal, title="💣 Vender Pólvora"):
    def __init__(self, user_id):
        super().__init__(timeout=300)
        self.user_id = user_id
    
    quantidade = discord.ui.TextInput(
        label="📦 Quantidade de Pólvora",
        placeholder="Digite a quantidade (ex: 100)",
        required=True,
        max_length=10
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        try:
            qtd = int(self.quantidade.value.strip())
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
            color=0xe67e22,
            timestamp=agora()
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

class AdicionarDinheiroModal(discord.ui.Modal, title="💰 Adicionar Dinheiro Sujo"):
    quantidade = discord.ui.TextInput(label="Valor do Dinheiro Sujo", placeholder="Digite o valor (ex: 5000)", required=True)
    def __init__(self, user_id):
        super().__init__()
        self.user_id = user_id
    async def on_submit(self, interaction: discord.Interaction):
        try:
            valor = int(self.quantidade.value.strip())
            if valor <= 0:
                raise ValueError
        except:
            await interaction.response.send_message("❌ Valor inválido!", ephemeral=True)
            return
        pool = get_db()
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

class FecharMetaModal(discord.ui.Modal, title="🔒 Fechar Meta"):
    data_inicio = discord.ui.TextInput(label="📅 Data de INÍCIO da meta", placeholder="Ex: 01/06/2026", required=True)
    data_fim = discord.ui.TextInput(label="📅 Data de FIM da meta", placeholder="Ex: 30/06/2026", required=True)
    def __init__(self, user_id):
        super().__init__()
        self.user_id = user_id
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            inicio = datetime.strptime(self.data_inicio.value.strip(), "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value.strip(), "%d/%m/%Y")
        except ValueError:
            await interaction.followup.send("❌ Formato de data inválido! Use DD/MM/AAAA", ephemeral=True)
            return
        if fim < inicio:
            await interaction.followup.send("❌ Data de FIM deve ser depois da data de INÍCIO!", ephemeral=True)
            return
        resultado = await fechar_meta(self.user_id, inicio, fim)
        if not resultado:
            await interaction.followup.send("❌ Meta não encontrada!", ephemeral=True)
            return
        embed = discord.Embed(title="🔒 META FECHADA", description=f"👤 <@{self.user_id}>", color=0xe74c3c)
        embed.add_field(name="💰 Dinheiro Sujo", value=formatar_dinheiro(resultado["dinheiro"]), inline=True)
        embed.add_field(name="🎯 Dinheiro Ações", value=formatar_dinheiro(resultado["dinheiro_acoes"]), inline=True)
        embed.add_field(name="💣 Pólvora", value=f"{fmt_num(resultado['polvora'])} unidades", inline=True)
        embed.add_field(name="📅 Período", value=f"{self.data_inicio.value} até {self.data_fim.value}", inline=False)
        embed.set_footer(text=f"Ação: {resultado['acao'] or 'N/A'}")
        canal = interaction.guild.get_channel(RESULTADOS_METAS_ID)
        if canal:
            await canal.send(embed=embed)
        await atualizar_embed_meta(self.user_id)
        await interaction.followup.send(f"✅ **Meta fechada com sucesso!**\n\n💰 Dinheiro: {formatar_dinheiro(resultado['dinheiro'])}\n🎯 Ações: {formatar_dinheiro(resultado['dinheiro_acoes'])}\n💣 Pólvora: {fmt_num(resultado['polvora'])} unidades", ephemeral=True)

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
        if not historico:
            await interaction.followup.send(f"📭 Nenhuma meta fechada no período **{self.data_inicio.value}** até **{self.data_fim.value}**.", ephemeral=True)
            return
        
        total_dinheiro = sum(r["dinheiro"] for r in historico)
        total_polvora = sum(r["polvora"] for r in historico)
        total_acoes = sum(r.get("dinheiro_acoes") or 0 for r in historico)
        total_geral = total_dinheiro + total_acoes
        
        guild = interaction.guild
        
        # --- CLASSIFICAR OS REGISTROS ---
        # Buscar o cargo de cada membro para classificar
        isentos = []
        pagaram = []
        nao_pagaram = []
        
        for item in historico:
            user_id = int(item["user_id"])
            member = guild.get_member(user_id) if guild else None
            
            # Verificar status do membro
            status = membro_deve_ter_meta(member) if member else None
            
            # Se não tem cargo relevante, pular (não entra no relatório)
            if status is None:
                continue
            
            total = item["dinheiro"] + (item.get("dinheiro_acoes") or 0)
            
            # Adicionar o status no item
            item_dict = dict(item)
            item_dict["total"] = total
            item_dict["status"] = status
            
            if status == "isento":
                isentos.append(item_dict)
            elif status == "obrigado" and total > 0:
                pagaram.append(item_dict)
            elif status == "obrigado" and total == 0:
                nao_pagaram.append(item_dict)
        
        # Ordenar quem pagou por valor (maior para menor)
        pagaram_ordenado = sorted(pagaram, key=lambda x: x["total"], reverse=True)
        
        # --- EMBED 1: RESUMO GERAL ---
        embed_resumo = discord.Embed(
            title="📊 RELATÓRIO DE METAS FECHADAS",
            description=f"📅 **Período:** {self.data_inicio.value} até {self.data_fim.value}",
            color=0x2ecc71,
            timestamp=agora()
        )
        embed_resumo.add_field(
            name="📊 RESUMO GERAL",
            value=(
                f"💰 **Dinheiro Sujo (Meta):** {formatar_dinheiro(total_dinheiro)}\n"
                f"🎯 **Dinheiro de Ações:** {formatar_dinheiro(total_acoes)}\n"
                f"💣 **Pólvora:** {fmt_num(total_polvora)} unidades\n"
                f"📦 **Total Geral:** {formatar_dinheiro(total_geral)}\n"
                f"👥 **Total de metas fechadas:** {len(historico)}\n"
                f"✅ **Pagaram:** {len(pagaram)}\n"
                f"❌ **Não pagaram:** {len(nao_pagaram)}\n"
                f"🟡 **Isentos (Gerentes):** {len(isentos)}"
            ),
            inline=False
        )
        embed_resumo.set_footer(text=f"Relatório gerado por {interaction.user.display_name}")
        
        # --- EMBEDS PARA QUEM PAGOU ---
        embeds_pagaram = []
        if pagaram_ordenado:
            for i in range(0, len(pagaram_ordenado), 10):
                grupo = pagaram_ordenado[i:i+10]
                embed = discord.Embed(
                    title=f"✅ QUEM PAGOU ({len(pagaram)} membros) - Parte {i//10 + 1}",
                    color=0x2ecc71
                )
                texto = ""
                for idx, item in enumerate(grupo, i + 1):
                    member = guild.get_member(int(item["user_id"])) if guild else None
                    if member:
                        nome = member.display_name
                    else:
                        user = await pegar_usuario(int(item["user_id"]))
                        nome = user.display_name if user else f"ID: {item['user_id']}"
                    texto += f"**{idx}.** {nome} - {formatar_dinheiro(item['total'])}\n"
                embed.add_field(name="📋 LISTA", value=texto, inline=False)
                embeds_pagaram.append(embed)
        
        # --- EMBEDS PARA QUEM NÃO PAGOU ---
        embeds_nao_pagaram = []
        if nao_pagaram:
            for i in range(0, len(nao_pagaram), 10):
                grupo = nao_pagaram[i:i+10]
                embed = discord.Embed(
                    title=f"❌ QUEM NÃO PAGOU ({len(nao_pagaram)} membros) - Parte {i//10 + 1}",
                    color=0xe74c3c
                )
                texto = ""
                for idx, item in enumerate(grupo, i + 1):
                    member = guild.get_member(int(item["user_id"])) if guild else None
                    if member:
                        nome = member.display_name
                    else:
                        user = await pegar_usuario(int(item["user_id"]))
                        nome = user.display_name if user else f"ID: {item['user_id']}"
                    texto += f"**{idx}.** {nome} - ❌ ZERADO\n"
                embed.add_field(name="📋 LISTA", value=texto, inline=False)
                embeds_nao_pagaram.append(embed)
        
        # --- EMBEDS PARA ISENTOS (GERENTES) ---
        embeds_isentos = []
        if isentos:
            for i in range(0, len(isentos), 10):
                grupo = isentos[i:i+10]
                embed = discord.Embed(
                    title=f"🟡 META ISENTA ({len(isentos)} gerentes) - Parte {i//10 + 1}",
                    color=0xf1c40f
                )
                texto = ""
                for idx, item in enumerate(grupo, i + 1):
                    member = guild.get_member(int(item["user_id"])) if guild else None
                    if member:
                        nome = member.display_name
                    else:
                        user = await pegar_usuario(int(item["user_id"]))
                        nome = user.display_name if user else f"ID: {item['user_id']}"
                    texto += f"**{idx}.** {nome} - 🟡 ISENTO (Gerente)\n"
                embed.add_field(name="📋 LISTA", value=texto, inline=False)
                embeds_isentos.append(embed)
        
        # --- ENVIAR TUDO ---
        canal_resultados = interaction.guild.get_channel(RESULTADOS_METAS_ID)
        if not canal_resultados:
            canal_resultados = interaction.channel
        
        # Enviar embed de resumo
        await canal_resultados.send(embed=embed_resumo)
        await asyncio.sleep(0.5)
        
        # Enviar embeds de quem pagou
        for embed in embeds_pagaram:
            await canal_resultados.send(embed=embed)
            await asyncio.sleep(0.3)
        
        # Enviar embeds de quem não pagou
        for embed in embeds_nao_pagaram:
            await canal_resultados.send(embed=embed)
            await asyncio.sleep(0.3)
        
        # Enviar embeds de isentos
        for embed in embeds_isentos:
            await canal_resultados.send(embed=embed)
            await asyncio.sleep(0.3)
        
        total_embeds = 1 + len(embeds_pagaram) + len(embeds_nao_pagaram) + len(embeds_isentos)
        await interaction.followup.send(
            f"✅ **Relatório enviado com sucesso!**\n"
            f"📊 {len(historico)} metas encontradas\n"
            f"📨 {total_embeds} mensagens enviadas",
            ephemeral=True
        )
        
class MetaView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=None)
        self.user_id = user_id
    
    @discord.ui.button(label="💣 Vender Pólvora", style=discord.ButtonStyle.primary, custom_id="meta_vender_polvora", emoji="💣")
    async def vender_polvora(self, interaction: discord.Interaction, button: discord.ui.Button):
        pool = get_db()
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
        await interaction.response.send_modal(VenderPolvoraModal(self.user_id))
    
    @discord.ui.button(label="💰 Adicionar Dinheiro Sujo", style=discord.ButtonStyle.success, custom_id="meta_adicionar_dinheiro", emoji="💰")
    async def adicionar_dinheiro(self, interaction: discord.Interaction, button: discord.ui.Button):
        pool = get_db()
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
    
    # ⚠️ APENAS 1 BOTÃO "Pólvora Paga" ⚠️
    @discord.ui.button(label="💰 Pólvora Paga", style=discord.ButtonStyle.success, custom_id="meta_polvora_paga", emoji="✅")
    async def polvora_paga(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        is_admin = interaction.user.guild_permissions.administrator
        
        if not is_gerente and not is_admin:
            await interaction.response.send_message("❌ Apenas **Gerentes** ou **ADM** podem marcar pólvora como paga!", ephemeral=True)
            return
        
        pendente = await buscar_polvora_pendente(self.user_id)
        if not pendente:
            await interaction.response.send_message("📭 Este membro não tem pólvora pendente para pagar!", ephemeral=True)
            return
        
        view = ConfirmarPagamentoPolvoraView(self.user_id, pendente)
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
    
    @discord.ui.button(label="🔒 Fechar Meta", style=discord.ButtonStyle.danger, custom_id="meta_fechar", emoji="🔒")
    async def fechar_meta(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_dono = str(interaction.user.id) == str(self.user_id)
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_dono and not is_gerente:
            await interaction.response.send_message("❌ Apenas o dono da sala ou gerentes podem fechar a meta!", ephemeral=True)
            return
        await interaction.response.send_modal(FecharMetaModal(self.user_id))
    
    @discord.ui.button(label="✏️ Editar Meta", style=discord.ButtonStyle.primary, custom_id="meta_editar", emoji="✏️")
    async def editar_meta(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_dono = str(interaction.user.id) == str(self.user_id)
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        is_admin = interaction.user.guild_permissions.administrator
        
        if not is_dono and not is_gerente and not is_admin:
            await interaction.response.send_message("❌ Apenas o dono da sala, gerentes ou ADM podem editar a meta!", ephemeral=True)
            return
        
        pool = get_db()
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
            "dinheiro_acoes": meta.get("dinheiro_acoes") or 0,
            "acao": meta.get("acao") or "Nenhuma"
        }
        
        await interaction.response.send_modal(EditarMetaModal(self.user_id, dados))
        
class RelatorioMetasButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="📊 Gerar Relatório de Metas", style=discord.ButtonStyle.success, custom_id="relatorio_metas_btn", emoji="📊")
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(RelatorioMetasModal())

class FecharTodasMetasButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="🔒 Fechar Todas as Metas (Semanal)", style=discord.ButtonStyle.danger, custom_id="fechar_todas_metas_btn", emoji="🔒")
    async def callback(self, interaction: discord.Interaction):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem fechar todas as metas!", ephemeral=True)
            return
        await interaction.response.send_modal(FecharTodasMetasModal())

class FecharTodasMetasModal(discord.ui.Modal, title="🔒 Fechar Metas Semanais"):
    data_inicio = discord.ui.TextInput(label="📅 Data de INÍCIO da semana", placeholder="Ex: 01/07/2026", required=True)
    data_fim = discord.ui.TextInput(label="📅 Data de FIM da semana", placeholder="Ex: 07/07/2026", required=True)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            inicio = datetime.strptime(self.data_inicio.value.strip(), "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value.strip(), "%d/%m/%Y")
        except ValueError:
            await interaction.followup.send("❌ Formato de data inválido!", ephemeral=True)
            return
        
        if fim < inicio:
            await interaction.followup.send("❌ Data de FIM deve ser depois da data de INÍCIO!", ephemeral=True)
            return
        
        try:
            relatorio, membros_sem_meta = await fechar_todas_metas(inicio, fim)
            if not relatorio and not membros_sem_meta:
                await interaction.followup.send("📭 Nenhuma meta para fechar.", ephemeral=True)
                return
            
            total_dinheiro = sum(r["dinheiro"] for r in relatorio)
            total_polvora = sum(r["polvora"] for r in relatorio)
            total_dinheiro_acoes = sum(r["dinheiro_acoes"] for r in relatorio)
            total_geral = sum(r["total"] for r in relatorio)
            
            # Separar por status
            isentos = [r for r in relatorio if r.get("status") == "isento"]
            pagaram = [r for r in relatorio if r.get("status") == "obrigado" and r["total"] > 0]
            nao_pagaram = [r for r in relatorio if r.get("status") == "obrigado" and r["total"] == 0]
            
            guild = interaction.guild
            
            # --- EMBED 1: RESUMO GERAL ---
            embed_resumo = discord.Embed(
                title="📊 RELATÓRIO SEMANAL - METAS FECHADAS",
                description=f"📅 **Período:** {self.data_inicio.value} até {self.data_fim.value}",
                color=0x2ecc71,
                timestamp=agora()
            )
            
            resumo_texto = (
                f"💰 **Dinheiro Sujo (Meta):** {formatar_dinheiro(total_dinheiro)}\n"
                f"🎯 **Dinheiro de Ações:** {formatar_dinheiro(total_dinheiro_acoes)}\n"
                f"💣 **Pólvora:** {fmt_num(total_polvora)} unidades\n"
                f"📦 **Total Geral:** {formatar_dinheiro(total_geral)}\n"
                f"👥 **Membros com meta:** {len(relatorio)}\n"
                f"✅ **Pagaram:** {len(pagaram)}\n"
                f"❌ **Não pagaram:** {len(nao_pagaram)}\n"
                f"🟡 **Isentos (Gerentes):** {len(isentos)}"
            )
            embed_resumo.add_field(name="📊 RESUMO GERAL", value=resumo_texto, inline=False)
            embed_resumo.set_footer(text=f"Relatório gerado por {interaction.user.display_name} • Fechamento Automático")
            
            # --- EMBEDS PARA QUEM PAGOU ---
            embeds_pagaram = []
            if pagaram:
                pagaram_ordenado = sorted(pagaram, key=lambda x: x["total"], reverse=True)
                for i in range(0, len(pagaram_ordenado), 10):
                    grupo = pagaram_ordenado[i:i+10]
                    embed = discord.Embed(
                        title=f"✅ QUEM PAGOU ({len(pagaram)} membros) - Parte {i//10 + 1}",
                        color=0x2ecc71
                    )
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        member = guild.get_member(int(item["user_id"])) if guild else None
                        if member:
                            nome = member.display_name
                        else:
                            user = await pegar_usuario(int(item["user_id"]))
                            nome = user.display_name if user else f"ID: {item['user_id']}"
                        texto += f"**{idx}.** {nome} - {formatar_dinheiro(item['total'])}\n"
                    embed.add_field(name="📋 LISTA", value=texto, inline=False)
                    embeds_pagaram.append(embed)
            
            # --- EMBEDS PARA QUEM NÃO PAGOU ---
            embeds_nao_pagaram = []
            if nao_pagaram:
                for i in range(0, len(nao_pagaram), 10):
                    grupo = nao_pagaram[i:i+10]
                    embed = discord.Embed(
                        title=f"❌ QUEM NÃO PAGOU ({len(nao_pagaram)} membros) - Parte {i//10 + 1}",
                        color=0xe74c3c
                    )
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        member = guild.get_member(int(item["user_id"])) if guild else None
                        if member:
                            nome = member.display_name
                        else:
                            user = await pegar_usuario(int(item["user_id"]))
                            nome = user.display_name if user else f"ID: {item['user_id']}"
                        texto += f"**{idx}.** {nome} - ❌ ZERADO\n"
                    embed.add_field(name="📋 LISTA", value=texto, inline=False)
                    embeds_nao_pagaram.append(embed)
            
            # --- EMBEDS PARA ISENTOS (GERENTES) ---
            embeds_isentos = []
            if isentos:
                for i in range(0, len(isentos), 10):
                    grupo = isentos[i:i+10]
                    embed = discord.Embed(
                        title=f"🟡 META ISENTA ({len(isentos)} gerentes) - Parte {i//10 + 1}",
                        color=0xf1c40f
                    )
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        member = guild.get_member(int(item["user_id"])) if guild else None
                        if member:
                            nome = member.display_name
                        else:
                            user = await pegar_usuario(int(item["user_id"]))
                            nome = user.display_name if user else f"ID: {item['user_id']}"
                        texto += f"**{idx}.** {nome} - 🟡 ISENTO (Gerente)\n"
                    embed.add_field(name="📋 LISTA", value=texto, inline=False)
                    embeds_isentos.append(embed)
            
            # --- EMBEDS PARA MEMBROS SEM META ---
            embeds_sem_meta = []
            if membros_sem_meta:
                for i in range(0, len(membros_sem_meta), 10):
                    grupo = membros_sem_meta[i:i+10]
                    embed = discord.Embed(
                        title=f"⚠️ MEMBROS SEM META ({len(membros_sem_meta)} membros) - Parte {i//10 + 1}",
                        color=0xf1c40f
                    )
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        member = guild.get_member(int(item["user_id"])) if guild else None
                        if member:
                            nome = member.display_name
                        else:
                            nome = item['nome']
                        texto += f"**{idx}.** {nome} - ❌ SEM META\n"
                    embed.add_field(name="📋 LISTA", value=texto, inline=False)
                    embeds_sem_meta.append(embed)
            
            # --- ENVIAR TUDO ---
            canal_resultados = interaction.guild.get_channel(RESULTADOS_METAS_ID)
            if not canal_resultados:
                canal_resultados = interaction.channel
            
            await canal_resultados.send(embed=embed_resumo)
            await asyncio.sleep(0.5)
            
            for embed in embeds_pagaram:
                await canal_resultados.send(embed=embed)
                await asyncio.sleep(0.3)
            
            for embed in embeds_nao_pagaram:
                await canal_resultados.send(embed=embed)
                await asyncio.sleep(0.3)
            
            for embed in embeds_isentos:
                await canal_resultados.send(embed=embed)
                await asyncio.sleep(0.3)
            
            for embed in embeds_sem_meta:
                await canal_resultados.send(embed=embed)
                await asyncio.sleep(0.3)
            
            total_embeds = 1 + len(embeds_pagaram) + len(embeds_nao_pagaram) + len(embeds_isentos) + len(embeds_sem_meta)
            await interaction.followup.send(
                f"✅ **Relatório enviado com sucesso!**\n"
                f"📊 {len(relatorio)} metas processadas\n"
                f"📨 {total_embeds} mensagens enviadas",
                ephemeral=True
            )
            
            for uid in metas_cache.keys():
                await atualizar_embed_meta(int(uid))
                await asyncio.sleep(0.3)
                
        except Exception as e:
            logger.error(f"❌ Erro ao fechar metas: {e}")
            await interaction.followup.send(f"❌ Erro ao fechar metas: {e}", ephemeral=True)
            
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
        embed = discord.Embed(title="🔒 FECHAR METAS - SEMANA ANTERIOR", description=f"📅 **Período a ser fechado:**\n**{data_inicio_str}** a **{data_fim_str}**\n\n⚠️ **ATENÇÃO:** Esta ação irá:\n• Fechar TODAS as metas deste período\n• Gerar o relatório completo\n• Resetar as metas dos membros\n\n🔄 **Esta semana é calculada automaticamente!**\n📌 Sempre a semana anterior (Segunda a Domingo)", color=0xe67e22)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

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
            data_inicio_naive = self.data_inicio.replace(tzinfo=None)
            data_fim_naive = self.data_fim.replace(tzinfo=None)
            relatorio, membros_sem_meta = await fechar_todas_metas(data_inicio_naive, data_fim_naive)
            
            if not relatorio and not membros_sem_meta:
                await interaction.followup.send("📭 Nenhuma meta para fechar.", ephemeral=True)
                return
            
            total_dinheiro = sum(r["dinheiro"] for r in relatorio)
            total_polvora = sum(r["polvora"] for r in relatorio)
            total_dinheiro_acoes = sum(r["dinheiro_acoes"] for r in relatorio)
            total_geral = sum(r["total"] for r in relatorio)
            
            # Separar por status
            isentos = [r for r in relatorio if r.get("status") == "isento"]
            pagaram = [r for r in relatorio if r.get("status") == "obrigado" and r["total"] > 0]
            nao_pagaram = [r for r in relatorio if r.get("status") == "obrigado" and r["total"] == 0]
            
            guild = interaction.guild
            
            # --- EMBED 1: RESUMO GERAL ---
            embed_resumo = discord.Embed(
                title="📊 RELATÓRIO SEMANAL - METAS FECHADAS",
                description=f"📅 **Período:** {self.data_inicio_str} até {self.data_fim_str}",
                color=0x2ecc71,
                timestamp=agora()
            )
            
            resumo_texto = (
                f"💰 **Dinheiro Sujo (Meta):** {formatar_dinheiro(total_dinheiro)}\n"
                f"🎯 **Dinheiro de Ações:** {formatar_dinheiro(total_dinheiro_acoes)}\n"
                f"💣 **Pólvora:** {fmt_num(total_polvora)} unidades\n"
                f"📦 **Total Geral:** {formatar_dinheiro(total_geral)}\n"
                f"👥 **Membros com meta:** {len(relatorio)}\n"
                f"✅ **Pagaram:** {len(pagaram)}\n"
                f"❌ **Não pagaram:** {len(nao_pagaram)}\n"
                f"🟡 **Isentos (Gerentes):** {len(isentos)}"
            )
            embed_resumo.add_field(name="📊 RESUMO GERAL", value=resumo_texto, inline=False)
            embed_resumo.set_footer(text=f"Relatório gerado por {interaction.user.display_name} • Fechamento Automático")
            
            # --- EMBEDS PARA QUEM PAGOU ---
            embeds_pagaram = []
            if pagaram:
                pagaram_ordenado = sorted(pagaram, key=lambda x: x["total"], reverse=True)
                for i in range(0, len(pagaram_ordenado), 10):
                    grupo = pagaram_ordenado[i:i+10]
                    embed = discord.Embed(
                        title=f"✅ QUEM PAGOU ({len(pagaram)} membros) - Parte {i//10 + 1}",
                        color=0x2ecc71
                    )
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        member = guild.get_member(int(item["user_id"])) if guild else None
                        if member:
                            nome = member.display_name
                        else:
                            user = await pegar_usuario(int(item["user_id"]))
                            nome = user.display_name if user else f"ID: {item['user_id']}"
                        texto += f"**{idx}.** {nome} - {formatar_dinheiro(item['total'])}\n"
                    embed.add_field(name="📋 LISTA", value=texto, inline=False)
                    embeds_pagaram.append(embed)
            
            # --- EMBEDS PARA QUEM NÃO PAGOU ---
            embeds_nao_pagaram = []
            if nao_pagaram:
                for i in range(0, len(nao_pagaram), 10):
                    grupo = nao_pagaram[i:i+10]
                    embed = discord.Embed(
                        title=f"❌ QUEM NÃO PAGOU ({len(nao_pagaram)} membros) - Parte {i//10 + 1}",
                        color=0xe74c3c
                    )
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        member = guild.get_member(int(item["user_id"])) if guild else None
                        if member:
                            nome = member.display_name
                        else:
                            user = await pegar_usuario(int(item["user_id"]))
                            nome = user.display_name if user else f"ID: {item['user_id']}"
                        texto += f"**{idx}.** {nome} - ❌ ZERADO\n"
                    embed.add_field(name="📋 LISTA", value=texto, inline=False)
                    embeds_nao_pagaram.append(embed)
            
            # --- EMBEDS PARA ISENTOS (GERENTES) ---
            embeds_isentos = []
            if isentos:
                for i in range(0, len(isentos), 10):
                    grupo = isentos[i:i+10]
                    embed = discord.Embed(
                        title=f"🟡 META ISENTA ({len(isentos)} gerentes) - Parte {i//10 + 1}",
                        color=0xf1c40f
                    )
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        member = guild.get_member(int(item["user_id"])) if guild else None
                        if member:
                            nome = member.display_name
                        else:
                            user = await pegar_usuario(int(item["user_id"]))
                            nome = user.display_name if user else f"ID: {item['user_id']}"
                        texto += f"**{idx}.** {nome} - 🟡 ISENTO (Gerente)\n"
                    embed.add_field(name="📋 LISTA", value=texto, inline=False)
                    embeds_isentos.append(embed)
            
            # --- EMBEDS PARA MEMBROS SEM META ---
            embeds_sem_meta = []
            if membros_sem_meta:
                for i in range(0, len(membros_sem_meta), 10):
                    grupo = membros_sem_meta[i:i+10]
                    embed = discord.Embed(
                        title=f"⚠️ MEMBROS SEM META ({len(membros_sem_meta)} membros) - Parte {i//10 + 1}",
                        color=0xf1c40f
                    )
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        member = guild.get_member(int(item["user_id"])) if guild else None
                        if member:
                            nome = member.display_name
                        else:
                            nome = item['nome']
                        texto += f"**{idx}.** {nome} - ❌ SEM META\n"
                    embed.add_field(name="📋 LISTA", value=texto, inline=False)
                    embeds_sem_meta.append(embed)
            
            # --- ENVIAR TUDO ---
            canal_resultados = interaction.guild.get_channel(RESULTADOS_METAS_ID)
            if not canal_resultados:
                canal_resultados = interaction.channel
            
            await canal_resultados.send(embed=embed_resumo)
            await asyncio.sleep(0.5)
            
            for embed in embeds_pagaram:
                await canal_resultados.send(embed=embed)
                await asyncio.sleep(0.3)
            
            for embed in embeds_nao_pagaram:
                await canal_resultados.send(embed=embed)
                await asyncio.sleep(0.3)
            
            for embed in embeds_isentos:
                await canal_resultados.send(embed=embed)
                await asyncio.sleep(0.3)
            
            for embed in embeds_sem_meta:
                await canal_resultados.send(embed=embed)
                await asyncio.sleep(0.3)
            
            total_embeds = 1 + len(embeds_pagaram) + len(embeds_nao_pagaram) + len(embeds_isentos) + len(embeds_sem_meta)
            await interaction.followup.send(
                f"✅ **Relatório enviado com sucesso!**\n"
                f"📊 {len(relatorio)} metas processadas\n"
                f"📨 {total_embeds} mensagens enviadas",
                ephemeral=True
            )
            
            for uid in metas_cache.keys():
                await atualizar_embed_meta(int(uid))
                await asyncio.sleep(0.3)
                
        except Exception as e:
            logger.error(f"❌ Erro ao fechar metas automático: {e}")
            await interaction.followup.send(f"❌ Erro ao fechar metas: {e}", ephemeral=True)
    
    @discord.ui.button(label="❌ Cancelar", style=discord.ButtonStyle.secondary, custom_id="cancelar_fechamento_auto", emoji="❌")
    async def cancelar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ Operação cancelada.", ephemeral=True)
        
class ZerarMetasButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="⚠️ Zerar Todas as Metas", style=discord.ButtonStyle.danger, custom_id="zerar_metas_btn_painel", emoji="⚠️")
    async def callback(self, interaction: discord.Interaction):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem zerar todas as metas!", ephemeral=True)
            return
        view = ConfirmarZerarView()
        await interaction.response.send_message("⚠️ **ATENÇÃO!** Você está prestes a zerar TODAS as metas.\n\nIsso vai resetar o dinheiro e pólvora de TODOS os membros.\n**Esta ação não pode ser desfeita!**\n\nClique em **'✅ Sim, zerar tudo'** para confirmar.", view=view, ephemeral=True)

class ConfirmarZerarView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=30)
    @discord.ui.button(label="✅ Sim, zerar tudo", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        try:
            metas = await zerar_todas_metas()
            atualizadas = 0
            for meta in metas:
                user_id = int(meta["user_id"])
                await atualizar_embed_meta(user_id)
                atualizadas += 1
                await asyncio.sleep(0.5)
            await interaction.followup.send(f"✅ **Todas as metas foram zeradas com sucesso!**\n\n📊 {atualizadas} metas resetadas.", ephemeral=True)
            canal_gerencia = interaction.guild.get_channel(CANAL_GERENCIA_ID)
            if canal_gerencia:
                embed = discord.Embed(title="⚠️ METAS ZERADAS", description=f"Todas as metas foram resetadas por {interaction.user.mention}", color=0xe74c3c, timestamp=agora())
                await canal_gerencia.send(embed=embed)
        except Exception as e:
            await interaction.followup.send(f"❌ Erro ao zerar metas: {e}", ephemeral=True)
    @discord.ui.button(label="❌ Cancelar", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancelar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ Operação cancelada.", ephemeral=True)

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
        pool = get_db()
        if pool:
            async with pool.acquire() as conn:
                canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(self.user_id))
                if canal_id:
                    canal_membro = interaction.guild.get_channel(int(canal_id))
        
        if canal_membro:
            embed_notificacao = discord.Embed(
                title="✅ PÓLVORA PAGA!",
                description=f"👤 <@{self.user_id}>",
                color=0x2ecc71,
                timestamp=agora()
            )
            embed_notificacao.add_field(name="📦 Quantidade", value=f"{fmt_num(self.pendente['quantidade'])} unidades", inline=True)
            embed_notificacao.add_field(name="💰 Valor recebido", value=formatar_dinheiro(self.pendente['valor']), inline=True)
            embed_notificacao.add_field(name="💵 Preço por unidade", value=f"R$ {PRECO_POLVORA:.2f}", inline=True)
            embed_notificacao.set_footer(text="Pólvora paga! ✅")
            
            await canal_membro.send(embed=embed_notificacao)
        
        embed = discord.Embed(
            title="✅ PÓLVORA PAGA COM SUCESSO!",
            description=f"👤 <@{self.user_id}>",
            color=0x2ecc71,
            timestamp=agora()
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

class SolicitarSalaView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="➕ Criar Minha Sala", style=discord.ButtonStyle.success, custom_id="criar_sala_manual")
    async def criar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        pool = get_db()
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

# --- PAINÉIS DAS METAS ---
async def enviar_painel_solicitar_sala():
    canal = bot.get_channel(CANAL_SOLICITAR_SALA_ID)
    if not canal:
        logger.error("❌ Canal solicitar sala não encontrado")
        return
    embed = discord.Embed(title="📂 Solicitar Sala", description="Clique no botão para criar sua sala.", color=0x2ecc71)
    await enviar_ou_atualizar_painel("painel_solicitar_sala", CANAL_SOLICITAR_SALA_ID, embed, SolicitarSalaView())

async def enviar_painel_relatorio_metas():
    canal = bot.get_channel(1521495685092999279)
    if not canal:
        logger.error("❌ Canal de relatório de metas não encontrado")
        return
    embed = discord.Embed(title="📊 GERENCIAMENTO DE METAS", description="**Gerencie as metas de todos os membros.**\n\n📌 **Opções disponíveis:**\n• 📊 **Gerar Relatório** - Relatório de metas fechadas (individual)\n• 🔒 **Fechar Metas Semanais** - Fecha TODAS as metas (com datas)\n• 🔒 **Fechar Metas (Automático)** - Fecha a semana anterior automaticamente\n• ⚠️ **Zerar Metas** - Resetar TODAS as metas (cuidado!)\n\n📋 **O relatório semanal mostra:**\n• Quem pagou e quanto\n• Quem NÃO pagou\n• Membros sem meta\n• Totais gerais", color=0x2ecc71)
    embed.add_field(name="📌 COMO USAR - FECHAR METAS (AUTOMÁTICO)", value="**Clique no botão verde e confirme:**\n• O sistema calcula a SEMANA ANTERIOR (Segunda a Domingo)\n• Fecha todas as metas do período\n• Gera o relatório automaticamente\n\n**Exemplo:**\n• Se fechar hoje (20/07/2026) → Fecha 13/07 a 19/07\n• Se fechar amanhã (21/07/2026) → Fecha 13/07 a 19/07\n• Sempre a SEMANA ANTERIOR completa!", inline=False)
    view = discord.ui.View(timeout=None)
    view.add_item(RelatorioMetasButton())
    view.add_item(FecharTodasMetasButton())
    view.add_item(FecharMetasAutomaticoButton())
    view.add_item(ZerarMetasButton())
    await enviar_ou_atualizar_painel("painel_relatorio_metas", 1521495685092999279, embed, view)
    logger.info("📊 Painel de gerenciamento de metas enviado")

@tasks.loop(hours=1)
async def verificar_avisos_meta():
    try:
        await verificar_avisos_quarta()
    except Exception as e:
        logger.error(f"❌ Erro ao verificar avisos de meta: {e}")

# =========================================================
# ==================== COMANDOS DE METAS ===================
# =========================================================

@bot.command(name="atualizar_paineis_metas")
@commands.has_permissions(administrator=True)
async def cmd_atualizar_paineis_metas(ctx):
    """Atualiza todos os painéis de metas."""
    await ctx.send("🔄 Atualizando painéis de metas...")
    
    try:
        # Recarregar cache de metas
        await carregar_metas_cache()
        
        # Atualizar cada sala de meta
        guild = ctx.guild
        contador = 0
        for uid, dados in metas_cache.items():
            canal = guild.get_channel(dados["canal_id"])
            if canal:
                await atualizar_embed_meta(int(uid))
                contador += 1
                await asyncio.sleep(0.3)
        
        # Atualizar painel de solicitar sala
        await enviar_painel_solicitar_sala()
        
        # Atualizar painel de relatório
        await enviar_painel_relatorio_metas()
        
        await ctx.send(f"✅ **{contador} painéis de metas atualizados!**")
        
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar painéis de metas: {e}")
        await ctx.send(f"❌ Erro ao atualizar painéis: {e}")

@bot.command(name="atualizar_metas")
@commands.has_permissions(administrator=True)
async def cmd_atualizar_metas(ctx):
    """Atualiza todas as salas de metas."""
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

# --- MODAL PARA EDITAR META ---
class EditarMetaModal(discord.ui.Modal, title="✏️ Editar Meta"):
    def __init__(self, user_id, dados_atuais):
        super().__init__(timeout=300)
        self.user_id = user_id
        
        self.dinheiro = discord.ui.TextInput(
            label="💰 Dinheiro Sujo (Meta)",
            placeholder="Digite o valor correto",
            default=str(dados_atuais.get("dinheiro", 0)),
            required=True,
            max_length=15
        )
        
        self.polvora = discord.ui.TextInput(
            label="💣 Pólvora",
            placeholder="Digite a quantidade correta",
            default=str(dados_atuais.get("polvora", 0)),
            required=True,
            max_length=10
        )
        
        self.dinheiro_acoes = discord.ui.TextInput(
            label="🎯 Dinheiro de Ações",
            placeholder="Digite o valor correto",
            default=str(dados_atuais.get("dinheiro_acoes", 0)),
            required=True,
            max_length=15
        )
        
        self.acao = discord.ui.TextInput(
            label="🎯 Ação Atual",
            placeholder="Digite a ação correta (ou Nenhuma)",
            default=dados_atuais.get("acao") or "Nenhuma",
            required=False,
            max_length=100
        )
        
        self.add_item(self.dinheiro)
        self.add_item(self.polvora)
        self.add_item(self.dinheiro_acoes)
        self.add_item(self.acao)
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        try:
            novo_dinheiro = int(self.dinheiro.value.replace(".", "").replace(",", ""))
            nova_polvora = int(self.polvora.value.replace(".", "").replace(",", ""))
            novo_dinheiro_acoes = int(self.dinheiro_acoes.value.replace(".", "").replace(",", ""))
            nova_acao = self.acao.value.strip() if self.acao.value.strip() else "Nenhuma"
            
            if novo_dinheiro < 0 or nova_polvora < 0 or novo_dinheiro_acoes < 0:
                raise ValueError("Valores não podem ser negativos")
                
        except ValueError as e:
            await interaction.followup.send(f"❌ **Valor inválido!** {str(e)}", ephemeral=True)
            return
        
        # Atualizar no banco de dados
        pool = get_db()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        
        try:
            async with pool.acquire() as conn:
                await conn.execute("""
                    UPDATE metas 
                    SET dinheiro = $1, polvora = $2, dinheiro_acoes = $3, acao = $4
                    WHERE user_id = $5
                """, novo_dinheiro, nova_polvora, novo_dinheiro_acoes, nova_acao, str(self.user_id))
            
            # Atualizar cache
            if str(self.user_id) in metas_cache:
                metas_cache[str(self.user_id)]["dinheiro"] = novo_dinheiro
                metas_cache[str(self.user_id)]["polvora"] = nova_polvora
                metas_cache[str(self.user_id)]["dinheiro_acoes"] = novo_dinheiro_acoes
                metas_cache[str(self.user_id)]["acao"] = nova_acao
            
            # Atualizar embed da meta
            await atualizar_embed_meta(self.user_id)
            
            embed = discord.Embed(
                title="✅ META ATUALIZADA COM SUCESSO!",
                description=f"**👤 <@{self.user_id}>**",
                color=0x2ecc71,
                timestamp=agora()
            )
            embed.add_field(name="💰 Dinheiro Sujo", value=formatar_dinheiro(novo_dinheiro), inline=True)
            embed.add_field(name="💣 Pólvora", value=f"{fmt_num(nova_polvora)} unidades", inline=True)
            embed.add_field(name="🎯 Dinheiro de Ações", value=formatar_dinheiro(novo_dinheiro_acoes), inline=True)
            embed.add_field(name="🎯 Ação", value=nova_acao, inline=False)
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            logger.error(f"❌ Erro ao editar meta: {e}")
            await interaction.followup.send(f"❌ Erro ao editar meta: {str(e)}", ephemeral=True)

async def verificar_avisos_quarta_forcado():
    """Versão forçada para testar o aviso (ignora o dia da semana)."""
    logger.info("📨 TESTE FORÇADO: Verificando avisos de quarta-feira...")
    
    pool = get_db()
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
            CARGO_AGREGADO_ID,
            CARGO_MEMBRO_ID,
            CARGO_SOLDADO_ID,
            CARGO_01_ID,
            CARGO_02_ID,
            CARGO_RESP_METAS_ID,
            CARGO_RESP_ACAO_ID,
            CARGO_RESP_VENDAS_ID,
            CARGO_RESP_PRODUCAO_ID
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
                            logger.info(f"📨 [TESTE] Aviso enviado para {member.display_name}")
        
        logger.info(f"✅ [TESTE] Avisos enviados: {avisos_enviados} membros")
        return True
        
    except Exception as e:
        logger.error(f"❌ Erro no teste de aviso: {e}")
        return False

@bot.command(name="testar_aviso_quarta")
@commands.has_permissions(administrator=True)
async def cmd_testar_aviso_quarta(ctx):
    """Comando para testar o aviso de quarta-feira manualmente."""
    await ctx.send("🔄 Testando aviso de quarta-feira...")
    
    resultado = await verificar_avisos_quarta_forcado()
    
    if resultado:
        await ctx.send("✅ Avisos enviados com sucesso!")
    else:
        await ctx.send("❌ Erro ao enviar avisos. Verifique os logs.")




# =========================================================
# ==================== SEÇÃO 6: AÇÕES =====================
# =========================================================

# --- IDs DAS AÇÕES ---
CANAL_ESCALACOES_ID = 1241406819545514064
CANAL_RELATORIO_ACOES_ID = 1477308788531921019

# --- CONSTANTES DAS AÇÕES ---
ACOES_SEMANA = {
    "Joalheria": 5,
    "Banco Fleeca": 4,
    "Banco de Paleto": 1,
    "Banco Central": 1,
    "Nióbio": 1,
    "Loja de Armas (Ammunation)": None,
    "Loja de Bebidas": None,
    "Lan House - (Bahamas)": None,
    "Loja de Departamento": None,
    "Mergulhador": None,
    "Grapeseed": None,
    "Companhia de Gás": None,
    "Life Invader": None,
    "Aeroporto de Sucata": None,
    "Carro Forte": None,
    "Banco Bahamas": None,
    "🚁 Helicrash (13h)": None,
    "🚁 Helicrash (15h)": None,
    "🚁 Helicrash (22h)": None,
    "🚁 Helicrash (02h)": None,
}

CARGOS_PERMITIDOS_ESCALACAO = [
    CARGO_AGREGADO_ID, CARGO_MEMBRO_ID, CARGO_SOLDADO_ID,
    CARGO_01_ID, CARGO_02_ID, CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID
]

# --- QUERIES DAS AÇÕES ---
async def salvar_acao_db(tipo, autor):
    pool = get_db()
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

async def buscar_acoes_semana():
    pool = get_db()
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

async def participar_acao_db(acao_id, user_id):
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO participantes_acoes (acao_id, user_id) VALUES ($1, $2)", acao_id, str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao participar ação: {e}")

async def concluir_acao_db(acao_id, resultado, valor=0):
    pool = get_db()
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

# --- VIEWS E MODAIS DAS AÇÕES ---
class PainelAcoesView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="🎯 Criar Nova Ação", style=discord.ButtonStyle.success, custom_id="criar_acao", emoji="🎯")
    async def criar_acao(self, interaction: discord.Interaction, button):
        await interaction.response.defer(ephemeral=True)
        view = SelecionarAcaoView()
        await interaction.followup.send("**Selecione o tipo de ação:**", view=view, ephemeral=True)
    @discord.ui.button(label="📊 Ver Relatório", style=discord.ButtonStyle.primary, custom_id="acoes_relatorio", emoji="📊")
    async def relatorio(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(RelatorioPeriodoModal())
    @discord.ui.button(label="♻️ Resetar Ações", style=discord.ButtonStyle.danger, custom_id="acoes_reset", emoji="♻️")
    async def reset(self, interaction: discord.Interaction, button):
        await interaction.response.defer(ephemeral=True)
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_gerente and not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("❌ Apenas gerentes podem resetar as ações!", ephemeral=True)
            return
        pool = get_db()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM acoes_semana")
            await conn.execute("DELETE FROM participantes_acoes")
        await enviar_painel_acoes(interaction.guild)
        await interaction.followup.send("✅ Todas as ações foram resetadas!", ephemeral=True)

class SelecionarAcaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)
        options = []
        for nome, limite in ACOES_SEMANA.items():
            if limite is not None:
                options.append(discord.SelectOption(label=nome, description=f"Limite: {limite}/semana", emoji="🎯"))
        for nome, limite in ACOES_SEMANA.items():
            if limite is None:
                emoji = "🚁" if "Helicrash" in nome else "🏪"
                options.append(discord.SelectOption(label=nome, description="Ilimitado", emoji=emoji))
        select = discord.ui.Select(placeholder="📋 Escolha a ação", options=options, max_values=1)
        select.callback = self.select_callback
        self.add_item(select)
        self.add_item(FecharButton())
    async def select_callback(self, interaction: discord.Interaction):
        acao_tipo = interaction.data["values"][0]
        await interaction.response.defer(ephemeral=True)
        limite = ACOES_SEMANA.get(acao_tipo)
        pool = get_db()
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
        cor = 0xe67e22 if "Helicrash" in acao_tipo else 0x3498db
        emoji = "🚁" if "Helicrash" in acao_tipo else "🎯"
        embed = discord.Embed(title=f"{emoji} {acao_tipo}", description="**Clique no botão ✅ PARTICIPAR para se inscrever nesta ação!**\n\n📌 Quem participar será registrado automaticamente.\n👤 Quando terminar a escalação, o criador clica em 📤 Concluir.", color=cor)
        if "Helicrash" in acao_tipo:
            horario = acao_tipo.split("(")[1].replace(")", "")
            embed.description += f"\n\n⏰ **Horário:** {horario} (horário de Brasília)"
        if limite and limite is not None:
            async with pool.acquire() as conn:
                qtd_feita = await conn.fetchval("SELECT COUNT(*) FROM acoes_semana WHERE tipo=$1 AND status='concluida' AND (resultado='ganhou' OR resultado='perdeu')", acao_tipo)
                embed.description += f"\n\n📊 **Limite semanal:** {qtd_feita}/{limite} ações realizadas"
        embed.add_field(name="👥 Participantes (0)", value="Nenhum participante ainda.\nClique no botão abaixo para participar!", inline=False)
        embed.add_field(name="👤 Criado por", value=interaction.user.mention, inline=True)
        embed.add_field(name="📅 Data", value=agora().strftime('%d/%m/%Y %H:%M'), inline=True)
        embed.set_footer(text=f"ID: {acao_id}")
        canal = interaction.guild.get_channel(CANAL_ESCALACOES_ID)
        if canal:
            await canal.send(embed=embed, view=AcaoCheckinView(acao_id, interaction.user.id))
            await interaction.followup.send(f"✅ Ação **{acao_tipo}** criada!", ephemeral=True)
        else:
            await interaction.followup.send("❌ Canal de escalações não encontrado!", ephemeral=True)

class AcaoCheckinView(discord.ui.View):
    def __init__(self, acao_id, criador_id):
        super().__init__(timeout=None)
        self.acao_id = acao_id
        self.criador_id = criador_id
    @discord.ui.button(label="✅ Participar", style=discord.ButtonStyle.success, custom_id="acao_participar", emoji="✅")
    async def participar(self, interaction: discord.Interaction, button):
        if not any(role.id in CARGOS_PERMITIDOS_ESCALACAO for role in interaction.user.roles):
            await interaction.response.send_message("❌ Você não tem permissão para participar de ações!", ephemeral=True)
            return
        pool = get_db()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.response.send_message("❌ Esta ação já foi concluída!", ephemeral=True)
                return
            ja_participa = await conn.fetchval("SELECT 1 FROM participantes_acoes WHERE acao_id=$1 AND user_id=$2", self.acao_id, str(interaction.user.id))
            if ja_participa:
                await interaction.response.send_message("⚠️ Você já está participando!", ephemeral=True)
                return
            await conn.execute("INSERT INTO participantes_acoes (acao_id, user_id) VALUES ($1, $2)", self.acao_id, str(interaction.user.id))
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)
        embed = interaction.message.embeds[0]
        lista_participantes = "\n".join([f"<@{p['user_id']}>" for p in participantes]) if participantes else "Nenhum participante ainda."
        for i, field in enumerate(embed.fields):
            if field.name.startswith("👥 Participantes"):
                embed.set_field_at(i, name=f"👥 Participantes ({len(participantes)})", value=lista_participantes, inline=False)
                break
        await interaction.message.edit(embed=embed)
        await interaction.response.send_message(f"✅ Você se inscreveu na ação **{acao['tipo']}**!", ephemeral=True)
    @discord.ui.button(label="📤 Concluir Escalação", style=discord.ButtonStyle.primary, custom_id="acao_concluir", emoji="📤")
    async def concluir(self, interaction: discord.Interaction, button):
        is_criador = interaction.user.id == self.criador_id
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_criador and not is_gerente:
            await interaction.response.send_message("❌ Apenas o criador ou gerentes podem concluir!", ephemeral=True)
            return
        pool = get_db()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.response.send_message("❌ Esta ação já foi concluída!", ephemeral=True)
                return
            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            is_helicrash = "Helicrash" in acao["tipo"]
            if not participantes:
                await interaction.response.send_message("⚠️ Nenhum participante! Ação cancelada.", ephemeral=True)
                await interaction.message.delete()
                return
            await conn.execute("UPDATE acoes_semana SET status='concluida' WHERE id=$1", self.acao_id)
            if is_helicrash:
                await conn.execute("UPDATE acoes_semana SET resultado='concluida', valor=0 WHERE id=$1", self.acao_id)
        lista_participantes = "\n".join([f"<@{p['user_id']}>" for p in participantes])
        if is_helicrash:
            embed_relatorio = discord.Embed(title="🚁 RELATÓRIO DE HELICRASH", description=f"**{acao['tipo']}**\n\n✅ Evento registrado com sucesso!", color=0xe67e22)
            embed_relatorio.add_field(name="🏦 Evento", value=acao["tipo"], inline=False)
            embed_relatorio.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
            embed_relatorio.add_field(name="📅 Data", value=agora().strftime('%d/%m/%Y %H:%M'), inline=False)
            embed_relatorio.set_footer(text=f"ID: {self.acao_id} • Criada por: <@{acao['autor']}>")
            canal_relatorio = interaction.guild.get_channel(CANAL_RELATORIO_ACOES_ID)
            if canal_relatorio:
                await canal_relatorio.send(embed=embed_relatorio)
                await interaction.message.delete()
                await interaction.response.send_message(f"✅ Helicrash **{acao['tipo']}** registrado!", ephemeral=True)
            else:
                await interaction.response.send_message("❌ Canal de relatório não encontrado!", ephemeral=True)
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
            await interaction.response.send_message(f"✅ Escalação concluída!", ephemeral=True)
            await enviar_painel_acoes(interaction.guild)
        else:
            await interaction.response.send_message("❌ Canal de relatório não encontrado!", ephemeral=True)

class ResultadoAcaoView(discord.ui.View):
    def __init__(self, acao_id, mensagem_original):
        super().__init__(timeout=None)
        self.acao_id = acao_id
        self.mensagem_original = mensagem_original
    @discord.ui.button(label="🏆 Ganhou", style=discord.ButtonStyle.success, custom_id="resultado_ganhou")
    async def ganhou(self, interaction: discord.Interaction, button):
        pool = get_db()
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
        pool = get_db()
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

class ResultadoGanhouModal(discord.ui.Modal, title="🎉 Resultado - GANHOU"):
    dinheiro = discord.ui.TextInput(label="Valor total ganho (em reais)", placeholder="Ex: 50000", required=True)
    def __init__(self, acao_id, mensagem_original):
        super().__init__()
        self.acao_id = acao_id
        self.mensagem_original = mensagem_original
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            valor_total = int(self.dinheiro.value.replace(".", "").replace(",", ""))
            if valor_total <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ Valor inválido!", ephemeral=True)
            return
        
        pool = get_db()
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
        
        lista_participantes = "\n".join([f"<@{uid}>" for uid in ids_participantes])
        embed = discord.Embed(
            title="🎉 RESULTADO DA AÇÃO - GANHOU!",
            description=f"⚠️ **ATENÇÃO:** O valor deve ser pago manualmente aos participantes!",
            color=0x2ecc71
        )
        embed.add_field(name="🎯 Ação", value=acao["tipo"], inline=False)
        embed.add_field(name="💰 Total Ganho", value=formatar_dinheiro(valor_total), inline=False)
        embed.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
        embed.add_field(name="📌 OBSERVAÇÃO", value="O valor NÃO foi depositado automaticamente. Pague manualmente cada participante.", inline=False)
        
        await self.mensagem_original.edit(embed=embed, view=None)
        await enviar_painel_acoes(interaction.guild)
        await interaction.followup.send(f"✅ Ação registrada como GANHOU! Pague os participantes manualmente.", ephemeral=True)

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
        pool = get_db()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        async with pool.acquire() as conn:
            await conn.execute("UPDATE acoes_semana SET valor=0, resultado='perdeu' WHERE id=$1", self.acao_id)
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            acao = await conn.fetchrow("SELECT tipo FROM acoes_semana WHERE id=$1", self.acao_id)
        ids_participantes = [str(p["user_id"]) for p in participantes]
        lista_participantes = "\n".join([f"<@{uid}>" for uid in ids_participantes]) if ids_participantes else "Ninguém"
        embed = discord.Embed(title="💀 RESULTADO DA AÇÃO - PERDEU!", description="A ação foi perdida, nenhum valor foi distribuído.", color=0xe74c3c)
        embed.add_field(name="🎯 Ação", value=acao["tipo"], inline=False)
        embed.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
        embed.add_field(name="💰 Total", value="R$ 0,00", inline=True)
        embed.add_field(name="📝 Status", value="❌ AÇÃO PERDIDA", inline=True)
        await self.mensagem_original.edit(embed=embed, view=None)
        await enviar_painel_acoes(interaction.guild)
        await interaction.followup.send(f"✅ Ação registrada como PERDIDA!", ephemeral=True)

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
        pool = get_db()
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

class FecharButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="❌ Fechar", style=discord.ButtonStyle.danger)
    async def callback(self, interaction: discord.Interaction):
        await interaction.message.delete()

# --- PAINEL DE AÇÕES ---
async def enviar_painel_acoes(guild):
    canal = guild.get_channel(CANAL_ESCALACOES_ID)
    if not canal:
        logger.error("❌ Canal ações não encontrado")
        return
    rows = await buscar_acoes_semana()
    feitas = {r["tipo"]: r["qtd"] for r in rows}
    linhas = []
    total_feitas = 0
    total_meta = 0
    for nome, limite in ACOES_SEMANA.items():
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
    if total_meta > 0:
        porcentagem = int((total_feitas / total_meta) * 100)
        barra_progresso = "▓" * (porcentagem // 5) + "░" * (20 - (porcentagem // 5))
        status_texto = f"📊 Progresso Semanal: {porcentagem}% {barra_progresso}\n\n"
    else:
        status_texto = ""
    embed = discord.Embed(title="📊 AÇÕES DA SEMANA", description="**Controle de ações realizadas no período**", color=0x2ecc71)
    embed.add_field(name="📌 AÇÕES REALIZADAS", value=status_texto + "\n".join(linhas), inline=False)
    embed.add_field(name="📊 TOTAL", value=f"{total_feitas}/{total_meta} ações realizadas" if total_meta > 0 else f"{total_feitas} ações realizadas (sem limite)", inline=False)
    embed.set_footer(text=f"Atualizado em {agora().strftime('%d/%m/%Y %H:%M')}")
    await enviar_ou_atualizar_painel("painel_acoes", CANAL_ESCALACOES_ID, embed, PainelAcoesView())

# =========================================================
# ==================== SEÇÃO 7: LAVAGEM ===================
# =========================================================

# --- IDs DA LAVAGEM ---
CANAL_INICIAR_LAVAGEM_ID = 1467152989499293768
CANAL_LAVAGEM_MEMBROS_ID = 1467159346923311216
CANAL_RELATORIO_LAVAGEM_ID = 1467150805273546878

# --- VARIÁVEIS GLOBAIS DA LAVAGEM ---
lavagens_pendentes = {}

# --- QUERIES DA LAVAGEM ---
async def salvar_lavagem_db(user_id, valor_sujo, taxa, valor_retorno):
    pool = get_db()
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

async def carregar_lavagens_db():
    pool = get_db()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM lavagens")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar lavagens: {e}")
        return []

async def limpar_lavagens_db():
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM lavagens")
    except Exception as e:
        logger.error(f"❌ Erro ao limpar lavagens: {e}")

# --- FUNÇÕES AUXILIARES DA LAVAGEM ---
def pode_gerenciar_lavagem(member: discord.Member):
    cargos_permitidos = [CARGO_GERENTE_ID, CARGO_01_ID, CARGO_02_ID, CARGO_GERENTE_GERAL_ID]
    return any(role.id in cargos_permitidos for role in member.roles)

# --- VIEWS E MODAIS DA LAVAGEM ---
class LavagemModal(discord.ui.Modal, title="Iniciar Lavagem"):
    valor = discord.ui.TextInput(label="Valor do dinheiro sujo")
    async def on_submit(self, interaction: discord.Interaction):
        await responder_interacao(interaction, defer=True)
        try:
            valor_sujo = int(self.valor.value.replace(".", "").replace(",", ""))
        except:
            await interaction.followup.send("Valor inválido.", ephemeral=True)
            return
        taxa = 20
        valor_retorno = int(valor_sujo * 0.8)
        msg_info = await interaction.channel.send(f"{interaction.user.mention} envie agora o PRINT da tela.")
        lavagens_pendentes[interaction.user.id] = {"sujo": valor_sujo, "retorno": valor_retorno, "taxa": taxa, "msg_info": msg_info}

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

@tasks.loop(minutes=15)
async def limpar_lavagens_pendentes():
    lavagens_pendentes.clear()

# --- PAINEL DA LAVAGEM ---
async def enviar_painel_lavagem():
    canal = bot.get_channel(CANAL_INICIAR_LAVAGEM_ID)
    if not canal:
        logger.error("❌ Canal de lavagem não encontrado")
        return
    embed = discord.Embed(title="🧼 Lavagem de Dinheiro", description="Clique para iniciar lavagem.", color=0x27ae60)
    await enviar_ou_atualizar_painel("painel_lavagem", CANAL_INICIAR_LAVAGEM_ID, embed, LavagemView())
    logger.info("🧼 Painel de lavagem verificado/atualizado")

@bot.event
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
# ==================== SEÇÃO 8: LIVES =====================
# =========================================================

# --- IDs DAS LIVES ---
CANAL_CADASTRO_LIVE_ID = 1466464557215256790
CANAL_DIVULGACAO_LIVE_ID = 1243325102917943335
ADM_ID = 467673818375389194

# --- VARIÁVEIS GLOBAIS DAS LIVES ---
cache_lives = {}
cache_lives_timestamp = 0
CACHE_LIVES_TTL = 120
twitch_token = None
twitch_token_expira = 0

# --- QUERIES DAS LIVES ---
async def carregar_lives_db():
    pool = get_db()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM lives")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar lives: {e}")
        return []

async def salvar_live_db(user_id, link):
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO lives (user_id, link, divulgado) VALUES ($1, $2, false)", str(user_id), link)
    except Exception as e:
        logger.error(f"❌ Erro ao salvar live: {e}")

async def atualizar_divulgado_db(link, valor):
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE lives SET divulgado=$1 WHERE link=$2", valor, link)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar divulgado: {e}")

async def remover_live_db(user_id):
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM lives WHERE user_id=$1", str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao remover live: {e}")

async def criar_tabela_lives_manual():
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
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
            logger.info("📋 Tabela lives_manual criada/verificada")
    except Exception as e:
        logger.error(f"❌ Erro ao criar tabela lives_manual: {e}")

async def salvar_live_manual(user_id, user_name, plataforma, link, titulo, categoria):
    pool = get_db()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE lives_manual SET ativo = false WHERE user_id = $1 AND ativo = true", str(user_id))
            return await conn.fetchval("INSERT INTO lives_manual (user_id, user_name, plataforma, link, titulo, categoria) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id", str(user_id), user_name, plataforma, link, titulo, categoria)
    except Exception as e:
        logger.error(f"❌ Erro ao salvar live manual: {e}")
        return None

async def buscar_lives_ativas():
    pool = get_db()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM lives_manual WHERE ativo = true ORDER BY data_cadastro DESC")
    except Exception as e:
        logger.error(f"❌ Erro ao buscar lives ativas: {e}")
        return []

async def desativar_live_manual(live_id):
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE lives_manual SET ativo = false WHERE id = $1", live_id)
    except Exception as e:
        logger.error(f"❌ Erro ao desativar live manual: {e}")

# --- TWITCH API ---
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

# --- FUNÇÕES AUXILIARES DAS LIVES ---
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
        await canal.send(content="@everyone 🔴 **LIVE INICIADA!**", embed=embed, allowed_mentions=discord.AllowedMentions(everyone=True))
        return True
    except Exception as e:
        logger.error(f"❌ ERRO ao divulgar live: {e}")
        return False

# --- VIEWS E MODAIS DAS LIVES ---
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

class CadastrarLiveView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="🎥 Cadastrar minha Live", style=discord.ButtonStyle.primary, custom_id="cadastrar_live_btn")
    async def cadastrar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(CadastrarLiveModal())

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
        await enviar_painel_admin_lives()
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

class FecharButtonRemover(discord.ui.Button):
    def __init__(self):
        super().__init__(label="❌ Fechar", style=discord.ButtonStyle.danger, emoji="❌")
    async def callback(self, interaction: discord.Interaction):
        try:
            await interaction.message.delete()
        except:
            pass

class PainelLivesAdmin(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="⚙️ Gerenciar Lives (ADM)", style=discord.ButtonStyle.danger, custom_id="abrir_painel_admin_lives_btn", emoji="⚙️")
    async def abrir(self, interaction: discord.Interaction, button):
        if interaction.user.id != ADM_ID:
            await interaction.response.send_message("❌ Apenas ADM.", ephemeral=True)
            return
        view = GerenciarLivesView()
        await interaction.response.send_message("**⚙️ PAINEL DE GERENCIAMENTO DE LIVES**\n\n📋 **Ver Lives** - Lista todas as lives cadastradas\n🗑️ **Remover Live** - Remove um usuário e todas as suas lives", view=view, ephemeral=True)

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
        pool = get_db()
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
        await canal_divulgacao.send(content=f"@everyone 🔴 **LIVE INICIADA!**", embed=embed, allowed_mentions=discord.AllowedMentions(everyone=True))
        await desativar_live_manual(live["id"])
        await interaction.response.send_message(f"✅ **LIVE ANUNCIADA COM SUCESSO!**\n📢 Anúncio enviado para <#{CANAL_DIVULGACAO_LIVE_ID}>", ephemeral=True)
    @discord.ui.button(label="❌ Cancelar Live", style=discord.ButtonStyle.danger, custom_id="cancelar_live_manual", emoji="❌")
    async def cancelar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) != str(self.user_id):
            await interaction.response.send_message("❌ Apenas o dono desta live pode cancelar!", ephemeral=True)
            return
        pool = get_db()
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

class PainelLivesManualView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="🎥 Minha Live", style=discord.ButtonStyle.primary, custom_id="minha_live_manual", emoji="🎥")
    async def minha_live(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = GerenciarLiveView(interaction.user.id, interaction.user.display_name)
        embed = discord.Embed(title="🎥 GERENCIAR MINHA LIVE", description="**📌 Como funciona:**\n\n1. Clique em **'Cadastrar/Atualizar Live'**\n2. Informe a plataforma (Kick, TikTok, etc)\n3. Cole o link da sua live\n4. Quando começar, clique em **'ANUNCIAR LIVE'**\n\n✅ **Plataformas suportadas:**\n• 🟢 Kick\n• 📱 TikTok\n• ▶️ YouTube\n• E qualquer outra!", color=0x3498db)
        embed.set_footer(text="Sistema de Lives • VDR")
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# --- PAINÉIS DAS LIVES ---
async def enviar_painel_lives():
    canal = bot.get_channel(CANAL_CADASTRO_LIVE_ID)
    if not canal:
        logger.error("❌ Canal cadastro live não encontrado")
        return
    embed = discord.Embed(title="🎥 SISTEMA DE LIVES", description="**Gerencie suas lives de forma simples e rápida!**\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n🟣 **TWITCH - AUTOMÁTICO**\n• Cadastre sua live **uma única vez**\n• Quando entrar ao vivo, o bot **anuncia automaticamente**\n• Você não precisa fazer mais nada!\n\n🟢 **KICK / TIKTOK / YOUTUBE - MANUAL**\n• **Toda vez** que for começar a live, publique manualmente\n• Preencha as informações e clique em 'Publicar Live'\n• O anúncio vai imediatamente para o canal de divulgação\n\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📢 **Todas as lives vão para:** <#1243325102917943335>\n⚠️ **Importante:** O link deve ser válido e acessível!", color=0x9146FF, timestamp=agora())
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
        logger.info("🎥 Painel único de lives enviado")
    except Exception as e:
        logger.error(f"❌ Erro ao enviar painel de lives: {e}")

async def enviar_painel_admin_lives():
    canal = bot.get_channel(CANAL_CADASTRO_LIVE_ID)
    if not canal:
        return
    embed = discord.Embed(title="⚙️ ADMIN - GERENCIAR LIVES", description="Clique no botão abaixo para gerenciar todas as lives cadastradas.", color=0xe74c3c)
    await enviar_ou_atualizar_painel("painel_admin_lives", CANAL_CADASTRO_LIVE_ID, embed, PainelLivesAdmin())

# --- LOOPS DAS LIVES ---
@tasks.loop(minutes=2)
async def verificar_lives():
    logger.info("🔄 Verificando lives da Twitch...")
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
# ==================== SEÇÃO 9: AUSÊNCIA ==================
# =========================================================

# --- IDs DA AUSÊNCIA ---
CANAL_BOTAO_AUSENCIA_ID = 1491427870277374162
CANAL_REGISTRO_AUSENCIA_ID = 1313854772545196032
CARGO_AUSENTE_ID = 1337420032212336823

# --- QUERIES DA AUSÊNCIA ---
async def salvar_ausencia_db(user_id, nome, motivo, data_inicio, data_fim):
    pool = get_db()
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

async def buscar_ausencias_ativas_db():
    pool = get_db()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM ausencias WHERE ativo = true AND data_fim > NOW() ORDER BY data_fim ASC")
    except Exception as e:
        logger.error(f"❌ Erro ao buscar ausências ativas: {e}")
        return []

async def buscar_ausencia_por_user(user_id):
    pool = get_db()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM ausencias WHERE user_id = $1 AND ativo = true AND data_fim > NOW()", str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao buscar ausência por usuário: {e}")
        return None

async def desativar_ausencia(user_id):
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE ausencias SET ativo = false WHERE user_id = $1 AND ativo = true", str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao desativar ausência: {e}")

async def remover_ausencias_expiradas():
    pool = get_db()
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

# --- VIEWS E MODAIS DA AUSÊNCIA ---
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

class AusenciaBotaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="📝 Solicitar Ausência", style=discord.ButtonStyle.primary, custom_id="ausencia_solicitar_botao")
    async def solicitar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(AusenciaModal())

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

class RemoverAusenciaView(discord.ui.View):
    def __init__(self, ausencias):
        super().__init__(timeout=60)
        self.add_item(RemoverAusenciaSelect(ausencias))

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

# --- LOOPS DA AUSÊNCIA ---
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
            logger.info(f"✅ Cargo ausente removido de {member.display_name}")
            canal_registro = guild.get_channel(CANAL_REGISTRO_AUSENCIA_ID)
            if canal_registro:
                embed_retorno = discord.Embed(title="🎉 RETORNO REGISTRADO", description=f"{member.mention} retornou! O cargo ausente foi removido automaticamente.", color=0x2ecc71)
                await canal_registro.send(embed=embed_retorno)

# --- PAINÉIS DA AUSÊNCIA ---
async def enviar_painel_botao_ausencia():
    canal = bot.get_channel(CANAL_BOTAO_AUSENCIA_ID)
    if not canal:
        logger.error(f"❌ Canal do botão NÃO ENCONTRADO! ID: {CANAL_BOTAO_AUSENCIA_ID}")
        return
    embed = discord.Embed(title="📋 Solicitar Ausência", description="Clique no botão abaixo para solicitar sua ausência.\n\n📌 **Como usar:**\n• Digite seu nome completo\n• Informe a **data de INÍCIO** (ex: `10/04/2026`)\n• Informe a **data de RETORNO** (ex: `15/04/2026`)\n• Digite o motivo\n\n✅ Você receberá o cargo **Ausente**\n✅ Quando o período acabar, o cargo será removido\n\n⚠️ **Ausências de 15 dias ou mais** serão notificadas à gerência", color=0xe67e22)
    embed.add_field(name="📅 Exemplo", value="• Data INÍCIO: `10/04/2026`\n• Data RETORNO: `15/04/2026`\n(contando todos os dias entre 10 e 15)", inline=False)
    await enviar_ou_atualizar_painel("painel_botao_ausencia", CANAL_BOTAO_AUSENCIA_ID, embed, AusenciaBotaoView())
    logger.info(f"✅ Painel do botão enviado para {CANAL_BOTAO_AUSENCIA_ID}")

async def enviar_painel_remover_ausencia():
    canal = bot.get_channel(CANAL_BOTAO_AUSENCIA_ID)
    if not canal:
        logger.error(f"❌ Canal do botão NÃO ENCONTRADO! ID: {CANAL_BOTAO_AUSENCIA_ID}")
        return
    try:
        async for msg in canal.history(limit=30):
            if msg.author == bot.user and msg.embeds and msg.embeds[0].title == "🔄 Remover Ausência (Retorno Antecipado)":
                return
        embed = discord.Embed(title="🔄 Remover Ausência (Retorno Antecipado)", description="Clique no botão abaixo caso um membro tenha **retornado antes do previsto**.\n\n⚠️ **Apenas para:** Gerente, Cargo 01, Cargo 02 e Gerente Geral", color=0x3498db)
        embed.add_field(name="📌 Como usar", value="1. Clique no botão\n2. Selecione o membro na lista\n3. Confirme a remoção\n\nO cargo **Ausente** será removido imediatamente.", inline=False)
        await enviar_ou_atualizar_painel("painel_remover_ausencia", CANAL_BOTAO_AUSENCIA_ID, embed, BotaoRemoverAusenciaView())
        logger.info(f"✅ Painel de remover ausência enviado para {CANAL_BOTAO_AUSENCIA_ID}")
    except Exception as e:
        logger.error(f"❌ Erro ao enviar painel remover ausência: {e}")

# =========================================================
# ==================== SEÇÃO 10: GRUPOS (FINAL) ===========
# =========================================================

# --- IDs DOS GRUPOS ---
CANAL_GRUPOS_ID = 1448563544386961479

# --- TIPOS DE ORGANIZAÇÃO ---
TIPOS_ORGANIZACAO = {
    "FAVELAS": {
        "nome": "🏚️ FAVELAS",
        "descricao": "PODE COMPRAR PT E SUB",
        "pode_pt": True,
        "pode_sub": True,
        "emoji": "🏚️",
        "produtos": ["HAXIXE", "AQUABLITS", "LEAN", "MD", "COCA", "LANÇA", "BALÃO", "K9", "KETAMINA"]
    },
    "MÁFIA": {
        "nome": "🤵 MÁFIA",
        "descricao": "PODE COMPRAR PT E SUB",
        "pode_pt": True,
        "pode_sub": True,
        "emoji": "🤵",
        "produtos": ["MUNIÇÃO FUZIL", "MUNIÇÃO PISTOLA", "SUB", "ARMAS", "LAVAGEM", "CONTRABANDO", "MEC ILEGAL", "KIT REPARO"]
    },
    "PISTA COM TABLET": {
        "nome": "📱 PISTA COM TABLET",
        "descricao": "PODE COMPRAR PT E SUB",
        "pode_pt": True,
        "pode_sub": True,
        "emoji": "📱",
        "produtos": ["PT", "SUB"]
    },
    "PISTA SEM TABLET": {
        "nome": "📋 PISTA SEM TABLET",
        "descricao": "PODE COMPRAR APENAS PT",
        "pode_pt": True,
        "pode_sub": False,
        "emoji": "📋",
        "produtos": ["PT"]
    }
}

# --- QUERIES DO BANCO DE DADOS ---
async def criar_tabela_grupos():
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS grupos (
                    grupo_id VARCHAR(50) PRIMARY KEY,
                    nome_org TEXT,
                    lider_nome TEXT,
                    lider_telefone TEXT,
                    braco_nome TEXT,
                    braco_telefone TEXT,
                    produto TEXT,
                    tipo_org VARCHAR(30) DEFAULT 'PISTA SEM TABLET',
                    observacoes TEXT,
                    data_criacao TIMESTAMP DEFAULT NOW(),
                    data_atualizacao TIMESTAMP,
                    data_exclusao TIMESTAMP,
                    ativo BOOLEAN DEFAULT true
                )
            """)
            
            await conn.execute("""
                DO $$ 
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                  WHERE table_name='grupos' AND column_name='tipo_org') THEN
                        ALTER TABLE grupos ADD COLUMN tipo_org VARCHAR(30) DEFAULT 'PISTA SEM TABLET';
                    END IF;
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                  WHERE table_name='grupos' AND column_name='observacoes') THEN
                        ALTER TABLE grupos ADD COLUMN observacoes TEXT;
                    END IF;
                END $$;
            """)
            
            logger.info("✅ TABELA GRUPOS VERIFICADA")
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")

async def salvar_grupo_db(grupo_id, nome_org, lider_nome, lider_telefone, braco_nome, braco_telefone, produto, tipo_org="PISTA SEM PAINEL", observacoes=""):
    pool = get_db()
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

async def carregar_grupo_db(grupo_id):
    pool = get_db()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM grupos WHERE grupo_id = $1 AND ativo = true", grupo_id)
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")
        return None

async def carregar_grupos_db():
    pool = get_db()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM grupos WHERE ativo = true ORDER BY nome_org ASC")
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")
        return []

async def atualizar_grupo_db(grupo_id, nome_org, lider_nome, lider_telefone, braco_nome, braco_telefone, produto, tipo_org=None, observacoes=None):
    pool = get_db()
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

async def desativar_grupo_db(grupo_id):
    pool = get_db()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE grupos SET ativo = false, data_exclusao = $1 WHERE grupo_id = $2", agora_db(), grupo_id)
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")

async def registrar_compra_grupo_db(grupo_id, tipo, quantidade, valor):
    pool = get_db()
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

async def carregar_compras_grupo_db(grupo_id):
    pool = get_db()
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

async def buscar_grupo_por_organizacao(nome_org):
    pool = get_db()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            return await conn.fetchrow("SELECT grupo_id FROM grupos WHERE LOWER(nome_org) = LOWER($1) AND ativo = true", nome_org)
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")
        return None

# --- FUNÇÃO PARA LIMPAR E RECRIAR O PAINEL (FORÇADO) ---
async def recriar_painel_grupos():
    """LIMPA TODAS AS MENSAGENS DO BOT E RECRIA O PAINEL."""
    canal = bot.get_channel(CANAL_GRUPOS_ID)
    if not canal:
        logger.error(f"❌ CANAL NÃO ENCONTRADO: {CANAL_GRUPOS_ID}")
        return False
    
    try:
        # DELETAR TODAS AS MENSAGENS DO BOT (FORÇADO)
        logger.info("🗑️ DELETANDO TODAS AS MENSAGENS DO BOT NO CANAL...")
        deletadas = 0
        
        # Buscar todas as mensagens do bot no canal
        async for msg in canal.history(limit=500):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    deletadas += 1
                    await asyncio.sleep(0.3)  # Delay para evitar rate limit
                except discord.Forbidden:
                    logger.error("❌ SEM PERMISSÃO PARA DELETAR MENSAGENS!")
                except discord.HTTPException as e:
                    if e.status == 429:
                        logger.warning("⚠️ RATE LIMIT, AGUARDANDO...")
                        await asyncio.sleep(2)
                    else:
                        logger.error(f"❌ ERRO AO DELETAR: {e}")
        
        logger.info(f"✅ {deletadas} MENSAGENS DELETADAS")
        
        # AGUARDAR PARA EVITAR RATE LIMIT
        await asyncio.sleep(2)
        
        # ENVIAR O NOVO PAINEL (APENAS 1 MENSAGEM)
        await enviar_painel_grupos()
        
        logger.info("✅ PAINEL RECRIADO COM SUCESSO!")
        return True
        
    except Exception as e:
        logger.error(f"❌ ERRO AO RECRIAR PAINEL: {e}")
        return False

# --- FUNÇÃO PARA ENVIAR PAINEL PRINCIPAL ---
async def enviar_painel_grupos():
    """ENVIA O PAINEL PRINCIPAL COM DROPDOWN."""
    canal = bot.get_channel(CANAL_GRUPOS_ID)
    if not canal:
        logger.error(f"❌ CANAL NÃO ENCONTRADO")
        return
    
    try:
        await criar_tabela_grupos()
        grupos = await carregar_grupos_db()
        
        embed = discord.Embed(
            title="📋 GERENCIAMENTO DE GRUPOS",
            description="**SELECIONE UM GRUPO NO MENU ABAIXO:**\n\n📌 **TIPOS:**\n• 🏚️ FAVELAS - PT E SUB\n• 🤵 MÁFIA - PT E SUB\n• 📱 PISTA COM TABLET - PT E SUB\n• 📋 PISTA SEM TABLET - APENAS PT",
            color=0x2ecc71,
            timestamp=agora()
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
            
            embed.add_field(
                name="📊 RESUMO",
                value=f"**{len(grupos)} GRUPOS** | PT: {fmt_num(total_pt)} | SUB: {fmt_num(total_sub)}",
                inline=False
            )
        else:
            embed.add_field(
                name="📭 NENHUM GRUPO",
                value="CLIQUE EM **➕ NOVO GRUPO** PARA CADASTRAR.",
                inline=False
            )
        
        embed.set_footer(text="👇 SELECIONE UM GRUPO NO DROPDOWN")
        
        view = PainelGruposView(grupos)
        await canal.send(embed=embed, view=view)
        logger.info("✅ PAINEL DE GRUPOS ENVIADO")
        
    except Exception as e:
        logger.error(f"❌ ERRO AO ENVIAR PAINEL: {e}")
        
# --- VIEW PRINCIPAL ---
class PainelGruposView(discord.ui.View):
    def __init__(self, grupos):
        super().__init__(timeout=None)
        self.grupos = grupos
        
        import time
        self.uid = str(int(time.time()))[-6:]
        
        # DROPDOWN
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
        
        # BOTÃO 1: NOVO GRUPO
        self.add_item(discord.ui.Button(
            label="➕ NOVO GRUPO",
            style=discord.ButtonStyle.success,
            custom_id=f"novo_{self.uid}",
            emoji="➕"
        ))
        
        # BOTÃO 2: ATUALIZAR
        self.add_item(discord.ui.Button(
            label="🔄 ATUALIZAR",
            style=discord.ButtonStyle.secondary,
            custom_id=f"atualizar_{self.uid}",
            emoji="🔄"
        ))
    
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
        await interaction.response.send_modal(RegistrarGrupoModal())
    
    @discord.ui.button(label="🔄 ATUALIZAR", style=discord.ButtonStyle.secondary, custom_id="atualizar_padrao", emoji="🔄")
    async def atualizar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await recriar_painel_grupos()
        await interaction.followup.send("✅ PAINEL ATUALIZADO!", ephemeral=True)

# --- VIEW PARA AÇÕES DO GRUPO ---
class GrupoView(discord.ui.View):
    def __init__(self, grupo_id, nome_org):
        super().__init__(timeout=300)
        self.grupo_id = grupo_id
        self.nome_org = nome_org
        
        import time
        self.uid = str(int(time.time()))[-6:]
        
        self.add_item(discord.ui.Button(
            label="✏️ EDITAR",
            style=discord.ButtonStyle.primary,
            custom_id=f"editar_{self.uid}",
            emoji="✏️"
        ))
        self.add_item(discord.ui.Button(
            label="🗑️ EXCLUIR",
            style=discord.ButtonStyle.danger,
            custom_id=f"excluir_{self.uid}",
            emoji="🗑️"
        ))
        self.add_item(discord.ui.Button(
            label="📊 COMPRAS",
            style=discord.ButtonStyle.success,
            custom_id=f"compras_{self.uid}",
            emoji="📦"
        ))
    
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
        
        await interaction.response.send_modal(EditarGrupoModal(self.grupo_id, dados))
    
    async def excluir(self, interaction: discord.Interaction):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ APENAS ADM OU GERENTES!", ephemeral=True)
            return
        
        view = ConfirmarExcluirView(self.grupo_id, self.nome_org)
        await interaction.response.send_message(
            f"⚠️ **EXCLUIR {self.nome_org}?**",
            view=view,
            ephemeral=True
        )
    
    async def compras(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        compras = await carregar_compras_grupo_db(self.grupo_id)
        pt = compras.get("PT", {})
        sub = compras.get("SUB", {})
        
        embed = discord.Embed(
            title=f"📦 COMPRAS - {self.nome_org}",
            color=0x2ecc71
        )
        
        if pt.get("quantidade", 0) > 0 or sub.get("quantidade", 0) > 0:
            if pt.get("quantidade", 0) > 0:
                embed.add_field(
                    name="🔫 PT",
                    value=f"**{fmt_num(pt['quantidade'])}** PACOTES\n💰 {formatar_dinheiro(pt['valor'])}",
                    inline=True
                )
            if sub.get("quantidade", 0) > 0:
                embed.add_field(
                    name="🔫 SUB",
                    value=f"**{fmt_num(sub['quantidade'])}** PACOTES\n💰 {formatar_dinheiro(sub['valor'])}",
                    inline=True
                )
            total = pt.get("quantidade", 0) + sub.get("quantidade", 0)
            total_valor = pt.get("valor", 0) + sub.get("valor", 0)
            embed.add_field(
                name="📦 TOTAL",
                value=f"**{fmt_num(total)}** PACOTES\n💰 {formatar_dinheiro(total_valor)}",
                inline=False
            )
        else:
            embed.add_field(
                name="📭 NENHUMA COMPRA",
                value="ESTE GRUPO AINDA NÃO REALIZOU COMPRAS.",
                inline=False
            )
        
        await interaction.followup.send(embed=embed, ephemeral=True)

# --- MODAL PARA REGISTRAR ---
class RegistrarGrupoModal(discord.ui.Modal, title="📋 REGISTRAR NOVO GRUPO"):
    def __init__(self):
        super().__init__(timeout=300)
        
        self.nome_org = discord.ui.TextInput(
            label="🏷️ NOME DA ORGANIZAÇÃO",
            placeholder="EX: VDR, POLÍCIA, MAFIA",
            required=True,
            max_length=50
        )
        
        self.lider = discord.ui.TextInput(
            label="👤 LÍDER (NOME - TELEFONE)",
            placeholder="EX: JOÃO SILVA - (11) 99999-9999",
            required=True,
            max_length=100
        )
        
        self.braco = discord.ui.TextInput(
            label="👤 BRAÇO (NOME - TELEFONE - OPCIONAL)",
            placeholder="EX: JOSÉ SANTOS - (11) 88888-8888",
            required=False,
            max_length=100
        )
        
        self.produto = discord.ui.TextInput(
            label="🔫 PRODUTO QUE FORNECE",
            placeholder="EX: HAXIXE, MUNIÇÃO FUZIL, PT, SUB",
            required=True,
            max_length=50
        )
        
        self.tipo_org = discord.ui.TextInput(
            label="📌 TIPO DE ORGANIZAÇÃO",
            placeholder="FAVELAS / MÁFIA / PISTA COM TABLET / PISTA SEM TABLET",
            required=True,
            max_length=30,
            default="PISTA SEM TABLET"
        )
        
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
        if tipo_org not in ['FAVELAS', 'MÁFIA', 'PISTA COM TABLET', 'PISTA SEM TABLET']:
            tipo_org = 'PISTA SEM TABLET'
        
        import time
        grupo_id = f"GRUPO_{int(time.time())}_{interaction.user.id}"
        
        await salvar_grupo_db(
            grupo_id,
            self.nome_org.value.strip().upper(),
            lider_nome.upper(),
            lider_telefone.upper(),
            braco_nome.upper() if braco_nome else None,
            braco_telefone.upper() if braco_telefone else None,
            self.produto.value.strip().upper(),
            tipo_org,
            ""
        )
        
        await recriar_painel_grupos()
        await interaction.followup.send(f"✅ **GRUPO {self.nome_org.value.upper()} REGISTRADO!**", ephemeral=True)
        
# --- MODAL PARA EDITAR ---
class EditarGrupoModal(discord.ui.Modal, title="✏️ EDITAR GRUPO"):
    def __init__(self, grupo_id, dados):
        super().__init__(timeout=300)
        self.grupo_id = grupo_id
        
        self.nome_org = discord.ui.TextInput(
            label="🏷️ NOME DA ORGANIZAÇÃO",
            default=dados.get('nome_org', '').upper(),
            required=True,
            max_length=50
        )
        
        lider_texto = f"{dados.get('lider_nome', '').upper()} - {dados.get('lider_telefone', '').upper()}"
        self.lider = discord.ui.TextInput(
            label="👤 LÍDER (NOME - TELEFONE)",
            default=lider_texto,
            required=True,
            max_length=100
        )
        
        if dados.get('braco_nome') and dados.get('braco_telefone'):
            braco_default = f"{dados.get('braco_nome', '').upper()} - {dados.get('braco_telefone', '').upper()}"
        elif dados.get('braco_nome'):
            braco_default = dados.get('braco_nome', '').upper()
        else:
            braco_default = ""
        
        self.braco = discord.ui.TextInput(
            label="👤 BRAÇO (NOME - TELEFONE - OPCIONAL)",
            default=braco_default,
            required=False,
            max_length=100
        )
        
        self.produto = discord.ui.TextInput(
            label="🔫 PRODUTO QUE FORNECE",
            default=dados.get('produto', '').upper(),
            required=True,
            max_length=50
        )
        
        tipo_atual = dados.get('tipo_org', 'PISTA SEM TABLET').upper()
        self.tipo_org = discord.ui.TextInput(
            label="📌 TIPO DE ORGANIZAÇÃO",
            default=tipo_atual,
            required=True,
            max_length=30
        )
        
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
        if tipo_org not in ['FAVELAS', 'MÁFIA', 'PISTA COM TABLET', 'PISTA SEM TABLET']:
            tipo_org = 'PISTA SEM TABLET'
        
        await atualizar_grupo_db(
            self.grupo_id,
            self.nome_org.value.strip().upper(),
            lider_nome.upper(),
            lider_telefone.upper(),
            braco_nome.upper() if braco_nome else None,
            braco_telefone.upper() if braco_telefone else None,
            self.produto.value.strip().upper(),
            tipo_org,
            ""
        )
        
        await recriar_painel_grupos()
        await interaction.followup.send(f"✅ **GRUPO {self.nome_org.value.upper()} ATUALIZADO!**", ephemeral=True)

# --- VIEW PARA CONFIRMAR EXCLUSÃO ---
class ConfirmarExcluirView(discord.ui.View):
    def __init__(self, grupo_id, nome_org):
        super().__init__(timeout=60)
        self.grupo_id = grupo_id
        self.nome_org = nome_org
        
        import time
        self.uid = str(int(time.time()))[-6:]
        
        self.add_item(discord.ui.Button(
            label="✅ SIM",
            style=discord.ButtonStyle.danger,
            custom_id=f"conf_{self.uid}",
            emoji="✅"
        ))
        self.add_item(discord.ui.Button(
            label="❌ CANCELAR",
            style=discord.ButtonStyle.secondary,
            custom_id=f"cancel_{self.uid}",
            emoji="❌"
        ))
    
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

# --- FUNÇÃO PARA SINCRONIZAR COM VENDAS ---
async def sync_grupo_com_vendas(org_nome, tipo, quantidade, valor):
    grupo = await buscar_grupo_por_organizacao(org_nome)
    if grupo:
        await registrar_compra_grupo_db(grupo["grupo_id"], tipo, quantidade, valor)
        return True
    return False


# =========================================================
# ==================== SEÇÃO 12: FINANCEIRO ===============
# =========================================================

# --- IDs DO FINANCEIRO ---
CANAL_RELATORIO_FINANCEIRO_ID = 1498664038559776768
CANAL_REGISTRAR_COMPRA_ID = 1498668853465448560
CANAL_COMPRAS_REGISTRADAS_ID = 1270467793363669053

# --- QUERIES DO FINANCEIRO ---
async def salvar_compra_db(produto, valor, comprado_por):
    pool = get_db()
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

# --- VIEWS E MODAIS DO FINANCEIRO ---
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
            valor_compra = int(self.valor.value.replace(".", "").replace(",", ""))
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

class RegistrarCompraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="📝 Registrar Nova Compra", style=discord.ButtonStyle.success, custom_id="registrar_compra_btn", emoji="💰")
    async def registrar_compra(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarCompraModal())

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
                    total_embalagens = int(self.embalagens.value.replace(".", "").replace(",", ""))
                    total_gasto_embalagens = int(total_embalagens * PRECO_EMBALAGEM_POR_UNIDADE)
                except:
                    pass
            pool = get_db()
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

# --- PAINÉIS DO FINANCEIRO ---
async def enviar_painel_registrar_compra():
    canal = bot.get_channel(CANAL_REGISTRAR_COMPRA_ID)
    if not canal:
        logger.error(f"❌ Canal de registrar compra não encontrado: {CANAL_REGISTRAR_COMPRA_ID}")
        return
    embed = discord.Embed(title="💰 REGISTRAR COMPRA", description="Clique no botão abaixo para registrar uma nova compra.\n\n📋 **Informações necessárias:**\n• 📦 Nome do produto\n• 💰 Valor da compra\n\nApós registrar, a compra aparecerá automaticamente no canal de registros.", color=0x3498db)
    embed.add_field(name="📌 EXEMPLO", value="**Produto:** Pólvora\n**Valor:** 50000", inline=False)
    embed.set_footer(text="Todas as compras ficam salvas no banco de dados para relatórios futuros")
    try:
        async for msg in canal.history(limit=10):
            if msg.author == bot.user and msg.embeds and msg.embeds[0].title == "💰 REGISTRAR COMPRA":
                try:
                    await msg.delete()
                except:
                    pass
        await canal.send(embed=embed, view=RegistrarCompraView())
        logger.info(f"💰 Painel de registrar compra enviado para o canal {CANAL_REGISTRAR_COMPRA_ID}")
    except Exception as e:
        logger.error(f"❌ Erro ao enviar painel registrar compra: {e}")

async def enviar_painel_relatorio_financeiro():
    canal = bot.get_channel(CANAL_RELATORIO_FINANCEIRO_ID)
    if not canal:
        logger.error("❌ Canal de relatório financeiro não encontrado")
        return
    embed = discord.Embed(title="💰 RELATÓRIO FINANCEIRO", description="Clique no botão abaixo para gerar um relatório financeiro completo.\n\n📋 **O relatório inclui:**\n• 💣 Pólvora utilizada na produção\n• 💰 Gasto total com pólvora\n• 🛒 Total de vendas no período\n• 📦 Gasto com embalagens (opcional)\n• 📦 Outras compras registradas\n• 📊 Saldo final (vendas - gastos)\n\n📅 **Você pode escolher:**\n• Data inicial e final\n• Incluir ou não outras compras (SIM/NAO)", color=0x1abc9c)
    embed.add_field(name="📌 EXEMPLO DE PREENCHIMENTO", value="**Data inicial:** `01/04/2026`\n**Data final:** `30/04/2026`\n**Incluir compras:** `SIM` (ou `NAO`)", inline=False)
    embed.set_footer(text="Os valores são calculados automaticamente com base no banco de dados")
    await enviar_ou_atualizar_painel("painel_relatorio_financeiro", CANAL_RELATORIO_FINANCEIRO_ID, embed, RelatorioFinanceiroView())
    logger.info("💰 Painel de relatório financeiro enviado/atualizado")

class RelatorioFinanceiroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    @discord.ui.button(label="📊 Gerar Relatório Financeiro", style=discord.ButtonStyle.success, custom_id="relatorio_financeiro_btn", emoji="💰")
    async def gerar_relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RelatorioFinanceiroModal())

# =========================================================
# ==================== SEÇÃO 13: MENSAGENS ================
# =========================================================

# --- IDs DAS MENSAGENS ---
CANAL_TEXTOS_VENDAS_ID = 1499045083994001500

# --- VARIÁVEIS GLOBAIS DAS MENSAGENS ---
mensagens_em_andamento = set()
mensagens_timers = {}  # Para controlar timeout

# --- FUNÇÃO PARA LIMPAR MENSAGEM EM ANDAMENTO ---
async def limpar_mensagem_andamento(user_id):
    """Remove o usuário da lista de mensagens em andamento."""
    if user_id in mensagens_em_andamento:
        mensagens_em_andamento.remove(user_id)
    if user_id in mensagens_timers:
        del mensagens_timers[user_id]

# --- VIEW E MODAIS DAS MENSAGENS ---
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
            # Limpar o estado do usuário ao fechar
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
        # Verificar se já tem mensagem em andamento
        if interaction.user.id in mensagens_em_andamento:
            # Se tiver, remover o bloqueio automaticamente
            await limpar_mensagem_andamento(interaction.user.id)
        
        # Adicionar à lista
        mensagens_em_andamento.add(interaction.user.id)
        
        # Configurar timer para limpar automaticamente após 5 minutos
        mensagens_timers[interaction.user.id] = asyncio.create_task(
            limpar_timer_mensagem(interaction.user.id, 300)  # 5 minutos
        )
        
        modal = MensagemPedidoProntoModal(interaction.user)
        await interaction.response.send_modal(modal)
    
    async def handle_pedido_cancelado(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(
            limpar_timer_mensagem(interaction.user.id, 300)
        )
        
        modal = MensagemPedidoCanceladoModal()
        await interaction.response.send_modal(modal)
    
    async def handle_pedido_finalizado(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(
            limpar_timer_mensagem(interaction.user.id, 300)
        )
        
        modal = MensagemPedidoFinalizadoModal()
        await interaction.response.send_modal(modal)
    
    async def handle_pendencia_pagamento(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(
            limpar_timer_mensagem(interaction.user.id, 300)
        )
        
        modal = MensagemPendenciaPagamentoModal()
        await interaction.response.send_modal(modal)
    
    async def handle_pagamento_pendente(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(
            limpar_timer_mensagem(interaction.user.id, 300)
        )
        
        modal = MensagemPagamentoPendenteModal(interaction.user)
        await interaction.response.send_modal(modal)

# --- TIMER PARA LIMPAR AUTOMATICAMENTE ---
async def limpar_timer_mensagem(user_id, tempo_segundos):
    """Limpa a mensagem em andamento após o tempo especificado."""
    await asyncio.sleep(tempo_segundos)
    await limpar_mensagem_andamento(user_id)

# --- MODAIS DAS MENSAGENS (CORRIGIDOS) ---
class MensagemPedidoProntoModal(discord.ui.Modal, title="📦 Pedido Pronto"):
    def __init__(self, usuario):
        super().__init__(timeout=300)  # 5 minutos de timeout
        self.usuario = usuario
    
    valor = discord.ui.TextInput(
        label="💰 Valor da encomenda (opcional)",
        placeholder="Ex: 50000 ou deixe em branco",
        required=False,
        max_length=50
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        # Limpar da lista ao finalizar
        await limpar_mensagem_andamento(interaction.user.id)
        
        valor_texto = ""
        if self.valor.value and self.valor.value.strip():
            try:
                valor = int(self.valor.value.replace(".", "").replace(",", ""))
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
        
        embed = discord.Embed(
            title="📋 MENSAGEM GERADA - PEDIDO PRONTO",
            description="**Copie a mensagem abaixo e cole no canal desejado:**",
            color=0x2ecc71
        )
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(
            name="📌 DETALHES",
            value=f"👤 Gerado por: {interaction.user.mention}\n📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}",
            inline=False
        )
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def on_error(self, interaction: discord.Interaction, error: Exception):
        # Em caso de erro, limpar da lista
        await limpar_mensagem_andamento(interaction.user.id)
        logger.error(f"Erro no modal: {error}")

class MensagemPedidoCanceladoModal(discord.ui.Modal, title="❌ Pedido Cancelado"):
    def __init__(self):
        super().__init__(timeout=300)
    
    valor = discord.ui.TextInput(
        label="💰 Valor da encomenda (opcional)",
        placeholder="Ex: 50000 ou deixe em branco",
        required=False,
        max_length=50
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        
        valor_texto = ""
        if self.valor.value and self.valor.value.strip():
            try:
                valor = int(self.valor.value.replace(".", "").replace(",", ""))
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
        
        embed = discord.Embed(
            title="📋 MENSAGEM GERADA - PEDIDO CANCELADO",
            description="**Copie a mensagem abaixo e cole no canal desejado:**",
            color=0xe74c3c
        )
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(
            name="📌 DETALHES",
            value=f"👤 Gerado por: {interaction.user.mention}\n📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}",
            inline=False
        )
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await limpar_mensagem_andamento(interaction.user.id)
        logger.error(f"Erro no modal: {error}")

class MensagemPedidoFinalizadoModal(discord.ui.Modal, title="✅ Pedido Finalizado"):
    def __init__(self):
        super().__init__(timeout=300)
    
    valor = discord.ui.TextInput(
        label="💰 Valor da encomenda (opcional)",
        placeholder="Ex: 50000 ou deixe em branco",
        required=False,
        max_length=50
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        
        valor_texto = ""
        if self.valor.value and self.valor.value.strip():
            try:
                valor = int(self.valor.value.replace(".", "").replace(",", ""))
                valor_texto = f"\n💰 Valor: {formatar_dinheiro(valor)}"
            except:
                valor_texto = f"\n💰 Valor: {self.valor.value}"
        
        mensagem = f"""✅ PEDIDO FINALIZADO

Sua encomenda foi entregue e o pagamento foi confirmado.

Agradecemos pela preferência!

{interaction.user.display_name} — {agora().strftime('%d/%m/%Y %H:%M')}
{valor_texto}"""
        
        embed = discord.Embed(
            title="📋 MENSAGEM GERADA - PEDIDO FINALIZADO",
            description="**Copie a mensagem abaixo e cole no canal desejado:**",
            color=0x2ecc71
        )
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(
            name="📌 DETALHES",
            value=f"👤 Gerado por: {interaction.user.mention}\n📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}",
            inline=False
        )
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await limpar_mensagem_andamento(interaction.user.id)
        logger.error(f"Erro no modal: {error}")

class MensagemPendenciaPagamentoModal(discord.ui.Modal, title="💰 Pendência de Pagamento"):
    def __init__(self):
        super().__init__(timeout=300)
    
    valor = discord.ui.TextInput(
        label="💰 Valor pendente",
        placeholder="Ex: 50000",
        required=True,
        max_length=50
    )
    chave_pix = discord.ui.TextInput(
        label="📱 Chave PIX (passaporte e nome)",
        placeholder="Ex: 820 - Leon",
        required=True,
        max_length=100
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        
        try:
            valor = int(self.valor.value.replace(".", "").replace(",", ""))
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
        
        embed = discord.Embed(
            title="📋 MENSAGEM GERADA - PENDÊNCIA DE PAGAMENTO",
            description="**Copie a mensagem abaixo e cole no canal desejado:**",
            color=0xf1c40f
        )
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(
            name="📌 DETALHES",
            value=f"👤 Gerado por: {interaction.user.mention}\n💰 Valor: R$ {valor_texto}\n📱 Chave PIX: {chave_pix}\n📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}",
            inline=False
        )
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await limpar_mensagem_andamento(interaction.user.id)
        logger.error(f"Erro no modal: {error}")

class MensagemPagamentoPendenteModal(discord.ui.Modal, title="⚠️ Pagamento Pendente"):
    def __init__(self, usuario):
        super().__init__(timeout=300)
        self.usuario = usuario
    
    valor = discord.ui.TextInput(
        label="💰 Valor pendente",
        placeholder="Ex: 50000",
        required=True,
        max_length=50
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        
        try:
            valor = int(self.valor.value.replace(".", "").replace(",", ""))
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
        
        embed = discord.Embed(
            title="📋 MENSAGEM GERADA - PAGAMENTO PENDENTE",
            description="**Copie a mensagem abaixo e cole no canal desejado:**",
            color=0xe67e22
        )
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(
            name="📌 DETALHES",
            value=f"👤 Gerado por: {interaction.user.mention}\n💰 Valor: R$ {valor_texto}\n📱 Chave PIX: {chave_pix}\n📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}",
            inline=False
        )
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)
    
    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await limpar_mensagem_andamento(interaction.user.id)
        logger.error(f"Erro no modal: {error}")

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

# --- PAINEL DE MENSAGENS ---
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
    embed.add_field(
        name="📌 DICA",
        value="• O passaporte é extraído automaticamente do seu apelido no servidor\n• Certifique-se de ter seu apelido no formato: `PASSAPORTE - NOME`\n• Se fechar a janela, espere 5 minutos ou clique em 'Fechar' para liberar",
        inline=False
    )
    embed.set_footer(text="Sistema de Mensagens • VDR 442")
    
    view = MenuMensagensView()
    
    try:
        async for msg in canal.history(limit=20):
            if msg.author == bot.user and msg.embeds and msg.embeds[0].title == "📝 GERADOR DE MENSAGENS DE VENDA":
                try:
                    await msg.edit(embed=embed, view=view)
                    logger.info("📝 Painel de mensagens atualizado")
                    return
                except:
                    pass
        await canal.send(embed=embed, view=view)
        logger.info("📝 Painel de mensagens enviado")
    except Exception as e:
        logger.error(f"❌ Erro ao enviar painel de mensagens: {e}")

# =========================================================

# =========================================================
# ==================== SEÇÃO 14: CLIPES ===================
# =========================================================

# --- IDs DOS CLIPES ---
CANAL_CLIPES_ID = 1229526645837271134
CANAL_POSTAGEM_X = 1486353689680547900

# --- VARIÁVEIS GLOBAIS DOS CLIPES ---
clips_postados = set()

# --- EVENTO DE REAÇÃO CLIPES ---
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

# --- WORKER DE CLIPES ---
async def worker_clipes():
    global fila_clipes
    logger.info("🎬 Worker clips iniciado")
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
# ==================== SEÇÃO 15: AÇÕES GLOBAIS ============
# =========================================================

# --- IDS GLOBAIS ---
CANAL_GERENCIA_ID = 1237393478414241854

# --- EVENTOS GLOBAIS ---
@bot.event
async def on_member_join(member):
    if member.bot:
        return
    try:
        cargo_em_registro = member.guild.get_role(EM_REGISTRO_ROLE_ID)
        if cargo_em_registro:
            await member.add_roles(cargo_em_registro)
            logger.info(f"✅ Cargo 'Em Registro' adicionado para {member.name}")
    except Exception as e:
        logger.error(f"❌ Erro ao adicionar cargo 'Em Registro' para {member.name}: {e}")

@bot.event
async def on_member_update(before, after):
    if after.bot:
        return
    tinha_agregado = any(r.id == AGREGADO_ROLE_ID for r in before.roles)
    tem_agregado = any(r.id == AGREGADO_ROLE_ID for r in after.roles)
    if not tinha_agregado and tem_agregado:
        await asyncio.sleep(2)
        logger.info(f"🔵 {after.name} ganhou cargo de Agregado, criando sala...")
        pool = get_db()
        if pool:
            async with pool.acquire() as conn:
                meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(after.id))
        else:
            meta = None
        if not meta:
            sala = await criar_sala_meta(after)
            if sala:
                logger.info(f"✅ Sala criada para {after.name}")
            else:
                logger.error(f"❌ Erro ao criar sala para {after.name}")
        else:
            canal = after.guild.get_channel(meta["canal_id"])
            if not canal:
                sala = await criar_sala_meta(after)
                if sala:
                    logger.info(f"✅ Sala recriada para {after.name}")
            else:
                await atualizar_embed_meta(after.id)
                logger.info(f"📊 Painel atualizado para {after.name}")
        return
    if str(after.id) in metas_cache:
        await atualizar_categoria_meta(after)

@bot.event
async def on_guild_channel_delete(channel):
    for uid, dados in list(metas_cache.items()):
        if dados["canal_id"] == channel.id:
            logger.info(f"🗑️ Canal de meta deletado: {channel.name} (Usuário: {uid})")
            metas_cache.pop(uid)
            try:
                pool = get_db()
                if pool:
                    async with pool.acquire() as conn:
                        await conn.execute("DELETE FROM metas WHERE user_id = $1", uid)
                logger.info(f"✅ Meta do usuário {uid} removida do banco")
            except Exception as e:
                logger.error(f"❌ Erro ao remover meta do banco: {e}")
            break

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

# =========================================================
# ==================== SEÇÃO 16: COMANDOS =================
# =========================================================

@bot.command(name="estoque")
async def cmd_ver_estoque(ctx):
    estoque_municoes = await carregar_estoque()
    estoque_insumos = await carregar_estoque_insumos()
    embed = discord.Embed(title="📦 ESTOQUE COMPLETO", color=0x3498db)
    embed.add_field(name="🔫 MUNIÇÕES", value=f"**PT:** {fmt_num(estoque_municoes['PT'])} pacotes ({fmt_num(estoque_municoes['PT'] * 50)} munições)\n**SUB:** {fmt_num(estoque_municoes['SUB'])} pacotes ({fmt_num(estoque_municoes['SUB'] * 50)} munições)", inline=False)
    embed.add_field(name="💊 INSUMOS", value=f"**Cápsulas:** {fmt_num(estoque_insumos['capsulas'])} unidades\n**Embalagens:** {fmt_num(estoque_insumos['embalagens'])} unidades", inline=False)
    await ctx.send(embed=embed)

@bot.command(name="historico_producao")
async def cmd_historico_producao(ctx, limite: int = 10):
    pool = get_db()
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

@bot.command(name="historico_vendas_estoque")
async def cmd_historico_vendas_estoque(ctx, limite: int = 10):
    pool = get_db()
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

# =========================================================
# ==================== SEÇÃO 17: ON_READY OTIMIZADO =======
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

    await criar_tabela_grupos()
    await criar_tabela_alugueis()
        
    # Carregar guild e membros
    guild = bot.get_guild(GUILD_ID)
    if guild:
        try:
            await guild.chunk()
            logger.info("👥 Membros carregados no cache.")
        except Exception as e:
            logger.error(f"Erro ao carregar membros: {e}")
    
    logger.info(f"🕒 Horário Brasília: {agora().strftime('%d/%m/%Y %H:%M:%S')}")
    
    # Iniciar worker de edição
    if not hasattr(bot, "edit_worker_started"):
        bot.loop.create_task(edit_worker())
        bot.edit_worker_started = True
        logger.info("🛠️ Edit worker iniciado.")
    
    # Iniciar fila de clipes
    fila_clipes = asyncio.Queue()
    bot.loop.create_task(worker_clipes())
    logger.info("🎬 Sistema de clips ON")
    
    # Iniciar tarefas de background
    await iniciar_tarefas_background()
    
    # Iniciar limpeza de cache
    bot.loop.create_task(limpeza_cache_periodica())
    
    # Iniciar health check para Railway
    bot.loop.create_task(health_check())
    
    # Carregar dados iniciais
    await carregar_dados_iniciais()
    
    # Enviar painéis
    await enviar_paineis_iniciais(guild)

    await recriar_painel_grupos()

       
    # Limpeza de memória
    gc.collect()
    logger.info("🧹 Limpeza de memória executada")
    logger.info("=" * 50)
    logger.info("✅ BOT ONLINE 100% ESTÁVEL")
    logger.info("=" * 50)

async def iniciar_tarefas_background():
    """Inicia todas as tarefas em background."""
    try:
        if not verificar_lives.is_running():
            verificar_lives.start()
            logger.info("🎥 Loop de lives iniciado")
    except Exception as e:
        logger.error(f"Erro loop lives: {e}")
    
    try:
        if not relatorio_semanal_polvoras.is_running():
            relatorio_semanal_polvoras.start()
            logger.info("💣 Loop de relatório de pólvora iniciado")
    except Exception as e:
        logger.error(f"Erro loop polvora: {e}")
    
    try:
        if not verificar_ausencias_expiradas.is_running():
            verificar_ausencias_expiradas.start()
            logger.info("📋 Loop de ausências iniciado")
    except Exception as e:
        logger.error(f"Erro loop ausência: {e}")
    
    try:
        if not limpar_lavagens_pendentes.is_running():
            limpar_lavagens_pendentes.start()
            logger.info("🧼 Loop de limpeza de lavagens iniciado")
    except Exception as e:
        logger.error(f"Erro loop limpeza lavagens: {e}")
    
    try:
        if not verificar_avisos_meta.is_running():
            verificar_avisos_meta.start()
            logger.info("🔔 Loop de avisos de meta iniciado")
    except Exception as e:
        logger.error(f"Erro loop avisos: {e}")

async def limpeza_cache_periodica():
    """Limpa o cache periodicamente."""
    while True:
        try:
            await asyncio.sleep(3600)  # A cada hora
            removidos = await cache.clean_expired()
            if removidos > 0:
                logger.info(f"🧹 Cache limpo: {removidos} entradas removidas")
        except Exception as e:
            logger.error(f"Erro na limpeza de cache: {e}")

async def health_check():
    """Health check para Railway."""
    while True:
        try:
            await asyncio.sleep(60)  # A cada minuto
            # Verifica se o bot está respondendo
            if bot.is_closed():
                logger.warning("⚠️ Bot está fechado! Tentando reconectar...")
                await bot.close()
                await bot.start(TOKEN)
            
            # Verifica conexão com banco
            pool = get_db()
            if not pool or pool._closed:
                logger.warning("⚠️ Pool do banco fechado! Reconectando...")
                await conectar_db()
        except Exception as e:
            logger.error(f"Erro no health check: {e}")

async def carregar_dados_iniciais():
    """Carrega dados iniciais em background."""
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
        logger.info(f"📊 Metas carregadas: {len(metas_cache)}")
    except Exception as e:
        logger.error(f"Erro ao carregar metas: {e}")
    
    # Restaurar produções
    await restaurar_producoes()

async def enviar_paineis_iniciais(guild):
    """Envia todos os painéis iniciais com delays para evitar rate limit."""
    try:
        logger.info("🖥️ Iniciando envio de painéis (com delays)...")
        
        # Lista de funções com delays entre elas
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
        
        # Enviar cada painel com delay
        for i, (nome, func) in enumerate(paineis):
            try:
                logger.info(f"📤 Enviando painel {i+1}/{len(paineis)}: {nome}")
                await func()
                # Delay entre cada painel para evitar rate limit
                if i < len(paineis) - 1:
                    await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"❌ Erro ao enviar painel {nome}: {e}")
                await asyncio.sleep(3)  # Delay maior em caso de erro
        
        # Ações separadas com delay extra
        if guild:
            try:
                logger.info("📤 Enviando painel de ações...")
                await enviar_painel_acoes(guild)
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"❌ Erro ao enviar painel de ações: {e}")
        
        # Forçar atualização do painel de grupos com cuidado
        try:
            logger.info("🔄 Forçando atualização do painel de grupos...")
            await recriar_painel_grupos()
        except Exception as e:
            logger.error(f"❌ Erro ao forçar atualização grupos: {e}")
        
        logger.info("✅ Todos os painéis enviados com sucesso!")
        
    except Exception as e:
        logger.error(f"❌ Erro geral ao enviar painéis: {e}")

async def restaurar_producoes():
    """Restaura produções ativas após reinicialização."""
    try:
        pool = get_db()
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
                logger.info(f"🔄 Produção restaurada: {pid}")
    except Exception as e:
        logger.error(f"❌ Erro ao restaurar produções: {e}")

# =========================================================
# ==================== SEÇÃO 18: START ====================
# =========================================================

if __name__ == "__main__":
    logger.info("🚀 Iniciando bot...")
    
    # Configurar loop de eventos
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    # Executar bot
    try:
        bot.run(TOKEN, reconnect=True)
    except discord.LoginFailure:
        logger.critical("❌ Falha no login! TOKEN inválido?")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"❌ Erro fatal: {e}")
        sys.exit(1)
