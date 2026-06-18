# =========================================================
# ======================== BOT VDR ========================
# =========================================================
# Versão: 2.0 - Reorganizado e Otimizado
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
from discord.ext import commands, tasks
from discord.utils import escape_markdown
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

# =========================================================
# ==================== CONFIGURAÇÕES ======================
# =========================================================

# --- TOKENS E CREDENCIAIS ---
TOKEN = os.environ.get("TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")

# Twitter
API_KEY = os.environ.get("API_KEY")
API_SECRET = os.environ.get("API_SECRET")
ACCESS_TOKEN = os.environ.get("ACCESS_TOKEN")
ACCESS_SECRET = os.environ.get("ACCESS_SECRET")

# --- FUSO HORÁRIO ---
BRASIL = ZoneInfo("America/Sao_Paulo")

# --- GUILD ---
GUILD_ID = 1229526644193099880

# =========================================================
# ==================== FUNÇÕES AUXILIARES =================
# =========================================================

def str_para_datetime_completa(data_str):
    """Converte string ISO para datetime com timezone"""
    if not data_str:
        return None
    if isinstance(data_str, datetime):
        if data_str.tzinfo is None:
            return data_str.replace(tzinfo=BRASIL)
        return data_str
    try:
        # Tenta converter ISO string
        dt = datetime.fromisoformat(data_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=BRASIL)
        return dt
    except:
        return None

def datetime_para_str(dt):
    """Converte datetime para string ISO"""
    if not dt:
        return None
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=BRASIL)
        return dt.isoformat()
    return str(dt)
# =========================================================
# ==================== IDS DOS CANAIS =====================
# =========================================================

# REGISTRO
CANAL_REGISTRO_ID = 1229556030397218878
CANAL_LOG_REGISTRO_ID = 1462457604939841851

# VENDAS
CANAL_CALCULADORA_ID = 1460984821458272347
CANAL_ENCOMENDAS_ID = 1460980984811098294
CANAL_VENDAS_ID = CANAL_CALCULADORA_ID

# PRODUÇÃO
CANAL_FABRICACAO_ID = 1466421612566810634
CANAL_REGISTRO_GALPAO_ID = 1356174712337862819
CANAL_BAU_GALPAO_SUL_ID = 1356174937764794521
CANAL_BAU_GALPAO_ID = 1448561598384963747

# PÓLVORA
CANAL_CALCULO_POLVORA_ID = 1462834441968943157
CANAL_REGISTRO_POLVORA_ID = 1448570795101261846

# LIVES
CANAL_CADASTRO_LIVE_ID = 1466464557215256790
CANAL_DIVULGACAO_LIVE_ID = 1243325102917943335

# LAVAGEM
CANAL_INICIAR_LAVAGEM_ID = 1467152989499293768
CANAL_LAVAGEM_MEMBROS_ID = 1467159346923311216
CANAL_RELATORIO_LAVAGEM_ID = 1467150805273546878

# METAS
CANAL_SOLICITAR_SALA_ID = 1337374500366450741
RESULTADOS_METAS_ID = 1341403574483288125

# AÇÕES
CANAL_ESCALACOES_ID = 1241406819545514064
CANAL_RELATORIO_ACOES_ID = 1477308788531921019

# POSTAGEM X
CANAL_POSTAGEM_X = 1486353689680547900

# AUSÊNCIA
CANAL_BOTAO_AUSENCIA_ID = 1491427870277374162
CANAL_REGISTRO_AUSENCIA_ID = 1313854772545196032

# GERÊNCIA
CANAL_GERENCIA_ID = 1237393478414241854

# CLIPES
CANAL_CLIPES_ID = 1229526645837271134

# FINANCEIRO
CANAL_RELATORIO_FINANCEIRO_ID = 1498664038559776768
CANAL_REGISTRAR_COMPRA_ID = 1498668853465448560
CANAL_COMPRAS_REGISTRADAS_ID = 1270467793363669053

# GRUPOS
CANAL_REGISTRO_GRUPOS_ID = 1516781653194833991  # Sala para registrar grupo
CANAL_GRUPOS_ID = 1448563544386961479  # Canal onde ficam os embeds

# =========================================================
# ==================== IDS DOS CARGOS =====================
# =========================================================

# REGISTRO
AGREGADO_ROLE_ID = 1422847202937536532
CONVIDADO_ROLE_ID = 1337382961456353342
STREAMER_ROLE_ID = 1229797158375653416
LIVE_ROLE_ID = 1481616175476641922

# METAS / LAVAGEM / PONTO
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
CARGO_MECANICO_ID = 1448526080645398641
CARGO_AUSENTE_ID = 1337420032212336823

CARGOS_ACAO = [
    CARGO_GERENTE_ID, CARGO_RESP_ACAO_ID, CARGO_RESP_VENDAS_ID,
    CARGO_RESP_PRODUCAO_ID, CARGO_MEMBRO_ID, CARGO_SOLDADO_ID, CARGO_AGREGADO_ID
]

CARGOS_PERMITIDOS_ESCALACAO = [
    CARGO_AGREGADO_ID, CARGO_MEMBRO_ID, CARGO_SOLDADO_ID,
    CARGO_01_ID, CARGO_02_ID, CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID
]

CARGOS_PERMITIDOS_REMOVER = [CARGO_GERENTE_ID, CARGO_01_ID, CARGO_02_ID, CARGO_GERENTE_GERAL_ID]

# =========================================================
# ==================== CATEGORIAS =========================
# =========================================================

CATEGORIA_META_GERENTE_ID = 1337374002422743122
CATEGORIA_META_RESPONSAVEIS_ID = 1462810826992783422
CATEGORIA_META_SOLDADO_ID = 1461335635519475894
CATEGORIA_META_MEMBRO_ID = 1461335697209163900
CATEGORIA_META_AGREGADO_ID = 1461335748870541323

# =========================================================
# ==================== CONFIGURAÇÕES ======================
# =========================================================

# --- AÇÕES SEMANA ---
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

# --- ORGANIZAÇÕES ---
ORGANIZACOES_CONFIG = {
    "VDR": {"emoji": "🔥", "cor": 0xe74c3c},
    "POLICIA": {"emoji": "🚓", "cor": 0x3498db},
    "EXERCITO": {"emoji": "🪖", "cor": 0x2ecc71},
    "MAFIA": {"emoji": "💀", "cor": 0x8e44ad},
    "CIVIL": {"emoji": "👤", "cor": 0x95a5a6},
}

# --- PREÇOS ---
PRECO_POLVORA = 80
PRECO_EMBALAGEM_POR_UNIDADE = 2000000 / 25000

# --- EMOJIS ---
EMOJI_APROVACAO = "✅"

# --- PATHS ---
BASE_PATH = "/mnt/data"

# =========================================================
# ==================== FUNÇÕES AUXILIARES =================
# =========================================================

def agora():
    """Retorna data/hora atual no horário do Brasil com timezone"""
    return datetime.now(BRASIL)

def agora_db():
    """Retorna data/hora sem fuso horário para salvar no banco (NAIVE)"""
    return datetime.now(BRASIL).replace(tzinfo=None)

def str_para_datetime(data_str):
    """Converte string do banco para datetime com timezone"""
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

def para_db_naive(dt):
    """Converte datetime para naive (sem timezone) para salvar no banco"""
    if dt.tzinfo is not None:
        dt = dt.astimezone(BRASIL)
    return dt.replace(tzinfo=None)

def formatar_dinheiro(valor):
    """Formata valor para reais"""
    try:
        valor = float(valor)
    except:
        valor = 0
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_num(valor):
    """Formata número com separadores de milhar"""
    return f"{valor:,.0f}".replace(",", ".")

def barra(pct, size=20):
    """Gera barra de progresso"""
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

def detectar_plataforma(link):
    """Detecta plataforma de streaming pelo link"""
    link = link.lower()
    if "twitch.tv" in link:
        return "twitch"
    if "kick.com" in link:
        return "kick"
    if "tiktok.com" in link:
        return "tiktok"
    return None

def extrair_canal(link):
    """Extrai nome do canal do link"""
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
    """Verifica se o membro tem permissão para remover ausência"""
    if not member:
        return False
    return any(role.id in CARGOS_PERMITIDOS_REMOVER for role in member.roles)

def obter_categoria_meta(member):
    """Retorna categoria apropriada para meta baseada no cargo"""
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

# =========================================================
# ==================== BANCO DE DADOS =====================
# =========================================================

db = None

async def conectar_db():
    """Conecta ao banco de dados PostgreSQL"""
    global db
    
    if not DATABASE_URL:
        print("❌ DATABASE_URL não encontrada!")
        return
    
    if not db:
        db = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            command_timeout=60
        )
        print("🟢 Pool PostgreSQL conectado!")
    return db

def get_db():
    """Retorna a conexão com o banco de dados"""
    return db

# =========================================================
# ==================== QUERIES DO BANCO ===================
# =========================================================

# --- VENDAS ---
async def proximo_pedido():
    async with get_db().acquire() as conn:
        row = await conn.fetchrow("SELECT ultimo FROM pedidos WHERE id=1")
        if not row:
            await conn.execute("INSERT INTO pedidos (id, ultimo) VALUES (1, 1)")
            return 1
        novo = row["ultimo"] + 1
        await conn.execute("UPDATE pedidos SET ultimo=$1 WHERE id=1", novo)
        return novo

async def salvar_venda_db(vendedor_id, valor, pedido_numero):
    async with get_db().acquire() as conn:
        await conn.execute(
            "INSERT INTO vendas (user_id, valor, data, pedido_numero) VALUES ($1, $2, $3, $4)",
            vendedor_id, valor, agora_db().strftime("%d/%m/%Y"), pedido_numero
        )

async def atualizar_valor_venda_db(pedido_numero, valor):
    async with get_db().acquire() as conn:
        await conn.execute("UPDATE vendas SET valor=$1 WHERE pedido_numero=$2", valor, pedido_numero)

async def carregar_vendas_db():
    async with get_db().acquire() as conn:
        return await conn.fetch("SELECT * FROM vendas")

# --- PRODUÇÃO ---
async def carregar_producao(pid):
    """Carrega produção do banco com tratamento correto de datas"""
    try:
        async with get_db().acquire() as conn:
            r = await conn.fetchrow("SELECT * FROM producoes WHERE pid=$1", pid)

        if not r:
            return None

        # =========================================================
        # ================= CONVERTER DATAS =======================
        # =========================================================
        
        # Converter inicio
        if isinstance(r["inicio"], datetime):
            inicio = r["inicio"].isoformat()
        else:
            inicio = r["inicio"]
        
        # Converter fim
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
        print(f"❌ Erro ao carregar produção {pid}: {e}")
        import traceback
        traceback.print_exc()
        return None

async def salvar_producao(pid, dados):
    """Salva produção no banco com datas no formato correto"""
    
    # =========================================================
    # ================= CONVERTER DATAS =======================
    # =========================================================
    
    # Converter inicio para string ISO se for datetime
    if isinstance(dados["inicio"], datetime):
        inicio_str = dados["inicio"].isoformat()
    else:
        inicio_str = dados["inicio"]
    
    if isinstance(dados["fim"], datetime):
        fim_str = dados["fim"].isoformat()
    else:
        fim_str = dados["fim"]
    
    # Segunda task
    segunda_user = None
    segunda_time = None
    
    if "segunda_task_confirmada" in dados:
        segunda_user = str(dados["segunda_task_confirmada"]["user"])
        segunda_time = dados["segunda_task_confirmada"]["time"]
    
    # Quantidade de galpões
    qtd_galpoes = dados.get("qtd_galpoes", 1)
    polvora_por_galpao = dados.get("polvora_por_galpao", 400)
    
    async with get_db().acquire() as conn:
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
            inicio_str,  # ← USAR STRING
            fim_str,     # ← USAR STRING
            dados.get("obs", ""),
            str(dados["msg_id"]),
            str(dados["canal_id"]),
            segunda_user,
            segunda_time,
            dados.get("polvora", 400),
            qtd_galpoes,
            polvora_por_galpao
        )
        print(f"💾 Produção {pid} salva no banco: inicio={inicio_str}, fim={fim_str}")
async def deletar_producao(pid):
    async with get_db().acquire() as conn:
        await conn.execute("DELETE FROM producoes WHERE pid=$1", pid)

# --- ESTOQUE ---
async def carregar_estoque():
    async with get_db().acquire() as conn:
        rows = await conn.fetch("SELECT tipo, quantidade FROM estoque_municoes")
    estoque = {"PT": 0, "SUB": 0}
    for row in rows:
        estoque[row["tipo"]] = row["quantidade"]
    return estoque

async def atualizar_estoque(tipo, quantidade, operacao="adicionar"):
    async with get_db().acquire() as conn:
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

async def carregar_estoque_insumos():
    async with get_db().acquire() as conn:
        capsulas_row = await conn.fetchrow("SELECT quantidade FROM estoque_capsulas WHERE id = 1")
        capsulas = capsulas_row["quantidade"] if capsulas_row else 0
        embalagens_row = await conn.fetchrow("SELECT quantidade FROM estoque_embalagens WHERE id = 1")
        embalagens = embalagens_row["quantidade"] if embalagens_row else 0
    return {"capsulas": capsulas, "embalagens": embalagens}

async def atualizar_estoque_capsulas(quantidade, operacao="adicionar"):
    async with get_db().acquire() as conn:
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

async def atualizar_estoque_embalagens(quantidade, operacao="adicionar"):
    async with get_db().acquire() as conn:
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

async def registrar_entrada_insumos(tipo, quantidade, registrado_por, obs=""):
    async with get_db().acquire() as conn:
        await conn.execute(
            "INSERT INTO entrada_insumos (tipo, quantidade, registrado_por, obs) VALUES ($1, $2, $3, $4)",
            tipo, quantidade, str(registrado_por), obs
        )
        if tipo == "capsulas":
            await atualizar_estoque_capsulas(quantidade, "adicionar")
        elif tipo == "embalagens":
            await atualizar_estoque_embalagens(quantidade, "adicionar")

async def registrar_saida_estoque(pedido_numero, tipo, pacotes, retirado_por):
    async with get_db().acquire() as conn:
        await conn.execute(
            "INSERT INTO saida_estoque (pedido_numero, tipo, pacotes, retirado_por, data) VALUES ($1, $2, $3, $4, NOW())",
            pedido_numero, tipo, pacotes, str(retirado_por)
        )
        await atualizar_estoque(tipo, pacotes, "remover")

async def verificar_estoque_suficiente(tipo, pacotes_necessarios):
    estoque = await carregar_estoque()
    return estoque.get(tipo, 0) >= pacotes_necessarios

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

async def registrar_producao_municao(tipo, pacotes, produzido_por, obs=""):
    municoes = pacotes * 50
    capsulas_consumidas, embalagens_consumidas = await consumir_insumos_producao(tipo, pacotes)
    
    async with get_db().acquire() as conn:
        await conn.execute(
            "INSERT INTO producao_municao (tipo, pacotes, municoes, produzido_por, obs, capsulas_consumidas, embalagens_consumidas) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            tipo, pacotes, municoes, str(produzido_por), obs, capsulas_consumidas, embalagens_consumidas
        )
        await atualizar_estoque(tipo, pacotes, "adicionar")

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

# --- PÓLVORA ---
async def salvar_polvora_db(user_id, vendedor, qtd, valor):
    async with get_db().acquire() as conn:
        await conn.execute(
            "INSERT INTO polvoras (user_id, vendedor, quantidade, valor, data) VALUES ($1,$2,$3,$4,$5)",
            str(user_id), vendedor, qtd, valor, agora_db()
        )

async def carregar_polvoras_db():
    async with get_db().acquire() as conn:
        return await conn.fetch("SELECT * FROM polvoras")

async def limpar_polvoras_db():
    async with get_db().acquire() as conn:
        await conn.execute("DELETE FROM polvoras")

# --- LAVAGEM ---
async def salvar_lavagem_db(user_id, valor_sujo, taxa, valor_retorno):
    async with get_db().acquire() as conn:
        await conn.execute(
            "INSERT INTO lavagens (user_id, valor, taxa, liquido, data) VALUES ($1,$2,$3,$4,$5)",
            str(user_id), valor_sujo, taxa, valor_retorno, agora_db()
        )

async def carregar_lavagens_db():
    async with get_db().acquire() as conn:
        return await conn.fetch("SELECT * FROM lavagens")

async def limpar_lavagens_db():
    async with get_db().acquire() as conn:
        await conn.execute("DELETE FROM lavagens")

# --- LIVES ---
async def carregar_lives_db():
    async with get_db().acquire() as conn:
        return await conn.fetch("SELECT * FROM lives")

async def salvar_live_db(user_id, link):
    async with get_db().acquire() as conn:
        await conn.execute("INSERT INTO lives (user_id, link, divulgado) VALUES ($1, $2, false)", str(user_id), link)

async def atualizar_divulgado_db(link, valor):
    async with get_db().acquire() as conn:
        await conn.execute("UPDATE lives SET divulgado=$1 WHERE link=$2", valor, link)

async def remover_live_db(user_id):
    async with get_db().acquire() as conn:
        await conn.execute("DELETE FROM lives WHERE user_id=$1", str(user_id))

# --- METAS ---
async def carregar_metas_db():
    async with get_db().acquire() as conn:
        return await conn.fetch("SELECT * FROM metas")

async def salvar_meta_db(user_id, canal_id, dinheiro, polvora, acao):
    async with get_db().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO metas (user_id, canal_id, dinheiro, polvora, acao)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (user_id)
            DO UPDATE SET canal_id=$2, dinheiro=$3, polvora=$4, acao=$5
            """,
            str(user_id), str(canal_id), dinheiro, polvora, acao
        )

async def depositar_na_meta_db(user_id, valor):
    async with get_db().acquire() as conn:
        meta = await conn.fetchrow("SELECT dinheiro FROM metas WHERE user_id = $1", str(user_id))
        if meta:
            novo_valor = meta["dinheiro"] + valor
            await conn.execute("UPDATE metas SET dinheiro = $1 WHERE user_id = $2", novo_valor, str(user_id))
            return True
        return False

# --- AÇÕES ---
async def salvar_acao_db(tipo, autor):
    async with get_db().acquire() as conn:
        return await conn.fetchval(
            "INSERT INTO acoes_semana (tipo, data, autor, status) VALUES ($1, $2, $3, 'aberta') RETURNING id",
            tipo, agora_db(), str(autor)
        )

async def buscar_acoes_semana():
    async with get_db().acquire() as conn:
        return await conn.fetch("""
            SELECT tipo, COUNT(*) as qtd
            FROM acoes_semana
            WHERE status = 'concluida' AND (resultado = 'ganhou' OR resultado = 'perdeu' OR resultado = 'concluida')
            GROUP BY tipo
        """)

async def participar_acao_db(acao_id, user_id):
    async with get_db().acquire() as conn:
        await conn.execute("INSERT INTO participantes_acoes (acao_id, user_id) VALUES ($1, $2)", acao_id, str(user_id))

async def concluir_acao_db(acao_id, resultado, valor=0):
    async with get_db().acquire() as conn:
        await conn.execute(
            "UPDATE acoes_semana SET status='concluida', resultado=$1, valor=$2 WHERE id=$3",
            resultado, valor, acao_id
        )

# --- AUSÊNCIA ---
async def salvar_ausencia_db(user_id, nome, motivo, data_inicio, data_fim):
    async with get_db().acquire() as conn:
        await conn.execute(
            "INSERT INTO ausencias (user_id, nome, motivo, data_inicio, data_fim, ativo) VALUES ($1, $2, $3, $4, $5, true)",
            str(user_id), nome, motivo, data_inicio, data_fim
        )

async def buscar_ausencias_ativas_db():
    async with get_db().acquire() as conn:
        return await conn.fetch("SELECT * FROM ausencias WHERE ativo = true AND data_fim > NOW() ORDER BY data_fim ASC")

async def buscar_ausencia_por_user(user_id):
    async with get_db().acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM ausencias WHERE user_id = $1 AND ativo = true AND data_fim > NOW()",
            str(user_id)
        )

async def desativar_ausencia(user_id):
    async with get_db().acquire() as conn:
        await conn.execute("UPDATE ausencias SET ativo = false WHERE user_id = $1 AND ativo = true", str(user_id))

async def remover_ausencias_expiradas():
    async with get_db().acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM ausencias WHERE ativo = true AND data_fim <= NOW()")
        for row in rows:
            await conn.execute("UPDATE ausencias SET ativo = false WHERE user_id = $1", row["user_id"])
        return [row["user_id"] for row in rows]

# --- ALUGUEL ---
async def salvar_aluguel_db(galpao, user_id, data_fim, dias):
    async with get_db().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO alugueis (galpao, user_id, data_inicio, data_fim, dias, ativo)
            VALUES ($1, $2, $3, $4, $5, true)
            ON CONFLICT (galpao) DO UPDATE SET user_id=$2, data_fim=$4, dias=$5, ativo=true
            """,
            galpao, str(user_id), agora_db(), data_fim, dias
        )

async def carregar_alugueis_db():
    async with get_db().acquire() as conn:
        return await conn.fetch("SELECT * FROM alugueis WHERE ativo = true AND data_fim > NOW()")

async def desativar_aluguel_db(galpao):
    async with get_db().acquire() as conn:
        await conn.execute("UPDATE alugueis SET ativo = false WHERE galpao = $1", galpao)

async def renovar_aluguel_db(galpao, user_id, data_fim, dias):
    async with get_db().acquire() as conn:
        await conn.execute(
            "UPDATE alugueis SET user_id = $2, data_fim = $3, dias = $4, ativo = true WHERE galpao = $1",
            galpao, str(user_id), data_fim, dias
        )

# --- COMPRAS ---
async def salvar_compra_db(produto, valor, comprado_por):
    async with get_db().acquire() as conn:
        await conn.execute(
            "INSERT INTO compras (produto, valor, comprado_por, data) VALUES ($1, $2, $3, $4)",
            produto, valor, str(comprado_por), agora_db()
        )

# --- PAINÉIS ---
async def enviar_ou_atualizar_painel(nome, canal_id, embed, view):
    """Envia ou atualiza um painel no Discord"""
    canal = bot.get_channel(canal_id)
    if not canal:
        print(f"❌ Canal não encontrado para painel: {nome}")
        return
    
    async with get_db().acquire() as conn:
        row = await conn.fetchrow("SELECT mensagem_id, canal_id FROM paineis WHERE nome=$1", nome)
        
        if row:
            try:
                canal_salvo = bot.get_channel(int(row["canal_id"])) or canal
                msg = await canal_salvo.fetch_message(int(row["mensagem_id"]))
                await msg.edit(embed=embed, view=view)
                return
            except Exception as e:
                print(f"⚠️ Erro ao atualizar painel {nome}:", e)
        
        msg = await canal.send(embed=embed, view=view)
        await conn.execute(
            "INSERT INTO paineis (nome, canal_id, mensagem_id) VALUES ($1,$2,$3) ON CONFLICT (nome) DO UPDATE SET canal_id=$2, mensagem_id=$3",
            nome, str(canal_id), str(msg.id)
        )
        
# --- GRUPOS ---

async def salvar_grupo_db(grupo_id, nome_org, lider_nome, lider_telefone, braco_nome, braco_telefone, produto):
    """Salva um novo grupo no banco de dados"""
    async with get_db().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO grupos (
                grupo_id, nome_org, lider_nome, lider_telefone, 
                braco_nome, braco_telefone, produto, data_criacao, ativo
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, true)
            """,
            grupo_id, nome_org, lider_nome, lider_telefone,
            braco_nome, braco_telefone, produto, agora_db()
        )


async def carregar_grupo_db(grupo_id):
    """Carrega um grupo específico do banco"""
    async with get_db().acquire() as conn:
        return await conn.fetchrow(
            "SELECT * FROM grupos WHERE grupo_id = $1 AND ativo = true",
            grupo_id
        )


async def carregar_grupos_db():
    """Carrega todos os grupos ativos"""
    async with get_db().acquire() as conn:
        return await conn.fetch("SELECT * FROM grupos WHERE ativo = true ORDER BY data_criacao DESC")


async def atualizar_grupo_db(grupo_id, nome_org, lider_nome, lider_telefone, braco_nome, braco_telefone, produto):
    """Atualiza os dados de um grupo"""
    async with get_db().acquire() as conn:
        await conn.execute(
            """
            UPDATE grupos SET 
                nome_org = $2, lider_nome = $3, lider_telefone = $4,
                braco_nome = $5, braco_telefone = $6, produto = $7,
                data_atualizacao = $8
            WHERE grupo_id = $1
            """,
            grupo_id, nome_org, lider_nome, lider_telefone,
            braco_nome, braco_telefone, produto, agora_db()
        )


async def desativar_grupo_db(grupo_id):
    """Desativa um grupo (exclusão lógica)"""
    async with get_db().acquire() as conn:
        await conn.execute(
            "UPDATE grupos SET ativo = false, data_exclusao = $1 WHERE grupo_id = $2",
            agora_db(), grupo_id
        )


async def registrar_compra_grupo_db(grupo_id, tipo, quantidade, valor):
    """Registra uma compra feita pelo grupo"""
    async with get_db().acquire() as conn:
        await conn.execute(
            """
            INSERT INTO compras_grupo (grupo_id, tipo, quantidade, valor, data)
            VALUES ($1, $2, $3, $4, $5)
            """,
            grupo_id, tipo, quantidade, valor, agora_db()
        )


async def carregar_compras_grupo_db(grupo_id):
    """Carrega todas as compras de um grupo"""
    async with get_db().acquire() as conn:
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
            compras[tipo] = {
                "quantidade": row["total_quantidade"] or 0,
                "valor": row["total_valor"] or 0
            }
        return compras

async def buscar_grupo_por_organizacao(nome_org):
    """Busca um grupo pelo nome da organização"""
    async with get_db().acquire() as conn:
        return await conn.fetchrow(
            "SELECT grupo_id FROM grupos WHERE LOWER(nome_org) = LOWER($1) AND ativo = true",
            nome_org
        )
        
# =========================================================
# ==================== TWITCH API ==========================
# =========================================================

twitch_token = None
twitch_token_expira = 0

async def obter_token_twitch():
    global twitch_token, twitch_token_expira
    
    agora_ts = time_module.time()
    if twitch_token and agora_ts < twitch_token_expira:
        return twitch_token
    
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type": "client_credentials"
    }
    
    async with http_session.post(url, params=params) as r:
        data = await r.json()
        if "access_token" not in data:
            print("Erro Twitch API:", data)
            return None
        twitch_token = data["access_token"]
        twitch_token_expira = agora_ts + data["expires_in"] - 100
        return twitch_token

async def checar_twitch(canal):
    try:
        token = await obter_token_twitch()
        if not token:
            print(f"❌ Não foi possível obter token Twitch para {canal}")
            return False, None, None, None
        
        headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {token}"
        }
        url = f"https://api.twitch.tv/helix/streams?user_login={canal}"
        
        async with http_session.get(url, headers=headers, timeout=10) as r:
            if r.status != 200:
                print(f"⚠️ Twitch API status {r.status} para {canal}")
                return False, None, None, None
            data = await r.json()
            if data.get("data"):
                info = data["data"][0]
                thumbnail = info["thumbnail_url"].replace("{width}", "1280").replace("{height}", "720")
                return True, info.get("title"), info.get("game_name"), thumbnail
        return False, None, None, None
    except Exception as e:
        print(f"Erro Twitch API para {canal}: {e}")
        return False, None, None, None

async def checar_kick(canal):
    """Verifica se um canal da Kick está ao vivo - CORRIGIDO"""
    try:
        # Primeiro tenta a API oficial
        url_api = f"https://kick.com/api/v2/channels/{canal}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json"
        }
        
        async with http_session.get(url_api, headers=headers, timeout=15) as r:
            if r.status == 200:
                data = await r.json()
                if data.get("livestream"):
                    livestream = data["livestream"]
                    titulo = livestream.get("session_title", f"Live na Kick - {canal}")
                    
                    thumbnail = None
                    if livestream.get("thumbnail"):
                        thumbnail = livestream["thumbnail"].get("url")
                    elif data.get("user", {}).get("profile_pic"):
                        thumbnail = data["user"]["profile_pic"]
                    
                    categoria = data.get("category", {})
                    jogo = categoria.get("name") if categoria else None
                    
                    print(f"✅ Kick API: {canal} está AO VIVO! Título: {titulo[:50]}")
                    return True, titulo, jogo, thumbnail
                else:
                    print(f"📴 Kick API: {canal} está OFFLINE")
                    # Continua para fallback HTML
            else:
                print(f"⚠️ Kick API retornou status {r.status}, tentando fallback HTML")
        
        # Fallback: scraping do HTML
        url_page = f"https://kick.com/{canal}"
        headers_html = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        }
        
        async with http_session.get(url_page, headers=headers_html, timeout=15) as r:
            if r.status == 200:
                html = await r.text()
                
                # Procura por indicadores de live
                if "isLive:true" in html or '"livestream":{' in html:
                    # Tenta extrair título
                    titulo_match = re.search(r'"sessionTitle":"([^"]+)"', html)
                    titulo = titulo_match.group(1) if titulo_match else f"Live na Kick - {canal}"
                    
                    # Tenta extrair thumbnail
                    thumb_match = re.search(r'"thumbnail":"([^"]+)"', html)
                    thumbnail = thumb_match.group(1) if thumb_match else None
                    
                    # Tenta extrair jogo
                    jogo_match = re.search(r'"category":"([^"]+)"', html)
                    jogo = jogo_match.group(1) if jogo_match else None
                    
                    print(f"✅ Kick HTML: {canal} está AO VIVO! Título: {titulo[:50]}")
                    return True, titulo, jogo, thumbnail
                else:
                    print(f"📴 Kick HTML: {canal} está OFFLINE")
                    return False, None, None, None
            else:
                print(f"❌ Kick HTML: status {r.status} para {canal}")
                return False, None, None, None
                
    except asyncio.TimeoutError:
        print(f"⏰ Timeout ao verificar Kick: {canal}")
        return False, None, None, None
    except Exception as e:
        print(f"❌ Erro ao verificar Kick para {canal}: {e}")
        import traceback
        traceback.print_exc()
        return False, None, None, None  # <--- ESSA LINHA ESTAVA FALTANDO!

async def checar_tiktok(username):
    try:
        username = username.lower().replace("@", "").strip()
        url = f"https://www.tiktok.com/@{username}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        }
        
        async with http_session.get(url, headers=headers, timeout=10) as response:
            if response.status != 200:
                print(f"⚠️ TikTok status {response.status} para {username}")
                return False, None, None, None
            html = await response.text()
        
        import re
        if re.search(r'"isLive":\s*true', html, re.IGNORECASE):
            titulo_match = re.search(r'"title":"([^"]+)"', html)
            titulo = titulo_match.group(1) if titulo_match else f"Live no TikTok - @{username}"
            return True, titulo, "TikTok", None
        
        return False, None, None, None
    except Exception as e:
        print(f"Erro TikTok para {username}: {e}")
        return False, None, None, None

# =========================================================
# ==================== CACHE DE USUÁRIOS ===================
# =========================================================

user_cache = {}

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
# ==================== FILA DE EDIÇÃO =====================
# =========================================================

edit_queue = asyncio.Queue()

async def edit_worker():
    while True:
        coro = await edit_queue.get()
        try:
            await coro
            await asyncio.sleep(1.2)
        except discord.NotFound:
            pass
        except discord.HTTPException as e:
            if e.status == 429:
                await asyncio.sleep(3)
            else:
                print("Erro HTTP edit_worker:", e)
        except Exception as e:
            print("Erro no edit_worker:", e)
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
        print("Erro responder_interacao:", e)

# =========================================================
# ==================== BOT ================================
# =========================================================

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.guilds = True
intents.presences = True
intents.reactions = True

bot = commands.Bot(command_prefix="!", intents=intents)

# =========================================================
# ==================== VARIÁVEIS GLOBAIS ===================
# =========================================================

http_session = None
metas_cache = {}
alugueis_ativos = {}
producoes_tasks = {}
galpoes_ativos = set()
lavagens_pendentes = {}
clips_postados = set()
fila_clipes = None

# GRUPOS - CACHE
cache_grupos = None
cache_grupos_timestamp = 0

# GRUPOS - CONTROLE DE RATE LIMIT
ultima_atualizacao_grupo = 0
fila_atualizacao_grupos = asyncio.Queue()
task_atualizacao_grupos = None

# =========================================================
# ============ CLASSE REGISTRO (MODAL E VIEWS) ============
# =========================================================

class RegistroModal(discord.ui.Modal, title="Registro de Entrada"):
    nome = discord.ui.TextInput(label="Nome Completo")
    passaporte = discord.ui.TextInput(label="Passaporte")
    indicado = discord.ui.TextInput(label="Indicado por", required=False)
    telefone = discord.ui.TextInput(label="Telefone In Game")
    
    async def on_submit(self, interaction: discord.Interaction):
        membro = interaction.user
        guild = interaction.guild
        
        await membro.edit(nick=f"{self.passaporte.value} - {self.nome.value}")
        
        view = TipoRegistroView(self.nome.value, self.passaporte.value, self.indicado.value, self.telefone.value)
        await interaction.response.send_message("Selecione o tipo de entrada:", view=view, ephemeral=True)


class TipoRegistroSelect(discord.ui.Select):
    def __init__(self, nome, passaporte, indicado, telefone):
        self.nome = nome
        self.passaporte = passaporte
        self.indicado = indicado
        self.telefone = telefone
        
        options = [
            discord.SelectOption(label="Agregado", description="Se for se tornar membro da fac", emoji="🕴️"),
            discord.SelectOption(label="Amigos", description="Se está apenas para resenha", emoji="🤝")
        ]
        super().__init__(placeholder="Escolha o tipo de acesso", min_values=1, max_values=1, options=options)
    
    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        membro = interaction.user
        
        agregado = guild.get_role(AGREGADO_ROLE_ID)
        amigos = guild.get_role(1309121290241704046)
        convidado = guild.get_role(CONVIDADO_ROLE_ID)
        
        escolha = self.values[0]
        
        if escolha == "Agregado" and agregado:
            await membro.add_roles(agregado)
        if escolha == "Amigos" and amigos:
            await membro.add_roles(amigos)
        if convidado:
            await membro.remove_roles(convidado)
        
        canal_log = guild.get_channel(CANAL_LOG_REGISTRO_ID)
        if canal_log:
            embed = discord.Embed(title="📋 Novo Registro", color=0x2ecc71)
            embed.add_field(name="Nome", value=self.nome)
            embed.add_field(name="Passaporte", value=self.passaporte)
            embed.add_field(name="Indicado por", value=self.indicado)
            embed.add_field(name="Telefone", value=self.telefone)
            embed.add_field(name="Tipo", value=escolha)
            embed.add_field(name="Usuário", value=membro.mention)
            await canal_log.send(embed=embed)
        
        await interaction.response.send_message(f"✅ Registro concluído como **{escolha}**", ephemeral=True)


class TipoRegistroView(discord.ui.View):
    def __init__(self, nome, passaporte, indicado, telefone):
        super().__init__(timeout=300)
        self.add_item(TipoRegistroSelect(nome, passaporte, indicado, telefone))


class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📋 Fazer Registro", style=discord.ButtonStyle.success, custom_id="registro_fazer")
    async def registro(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistroModal())


# =========================================================
# ==================== FUNÇÕES DE SAÍDA ====================
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

@bot.event
async def on_member_remove(member):
    """Quando um membro sai do servidor, envia mensagem no DM e log na gerência"""
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
        print(f"✅ [SAÍDA] DM enviada para {member.name} (ID: {member.id})")
        
    except discord.Forbidden:
        status_dm = "❌ **MENSAGEM NÃO ENVIADA**\nMotivo: Usuário bloqueou o bot ou tem DM fechada"
        dm_sucesso = False
        cor_log = 0xf1c40f
        print(f"❌ [SAÍDA] DM bloqueada para {member.name}")
    except discord.HTTPException as e:
        status_dm = f"❌ **MENSAGEM NÃO ENVIADA**\nMotivo: Erro HTTP - {e}"
        dm_sucesso = False
        cor_log = 0xf1c40f
        print(f"❌ [SAÍDA] Erro HTTP para {member.name}: {e}")
    except Exception as e:
        status_dm = f"❌ **MENSAGEM NÃO ENVIADA**\nMotivo: Erro inesperado - {str(e)[:100]}"
        dm_sucesso = False
        cor_log = 0xf1c40f
        print(f"❌ [SAÍDA] Erro para {member.name}: {e}")
    
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
        print(f"📋 [SAÍDA] Log enviado para gerência: {member.name} (DM: {'OK' if dm_sucesso else 'FALHA'})")


# =========================================================
# ==================== EVENTO DE REAÇÃO (CLIPES) ==========
# =========================================================

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
        print("Erro reação clip:", e)


# =========================================================
# ==================== EVENTO DE MENSAGEM (LAVAGEM) =======
# =========================================================

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return
    
    # LAVAGEM - captura print
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
            return
    
    await bot.process_commands(message)


# =========================================================
# ==================== EVENTO DE MEMBRO (METAS) ===========
# =========================================================

@bot.event
async def on_member_update(before, after):
    if after.bot:
        return
    
    tinha_agregado = any(r.id == AGREGADO_ROLE_ID for r in before.roles)
    tem_agregado = any(r.id == AGREGADO_ROLE_ID for r in after.roles)
    
    if not tinha_agregado and tem_agregado:
        await asyncio.sleep(2)
        if str(after.id) not in metas_cache:
            await criar_sala_meta(after)
        return
    
    if str(after.id) in metas_cache:
        await atualizar_categoria_meta(after)


@bot.event
async def on_member_join(member):
    if member.bot:
        return
    
    if any(r.id == AGREGADO_ROLE_ID for r in member.roles):
        await asyncio.sleep(2)
        if str(member.id) not in metas_cache:
            await criar_sala_meta(member)


@bot.event
async def on_guild_channel_delete(channel):
    for uid, dados in list(metas_cache.items()):
        if dados["canal_id"] == channel.id:
            metas_cache.pop(uid)
            try:
                async with get_db().acquire() as conn:
                    await conn.execute("DELETE FROM metas WHERE user_id=$1", uid)
            except Exception as e:
                print("Erro removendo meta:", e)
            break


# =========================================================
# ==================== SISTEMA DE METAS ====================
# =========================================================

async def criar_sala_meta(member: discord.Member):
    guild = member.guild
    nome_canal = f"📁-{member.display_name}".lower()
    
    for canal in guild.text_channels:
        if canal.name == nome_canal:
            return canal
    
    canal_encontrado = None
    for cat in guild.categories:
        if not cat.name.lower().startswith("metas"):
            continue
        for c in cat.channels:
            if not isinstance(c, discord.TextChannel):
                continue
            if c.overwrites_for(member).view_channel:
                canal_encontrado = c
                break
        if canal_encontrado:
            break
    
    if canal_encontrado:
        await salvar_meta_db(member.id, canal_encontrado.id, 0, 0, 0)
        return canal_encontrado
    
    categoria_id = obter_categoria_meta(member)
    if not categoria_id:
        return
    
    categoria = guild.get_channel(categoria_id)
    if not categoria:
        return
    
    nick = member.display_name.lower().replace(" ", "-")
    nome_canal = f"📁・{nick}"
    
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        member: discord.PermissionOverwrite(view_channel=True, send_messages=True),
    }
    
    gerente = guild.get_role(CARGO_GERENTE_ID)
    if gerente:
        overwrites[gerente] = discord.PermissionOverwrite(view_channel=True)
    
    canal = await guild.create_text_channel(nome_canal, category=categoria, overwrites=overwrites)
    await salvar_meta_db(member.id, canal.id, 0, 0, 0)
    print(f"📊 Sala criada para {member.display_name}")
    return canal


async def atualizar_categoria_meta(member):
    if str(member.id) not in metas_cache:
        return
    
    canal = member.guild.get_channel(metas_cache[str(member.id)]["canal_id"])
    if not canal:
        return
    
    nova = obter_categoria_meta(member)
    if nova and canal.category_id != nova:
        await canal.edit(category=member.guild.get_channel(nova))


async def depositar_na_meta(user_id, valor, motivo):
    """Deposita dinheiro na meta do usuário"""
    async with get_db().acquire() as conn:
        meta = await conn.fetchrow("SELECT dinheiro FROM metas WHERE user_id = $1", str(user_id))
        
        if meta:
            novo_valor = meta["dinheiro"] + valor
            await conn.execute("UPDATE metas SET dinheiro = $1 WHERE user_id = $2", novo_valor, str(user_id))
            
            canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(user_id))
            if canal_id:
                canal = bot.get_channel(int(canal_id))
                if canal:
                    valor_fmt = formatar_dinheiro(valor)
                    await canal.send(
                        f"💰 **Depósito recebido!**\n"
                        f"📝 Motivo: {motivo}\n"
                        f"💵 Valor: R$ {valor_fmt}\n"
                        f"✨ **Saldo atualizado na sua meta!**"
                    )
            return True
        else:
            print(f"⚠️ Usuário {user_id} não tem meta, criando...")
            guild = bot.get_guild(GUILD_ID)
            member = guild.get_member(int(user_id))
            if member:
                canal = await criar_sala_meta(member)
                if canal:
                    await conn.execute("UPDATE metas SET dinheiro = $1 WHERE user_id = $2", valor, str(user_id))
                    return True
            return False


# =========================================================
# ==================== SISTEMA DE VENDAS ===================
# =========================================================

class StatusView(discord.ui.View):
    def __init__(self, disabled: bool = False):
        super().__init__(timeout=None)
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
    
    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="status_pago")
    async def pago(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)
        
        if self.pedido_cancelado(linhas):
            await interaction.response.send_message("⚠️ Este pedido foi cancelado e não pode ser pago.", ephemeral=True)
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
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)
        
        if self.pedido_cancelado(linhas):
            await interaction.response.send_message("⚠️ Este pedido foi cancelado.", ephemeral=True)
            return
        
        # Verificar estoque
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
                await interaction.response.send_message(
                    f"❌ **ESTOQUE INSUFICIENTE!**\n\n🔫 PT: {pacotes_pt} pacotes necessários\n📦 Estoque atual: {estoque_atual['PT']} pacotes",
                    ephemeral=True
                )
                return
        
        if pacotes_sub > 0:
            estoque_suficiente = await verificar_estoque_suficiente("SUB", pacotes_sub)
            if not estoque_suficiente:
                estoque_atual = await carregar_estoque()
                await interaction.response.send_message(
                    f"❌ **ESTOQUE INSUFICIENTE!**\n\n🔫 SUB: {pacotes_sub} pacotes necessários\n📦 Estoque atual: {estoque_atual['SUB']} pacotes",
                    ephemeral=True
                )
                return
        
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
                    print("Erro envio baú:", e)
    
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
        
        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        user = interaction.user.mention
        linhas = [f"❌ Pedido cancelado por {user} • {agora_str}"]
        embed = self.set_status(embed, idx, linhas)
        await interaction.message.edit(embed=embed, view=StatusView(disabled=True))
        await responder_interacao(interaction, defer=True)
    
    @discord.ui.button(label="✏️ Editar Venda", style=discord.ButtonStyle.secondary, custom_id="status_editar_venda")
    async def editar(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)
        if self.pedido_cancelado(linhas):
            await interaction.response.send_message("⚠️ Pedido cancelado não pode ser editado.", ephemeral=True)
            return
        await interaction.response.send_modal(EditarVendaModal(interaction.message))


class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(
        label="Organização",
        placeholder="Digite o nome da organização (ex: VDR, POLICIA)",
        required=True
    )
    qtd_pt = discord.ui.TextInput(
        label="Quantidade PT",
        placeholder="Digite a quantidade de munição PT"
    )
    qtd_sub = discord.ui.TextInput(
        label="Quantidade SUB",
        placeholder="Digite a quantidade de munição SUB"
    )
    observacoes = discord.ui.TextInput(
        label="Observações",
        style=discord.TextStyle.paragraph,
        required=False,
        placeholder="Observações adicionais sobre a venda"
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            pt = int(self.qtd_pt.value.strip())
            sub = int(self.qtd_sub.value.strip())
        except:
            await interaction.response.send_message(
                "❌ Valores inválidos. Digite apenas números.",
                ephemeral=True
            )
            return
        
        # =========================================================
        # ================= CALCULAR VALORES ======================
        # =========================================================
        
        numero_pedido = await proximo_pedido()
        
        # Calcular pacotes e munições
        pacotes_pt = pt // 50
        pacotes_sub = sub // 50
        
        # Calcular total (PT: R$50 por munição, SUB: R$90 por munição)
        total = (pt * 50) + (sub * 90)
        valor_novo = total
        
        # =========================================================
        # ================= SALVAR NO BANCO =======================
        # =========================================================
        
        await salvar_venda_db(
            str(interaction.user.id),
            total,
            numero_pedido
        )
        
        valor_formatado = (
            f"{total:,.2f}"
            .replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )
        
        org_nome = self.organizacao.value.strip().upper()
        config = ORGANIZACOES_CONFIG.get(
            org_nome,
            {"emoji": "🏷️", "cor": 0x1e3a8a}
        )
        
        # =========================================================
        # ================= INTEGRAÇÃO COM GRUPOS =================
        # =========================================================
        
        # Buscar grupo pela organização
        grupo = await buscar_grupo_por_organizacao(org_nome)
        
        if grupo:
            # Registrar compra no grupo (PT e SUB)
            if pacotes_pt > 0:
                valor_pt = pacotes_pt * 50  # Preço por pacote PT (R$50)
                await registrar_compra_grupo_db(grupo["grupo_id"], "PT", pacotes_pt, valor_pt)
                print(f"📦 PT registrado no grupo {org_nome}: {pacotes_pt} pacotes")
            
            if pacotes_sub > 0:
                valor_sub = pacotes_sub * 90  # Preço por pacote SUB (R$90)
                await registrar_compra_grupo_db(grupo["grupo_id"], "SUB", pacotes_sub, valor_sub)
                print(f"📦 SUB registrado no grupo {org_nome}: {pacotes_sub} pacotes")
            
            # Atualizar o embed do grupo
            await enviar_embed_grupo(grupo["grupo_id"])
            
            print(f"✅ Venda integrada com grupo: {org_nome}")
        
        # =========================================================
        # ================= CRIAR EMBED DA VENDA ==================
        # =========================================================
        
        embed = discord.Embed(
            title=f"📦 NOVA ENCOMENDA • Pedido #{numero_pedido:04d}",
            color=config["cor"]
        )
        
        embed.add_field(
            name="👤 Vendedor",
            value=interaction.user.mention,
            inline=False
        )
        
        embed.add_field(
            name="🏷 Organização",
            value=f"{config['emoji']} {org_nome}",
            inline=False
        )
        
        embed.add_field(
            name="🔫 PT",
            value=f"{pt} munições\n📦 {pacotes_pt} pacotes",
            inline=True
        )
        
        embed.add_field(
            name="🔫 SUB",
            value=f"{sub} munições\n📦 {pacotes_sub} pacotes",
            inline=True
        )
        
        embed.add_field(
            name="💰 Total",
            value=f"**R$ {valor_formatado}**",
            inline=False
        )
        
        embed.add_field(
            name="📌 Status",
            value="📦 A Entregar\n⏳ Pagamento pendente",
            inline=False
        )
        
        if self.observacoes.value:
            embed.add_field(
                name="📝 Observações",
                value=self.observacoes.value,
                inline=False
            )
        
        # Adicionar informação de integração com grupo
        if grupo:
            embed.add_field(
                name="📊 INTEGRAÇÃO COM GRUPO",
                value=f"✅ Compra registrada automaticamente no grupo **{org_nome}**",
                inline=False
            )
        
        embed.set_footer(
            text="🛡 Sistema de Encomendas • VDR 442"
        )
        
        # =========================================================
        # ================= ENVIAR MENSAGEM =======================
        # =========================================================
        
        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        
        if not canal:
            await interaction.response.send_message(
                "❌ Canal de encomendas não encontrado!",
                ephemeral=True
            )
            return
        
        await canal.send(
            embed=embed,
            view=StatusView()
        )
        
        # =========================================================
        # ================= RESPOSTA AO USUÁRIO ===================
        # =========================================================
        
        msg_resposta = f"✅ **Venda registrada com sucesso!**\n\n"
        msg_resposta += f"📦 **Pedido #{numero_pedido:04d}**\n"
        msg_resposta += f"🏷 **Organização:** {org_nome}\n"
        msg_resposta += f"🔫 **PT:** {pt} munições ({pacotes_pt} pacotes)\n"
        msg_resposta += f"🔫 **SUB:** {sub} munições ({pacotes_sub} pacotes)\n"
        msg_resposta += f"💰 **Total:** R$ {valor_formatado}\n\n"
        
        if grupo:
            msg_resposta += f"📊 **Grupo integrado:** ✅ {org_nome}\n"
        else:
            msg_resposta += f"📊 **Grupo integrado:** ❌ Nenhum grupo encontrado para {org_nome}\n"
            msg_resposta += f"💡 Para integrar, cadastre um grupo com o nome **{org_nome}**"
        
        await interaction.response.send_message(
            msg_resposta,
            ephemeral=True
        )


class EditarVendaModal(discord.ui.Modal, title="✏️ Editar Venda"):
    qtd_pt = discord.ui.TextInput(
        label="Nova Quantidade PT",
        placeholder="Digite a nova quantidade de PT"
    )
    qtd_sub = discord.ui.TextInput(
        label="Nova Quantidade SUB",
        placeholder="Digite a nova quantidade de SUB"
    )
    organizacao = discord.ui.TextInput(
        label="Nova Organização (opcional)",
        placeholder="Digite o novo nome da organização",
        required=False
    )
    observacao = discord.ui.TextInput(
        label="Nova Observação (opcional)",
        style=discord.TextStyle.paragraph,
        required=False,
        placeholder="Novas observações para a venda"
    )
    
    def __init__(self, message):
        super().__init__()
        self.message = message
    
    async def on_submit(self, interaction: discord.Interaction):
        # =========================================================
        # ================= VALIDAR VALORES =======================
        # =========================================================
        
        try:
            pt = int(self.qtd_pt.value.strip())
            sub = int(self.qtd_sub.value.strip())
        except:
            await interaction.response.send_message(
                "❌ Valores inválidos. Digite apenas números.",
                ephemeral=True
            )
            return
        
        # =========================================================
        # ================= CALCULAR NOVOS VALORES ================
        # =========================================================
        
        pacotes_pt = pt // 50
        pacotes_sub = sub // 50
        total = (pt * 50) + (sub * 90)
        valor_novo = total
        
        valor_formatado = (
            f"{total:,.2f}"
            .replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )
        
        embed = self.message.embeds[0]
        
        # =========================================================
        # ================= EXTRAIR VALORES ANTIGOS ===============
        # =========================================================
        
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
                    valor_antigo = float(
                        field.value
                        .replace("**R$ ", "")
                        .replace("**", "")
                        .replace(".", "")
                        .replace(",", ".")
                    )
                except:
                    pass
            
            if field.name == "🏷 Organização":
                organizacao_antiga = field.value
        
        pacotes_pt_antigo = pt_antigo // 50
        pacotes_sub_antigo = sub_antigo // 50
        
        # =========================================================
        # ================= ATUALIZAR EMBED =======================
        # =========================================================
        
        for i, field in enumerate(embed.fields):
            if field.name == "🔫 PT":
                embed.set_field_at(
                    i,
                    name="🔫 PT",
                    value=f"{pt} munições\n📦 {pacotes_pt} pacotes",
                    inline=True
                )
            
            if field.name == "🔫 SUB":
                embed.set_field_at(
                    i,
                    name="🔫 SUB",
                    value=f"{sub} munições\n📦 {pacotes_sub} pacotes",
                    inline=True
                )
            
            if field.name == "💰 Total":
                embed.set_field_at(
                    i,
                    name="💰 Total",
                    value=f"**R$ {valor_formatado}**",
                    inline=False
                )
            
            if field.name == "🏷 Organização" and self.organizacao.value:
                embed.set_field_at(
                    i,
                    name="🏷 Organização",
                    value=self.organizacao.value.strip(),
                    inline=False
                )
            
            if field.name == "📝 Observações" and self.observacao.value:
                embed.set_field_at(
                    i,
                    name="📝 Observações",
                    value=self.observacao.value.strip(),
                    inline=False
                )
        
        titulo = embed.title
        pedido_numero = int(titulo.split("#")[1])
        
        # =========================================================
        # ================= ATUALIZAR BANCO =======================
        # =========================================================
        
        await atualizar_valor_venda_db(pedido_numero, total)
        await self.message.edit(embed=embed)
        
        # =========================================================
        # ================= DETECTAR ALTERAÇÕES ===================
        # =========================================================
        
        alteracoes = []
        
        if pt_antigo != pt:
            alteracoes.append(f"PT: {pt_antigo} → {pt}")
        
        if sub_antigo != sub:
            alteracoes.append(f"SUB: {sub_antigo} → {sub}")
        
        if valor_antigo != valor_novo:
            valor_antigo_fmt = (
                f"{valor_antigo:,.2f}"
                .replace(",", "X")
                .replace(".", ",")
                .replace("X", ".")
            )
            alteracoes.append(
                f"Valor: R$ {valor_antigo_fmt} → R$ {valor_formatado}"
            )
        
        if self.organizacao.value:
            alteracoes.append("Organização alterada")
        
        if self.observacao.value:
            alteracoes.append("Observação alterada")
        
        alteracao_texto = (
            "\n".join(alteracoes)
            if alteracoes
            else "Nenhuma alteração detectada"
        )
        
        # =========================================================
        # ================= INTEGRAÇÃO COM GRUPOS =================
        # =========================================================
        
        # Organização final (nova ou antiga)
        org_nome_final = self.organizacao.value.strip().upper() if self.organizacao.value else organizacao_antiga
        
        if org_nome_final:
            grupo = await buscar_grupo_por_organizacao(org_nome_final)
            
            if grupo:
                # Calcular diferença de quantidades
                diff_pt = pacotes_pt - pacotes_pt_antigo
                diff_sub = pacotes_sub - pacotes_sub_antigo
                
                if diff_pt > 0:
                    valor_pt = diff_pt * 50
                    await registrar_compra_grupo_db(grupo["grupo_id"], "PT", diff_pt, valor_pt)
                    print(f"📦 PT ajustado no grupo {org_nome_final}: +{diff_pt} pacotes")
                elif diff_pt < 0:
                    # Remover (registrar negativo)
                    valor_pt = abs(diff_pt) * 50
                    await registrar_compra_grupo_db(grupo["grupo_id"], "PT", diff_pt, -valor_pt)
                    print(f"📦 PT ajustado no grupo {org_nome_final}: {diff_pt} pacotes")
                
                if diff_sub > 0:
                    valor_sub = diff_sub * 90
                    await registrar_compra_grupo_db(grupo["grupo_id"], "SUB", diff_sub, valor_sub)
                    print(f"📦 SUB ajustado no grupo {org_nome_final}: +{diff_sub} pacotes")
                elif diff_sub < 0:
                    valor_sub = abs(diff_sub) * 90
                    await registrar_compra_grupo_db(grupo["grupo_id"], "SUB", diff_sub, -valor_sub)
                    print(f"📦 SUB ajustado no grupo {org_nome_final}: {diff_sub} pacotes")
                
                # Atualizar o embed do grupo
                await enviar_embed_grupo(grupo["grupo_id"])
                
                print(f"✅ Venda editada integrada com grupo: {org_nome_final}")
        
        # =========================================================
        # ================= LOG DE ALTERAÇÕES =====================
        # =========================================================
        
        canal_log = interaction.guild.get_channel(1478381934026424391)
        
        if canal_log:
            embed_log = discord.Embed(
                title="✏️ Venda Editada",
                color=0xf1c40f
            )
            
            embed_log.add_field(
                name="👤 Editado por",
                value=interaction.user.mention,
                inline=False
            )
            
            embed_log.add_field(
                name="🧾 Pedido",
                value=embed.title,
                inline=False
            )
            
            embed_log.add_field(
                name="🔧 Alterações",
                value=alteracao_texto,
                inline=False
            )
            
            await canal_log.send(embed=embed_log)
        
        # =========================================================
        # ================= RESPOSTA AO USUÁRIO ===================
        # =========================================================
        
        msg_resposta = "✅ **Venda editada com sucesso!**\n\n"
        msg_resposta += f"📦 **Pedido #{pedido_numero:04d}**\n"
        msg_resposta += f"🔫 **PT:** {pt} munições ({pacotes_pt} pacotes)\n"
        msg_resposta += f"🔫 **SUB:** {sub} munições ({pacotes_sub} pacotes)\n"
        msg_resposta += f"💰 **Total:** R$ {valor_formatado}\n\n"
        
        if grupo:
            msg_resposta += f"📊 **Grupo integrado:** ✅ {org_nome_final}\n"
            msg_resposta += f"📦 **Ajustes registrados no grupo**"
        
        await interaction.response.send_message(
            msg_resposta,
            ephemeral=True
        )


class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="Registrar Venda", style=discord.ButtonStyle.primary, custom_id="calc_registrar_venda")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(VendaModal())
    
    @discord.ui.button(label="Relatório", style=discord.ButtonStyle.success, custom_id="calc_relatorio_vendas")
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RelatorioModal())


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
            await interaction.followup.send("Formato inválido. Use **DD/MM/AAAA**", ephemeral=True)
            return
        
        async with get_db().acquire() as conn:
            rows = await conn.fetch(
                "SELECT user_id, SUM(valor) as total FROM vendas WHERE TO_DATE(data, 'DD/MM/YYYY') BETWEEN $1 AND $2 GROUP BY user_id",
                inicio, fim
            )
        
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
        await interaction.followup.send("Relatório enviado no canal de vendas.", ephemeral=True)


# =========================================================
# ==================== SISTEMA DE PRODUÇÃO =================
# =========================================================

async def gerar_desc_producao(prod, pct=None, restante=None):
    """Gera a descrição da produção para o embed - VERSÃO CORRIGIDA"""
    try:
        # =========================================================
        # ================= CONVERTER DATAS =======================
        # =========================================================
        
        # Converter inicio
        if isinstance(prod["inicio"], str):
            inicio = str_para_datetime_completa(prod["inicio"])
        else:
            inicio = prod["inicio"]
            if isinstance(inicio, datetime) and inicio.tzinfo is None:
                inicio = inicio.replace(tzinfo=BRASIL)
        
        # Converter fim
        if isinstance(prod["fim"], str):
            fim = str_para_datetime_completa(prod["fim"])
        else:
            fim = prod["fim"]
            if isinstance(fim, datetime) and fim.tzinfo is None:
                fim = fim.replace(tzinfo=BRASIL)
        
        if not inicio or not fim:
            print(f"❌ Datas inválidas: inicio={prod['inicio']}, fim={prod['fim']}")
            return f"**Galpão:** {prod.get('galpao', 'Desconhecido')}\n⏳ **Aguardando dados...**"
        
        agora_dt = agora()
        
        # =========================================================
        # ================= CALCULAR PROGRESSO ====================
        # =========================================================
        
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
        
        # =========================================================
        # ================= MONTAR DESCRIÇÃO ======================
        # =========================================================
        
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
        print(f"❌ Erro ao gerar descrição: {e}")
        import traceback
        traceback.print_exc()
        return f"**Galpão:** {prod.get('galpao', 'Desconhecido')}\n⏳ **Erro ao carregar dados...**"
        
async def acompanhar_producao(pid):
    """Acompanha a produção com verificação robusta - VERSÃO CORRIGIDA"""
    print(f"▶ Produção iniciada/restaurada: {pid}")
    msg = None
    ultimo_pct = -1
    falhas_consecutivas = 0
    
    while True:
        try:
            # =========================================================
            # ================= CARREGAR PRODUÇÃO ======================
            # =========================================================
            
            prod = await carregar_producao(pid)
            if not prod:
                print(f"❌ Produção {pid} não encontrada no banco")
                return
            
            print(f"📊 Dados carregados: inicio={prod['inicio']}, fim={prod['fim']}")
            
            # Converter datas
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
                print(f"❌ Datas inválidas para {pid}")
                await asyncio.sleep(10)
                continue
            
            agora_dt = agora()
            
            # =========================================================
            # ================= VERIFICAR SE EXPIR0U ==================
            # =========================================================
            
            if agora_dt >= fim:
                print(f"⏰ Produção {pid} expirou, finalizando...")
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
            
            # =========================================================
            # ================= BUSCAR MENSAGEM =======================
            # =========================================================
            
            canal = bot.get_channel(prod["canal_id"])
            if not canal:
                print(f"❌ Canal não encontrado para {pid}")
                await asyncio.sleep(10)
                continue
            
            if msg is None:
                try:
                    msg = await canal.fetch_message(prod["msg_id"])
                    print(f"✅ Mensagem encontrada para {pid}")
                    falhas_consecutivas = 0
                except discord.NotFound:
                    print(f"⚠️ Mensagem {pid} não encontrada, recriando...")
                    desc = await gerar_desc_producao(prod)
                    embed = discord.Embed(title="🏭 Produção", description=desc, color=0x3498db)
                    view = None if prod.get("segunda_task_confirmada") else SegundaTaskView(pid)
                    msg = await canal.send(embed=embed, view=view)
                    prod["msg_id"] = msg.id
                    await salvar_producao(pid, prod)
                    print(f"✅ Mensagem recriada para {pid}")
                    falhas_consecutivas = 0
                except Exception as e:
                    print(f"⚠️ Erro ao buscar mensagem {pid}: {e}")
                    falhas_consecutivas += 1
                    await asyncio.sleep(5)
                    continue
            
            # =========================================================
            # ================= ATUALIZAR PROGRESSO ===================
            # =========================================================
            
            if msg:
                total = (fim - inicio).total_seconds()
                restante = (fim - agora_dt).total_seconds()
                restante = max(0, restante)
                
                if total <= 0:
                    total = 1
                
                pct = 1 - (restante / total)
                pct = max(0, min(1, pct))
                
                pct_int = int(pct * 100)
                
                # Atualizar a cada 1% ou a cada 10 segundos
                if pct_int != ultimo_pct or pct_int % 5 == 0:
                    ultimo_pct = pct_int
                    desc = await gerar_desc_producao(prod, pct, restante)
                    
                    try:
                        await msg.edit(embed=discord.Embed(title="🏭 Produção", description=desc, color=0x34495e))
                        print(f"🔄 Produção {pid}: {pct_int}% | Restante: {int(restante//60)}m {int(restante%60)}s")
                        falhas_consecutivas = 0
                    except discord.NotFound:
                        print(f"⚠️ Mensagem {pid} deletada, recriando...")
                        msg = None
                        continue
                    except discord.HTTPException as e:
                        if e.status == 429:
                            print(f"⏰ Rate limit {pid}, aguardando...")
                            await asyncio.sleep(5)
                        else:
                            print(f"⚠️ Erro ao editar {pid}: {e}")
                            falhas_consecutivas += 1
        
        except Exception as e:
            print(f"❌ Erro no acompanhar_producao {pid}: {e}")
            import traceback
            traceback.print_exc()
            falhas_consecutivas += 1
        
        await asyncio.sleep(10)
async def finalizar_producao(pid, msg, prod):
    print(f"🔵 FINALIZANDO produção {pid}")
    
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
        
        print(f"📊 {galpao}: {qtd_galpoes} galpões, {capsulas_total} cápsulas totais, {polvora_total} pólvora total")
        
        async with get_db().acquire() as conn:
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
                print(f"Erro ao editar mensagem final: {e}")
        
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
        print(f"✅ Produção {pid} finalizada com {capsulas_total} cápsulas ({qtd_galpoes} galpões)")
    
    except Exception as e:
        print(f"❌ ERRO ao finalizar produção {pid}: {e}")
        import traceback
        traceback.print_exc()


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
            
            # =========================================================
            # ================= PRODUÇÃO JÁ FINALIZADA =================
            # =========================================================
            
            if not prod:
                # Tentar encontrar a produção finalizada
                async with get_db().acquire() as conn:
                    finalizada = await conn.fetchrow(
                        "SELECT * FROM producoes_finalizadas WHERE user_id = $1 ORDER BY data DESC LIMIT 1",
                        str(interaction.user.id)
                    )
                
                if finalizada:
                    await interaction.followup.send(
                        f"✅ **Produção já foi finalizada!**\n\n"
                        f"🧪 Cápsulas produzidas: {fmt_num(finalizada['capsulas'])}\n"
                        f"💣 Pólvora utilizada: {fmt_num(finalizada['polvora'])}",
                        ephemeral=True
                    )
                    # Remover o botão da mensagem
                    try:
                        await interaction.message.edit(view=None)
                    except:
                        pass
                    return
                else:
                    await interaction.followup.send(
                        "❌ Produção não encontrada!\n\n"
                        "💡 Pode ter sido finalizada automaticamente.",
                        ephemeral=True
                    )
                    try:
                        await interaction.message.edit(view=None)
                    except:
                        pass
                    return
            
            # =========================================================
            # ================= PRODUÇÃO ATIVA =========================
            # =========================================================
            
            if prod.get("segunda_task_confirmada"):
                await interaction.followup.send("⚠️ A segunda task já foi confirmada!", ephemeral=True)
                return
            
            fim = prod["fim"]
            if isinstance(fim, str):
                fim = str_para_datetime(fim)
            
            # Se já terminou, finalizar imediatamente
            if agora() >= fim:
                await interaction.followup.send(
                    "⏰ **Produção já terminou!**\n\n"
                    "🔄 A produção será finalizada automaticamente.",
                    ephemeral=True
                )
                # Forçar finalização
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
            
            # Confirmar segunda task
            prod["segunda_task_confirmada"] = {
                "user": interaction.user.id,
                "time": agora().isoformat()
            }
            await salvar_producao(self.pid, prod)
            
            # Remover botão
            try:
                await interaction.message.edit(view=None)
            except:
                pass
            
            await interaction.followup.send(
                "✅ **Segunda task confirmada com sucesso!**\n\n"
                "🔄 A produção continuará normalmente.",
                ephemeral=True
            )
            
        except Exception as e:
            print("Erro segunda task:", e)
            import traceback
            traceback.print_exc()
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
            await interaction.followup.send("❌ Quantidade de pólvora inválida! Digite um número positivo.", ephemeral=True)
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
            
            await interaction.followup.send(f"❌ **INSUMOS INSUFICIENTES!**\n\n" + "\n".join(faltando) + f"\n\n⚠️ Registre mais insumos antes de produzir!", ephemeral=True)
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
        
        embed_privado = discord.Embed(title="✅ PRODUÇÃO REALIZADA COM SUCESSO!", color=0x2ecc71)
        embed_privado.add_field(name="🔫 Tipo", value=f"**{tipo}**", inline=True)
        embed_privado.add_field(name="📦 Pacotes", value=f"**{fmt_num(pacotes)}** pacotes", inline=True)
        embed_privado.add_field(name="🔫 Munições", value=f"**{fmt_num(municoes)}** unidades", inline=True)
        embed_privado.add_field(name="📦 Insumos consumidos", value=f"💊 {fmt_num(capsulas_usadas)} cápsulas\n📦 {fmt_num(embalagens_usadas)} embalagens", inline=False)
        embed_privado.add_field(name="📊 Estoque atual de munição", value=f"🔫 PT: **{fmt_num(estoque_municoes['PT'])}** pacotes\n🔫 SUB: **{fmt_num(estoque_municoes['SUB'])}** pacotes", inline=False)
        
        await interaction.followup.send(embed=embed_privado, ephemeral=True)
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
        
        embed_privado = discord.Embed(title="✅ CÁPSULAS REGISTRADAS", description=f"**{fmt_num(quantidade)}** cápsulas adicionadas!", color=0x2ecc71)
        embed_privado.add_field(name="📊 Estoque atual", value=f"**{fmt_num(estoque['capsulas'])}** cápsulas", inline=False)
        await interaction.followup.send(embed=embed_privado, ephemeral=True)
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
        
        embed_privado = discord.Embed(title="✅ EMBALAGENS REGISTRADAS", description=f"**{fmt_num(quantidade)}** embalagens adicionadas!", color=0x2ecc71)
        embed_privado.add_field(name="📊 Estoque atual", value=f"**{fmt_num(estoque['embalagens'])}** embalagens", inline=False)
        await interaction.followup.send(embed=embed_privado, ephemeral=True)
        await enviar_painel_fabricacao()


class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🏭 Galpões Norte", style=discord.ButtonStyle.primary, custom_id="fabricacao_norte")
    async def norte(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with get_db().acquire() as conn:
            ativo = await conn.fetchval("SELECT 1 FROM producoes WHERE galpao LIKE 'GALPÕES NORTE%' AND CAST(fim AS timestamp) > NOW()")
        if ativo:
            await interaction.response.send_message("⚠️ Galpões Norte já está em produção.", ephemeral=True)
            return
        await interaction.response.send_modal(ProducaoCompletaModal("GALPÕES NORTE", 65))
    
    @discord.ui.button(label="🏭 Galpões Sul", style=discord.ButtonStyle.secondary, custom_id="fabricacao_sul")
    async def sul(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with get_db().acquire() as conn:
            ativo = await conn.fetchval("SELECT 1 FROM producoes WHERE galpao LIKE 'GALPÕES SUL%' AND CAST(fim AS timestamp) > NOW()")
        if ativo:
            await interaction.response.send_message("⚠️ Galpões Sul já está em produção.", ephemeral=True)
            return
        await interaction.response.send_modal(ProducaoCompletaModal("GALPÕES SUL", 130))
    
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
        embed.add_field(name="📋 TABELA DE CONSUMO", value=("**Por pacote PT:**\n• 25 cápsulas\n• 5 embalagens\n\n**Por pacote SUB:**\n• 65 cápsulas\n• 10 embalagens"), inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)
    
    @discord.ui.button(label="📊 Relatório Produção", style=discord.ButtonStyle.secondary, custom_id="fabricacao_relatorio")
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RelatorioProducaoModal())
    
    @discord.ui.button(label="🔄 Atualizar Painel", style=discord.ButtonStyle.secondary, custom_id="atualizar_painel_btn", emoji="🔄")
    async def atualizar_painel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await enviar_painel_fabricacao()
        await interaction.followup.send("✅ Painel atualizado!", ephemeral=True)


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
            
            async with get_db().acquire() as conn:
                rows = await conn.fetch(
                    "SELECT user_id, SUM(capsulas) as total_capsulas, SUM(polvora) as total_polvora FROM producoes_finalizadas WHERE data >= $1 AND data <= $2 GROUP BY user_id ORDER BY total_capsulas DESC",
                    inicio_dt, fim_dt
                )
            
            if not rows:
                await interaction.followup.send(f"📭 Nenhuma produção no período **{self.data_inicio.value}** a **{self.data_fim.value}**.", ephemeral=True)
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
                await interaction.followup.send(f"✅ Relatório enviado!\n📅 {self.data_inicio.value} a {self.data_fim.value}\n💰 Total: {fmt_num(total_capsulas)} cápsulas\n💣 Pólvora: {fmt_num(total_polvora)}", ephemeral=True)
            else:
                await interaction.followup.send(embed=embed, ephemeral=True)
        
        except ValueError:
            await interaction.followup.send("❌ **Formato de data inválido!**\nUse o formato: `DD/MM/AAAA`", ephemeral=True)
        except Exception as e:
            print("ERRO RELATORIO:", e)
            await interaction.followup.send("❌ Erro ao gerar relatório.", ephemeral=True)


# =========================================================
# ==================== SISTEMA DE PÓLVORA ==================
# =========================================================

class PolvoraModal(discord.ui.Modal, title="Registro de Compra de Pólvora"):
    vendedor = discord.ui.TextInput(label="Vendedor (preencha o nome)", placeholder="Digite o nome do vendedor")
    quantidade = discord.ui.TextInput(label="Quantidade", placeholder="Ex: 100")
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value)
        except:
            await interaction.response.send_message("Quantidade inválida.", ephemeral=True)
            return
        
        valor = qtd * 80
        await salvar_polvora_db(interaction.user.id, self.vendedor.value, qtd, valor)
        
        canal = interaction.guild.get_channel(CANAL_REGISTRO_POLVORA_ID)
        embed = discord.Embed(title="🧨 Registro de Pólvora", color=0xe67e22)
        embed.add_field(name="Vendedor", value=self.vendedor.value, inline=False)
        embed.add_field(name="Comprado por", value=interaction.user.mention, inline=False)
        embed.add_field(name="Quantidade", value=str(qtd), inline=True)
        embed.add_field(name="Valor", value=f"**{formatar_dinheiro(valor)}**", inline=True)
        await canal.send(embed=embed)
        await interaction.response.send_message("Registro feito com sucesso!", ephemeral=True)


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


# =========================================================
# ==================== SISTEMA DE LAVAGEM ==================
# =========================================================

def pode_gerenciar_lavagem(member: discord.Member):
    cargos_permitidos = [CARGO_GERENTE_ID, CARGO_01_ID, CARGO_02_ID, CARGO_GERENTE_GERAL_ID]
    return any(role.id in cargos_permitidos for role in member.roles)


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
        
        lavagens_pendentes[interaction.user.id] = {
            "sujo": valor_sujo,
            "retorno": valor_retorno,
            "taxa": taxa,
            "msg_info": msg_info
        }


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
                await user.send(
                    f"🧼 **Seu dinheiro foi lavado com sucesso!**\n\n"
                    f"💵 Dinheiro informado: {formatar_dinheiro(item['valor'])}\n"
                    f"💰 Valor repassado: {formatar_dinheiro(item['liquido'])}"
                )
                enviados += 1
            except:
                falhas += 1
        
        await interaction.response.send_message(f"DM enviada para {enviados} membros.\nFalhas: {falhas}", ephemeral=True)


@tasks.loop(minutes=15)
async def limpar_lavagens_pendentes():
    lavagens_pendentes.clear()


# =========================================================
# ==================== SISTEMA DE LIVES ====================
# =========================================================

ADM_ID = 467673818375389194

class CadastrarLiveModal(discord.ui.Modal, title="🎥 Cadastrar Live"):
    link = discord.ui.TextInput(label="Cole o link da sua live", placeholder="https://kick.com/seucanal ou https://twitch.tv/seucanal")
    
    async def on_submit(self, interaction: discord.Interaction):
        lives = await carregar_lives_db()
        novo_link = self.link.value.strip().lower()
        novo_link = novo_link.split("?")[0].rstrip("/")
        
        plataforma = detectar_plataforma(novo_link)
        novo_canal = extrair_canal(novo_link)
        
        if not plataforma or not novo_canal:
            await interaction.response.send_message("❌ Link inválido. Use links da Twitch, Kick ou TikTok.", ephemeral=True)
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


class RemoverLiveSelect(discord.ui.Select):
    def __init__(self, lives):
        options = []
        for row in lives:
            uid = row["user_id"]
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
        await interaction.response.send_message(
            "**⚙️ PAINEL DE GERENCIAMENTO DE LIVES**\n\n"
            "📋 **Ver Lives** - Lista todas as lives cadastradas\n"
            "🗑️ **Remover Live** - Remove um usuário e todas as suas lives",
            view=view, ephemeral=True
        )


@tasks.loop(minutes=2)
async def verificar_lives():
    """Verifica todas as lives cadastradas (Twitch, Kick e TikTok)"""
    
    print("🔄 Verificando lives...")
    
    try:
        lives = await carregar_lives_db()
        
        if not lives:
            print("📭 Nenhuma live cadastrada")
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
                print(f"⚠️ Link inválido ou não suportado: {link}")
                continue
            
            ao_vivo = False
            titulo = None
            jogo = None
            thumbnail = None
            
            try:
                if plataforma == "twitch":
                    ao_vivo, titulo, jogo, thumbnail = await checar_twitch(canal_name)
                    print(f"📺 Twitch {canal_name}: ao_vivo={ao_vivo}")
                
                elif plataforma == "kick":
                    resultado = await checar_kick(canal_name)
                    # VERIFICA SE O RETORNO É VÁLIDO
                    if resultado is not None and len(resultado) == 4:
                        ao_vivo, titulo, jogo, thumbnail = resultado
                        print(f"🟢 Kick {canal_name}: ao_vivo={ao_vivo}")
                    else:
                        print(f"⚠️ Kick {canal_name}: retorno inválido - {resultado}")
                        continue
                
                elif plataforma == "tiktok":
                    ao_vivo, titulo, jogo, thumbnail = await checar_tiktok(canal_name)
                    print(f"📱 TikTok {canal_name}: ao_vivo={ao_vivo}")
                
                else:
                    print(f"⚠️ Plataforma não suportada: {plataforma}")
                    continue
                
            except Exception as e:
                print(f"❌ Erro ao verificar {plataforma}/{canal_name}: {e}")
                continue
            
            if not ao_vivo and divulgado:
                await atualizar_divulgado_db(link, False)
                print(f"📴 Live encerrada: {plataforma}/{canal_name}")
            
            if ao_vivo and not divulgado:
                print(f"🔴🔴🔴 LIVE DETECTADA ({plataforma.upper()}): {canal_name}")
                print(f"   → User ID: {user_id}")
                print(f"   → Link: {link}")
                print(f"   → Título: {titulo}")
                
                resultado = await divulgar_live(user_id, link, titulo, jogo, thumbnail)
                
                if resultado:
                    await atualizar_divulgado_db(link, True)
                    print(f"✅ Live divulgada: {plataforma}/{canal_name}")
                else:
                    print(f"❌ FALHA ao divulgar live: {plataforma}/{canal_name}")
                    
    except Exception as e:
        print(f"❌ Erro no loop de lives: {e}")
        import traceback
        traceback.print_exc()
async def divulgar_live(user_id, link, titulo, jogo, thumbnail):
    try:
        canal = bot.get_channel(CANAL_DIVULGACAO_LIVE_ID)
        if not canal:
            print(f"❌ Canal de divulgação NÃO ENCONTRADO! ID: {CANAL_DIVULGACAO_LIVE_ID}")
            return False
        
        user = await pegar_usuario(int(user_id))
        if not user:
            print(f"❌ Usuário {user_id} não encontrado")
            return False
        
        plataforma = detectar_plataforma(link) or "desconhecida"
        
        cores = {"twitch": 0x9146FF, "kick": 0x53FC18, "tiktok": 0x000000, "desconhecida": 0x808080}
        nomes = {"twitch": "Twitch", "kick": "Kick", "tiktok": "TikTok", "desconhecida": "Desconhecida"}
        icones = {"twitch": "🟣", "kick": "🟢", "tiktok": "📱", "desconhecida": "🔴"}
        
        embed = discord.Embed(title=f"{icones.get(plataforma, '🔴')} LIVE AO VIVO!", color=cores.get(plataforma, 0xff0000))
        embed.description = f"👤 **Streamer:** {user.mention}\n📺 **Plataforma:** {nomes.get(plataforma, plataforma.upper())}\n"
        if jogo and jogo != "TikTok" and jogo != "None" and jogo.strip():
            embed.description += f"🎮 **Jogo:** {jogo}\n"
        embed.description += f"📝 **Título:** {titulo or 'Sem título'}\n\n🔗 **Assistir:** {link}"
        
        if thumbnail and thumbnail != "None" and thumbnail.startswith("http"):
            embed.set_image(url=thumbnail)
        elif plataforma == "kick":
            embed.set_thumbnail(url="https://kick.com/favicon.ico")
        elif plataforma == "twitch":
            embed.set_thumbnail(url="https://www.twitch.tv/favicon.ico")
        
        embed.set_footer(text=f"Live detectada • {agora().strftime('%d/%m/%Y %H:%M:%S')}")
        
        if plataforma == "tiktok":
            await canal.send(content=f"🔴 {user.mention} está ao vivo no TikTok!", embed=embed, allowed_mentions=discord.AllowedMentions(users=True))
        else:
            await canal.send(content="@everyone 🔴 **LIVE INICIADA!**", embed=embed, allowed_mentions=discord.AllowedMentions(everyone=True))
        
        return True
    except Exception as e:
        print(f"❌ ERRO ao divulgar live: {e}")
        return False


# =========================================================
# ==================== SISTEMA DE AÇÕES ====================
# =========================================================

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
        
        async with get_db().acquire() as conn:
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
        if limite and limite is not None:
            async with get_db().acquire() as conn:
                qtd = await conn.fetchval(
                    "SELECT COUNT(*) FROM acoes_semana WHERE tipo=$1 AND status='concluida' AND (resultado='ganhou' OR resultado='perdeu')",
                    acao_tipo
                )
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
            async with get_db().acquire() as conn:
                qtd_feita = await conn.fetchval("SELECT COUNT(*) FROM acoes_semana WHERE tipo=$1 AND status='concluida' AND (resultado='ganhou' OR resultado='perdeu')", acao_tipo)
                embed.description += f"\n\n📊 **Limite semanal:** {qtd_feita}/{limite} ações realizadas"
        
        embed.add_field(name="👥 Participantes (0)", value="Nenhum participante ainda.\nClique no botão abaixo para participar!", inline=False)
        embed.add_field(name="👤 Criado por", value=interaction.user.mention, inline=True)
        embed.add_field(name="📅 Data", value=agora().strftime('%d/%m/%Y %H:%M'), inline=True)
        embed.set_footer(text=f"ID: {acao_id}")
        
        canal = interaction.guild.get_channel(CANAL_ESCALACOES_ID)
        if canal:
            await canal.send(embed=embed, view=AcaoCheckinView(acao_id, interaction.user.id))
            await interaction.followup.send(f"✅ Ação **{acao_tipo}** criada! Os membros podem clicar em **✅ Participar** para se inscrever.", ephemeral=True)
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
        
        async with get_db().acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.response.send_message("❌ Esta ação já foi concluída e não aceita mais participantes!", ephemeral=True)
                return
            
            ja_participa = await conn.fetchval("SELECT 1 FROM participantes_acoes WHERE acao_id=$1 AND user_id=$2", self.acao_id, str(interaction.user.id))
            if ja_participa:
                await interaction.response.send_message("⚠️ Você já está participando desta ação!", ephemeral=True)
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
            await interaction.response.send_message("❌ Apenas o criador da ação ou gerentes podem concluir a escalação!", ephemeral=True)
            return
        
        async with get_db().acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.response.send_message("❌ Esta ação já foi concluída!", ephemeral=True)
                return
            
            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            is_helicrash = "Helicrash" in acao["tipo"]
            
            if not participantes:
                await interaction.response.send_message("⚠️ Nenhum participante se inscreveu! Ação cancelada.", ephemeral=True)
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
                await interaction.response.send_message(f"✅ Helicrash **{acao['tipo']}** registrado! Participantes: {len(participantes)}", ephemeral=True)
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
            await interaction.response.send_message(f"✅ Escalação concluída! Relatório enviado para <#{CANAL_RELATORIO_ACOES_ID}>.", ephemeral=True)
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
        async with get_db().acquire() as conn:
            acao = await conn.fetchrow("SELECT autor FROM acoes_semana WHERE id=$1", self.acao_id)
        
        is_autor = str(interaction.user.id) == acao["autor"]
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_autor and not is_gerente:
            await interaction.response.send_message("❌ Sem permissão!", ephemeral=True)
            return
        
        await interaction.response.send_modal(ResultadoGanhouModal(self.acao_id, self.mensagem_original))
    
    @discord.ui.button(label="💀 Perdeu", style=discord.ButtonStyle.danger, custom_id="resultado_perdeu")
    async def perdeu(self, interaction: discord.Interaction, button):
        async with get_db().acquire() as conn:
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
        
        async with get_db().acquire() as conn:
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
            await interaction.followup.send("⚠️ Nenhum participante registrado!", ephemeral=True)
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


class ResultadoPerdeuModal(discord.ui.Modal, title="💀 Resultado - PERDEU"):
    confirmacao = discord.ui.TextInput(label="Digite CONFIRMAR para registrar a perda", required=True)
    
    def __init__(self, acao_id, mensagem_original):
        super().__init__()
        self.acao_id = acao_id
        self.mensagem_original = mensagem_original
    
    async def on_submit(self, interaction: discord.Interaction):
        if self.confirmacao.value.strip().upper() != "CONFIRMAR":
            await interaction.response.send_message("❌ Confirmação incorreta! Digite 'CONFIRMAR' para prosseguir.", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        async with get_db().acquire() as conn:
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
        
        async with get_db().acquire() as conn:
            total = await conn.fetchval("SELECT COALESCE(SUM(valor), 0) FROM acoes_semana WHERE DATE(data) BETWEEN DATE($1) AND DATE($2) AND resultado = 'ganhou'", inicio, fim)
            rows = await conn.fetch(
                "SELECT p.user_id, COUNT(*) as qtd FROM participantes_acoes p JOIN acoes_semana a ON a.id = p.acao_id WHERE DATE(a.data) BETWEEN DATE($1) AND DATE($2) GROUP BY p.user_id ORDER BY qtd DESC",
                inicio, fim
            )
        
        linhas = [f"<@{r['user_id']}> • {r['qtd']} participações" for r in rows]
        embed = discord.Embed(title="📊 Relatório de Ações", color=0x3498db)
        embed.add_field(name="📅 Período", value=f"{self.data_inicio.value} até {self.data_fim.value}", inline=False)
        embed.add_field(name="💰 Total Movimentado (vitórias)", value=formatar_dinheiro(total), inline=False)
        embed.add_field(name="👥 Participações", value="\n".join(linhas) if linhas else "Nenhuma", inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)


class ResetAcoesView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="♻️ Resetar Ações", style=discord.ButtonStyle.danger, custom_id="reset_acoes_btn")
    async def resetar(self, interaction: discord.Interaction, button):
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_gerente and not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Apenas gerentes podem resetar as ações!", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        async with get_db().acquire() as conn:
            await conn.execute("DELETE FROM acoes_semana")
            await conn.execute("DELETE FROM participantes_acoes")
        
        await enviar_painel_acoes(interaction.guild)
        await interaction.followup.send("✅ Todas as ações foram resetadas!", ephemeral=True)


class FecharButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="❌ Fechar", style=discord.ButtonStyle.danger)
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.message.delete()


# =========================================================
# ==================== SISTEMA DE AUSÊNCIA =================
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
            await interaction.followup.send("❌ Formato de data inválido!\nUse o formato: `10/04/2026`", ephemeral=True)
            return
        
        if data_fim_naive <= data_inicio_naive:
            await interaction.followup.send(f"❌ A data de RETORNO deve ser **depois** da data de INÍCIO!\nInício: {self.data_inicio.value}\nRetorno: {self.data_fim.value}", ephemeral=True)
            return
        
        ausencia_existente = await buscar_ausencia_por_user(interaction.user.id)
        if ausencia_existente:
            await interaction.followup.send("❌ Você já possui uma ausência ativa!\nEspere ela terminar ou peça para um admin cancelar.", ephemeral=True)
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
            print(f"✅ Cargo ausente removido de {member.display_name}")
            
            canal_registro = guild.get_channel(CANAL_REGISTRO_AUSENCIA_ID)
            if canal_registro:
                embed_retorno = discord.Embed(title="🎉 RETORNO REGISTRADO", description=f"{member.mention} retornou! O cargo ausente foi removido automaticamente.", color=0x2ecc71)
                await canal_registro.send(embed=embed_retorno)


# =========================================================
# ==================== SISTEMA DE ALUGUEL ==================
# =========================================================

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


class AluguelModal(discord.ui.Modal, title="💰 Alugar Galpão"):
    dias = discord.ui.TextInput(label="Quantos dias de aluguel?", placeholder="Digite o número de dias (ex: 15, 30, 7)", required=True)
    
    def __init__(self, galpao, editando=False, user_id=None):
        super().__init__()
        self.galpao = galpao
        self.editando = editando
        self.user_id = user_id
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        try:
            dias_aluguel = int(self.dias.value)
            if dias_aluguel <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ Número de dias inválido! Use números positivos (ex: 15, 30, 7)", ephemeral=True)
            return
        
        data_fim = agora() + timedelta(days=dias_aluguel)
        
        if self.editando:
            await renovar_aluguel_db(self.galpao, self.user_id, data_fim.replace(tzinfo=None), dias_aluguel)
            alugueis_ativos[self.galpao] = {"user_id": self.user_id, "fim": data_fim, "dias": dias_aluguel}
            await interaction.followup.send(f"✅ **{self.galpao}** atualizado com sucesso!\n👤 **Alugado por:** <@{self.user_id}>\n📅 **Dias:** {dias_aluguel} dia(s)\n⏰ **Vence em:** {data_fim.strftime('%d/%m/%Y às %H:%M')}", ephemeral=True)
        else:
            if self.galpao in alugueis_ativos:
                await interaction.followup.send(f"❌ **{self.galpao}** já está alugado!\n👤 Alugado por: <@{alugueis_ativos[self.galpao]['user_id']}>\n⏰ Vence em: {alugueis_ativos[self.galpao]['fim'].strftime('%d/%m/%Y às %H:%M')}", ephemeral=True)
                return
            
            await salvar_aluguel_db(self.galpao, interaction.user.id, data_fim.replace(tzinfo=None), dias_aluguel)
            alugueis_ativos[self.galpao] = {"user_id": interaction.user.id, "fim": data_fim, "dias": dias_aluguel}
            await interaction.followup.send(f"✅ **{self.galpao}** alugado com sucesso!\n👤 **Alugado por:** {interaction.user.mention}\n📅 **Dias:** {dias_aluguel} dia(s)\n⏰ **Vence em:** {data_fim.strftime('%d/%m/%Y às %H:%M')}", ephemeral=True)
        
        await enviar_painel_alugueis()


class EditarAluguelModal(discord.ui.Modal, title="✏️ Editar Aluguel"):
    dias = discord.ui.TextInput(label="Novo número de dias", placeholder="Digite o novo número de dias (ex: 15, 30, 7)", required=True)
    
    def __init__(self, galpao, user_id, dias_atuais):
        super().__init__()
        self.galpao = galpao
        self.user_id = user_id
        self.dias_atuais = dias_atuais
        self.dias.placeholder = f"Dias atuais: {dias_atuais}"
    
    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        is_dono = interaction.user.id == self.user_id
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_dono and not is_gerente:
            await interaction.followup.send("❌ Apenas o dono do aluguel ou gerentes podem editar!", ephemeral=True)
            return
        
        try:
            dias_aluguel = int(self.dias.value)
            if dias_aluguel <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ Número de dias inválido! Use números positivos (ex: 15, 30, 7)", ephemeral=True)
            return
        
        data_fim = agora() + timedelta(days=dias_aluguel)
        await renovar_aluguel_db(self.galpao, self.user_id, data_fim.replace(tzinfo=None), dias_aluguel)
        alugueis_ativos[self.galpao] = {"user_id": self.user_id, "fim": data_fim, "dias": dias_aluguel}
        
        await interaction.followup.send(f"✅ **{self.galpao}** atualizado com sucesso!\n📅 **Dias:** {self.dias_atuais} → {dias_aluguel} dia(s)\n⏰ **Nova data de vencimento:** {data_fim.strftime('%d/%m/%Y às %H:%M')}", ephemeral=True)
        await enviar_painel_alugueis()


class EditarAluguelButton(discord.ui.Button):
    def __init__(self, galpao, user_id, dias_atuais):
        super().__init__(label="✏️ Editar Aluguel", style=discord.ButtonStyle.secondary, custom_id=f"editar_aluguel_{galpao}", emoji="✏️", row=1)
        self.galpao = galpao
        self.user_id = user_id
        self.dias_atuais = dias_atuais
    
    async def callback(self, interaction: discord.Interaction):
        is_dono = interaction.user.id == self.user_id
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_dono and not is_gerente:
            await interaction.response.send_message("❌ Apenas o dono do aluguel ou gerentes podem editar!", ephemeral=True)
            return
        
        await interaction.response.send_modal(EditarAluguelModal(self.galpao, self.user_id, self.dias_atuais))


class AlugarButton(discord.ui.Button):
    def __init__(self, galpao, label, custom_id):
        super().__init__(label=label, style=discord.ButtonStyle.success, custom_id=custom_id, emoji="💰")
        self.galpao = galpao
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(AluguelModal(self.galpao, editando=False))


class AluguelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(AlugarButton(galpao="GALPÕES NORTE", label="💰 Alugar Galpões Norte", custom_id="aluguel_norte_btn"))
        self.add_item(AlugarButton(galpao="GALPÕES SUL", label="💰 Alugar Galpões Sul", custom_id="aluguel_sul_btn"))
    
    def adicionar_botoes_edicao(self, galpao, user_id, dias_atuais):
        self.add_item(EditarAluguelButton(galpao, user_id, dias_atuais))


@tasks.loop(minutes=1)
async def atualizar_contagem_alugueis():
    if alugueis_ativos:
        await enviar_painel_alugueis()


@tasks.loop(minutes=5)
async def verificar_alugueis_expirados():
    agora_br = agora()
    expirou = False
    
    for galpao, dados in list(alugueis_ativos.items()):
        if agora_br >= dados["fim"]:
            await desativar_aluguel_db(galpao)
            expirou = True
            print(f"⏰ Aluguel expirado: {galpao}")
            
            canal = bot.get_channel(CANAL_FABRICACAO_ID)
            if canal:
                await canal.send(f"⚠️ **{galpao}** - O aluguel EXPIRou!\n👤 Dono: <@{dados['user_id']}>\n📅 Venceu em: {dados['fim'].strftime('%d/%m/%Y às %H:%M')}\n\n💰 O galpão está disponível para um novo aluguel!")
    
    if expirou:
        await enviar_painel_alugueis()


# =========================================================
# ==================== SISTEMA DE COMPRAS ==================
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
            valor_compra = int(self.valor.value.replace(".", "").replace(",", ""))
            if valor_compra <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ **Valor inválido!**\nDigite apenas números (ex: 50000, 1500, 2000)", ephemeral=True)
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
            await interaction.followup.send(f"✅ **Compra registrada com sucesso!**\n📦 Produto: {produto}\n💰 Valor: {formatar_dinheiro(valor_compra)}\n📅 Data: {data_atual.strftime('%d/%m/%Y às %H:%M')}\n\n📨 A compra foi registrada no canal <#{CANAL_COMPRAS_REGISTRADAS_ID}>", ephemeral=True)
        else:
            await interaction.followup.send(f"✅ **Compra registrada com sucesso!**\n📦 Produto: {produto}\n💰 Valor: {formatar_dinheiro(valor_compra)}", ephemeral=True)


class RegistrarCompraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📝 Registrar Nova Compra", style=discord.ButtonStyle.success, custom_id="registrar_compra_btn", emoji="💰")
    async def registrar_compra(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarCompraModal())


# =========================================================
# ==================== SISTEMA DE FINANCEIRO ===============
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
                    total_embalagens = int(self.embalagens.value.replace(".", "").replace(",", ""))
                    total_gasto_embalagens = int(total_embalagens * PRECO_EMBALAGEM_POR_UNIDADE)
                except:
                    pass
            
            async with get_db().acquire() as conn:
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
            
            embed.add_field(name="💣 PÓLVORA", value=(f"**Utilizada na produção:** {fmt_num(total_polvora_gasta)} unidades\n**💰 Gasto com pólvora:** {formatar_dinheiro(total_gasto_polvora)}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n**Comprada no período:** {fmt_num(total_polvora_comprada)} unidades\n**💰 Gasto na compra:** {formatar_dinheiro(total_gasto_polvora_comprada)}"), inline=False)
            
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
            
            embed.add_field(name="📊 RESUMO FINANCEIRO", value=(f"**💰 Total de Vendas:** {formatar_dinheiro(total_vendas)}\n**💸 Total de Gastos:** {formatar_dinheiro(total_gastos)}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n{emoji_saldo} **SALDO:** {formatar_dinheiro(saldo)}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n**📋 DETALHAMENTO DOS GASTOS:**\n{detalhe_gastos}"), inline=False)
            
            embed.set_footer(text=f"Relatório gerado em {agora().strftime('%d/%m/%Y às %H:%M')}")
            
            canal = interaction.guild.get_channel(CANAL_RELATORIO_FINANCEIRO_ID)
            if canal:
                await canal.send(embed=embed)
                await interaction.followup.send(f"✅ Relatório financeiro enviado no canal <#{CANAL_RELATORIO_FINANCEIRO_ID}>!", ephemeral=True)
            else:
                await interaction.followup.send(embed=embed, ephemeral=True)
        
        except ValueError:
            await interaction.followup.send("❌ **Formato de data inválido!**\nUse o formato: `DD/MM/AAAA`\nExemplo: `01/04/2026`", ephemeral=True)
        except Exception as e:
            print("ERRO RELATORIO FINANCEIRO:", e)
            await interaction.followup.send(f"❌ Erro ao gerar relatório: {str(e)}", ephemeral=True)


class RelatorioFinanceiroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📊 Gerar Relatório Financeiro", style=discord.ButtonStyle.success, custom_id="relatorio_financeiro_btn", emoji="💰")
    async def gerar_relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RelatorioFinanceiroModal())


# =========================================================
# ==================== COMANDOS ============================
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
    async with get_db().acquire() as conn:
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
    async with get_db().acquire() as conn:
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


@bot.command(name="alugueis")
async def cmd_ver_alugueis(ctx):
    await enviar_painel_alugueis()
    await ctx.send("📊 Painel de aluguéis atualizado!", ephemeral=True)


@bot.command(name="editar_aluguel")
async def cmd_editar_aluguel(ctx, galpao: str = None, dias: int = None):
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    if not galpao or not dias:
        await ctx.send("❌ Uso: `!editar_aluguel NORTE 30`\nOpções: NORTE, SUL")
        return
    galpao = galpao.upper()
    if galpao == "NORTE":
        nome_galpao = "GALPÕES NORTE"
    elif galpao == "SUL":
        nome_galpao = "GALPÕES SUL"
    else:
        await ctx.send("❌ Galpões válidos: NORTE, SUL")
        return
    if nome_galpao not in alugueis_ativos:
        await ctx.send(f"❌ {nome_galpao} não está alugado no momento!")
        return
    user_id = alugueis_ativos[nome_galpao]["user_id"]
    data_fim = agora() + timedelta(days=dias)
    await renovar_aluguel_db(nome_galpao, user_id, data_fim.replace(tzinfo=None), dias)
    alugueis_ativos[nome_galpao] = {"user_id": user_id, "fim": data_fim, "dias": dias}
    await enviar_painel_alugueis()
    await ctx.send(f"✅ **{nome_galpao}** editado para {dias} dias!")


@bot.command(name="renovar")
async def cmd_renovar_aluguel(ctx, galpao: str, dias: int):
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    galpao = galpao.upper()
    if galpao == "NORTE":
        nome_galpao = "GALPÕES NORTE"
    elif galpao == "SUL":
        nome_galpao = "GALPÕES SUL"
    else:
        await ctx.send("❌ Galpões válidos: NORTE, SUL")
        return
    data_fim = agora() + timedelta(days=dias)
    await salvar_aluguel_db(nome_galpao, ctx.author.id, data_fim.replace(tzinfo=None), dias)
    alugueis_ativos[nome_galpao] = {"user_id": ctx.author.id, "fim": data_fim, "dias": dias}
    await enviar_painel_alugueis()
    await ctx.send(f"✅ **{nome_galpao}** alugado por {dias} dias!")


@bot.command(name="testar_kick")
async def testar_kick(ctx, canal: str = None):
    if not canal:
        await ctx.send("❌ Use: !testar_kick <nome_do_canal>\nEx: !testar_kick alanzoka")
        return
    await ctx.send(f"🔍 Verificando canal Kick: **{canal}**...")
    ao_vivo, titulo, jogo, thumbnail = await checar_kick(canal)
    if ao_vivo:
        embed = discord.Embed(title="✅ ESTÁ AO VIVO!", description=f"**Canal:** {canal}\n**Título:** {titulo}\n**Jogo:** {jogo or 'N/A'}", color=0x2ecc71)
        if thumbnail:
            embed.set_image(url=thumbnail)
        await ctx.send(embed=embed)
    else:
        await ctx.send(f"❌ O canal **{canal}** NÃO está ao vivo no momento.")


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
    elif plataforma == "kick":
        ao_vivo, titulo, jogo, thumbnail = await checar_kick(canal)
    elif plataforma == "tiktok":
        ao_vivo, titulo, jogo, thumbnail = await checar_tiktok(canal)
    else:
        await ctx.send("❌ Plataforma inválida! Use: `twitch`, `kick` ou `tiktok`")
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


@bot.command(name="forcar_verificacao")
async def forcar_verificacao_lives(ctx):
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    await ctx.send("🔄 **Forçando verificação de todas as lives...**")
    await verificar_lives()
    await ctx.send("✅ **Verificação concluída!** Confira o canal de divulgação.")


@bot.command(name="remover_live")
async def cmd_remover_live(ctx, user_id: int = None):
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    if not user_id:
        await ctx.send("❌ Use: `!remover_live ID_DO_USUARIO`")
        return
    lives = await carregar_lives_db()
    if str(user_id) not in [str(row["user_id"]) for row in lives]:
        await ctx.send(f"❌ Usuário ID `{user_id}` não possui lives cadastradas.")
        return
    user = await pegar_usuario(user_id)
    nome = user.display_name if user else str(user_id)
    await remover_live_db(str(user_id))
    await ctx.send(f"✅ **Todas as lives de {nome} foram removidas com sucesso!**")
    await enviar_painel_lives()
    await enviar_painel_admin_lives()

@bot.command(name="forcar_producao")
async def forcar_producao(ctx):
    """Força a atualização de todas as produções ativas (ADM apenas)"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    await ctx.send("🔄 Forçando atualização das produções...")
    
    try:
        async with get_db().acquire() as conn:
            rows = await conn.fetch("SELECT pid FROM producoes WHERE CAST(fim AS timestamp) > NOW()")
        
        if not rows:
            await ctx.send("📭 Nenhuma produção ativa.")
            return
        
        atualizadas = 0
        for r in rows:
            pid = r["pid"]
            
            if pid in producoes_tasks:
                producoes_tasks[pid].cancel()
                del producoes_tasks[pid]
            
            task = asyncio.create_task(acompanhar_producao(pid))
            producoes_tasks[pid] = task
            atualizadas += 1
            
            prod = await carregar_producao(pid)
            if prod:
                canal = bot.get_channel(prod["canal_id"])
                if canal:
                    try:
                        msg = await canal.fetch_message(prod["msg_id"])
                        if msg:
                            desc = await gerar_desc_producao(prod)
                            await msg.edit(embed=discord.Embed(title="🏭 Produção", description=desc, color=0x34495e))
                    except:
                        pass
            
            await asyncio.sleep(0.5)
        
        await ctx.send(f"✅ {atualizadas} produção(ões) reiniciada(s)!")
        
    except Exception as e:
        await ctx.send(f"❌ Erro: {e}")
        print(f"Erro forcar_producao: {e}")

@bot.command(name="atualizar_producao")
async def cmd_atualizar_producao(ctx, pid: str = None):
    """Força a atualização de uma produção específica ou de todas"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    await ctx.send("🔄 Forçando atualização...")
    
    try:
        if pid:
            # Atualizar produção específica
            prod = await carregar_producao(pid)
            if not prod:
                await ctx.send(f"❌ Produção {pid} não encontrada!")
                return
            
            canal = bot.get_channel(prod["canal_id"])
            if canal:
                try:
                    msg = await canal.fetch_message(prod["msg_id"])
                    desc = await gerar_desc_producao(prod)
                    await msg.edit(embed=discord.Embed(title="🏭 Produção", description=desc, color=0x34495e))
                    await ctx.send(f"✅ Produção {pid} atualizada!")
                except Exception as e:
                    await ctx.send(f"❌ Erro: {e}")
        else:
            # Atualizar todas
            async with get_db().acquire() as conn:
                rows = await conn.fetch("SELECT pid FROM producoes WHERE CAST(fim AS timestamp) > NOW()")
            
            if not rows:
                await ctx.send("📭 Nenhuma produção ativa.")
                return
            
            atualizadas = 0
            for r in rows:
                try:
                    prod = await carregar_producao(r["pid"])
                    if prod:
                        canal = bot.get_channel(prod["canal_id"])
                        if canal:
                            msg = await canal.fetch_message(prod["msg_id"])
                            desc = await gerar_desc_producao(prod)
                            await msg.edit(embed=discord.Embed(title="🏭 Produção", description=desc, color=0x34495e))
                            atualizadas += 1
                            await asyncio.sleep(2)
                except Exception as e:
                    print(f"Erro ao atualizar {r['pid']}: {e}")
            
            await ctx.send(f"✅ {atualizadas} produção(ões) atualizadas!")
            
    except Exception as e:
        await ctx.send(f"❌ Erro: {e}")
        print(f"Erro atualizar_producao: {e}")

@bot.command(name="status_producoes")
async def cmd_status_producoes(ctx):
    """Mostra o status das produções ativas"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    try:
        async with get_db().acquire() as conn:
            rows = await conn.fetch("SELECT pid, galpao, fim FROM producoes WHERE CAST(fim AS timestamp) > NOW()")
        
        if not rows:
            await ctx.send("📭 Nenhuma produção ativa.")
            return
        
        embed = discord.Embed(title="🏭 STATUS DAS PRODUÇÕES", color=0x3498db)
        
        for r in rows:
            pid = r["pid"]
            fim = r["fim"]
            if isinstance(fim, str):
                fim = str_para_datetime(fim)
            
            restante = (fim - agora()).total_seconds()
            restante = max(0, restante)
            
            task_viva = pid in producoes_tasks and not producoes_tasks[pid].done()
            
            status = "🟢 ATIVA" if task_viva else "🔴 TRAVADA"
            
            embed.add_field(
                name=f"📦 {r['galpao']}",
                value=f"**ID:** `{pid}`\n**Status:** {status}\n**Restante:** {int(restante//60)}m {int(restante%60)}s",
                inline=False
            )
        
        await ctx.send(embed=embed)
        
    except Exception as e:
        await ctx.send(f"❌ Erro: {e}")
        print(f"Erro status_producoes: {e}")

# =========================================================
# ==================== COMANDOS DE GRUPOS ==================
# =========================================================

@bot.command(name="listar_grupos")
async def cmd_listar_grupos(ctx):
    """Lista todos os grupos cadastrados"""
    
    grupos = await carregar_grupos_db()
    
    if not grupos:
        await ctx.send("📭 Nenhum grupo cadastrado.")
        return
    
    embed = discord.Embed(
        title="📋 GRUPOS CADASTRADOS",
        description=f"Total: {len(grupos)} grupo(s)",
        color=0x3498db
    )
    
    for grupo in grupos:
        compras = await carregar_compras_grupo_db(grupo["grupo_id"])
        total_pt = compras.get("PT", {}).get("quantidade", 0)
        total_sub = compras.get("SUB", {}).get("quantidade", 0)
        
        embed.add_field(
            name=f"🏷️ {grupo['nome_org']}",
            value=(
                f"**👤 Líder:** {grupo['lider_nome']}\n"
                f"**🔫 Produto:** {grupo['produto']}\n"
                f"**📦 PT:** {fmt_num(total_pt)} pacotes\n"
                f"**📦 SUB:** {fmt_num(total_sub)} pacotes"
            ),
            inline=False
        )
    
    await ctx.send(embed=embed)


@bot.command(name="ver_grupo")
async def cmd_ver_grupo(ctx, *, nome_org: str):
    """Ver detalhes de um grupo específico"""
    
    grupos = await carregar_grupos_db()
    
    grupo_encontrado = None
    for grupo in grupos:
        if nome_org.lower() in grupo["nome_org"].lower():
            grupo_encontrado = grupo
            break
    
    if not grupo_encontrado:
        await ctx.send(f"❌ Grupo **{nome_org}** não encontrado.")
        return
    
    await enviar_embed_grupo(grupo_encontrado["grupo_id"])
    await ctx.send(f"✅ Embed do grupo **{grupo_encontrado['nome_org']}** atualizado!")


@bot.command(name="remover_grupo")
async def cmd_remover_grupo(ctx, *, nome_org: str):
    """Remove um grupo pelo nome (ADM apenas)"""
    
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    grupos = await carregar_grupos_db()
    
    grupo_encontrado = None
    for grupo in grupos:
        if nome_org.lower() in grupo["nome_org"].lower():
            grupo_encontrado = grupo
            break
    
    if not grupo_encontrado:
        await ctx.send(f"❌ Grupo **{nome_org}** não encontrado.")
        return
    
    await desativar_grupo_db(grupo_encontrado["grupo_id"])
    
    canal = ctx.guild.get_channel(CANAL_GRUPOS_ID)
    if canal:
        async for msg in canal.history(limit=50):
            if msg.author == bot.user and msg.embeds:
                for embed in msg.embeds:
                    if embed.footer and grupo_encontrado["grupo_id"] in embed.footer.text:
                        try:
                            await msg.delete()
                        except:
                            pass
    
    await ctx.send(f"✅ Grupo **{grupo_encontrado['nome_org']}** removido com sucesso!")


# =========================================================
# ==================== PAINÉIS =============================
# =========================================================

async def enviar_painel_registro():
    canal = bot.get_channel(CANAL_REGISTRO_ID)
    if not canal:
        print("❌ Canal registro não encontrado")
        return
    embed = discord.Embed(title="📋 Registro", description="Clique no botão abaixo para realizar seu registro.", color=0x2ecc71)
    async for msg in canal.history(limit=20):
        if msg.author == bot.user and msg.embeds and msg.embeds[0].title == "📋 Registro":
            try:
                await msg.edit(embed=embed, view=RegistroView())
                return
            except:
                pass
    await canal.send(embed=embed, view=RegistroView())
    print("✅ Painel de registro criado")


async def enviar_painel_vendas():
    canal = bot.get_channel(CANAL_VENDAS_ID)
    if not canal:
        print("❌ Canal de vendas não encontrado")
        return
    estoque = await carregar_estoque()
    embed = discord.Embed(title="🛒 Painel de Vendas", description="Escolha uma opção abaixo.\n\n⚠️ **ATENÇÃO:** Antes de entregar um pedido, verifique se há ESTOQUE disponível!", color=0x2ecc71)
    embed.add_field(name="📦 ESTOQUE DISPONÍVEL", value=f"🔫 PT: **{fmt_num(estoque['PT'])}** pacotes\n🔫 SUB: **{fmt_num(estoque['SUB'])}** pacotes", inline=False)
    await enviar_ou_atualizar_painel("painel_vendas", CANAL_VENDAS_ID, embed, CalculadoraView())
    print("🛒 Painel de vendas verificado/atualizado")


async def enviar_painel_fabricacao():
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    if not canal:
        print("❌ Canal de fabricação não encontrado")
        return
    estoque_municoes = await carregar_estoque()
    estoque_insumos = await carregar_estoque_insumos()
    embed = discord.Embed(title="🏭 PAINEL DE FABRICAÇÃO", description="**Gerencie a produção e estoque:**", color=0x2ecc71)
    embed.add_field(name="📦 ESTOQUE DE MUNIÇÃO", value=f"🔫 **PT:** {fmt_num(estoque_municoes['PT'])} pacotes\n🔫 **SUB:** {fmt_num(estoque_municoes['SUB'])} pacotes", inline=False)
    embed.add_field(name="💊 ESTOQUE DE INSUMOS", value=f"**Cápsulas:** {fmt_num(estoque_insumos['capsulas'])} unidades\n**Embalagens:** {fmt_num(estoque_insumos['embalagens'])} unidades", inline=False)
    embed.add_field(name="🏭 PRODUÇÃO DE CÁPSULAS", value="• **Galpões Norte:** 65 minutos (3 galpões)\n• **Galpões Sul:** 130 minutos (3 galpões)\n\n💡 Ao clicar, informe:\n   - Quantos galpões (1, 2 ou 3)\n   - Pólvora por galpão", inline=False)
    embed.set_footer(text="Utilize os botões abaixo para gerenciar o estoque")
    
    async for msg in canal.history(limit=10):
        if msg.author == bot.user and msg.embeds and msg.embeds[0].title == "🏭 PAINEL DE FABRICAÇÃO":
            try:
                await msg.delete()
            except:
                pass
    
    await canal.send(embed=embed, view=FabricacaoView())
    print("🏭 Painel de fabricação enviado")


async def enviar_painel_polvoras():
    canal = bot.get_channel(CANAL_CALCULO_POLVORA_ID)
    if not canal:
        print("❌ Canal de pólvora não encontrado")
        return
    embed = discord.Embed(title="💣 Registro de Pólvora", description="Clique no botão para registrar.", color=0xe67e22)
    await enviar_ou_atualizar_painel("painel_polvora", CANAL_CALCULO_POLVORA_ID, embed, PolvoraView())
    print("💣 Painel de pólvora verificado/atualizado")


async def enviar_painel_lavagem():
    canal = bot.get_channel(CANAL_INICIAR_LAVAGEM_ID)
    if not canal:
        print("❌ Canal de lavagem não encontrado")
        return
    embed = discord.Embed(title="🧼 Lavagem de Dinheiro", description="Clique para iniciar lavagem.", color=0x27ae60)
    await enviar_ou_atualizar_painel("painel_lavagem", CANAL_INICIAR_LAVAGEM_ID, embed, LavagemView())
    print("🧼 Painel de lavagem verificado/atualizado")


async def enviar_painel_lives():
    canal = bot.get_channel(CANAL_CADASTRO_LIVE_ID)
    if not canal:
        print("❌ Canal cadastro live não encontrado")
        return
    embed = discord.Embed(title="🎥 CADASTRO DE LIVE", description="**Clique no botão abaixo para cadastrar sua live!**\n\n📌 **Plataformas suportadas:**\n• Twitch\n• Kick\n• TikTok\n\n⚠️ **Importante:**\n• Cadastre apenas suas próprias lives\n• O link será verificado automaticamente\n• Quando você iniciar uma live, será divulgada automaticamente!", color=0x9146FF)
    await enviar_ou_atualizar_painel("painel_lives", CANAL_CADASTRO_LIVE_ID, embed, CadastrarLiveView())
    print("🎥 Painel de cadastro de live verificado/atualizado")


async def enviar_painel_admin_lives():
    embed = discord.Embed(title="⚙️ Painel ADM - Lives", description="Gerencie todas as lives cadastradas.", color=0xe74c3c)
    await enviar_ou_atualizar_painel("painel_lives_admin", CANAL_CADASTRO_LIVE_ID, embed, PainelLivesAdmin())


class SolicitarSalaView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="➕ Criar Minha Sala", style=discord.ButtonStyle.success, custom_id="criar_sala_manual")
    async def criar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if str(interaction.user.id) in metas_cache:
            await interaction.response.send_message("Você já possui uma sala.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await criar_sala_meta(interaction.user)
        await interaction.followup.send("Sua sala foi criada com sucesso!", ephemeral=True)


async def enviar_painel_solicitar_sala():
    canal = bot.get_channel(CANAL_SOLICITAR_SALA_ID)
    if not canal:
        print("❌ Canal solicitar sala não encontrado")
        return
    embed = discord.Embed(title="📂 Solicitar Sala", description="Clique no botão para criar sua sala.", color=0x2ecc71)
    await enviar_ou_atualizar_painel("painel_solicitar_sala", CANAL_SOLICITAR_SALA_ID, embed, SolicitarSalaView())


async def enviar_painel_acoes(guild):
    canal = guild.get_channel(CANAL_ESCALACOES_ID)
    if not canal:
        print("❌ Canal ações não encontrado")
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


async def enviar_painel_botao_ausencia():
    canal = bot.get_channel(CANAL_BOTAO_AUSENCIA_ID)
    if not canal:
        print(f"❌ Canal do botão NÃO ENCONTRADO! ID: {CANAL_BOTAO_AUSENCIA_ID}")
        return
    embed = discord.Embed(title="📋 Solicitar Ausência", description="Clique no botão abaixo para solicitar sua ausência.\n\n📌 **Como usar:**\n• Digite seu nome completo\n• Informe a **data de INÍCIO** (ex: `10/04/2026`)\n• Informe a **data de RETORNO** (ex: `15/04/2026`)\n• Digite o motivo\n\n✅ Você receberá o cargo **Ausente**\n✅ Quando o período acabar, o cargo será removido\n\n⚠️ **Ausências de 15 dias ou mais** serão notificadas à gerência", color=0xe67e22)
    embed.add_field(name="📅 Exemplo", value="• Data INÍCIO: `10/04/2026`\n• Data RETORNO: `15/04/2026`\n(contando todos os dias entre 10 e 15)", inline=False)
    await enviar_ou_atualizar_painel("painel_botao_ausencia", CANAL_BOTAO_AUSENCIA_ID, embed, AusenciaBotaoView())
    print(f"✅ Painel do botão enviado para {CANAL_BOTAO_AUSENCIA_ID}")


async def enviar_painel_remover_ausencia():
    canal = bot.get_channel(CANAL_BOTAO_AUSENCIA_ID)
    if not canal:
        print(f"❌ Canal do botão NÃO ENCONTRADO! ID: {CANAL_BOTAO_AUSENCIA_ID}")
        return
    async for msg in canal.history(limit=30):
        if msg.author == bot.user and msg.embeds and msg.embeds[0].title == "🔄 Remover Ausência (Retorno Antecipado)":
            return
    embed = discord.Embed(title="🔄 Remover Ausência (Retorno Antecipado)", description="Clique no botão abaixo caso um membro tenha **retornado antes do previsto**.\n\n⚠️ **Apenas para:** Gerente, Cargo 01, Cargo 02 e Gerente Geral", color=0x3498db)
    embed.add_field(name="📌 Como usar", value="1. Clique no botão\n2. Selecione o membro na lista\n3. Confirme a remoção\n\nO cargo **Ausente** será removido imediatamente.", inline=False)
    await enviar_ou_atualizar_painel("painel_remover_ausencia", CANAL_BOTAO_AUSENCIA_ID, embed, BotaoRemoverAusenciaView())
    print(f"✅ Painel de remover ausência enviado para {CANAL_BOTAO_AUSENCIA_ID}")


async def enviar_painel_alugueis():
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    if not canal:
        print("❌ Canal de fabricação não encontrado")
        return
    
    await carregar_alugueis_db()
    
    embed = discord.Embed(title="🏭💰 CONTROLE DE ALUGUEL - GALPÕES", description="**Contagem regressiva atualizada automaticamente**", color=0x3498db)
    view = AluguelView()
    
    if "GALPÕES NORTE" in alugueis_ativos:
        dados = alugueis_ativos["GALPÕES NORTE"]
        data_fim = dados["fim"]
        barra_prog = calcular_barra_progresso(data_fim, dados["dias"])
        embed.add_field(name="🏭 GALPÕES NORTE", value=f"**STATUS:** 🔵 ALUGADO\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n**👤 Alugado Por:** <@{dados['user_id']}>\n**📅 Dias alugados:** {dados['dias']} dia(s)\n**⏰ Vence em:** <t:{int(data_fim.timestamp())}:F>\n**🕐 TEMPO RESTANTE:**\n{formatar_tempo_detalhado(data_fim)}\n{barra_prog}", inline=False)
        view.adicionar_botoes_edicao("GALPÕES NORTE", dados['user_id'], dados['dias'])
    else:
        embed.add_field(name="🏭 GALPÕES NORTE", value=f"**STATUS:** 🟢 DISPONÍVEL\n━━━━━━━━━━━━━━━━━━━━\n**💰 Clique no botão abaixo para alugar**\n**📅 Aluguel mínimo:** 1 dia", inline=False)
    
    if "GALPÕES SUL" in alugueis_ativos:
        dados = alugueis_ativos["GALPÕES SUL"]
        data_fim = dados["fim"]
        barra_prog = calcular_barra_progresso(data_fim, dados["dias"])
        embed.add_field(name="🏭 GALPÕES SUL", value=f"**STATUS:** 🔵 ALUGADO\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n**👤 Alugado Por:** <@{dados['user_id']}>\n**📅 Dias alugados:** {dados['dias']} dia(s)\n**⏰ Vence em:** <t:{int(data_fim.timestamp())}:F>\n**🕐 TEMPO RESTANTE:**\n{formatar_tempo_detalhado(data_fim)}\n{barra_prog}", inline=False)
        view.adicionar_botoes_edicao("GALPÕES SUL", dados['user_id'], dados['dias'])
    else:
        embed.add_field(name="🏭 GALPÕES SUL", value=f"**STATUS:** 🟢 DISPONÍVEL\n━━━━━━━━━━━━━━━━━━━━\n**💰 Clique no botão abaixo para alugar**\n**📅 Aluguel mínimo:** 1 dia", inline=False)
    
    embed.set_footer(text="⏱️ Atualiza automaticamente a cada minuto")
    await enviar_ou_atualizar_painel("painel_alugueis", CANAL_FABRICACAO_ID, embed, view)


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


async def enviar_painel_relatorio_financeiro():
    canal = bot.get_channel(CANAL_RELATORIO_FINANCEIRO_ID)
    if not canal:
        print("❌ Canal de relatório financeiro não encontrado")
        return
    embed = discord.Embed(title="💰 RELATÓRIO FINANCEIRO", description="Clique no botão abaixo para gerar um relatório financeiro completo.\n\n📋 **O relatório inclui:**\n• 💣 Pólvora utilizada na produção\n• 💰 Gasto total com pólvora\n• 🛒 Total de vendas no período\n• 📦 Gasto com embalagens (opcional)\n• 📦 Outras compras registradas\n• 📊 Saldo final (vendas - gastos)\n\n📅 **Você pode escolher:**\n• Data inicial e final\n• Incluir ou não outras compras (SIM/NAO)", color=0x1abc9c)
    embed.add_field(name="📌 EXEMPLO DE PREENCHIMENTO", value="**Data inicial:** `01/04/2026`\n**Data final:** `30/04/2026`\n**Incluir compras:** `SIM` (ou `NAO`)", inline=False)
    embed.set_footer(text="Os valores são calculados automaticamente com base no banco de dados")
    await enviar_ou_atualizar_painel("painel_relatorio_financeiro", CANAL_RELATORIO_FINANCEIRO_ID, embed, RelatorioFinanceiroView())
    print("💰 Painel de relatório financeiro enviado/atualizado")


async def enviar_painel_registrar_compra():
    canal = bot.get_channel(CANAL_REGISTRAR_COMPRA_ID)
    if not canal:
        print(f"❌ Canal de registrar compra não encontrado: {CANAL_REGISTRAR_COMPRA_ID}")
        return
    embed = discord.Embed(title="💰 REGISTRAR COMPRA", description="Clique no botão abaixo para registrar uma nova compra.\n\n📋 **Informações necessárias:**\n• 📦 Nome do produto\n• 💰 Valor da compra\n\nApós registrar, a compra aparecerá automaticamente no canal de registros.", color=0x3498db)
    embed.add_field(name="📌 EXEMPLO", value="**Produto:** Pólvora\n**Valor:** 50000", inline=False)
    embed.set_footer(text="Todas as compras ficam salvas no banco de dados para relatórios futuros")
    
    async for msg in canal.history(limit=10):
        if msg.author == bot.user and msg.embeds and msg.embeds[0].title == "💰 REGISTRAR COMPRA":
            try:
                await msg.delete()
            except:
                pass
    
    await canal.send(embed=embed, view=RegistrarCompraView())
    print(f"💰 Painel de registrar compra enviado para o canal {CANAL_REGISTRAR_COMPRA_ID}")


# =========================================================
# ==================== LOOP DE VERIFICAÇÃO ================
# =========================================================

@tasks.loop(seconds=30)
async def verificar_producoes_ativas():
    """Verifica se as produções estão sendo atualizadas"""
    try:
        async with get_db().acquire() as conn:
            rows = await conn.fetch("SELECT pid FROM producoes WHERE CAST(fim AS timestamp) > NOW()")
        
        for r in rows:
            pid = r["pid"]
            
            if pid not in producoes_tasks or producoes_tasks[pid].done():
                print(f"🔧 Task morta para {pid}, recriando...")
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
                        print(f"⚠️ Mensagem perdida para {pid}, recriando...")
                        desc = await gerar_desc_producao(prod)
                        embed = discord.Embed(title="🏭 Produção", description=desc, color=0x3498db)
                        view = None if prod.get("segunda_task_confirmada") else SegundaTaskView(pid)
                        msg = await canal.send(embed=embed, view=view)
                        prod["msg_id"] = msg.id
                        await salvar_producao(pid, prod)
                        print(f"✅ Mensagem recriada para {pid}")
    
    except Exception as e:
        print(f"❌ Erro no verificar_producoes_ativas: {e}")

# =========================================================
# ==================== RESTAURAR PRODUÇÕES ================
# =========================================================

async def restaurar_producoes():
    try:
        print("🔄 Iniciando restauração de produções...")
        async with get_db().acquire() as conn:
            rows = await conn.fetch("SELECT pid FROM producoes WHERE CAST(fim AS timestamp) > NOW()")
        
        if not rows:
            print("📭 Nenhuma produção ativa para restaurar")
            return
        
        print(f"🏭 Encontradas {len(rows)} produções ativas para restaurar")
        for r in rows:
            pid = r["pid"]
            if pid in producoes_tasks:
                print(f"⏩ Produção {pid} já está sendo acompanhada")
                continue
            print(f"🔧 Restaurando produção: {pid}")
            task = bot.loop.create_task(acompanhar_producao(pid))
            producoes_tasks[pid] = task
    except Exception as e:
        print(f"❌ ERRO CRÍTICO ao restaurar produções: {e}")
        import traceback
        traceback.print_exc()

# =========================================================
# ==================== SISTEMA DE GRUPOS ===================
# =========================================================

# =========================================================
# ==================== MODAIS DE GRUPOS ====================
# =========================================================

class RegistrarGrupoModal(discord.ui.Modal, title="📋 Registrar Novo Grupo"):
    
    nome_org = discord.ui.TextInput(
        label="🏷️ Nome da Organização",
        placeholder="Ex: VDR, Polícia, Mafia, etc",
        required=True,
        max_length=50
    )
    
    lider = discord.ui.TextInput(
        label="👤 Líder (Nome e Telefone)",
        placeholder="Ex: João Silva - (11) 99999-9999",
        required=True,
        max_length=100
    )
    
    braco = discord.ui.TextInput(
        label="👤 Braço (Nome e Telefone - opcional)",
        placeholder="Ex: José Santos - (11) 88888-8888",
        required=False,
        max_length=100
    )
    
    produto = discord.ui.TextInput(
        label="🔫 Produto que fornece",
        placeholder="Ex: PT, SUB, Ambos, etc",
        required=True,
        max_length=50
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        
        await interaction.response.defer(ephemeral=True)
        
        # Separar nome e telefone do líder
        lider_parts = self.lider.value.strip().split(" - ")
        lider_nome = lider_parts[0] if lider_parts else self.lider.value
        lider_telefone = lider_parts[1] if len(lider_parts) > 1 else "Não informado"
        
        # Separar nome e telefone do braço (se informado)
        braco_nome = None
        braco_telefone = None
        if self.braco.value:
            braco_parts = self.braco.value.strip().split(" - ")
            braco_nome = braco_parts[0] if braco_parts else self.braco.value
            braco_telefone = braco_parts[1] if len(braco_parts) > 1 else "Não informado"
        
        import time
        grupo_id = f"GRUPO_{int(time.time())}_{interaction.user.id}"
        
        await salvar_grupo_db(
            grupo_id,
            self.nome_org.value.strip(),
            lider_nome,
            lider_telefone,
            braco_nome,
            braco_telefone,
            self.produto.value.strip()
        )
        
        await enviar_embed_grupo(grupo_id)
        
        await interaction.followup.send(
            f"✅ **Grupo {self.nome_org.value} registrado com sucesso!**\n"
            f"📋 ID: `{grupo_id}`",
            ephemeral=True
        )


class EditarGrupoModal(discord.ui.Modal, title="✏️ Editar Grupo"):
    
    def __init__(self, grupo_id, dados):
        super().__init__()
        self.grupo_id = grupo_id
        self.dados = dados
        
        self.nome_org.default = dados["nome_org"]
        lider_texto = f"{dados['lider_nome']} - {dados['lider_telefone']}"
        self.lider.default = lider_texto
        if dados.get('braco_nome') and dados.get('braco_telefone'):
            self.braco.default = f"{dados['braco_nome']} - {dados['braco_telefone']}"
        elif dados.get('braco_nome'):
            self.braco.default = dados['braco_nome']
        else:
            self.braco.default = ""
        self.produto.default = dados["produto"]
    
    nome_org = discord.ui.TextInput(
        label="🏷️ Nome da Organização",
        required=True,
        max_length=50
    )
    
    lider = discord.ui.TextInput(
        label="👤 Líder (Nome - Telefone)",
        required=True,
        max_length=100
    )
    
    braco = discord.ui.TextInput(
        label="👤 Braço (Nome - Telefone - opcional)",
        required=False,
        max_length=100
    )
    
    produto = discord.ui.TextInput(
        label="🔫 Produto que fornece",
        required=True,
        max_length=50
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        
        await interaction.response.defer(ephemeral=True)
        
        lider_parts = self.lider.value.strip().split(" - ")
        lider_nome = lider_parts[0] if lider_parts else self.lider.value
        lider_telefone = lider_parts[1] if len(lider_parts) > 1 else "Não informado"
        
        braco_nome = None
        braco_telefone = None
        if self.braco.value:
            braco_parts = self.braco.value.strip().split(" - ")
            braco_nome = braco_parts[0] if braco_parts else self.braco.value
            braco_telefone = braco_parts[1] if len(braco_parts) > 1 else "Não informado"
        
        await atualizar_grupo_db(
            self.grupo_id,
            self.nome_org.value.strip(),
            lider_nome,
            lider_telefone,
            braco_nome,
            braco_telefone,
            self.produto.value.strip()
        )
        
        await enviar_embed_grupo(self.grupo_id)
        
        await interaction.followup.send(
            f"✅ **Grupo {self.nome_org.value} atualizado com sucesso!**",
            ephemeral=True
        )


# =========================================================
# ==================== VIEWS DE GRUPOS ====================
# =========================================================

class ConfirmarExcluirView(discord.ui.View):
    def __init__(self, grupo_id):
        super().__init__(timeout=30)
        self.grupo_id = grupo_id
    
    @discord.ui.button(label="✅ Sim, Excluir", style=discord.ButtonStyle.danger, emoji="✅")
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        await desativar_grupo_db(self.grupo_id)
        
        canal = interaction.guild.get_channel(CANAL_GRUPOS_ID)
        if canal:
            async for msg in canal.history(limit=50):
                if msg.author == interaction.client.user and msg.embeds:
                    for embed in msg.embeds:
                        if embed.footer and self.grupo_id in embed.footer.text:
                            try:
                                await msg.delete()
                                await interaction.followup.send("✅ **Grupo excluído com sucesso!**", ephemeral=True)
                                return
                            except:
                                pass
        
        await interaction.followup.send("✅ **Grupo excluído do banco de dados!**", ephemeral=True)
    
    @discord.ui.button(label="❌ Cancelar", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancelar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ Operação cancelada.", ephemeral=True)


class GrupoView(discord.ui.View):
    def __init__(self, grupo_id, nome_org):
        super().__init__(timeout=None)
        self.grupo_id = grupo_id
        self.nome_org = nome_org
    
    @discord.ui.button(
        label="✏️ Editar Grupo",
        style=discord.ButtonStyle.primary,
        custom_id="editar_grupo",
        emoji="✏️"
    )
    async def editar_grupo(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem editar grupos!", ephemeral=True)
            return
        
        dados = await carregar_grupo_db(self.grupo_id)
        if not dados:
            await interaction.response.send_message("❌ Grupo não encontrado!", ephemeral=True)
            return
        
        await interaction.response.send_modal(EditarGrupoModal(self.grupo_id, dados))
    
    @discord.ui.button(
        label="🗑️ Excluir Grupo",
        style=discord.ButtonStyle.danger,
        custom_id="excluir_grupo",
        emoji="🗑️"
    )
    async def excluir_grupo(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem excluir grupos!", ephemeral=True)
            return
        
        view = ConfirmarExcluirView(self.grupo_id)
        await interaction.response.send_message(
            f"⚠️ **Tem certeza que deseja excluir o grupo {self.nome_org}?**\n"
            f"Esta ação não pode ser desfeita!",
            view=view,
            ephemeral=True
        )


class RegistrarGrupoView(discord.ui.View):
    """View simples com botão para registrar grupo"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(
        label="📋 Registrar Novo Grupo",
        style=discord.ButtonStyle.success,
        custom_id="registrar_grupo_btn_simples",
        emoji="📋"
    )
    async def registrar_grupo(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem registrar grupos!", ephemeral=True)
            return
        
        await interaction.response.send_modal(RegistrarGrupoModal())


# =========================================================
# ==================== FUNÇÕES DE GRUPOS ===================
# =========================================================

async def enviar_embed_grupo(grupo_id):
    """Envia ou atualiza o embed de um grupo"""
    
    dados = await carregar_grupo_db(grupo_id)
    if not dados:
        print(f"❌ Grupo {grupo_id} não encontrado")
        return
    
    compras = await carregar_compras_grupo_db(grupo_id)
    
    canal = bot.get_channel(CANAL_GRUPOS_ID)
    if not canal:
        print(f"❌ Canal de grupos não encontrado")
        return
    
    # TUDO EM MAIÚSCULO
    nome_org = dados['nome_org'].upper()
    lider_nome = dados['lider_nome'].upper()
    lider_telefone = dados['lider_telefone'].upper()
    braco_nome = dados['braco_nome'].upper() if dados['braco_nome'] else None
    braco_telefone = dados['braco_telefone'].upper() if dados['braco_telefone'] else None
    produto = dados['produto'].upper()
    
    embed = discord.Embed(
        title=f"🏷️ {nome_org}",
        color=0x3498db,
        timestamp=agora()
    )
    
    info_grupo = (
        f"**👤 LÍDER:** {lider_nome}\n"
        f"**📱 TELEFONE:** {lider_telefone}\n"
    )
    
    if braco_nome:
        info_grupo += f"**👤 BRAÇO:** {braco_nome}\n"
    if braco_telefone:
        info_grupo += f"**📱 TELEFONE BRAÇO:** {braco_telefone}\n"
    
    info_grupo += f"\n**🔫 PRODUTO:** {produto}"
    
    embed.add_field(name="📋 INFORMAÇÕES DO GRUPO", value=info_grupo, inline=False)
    
    total_pt = compras.get("PT", {})
    total_sub = compras.get("SUB", {})
    
    compras_texto = ""
    
    if total_pt["quantidade"] > 0 or total_sub["quantidade"] > 0:
        if total_pt["quantidade"] > 0:
            compras_texto += (
                f"**🔫 PT:** {fmt_num(total_pt['quantidade'])} PACOTES\n"
                f"💰 {formatar_dinheiro(total_pt['valor'])}\n"
            )
        
        if total_sub["quantidade"] > 0:
            compras_texto += (
                f"**🔫 SUB:** {fmt_num(total_sub['quantidade'])} PACOTES\n"
                f"💰 {formatar_dinheiro(total_sub['valor'])}\n"
            )
        
        compras_texto += f"\n**📅 TOTAL DE COMPRAS:** {fmt_num(total_pt['quantidade'] + total_sub['quantidade'])} PACOTES"
    else:
        compras_texto = "📭 NENHUMA COMPRA REGISTRADA AINDA."
    
    embed.add_field(name="📦 HISTÓRICO DE COMPRAS", value=compras_texto, inline=False)
    embed.add_field(name="📌 STATUS", value="🟢 **ATIVO**", inline=False)
    embed.set_footer(text=f"ID: {grupo_id} • CRIADO EM {dados['data_criacao'].strftime('%d/%m/%Y')}")
    
    async for msg in canal.history(limit=50):
        if msg.author == bot.user and msg.embeds:
            for embed_msg in msg.embeds:
                if embed_msg.footer and grupo_id in embed_msg.footer.text:
                    try:
                        await msg.edit(embed=embed, view=GrupoView(grupo_id, nome_org))
                        print(f"✅ Grupo {nome_org} atualizado")
                        return
                    except Exception as e:
                        print(f"Erro ao atualizar embed: {e}")
    
    await canal.send(embed=embed, view=GrupoView(grupo_id, nome_org))
    print(f"✅ Grupo {nome_org} criado")


async def enviar_painel_registro_grupos():
    """Envia o painel com botão para registrar grupos"""
    
    canal = bot.get_channel(CANAL_REGISTRO_GRUPOS_ID)
    if not canal:
        print(f"❌ Canal de registro de grupos não encontrado: {CANAL_REGISTRO_GRUPOS_ID}")
        return
    
    embed = discord.Embed(
        title="📋 REGISTRO DE GRUPOS",
        description=(
            "**Clique no botão abaixo para registrar um novo grupo de clientes.**\n\n"
            "📌 **Informações necessárias:**\n"
            "• 🏷️ Nome da Organização\n"
            "• 👤 Nome do Líder\n"
            "• 📱 Telefone do Líder\n"
            "• 👤 Nome do Braço (opcional)\n"
            "• 📱 Telefone do Braço (opcional)\n"
            "• 🔫 Produto que fornece\n\n"
            "✅ Após o registro, o grupo aparecerá no canal de grupos."
        ),
        color=0x2ecc71
    )
    
    embed.add_field(
        name="📌 EXEMPLO",
        value=(
            "**Organização:** VDR\n"
            "**Líder:** João Silva - (11) 99999-9999\n"
            "**Produto:** PT e SUB"
        ),
        inline=False
    )
    
    embed.set_footer(text="Apenas ADM e Gerentes podem gerenciar grupos")
    
    # DELETAR MENSAGENS ANTIGAS
    async for msg in canal.history(limit=20):
        if msg.author == bot.user and msg.embeds:
            if msg.embeds[0].title == "📋 REGISTRO DE GRUPOS" or msg.embeds[0].title == "📋 GERENCIAMENTO DE GRUPOS":
                try:
                    await msg.delete()
                    print(f"🗑️ Mensagem antiga deletada")
                except:
                    pass
    
    await canal.send(embed=embed, view=RegistrarGrupoView())
    print(f"📋 Painel de registro de grupos enviado")


async def restaurar_grupos():
    """Restaura os embeds dos grupos ativos após reinício"""
    try:
        print("🔄 Restaurando grupos...")
        
        grupos = await carregar_grupos_db()
        if not grupos:
            print("📭 Nenhum grupo ativo para restaurar")
            return
        
        print(f"🏷️ Restaurando {len(grupos)} grupos...")
        
        for grupo in grupos:
            await enviar_embed_grupo(grupo["grupo_id"])
            await asyncio.sleep(2)
        
        print(f"✅ {len(grupos)} grupos restaurados")
        
    except Exception as e:
        print(f"❌ Erro ao restaurar grupos: {e}")
        import traceback
        traceback.print_exc()
        
# =========================================================
# ==================== ON_READY ===========================
# =========================================================

@bot.event
async def on_ready():
    global http_session, fila_clipes
    
    if hasattr(bot, "ja_iniciado"):
        return
    
    bot.ja_iniciado = True
    
    print("🔄 Iniciando configuração do bot...")
    print(f"Logado como {bot.user}")
    
    if not http_session:
        http_session = aiohttp.ClientSession()
    
    await conectar_db()
    
    guild = bot.get_guild(GUILD_ID)
    if guild:
        try:
            await guild.chunk()
            print("👥 Membros carregados no cache.")
        except Exception as e:
            print("Erro ao carregar membros:", e)
    
    print(f"🕒 Horário Brasília: {agora().strftime('%d/%m/%Y %H:%M:%S')}")
    
    # Carregar metas
    try:
        rows = await carregar_metas_db()
        for r in rows:
            metas_cache[str(r["user_id"])] = {"canal_id": int(r["canal_id"]), "dinheiro": r["dinheiro"], "polvora": r["polvora"], "acao": r["acao"]}
        print(f"📊 Metas carregadas: {len(metas_cache)}")
    except Exception as e:
        print("Erro ao carregar metas:", e)
    
    # Carregar aluguéis
    try:
        rows = await carregar_alugueis_db()
        for r in rows:
            data_fim = r["data_fim"]
            if data_fim.tzinfo is None:
                data_fim = data_fim.replace(tzinfo=BRASIL)
            alugueis_ativos[r["galpao"]] = {"user_id": int(r["user_id"]), "fim": data_fim, "dias": r["dias"]}
        print(f"💰 Aluguéis carregados: {len(alugueis_ativos)}")
    except Exception as e:
        print("Erro ao carregar aluguéis:", e)
    
    # Registrar views
    try:
        bot.add_view(RegistroView())
        bot.add_view(SolicitarSalaView())
        bot.add_view(CadastrarLiveView())
        bot.add_view(PainelLivesAdmin())
        bot.add_view(PolvoraView())
        bot.add_view(ConfirmarPagamentoView())
        bot.add_view(LavagemView())
        bot.add_view(FabricacaoView())
        bot.add_view(CalculadoraView())
        bot.add_view(StatusView())
        bot.add_view(PainelAcoesView())
    except Exception as e:
        print(f"Erro ao registrar views: {e}")
    
    # Filas
    fila_clipes = asyncio.Queue()
    bot.loop.create_task(worker_clipes())
    print("🎬 Sistema de clips ON")
    
    if not hasattr(bot, "edit_worker_started"):
        bot.loop.create_task(edit_worker())
        bot.edit_worker_started = True
        print("🛠️ Edit worker iniciado.")
    
    # Iniciar loops
    try:
        if not verificar_lives.is_running():
            verificar_lives.start()
    except Exception as e:
        print("Erro loop lives:", e)
    
    try:
        if not relatorio_semanal_polvoras.is_running():
            relatorio_semanal_polvoras.start()
    except Exception as e:
        print("Erro loop polvora:", e)
    
    try:
        if not verificar_ausencias_expiradas.is_running():
            verificar_ausencias_expiradas.start()
    except Exception as e:
        print("Erro loop ausência:", e)
    
    try:
        if not limpar_lavagens_pendentes.is_running():
            limpar_lavagens_pendentes.start()
    except Exception as e:
        print("Erro loop limpeza lavagens:", e)
    
    try:
        if not atualizar_contagem_alugueis.is_running():
            atualizar_contagem_alugueis.start()
    except Exception as e:
        print("Erro loop contagem:", e)
    
    try:
        if not verificar_alugueis_expirados.is_running():
            verificar_alugueis_expirados.start()
    except Exception as e:
        print("Erro loop expirados:", e)

    # Iniciar loop de verificação de produções
    try:
        if not verificar_producoes_ativas.is_running():
            verificar_producoes_ativas.start()
            print("🔄 Loop de verificação de produções iniciado")
    except Exception as e:
        print("Erro ao iniciar loop de produções:", e)
    
    # Restaurar produções
    await restaurar_producoes()

    # Restaurar grupos
    try:
        await restaurar_grupos()
    except Exception as e:
        print("Erro ao restaurar grupos:", e)

   # Enviar painel de grupos
    try:
        await enviar_painel_registro_grupos()
    except Exception as e:
        print("Erro ao enviar painel registro grupos:", e)
    
# Iniciar task de processamento de grupos
    #try:
    #    global task_atualizacao_grupos
    #    if task_atualizacao_grupos is None or task_atualizacao_grupos.done():
     #       task_atualizacao_grupos = asyncio.create_task(processar_fila_grupos())
    #        print("🔄 Task de processamento de grupos iniciada")
   # except Exception as e:
   #     print(f"Erro ao iniciar task de grupos: {e}")
    #    import traceback
     #   traceback.print_exc()
    
    # Restaurar galpões ativos
    try:
        async with get_db().acquire() as conn:
            rows = await conn.fetch("SELECT galpao FROM producoes WHERE CAST(fim AS timestamp) > NOW()")
        for r in rows:
            galpoes_ativos.add(r["galpao"])
        print(f"🏭 Galpões ativos restaurados: {len(rows)}")
    except Exception as e:
        print("Erro restaurar galpões:", e)
    
    # Enviar painéis
    await asyncio.sleep(2)
    
    try:
        tasks = [
            enviar_painel_registro(),
            enviar_painel_fabricacao(),
            enviar_painel_lives(),
            enviar_painel_admin_lives(),
            enviar_painel_polvoras(),
            enviar_painel_lavagem(),
            enviar_painel_vendas(),
            enviar_painel_remover_ausencia(),
            enviar_painel_relatorio_financeiro(),
            enviar_painel_registrar_compra(),
            enviar_painel_solicitar_sala(),
            enviar_painel_botao_ausencia(),
            enviar_painel_registro_grupos(),
        ]
        
        if guild:
            tasks.append(enviar_painel_acoes(guild))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                print("Erro em painel específico:", r)
        
        print("🖥️ Painéis verificados/enviados.")
    except Exception as e:
        print("Erro geral ao enviar painéis:", e)
    
    # Botão reset ações
    if guild:
        try:
            canal = guild.get_channel(CANAL_RELATORIO_ACOES_ID)
            if canal:
                async for msg in canal.history(limit=10):
                    if msg.author == bot.user and msg.components:
                        break
                else:
                    await canal.send("🔄 Controle de Ações:", view=ResetAcoesView())
        except Exception as e:
            print("Erro botão reset ações:", e)
    
    gc.collect()
    print("🧹 Limpeza de memória executada")
    print("=========================================")
    print("✅ BOT ONLINE 100% ESTÁVEL")
    print("=========================================")


async def worker_clipes():
    global fila_clipes
    print("🎬 Worker clips iniciado")
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
            print("ERRO CLIP:", e)
            await message.reply("❌ Erro ao enviar.")
        await asyncio.sleep(5)
        fila_clipes.task_done()


# =========================================================
# ==================== START ==============================
# =========================================================

if __name__ == "__main__":
    print("🚀 Iniciando bot...")
    bot.run(TOKEN)
