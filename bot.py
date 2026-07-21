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
CANAL_TEXTOS_VENDAS_ID = 1499045083994001500

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
CANAL_CONTROLE_ARMAS_ID = 1528938987362848972
CANAL_BAU_ENTROU_ID = 1337358932158578719
CANAL_BAU_SAIU_ID = 1337358898784632882
CANAL_CONTROLE_HC_ID = 1528932487500398752
CANAL_CONTROLE_ACAO_ID = 1528934890135883946
CANAL_ARMAS_ENTROU_ID = 1500983878045798430
CANAL_ARMAS_SAIU_ID = 1500983930533187734
CANAL_ARMAS_PERDEU_ID = 1500984222104686693

# =========================================================
# ==================== IDS DOS CARGOS =====================
# =========================================================

# REGISTRO
AGREGADO_ROLE_ID = 1422847202937536532
CONVIDADO_ROLE_ID = 1337382961456353342
EM_REGISTRO_ROLE_ID = 1337382961456353342
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

# =========================================================
# ==================== FUNÇÃO CALCULAR SEMANA ANTERIOR =====
# =========================================================

def calcular_semana_anterior():
    """
    Calcula a semana anterior completa (Segunda a Domingo)
    SEMPRE retorna a semana anterior, independente do dia atual
    """
    hoje = agora()
    
    # Descobrir em que dia da semana estamos
    # 0 = Segunda, 1 = Terça, 2 = Quarta, 3 = Quinta, 4 = Sexta, 5 = Sábado, 6 = Domingo
    dia_semana = hoje.weekday()
    
    # Calcular quantos dias voltar para chegar no domingo anterior
    # Se hoje é Segunda (0), voltar 1 dia para o domingo anterior
    # Se hoje é Terça (1), voltar 2 dias
    # Se hoje é Quarta (2), voltar 3 dias
    # ...
    # Se hoje é Domingo (6), voltar 7 dias
    dias_para_domingo_anterior = dia_semana + 1
    
    domingo_anterior = hoje - timedelta(days=dias_para_domingo_anterior)
    segunda_anterior = domingo_anterior - timedelta(days=6)
    
    # Ajustar para meia-noite
    segunda_anterior = segunda_anterior.replace(hour=0, minute=0, second=0, microsecond=0)
    domingo_anterior = domingo_anterior.replace(hour=23, minute=59, second=59, microsecond=0)
    
    return segunda_anterior, domingo_anterior

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
    if not member:
        return None
    
    roles = [r.id for r in member.roles]
    
    # Verificar cargos em ordem de prioridade (do maior para o menor)
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
    
    # Se não tem nenhum cargo específico, retorna None
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
async def salvar_polvora_db(user_id, qtd, valor):
    """Salva compra de pólvora no banco"""
    async with get_db().acquire() as conn:
        # Converter para string ISO (já que data é TEXT)
        data_str = agora().isoformat()
        await conn.execute(
            "INSERT INTO polvoras (user_id, quantidade, valor, data) VALUES ($1, $2, $3, $4)",
            str(user_id), 
            qtd, 
            valor, 
            data_str  # ← STRING
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
    """Salva meta no banco garantindo que acao seja string"""
    async with get_db().acquire() as conn:
        # 🔥 CORREÇÃO: Garantir que acao seja string
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

async def depositar_na_meta_db(user_id, valor):
    async with get_db().acquire() as conn:
        meta = await conn.fetchrow("SELECT dinheiro FROM metas WHERE user_id = $1", str(user_id))
        if meta:
            novo_valor = meta["dinheiro"] + valor
            await conn.execute("UPDATE metas SET dinheiro = $1 WHERE user_id = $2", novo_valor, str(user_id))
            return True
        return False

# =========================================================
# ==================== METAS - NOVAS QUERIES ==============
# =========================================================

async def adicionar_polvora_meta(user_id, quantidade):
    """Adiciona pólvora à meta do usuário"""
    async with get_db().acquire() as conn:
        meta = await conn.fetchrow("SELECT polvora FROM metas WHERE user_id = $1", str(user_id))
        if meta:
            novo_valor = meta["polvora"] + quantidade
            await conn.execute("UPDATE metas SET polvora = $1 WHERE user_id = $2", novo_valor, str(user_id))
            return True
        return False

async def adicionar_dinheiro_meta(user_id, valor):
    """Adiciona dinheiro à meta do usuário"""
    async with get_db().acquire() as conn:
        meta = await conn.fetchrow("SELECT dinheiro FROM metas WHERE user_id = $1", str(user_id))
        if meta:
            novo_valor = meta["dinheiro"] + valor
            await conn.execute("UPDATE metas SET dinheiro = $1 WHERE user_id = $2", novo_valor, str(user_id))
            return True
        return False

async def fechar_meta(user_id, data_inicio, data_fim):
    """Fecha uma meta e registra no histórico"""
    async with get_db().acquire() as conn:
        meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(user_id))
        if not meta:
            return None
        
        # 🔥 CORREÇÃO: Converter acao para string
        acao = meta.get("acao")
        if acao is None:
            acao = "N/A"
        elif isinstance(acao, (int, float)):
            acao = str(acao)
        else:
            acao = str(acao)
        
        print(f"📝 Salvando acao como: {acao} (tipo: {type(acao)})")
        
        # Registrar no histórico
        await conn.execute(
            """
            INSERT INTO metas_historico (user_id, dinheiro, polvora, acao, data_inicio, data_fim, data_fechamento)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            str(user_id), 
            meta["dinheiro"], 
            meta["polvora"], 
            acao,  # ← AGORA É STRING
            data_inicio, 
            data_fim, 
            agora_db()
        )
        
        # Resetar a meta
        await conn.execute(
            "UPDATE metas SET dinheiro = 0, polvora = 0 WHERE user_id = $1",
            str(user_id)
        )
        
        return {
            "dinheiro": meta["dinheiro"],
            "polvora": meta["polvora"],
            "acao": acao
        }
        
async def buscar_historico_metas(data_inicio, data_fim):
    """Busca histórico de metas fechadas no período"""
    async with get_db().acquire() as conn:
        return await conn.fetch(
            """
            SELECT * FROM metas_historico 
            WHERE data_fechamento BETWEEN $1 AND $2
            ORDER BY data_fechamento DESC
            """,
            data_inicio, data_fim
        )

async def fechar_todas_metas(data_inicio, data_fim):
    """Fecha todas as metas ativas e retorna o relatório"""
    async with get_db().acquire() as conn:
        # Buscar todas as metas ativas
        metas = await conn.fetch("SELECT * FROM metas")
        
        if not metas:
            return None, []
        
        relatorio = []
        
        for meta in metas:
            user_id = meta["user_id"]
            dinheiro = meta["dinheiro"] or 0
            polvora = meta["polvora"] or 0
            dinheiro_acoes = meta.get("dinheiro_acoes") or 0
            
            # 🔥 CORREÇÃO: Converter acao para string com tratamento seguro
            acao = meta.get("acao")
            if acao is None:
                acao = "N/A"
            elif isinstance(acao, (int, float)):
                acao = str(acao)  # Se for número, vira string
            else:
                acao = str(acao)  # Garantir que é string
            
            print(f"📝 Salvando acao como: {acao} (tipo: {type(acao)})")
            
            # Registrar no histórico - usando $1, $2, etc com tipos corretos
            await conn.execute(
                """
                INSERT INTO metas_historico (user_id, dinheiro, polvora, acao, dinheiro_acoes, data_inicio, data_fim, data_fechamento)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                str(user_id),  # $1 - string
                dinheiro,      # $2 - int
                polvora,       # $3 - int
                acao,          # $4 - string (AGORA É STRING)
                dinheiro_acoes, # $5 - int
                data_inicio,   # $6 - date
                data_fim,      # $7 - date
                agora_db()     # $8 - datetime
            )
            
            # Resetar a meta
            await conn.execute(
                "UPDATE metas SET dinheiro = 0, polvora = 0, dinheiro_acoes = 0 WHERE user_id = $1",
                str(user_id)
            )
            
            relatorio.append({
                "user_id": user_id,
                "dinheiro": dinheiro,
                "polvora": polvora,
                "acao": acao,
                "dinheiro_acoes": dinheiro_acoes,
                "total": dinheiro + dinheiro_acoes
            })
        
        # Buscar membros que NÃO têm meta
        guild = bot.get_guild(GUILD_ID)
        membros_sem_meta = []
        
        if guild:
            cargos_meta = [
                AGREGADO_ROLE_ID,
                CARGO_MEMBRO_ID,
                CARGO_SOLDADO_ID,
                CARGO_01_ID,
                CARGO_02_ID,
                CARGO_GERENTE_ID,
                CARGO_GERENTE_GERAL_ID,
                CARGO_RESP_METAS_ID,
                CARGO_RESP_ACAO_ID,
                CARGO_RESP_VENDAS_ID,
                CARGO_RESP_PRODUCAO_ID
            ]
            
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

async def verificar_meta_concluida(user_id, valor_total):
    """Verifica se a meta atingiu R$ 300.000 e envia aviso"""
    if valor_total >= 300000:
        async with get_db().acquire() as conn:
            # Verificar se já foi avisado
            ja_avisado = await conn.fetchval(
                "SELECT 1 FROM metas_avisos WHERE user_id = $1 AND tipo = 'concluida' AND data > NOW() - INTERVAL '1 day'",
                str(user_id)
            )
            
            if not ja_avisado:
                # Registrar aviso
                await conn.execute(
                    "INSERT INTO metas_avisos (user_id, tipo, data) VALUES ($1, 'concluida', $2)",
                    str(user_id), agora_db()
                )
                
                # Buscar canal da meta
                canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(user_id))
                if canal_id:
                    canal = bot.get_channel(int(canal_id))
                    if canal:
                        user = await pegar_usuario(user_id)
                        embed = discord.Embed(
                            title="🎉 META SEMANAL CONCLUÍDA!",
                            description=f"{user.mention} **parabéns!** Sua meta semanal de **R$ 300.000,00** foi atingida! 🎉",
                            color=0x2ecc71
                        )
                        embed.add_field(
                            name="💰 Total atingido",
                            value=formatar_dinheiro(valor_total),
                            inline=True
                        )
                        embed.add_field(
                            name="📅 Data",
                            value=agora().strftime('%d/%m/%Y %H:%M'),
                            inline=True
                        )
                        await canal.send(embed=embed)
                        return True
        return False
    return False


async def verificar_avisos_quarta():
    """Verifica na quarta-feira se membros não fizeram depósitos"""
    hoje = agora()
    
    # Verificar se é quarta-feira
    if hoje.weekday() != 2:  # 2 = Quarta-feira
        return
    
    async with get_db().acquire() as conn:
        # Buscar todas as metas
        metas = await conn.fetch("SELECT * FROM metas")
        
        for meta in metas:
            user_id = meta["user_id"]
            dinheiro = meta["dinheiro"] or 0
            dinheiro_acoes = meta.get("dinheiro_acoes") or 0
            total = dinheiro + dinheiro_acoes
            
            # Se não fez depósito (total = 0) e não foi avisado hoje
            if total == 0:
                ja_avisado = await conn.fetchval(
                    "SELECT 1 FROM metas_avisos WHERE user_id = $1 AND tipo = 'quarta' AND data::date = $2",
                    str(user_id), hoje.date()
                )
                
                if not ja_avisado:
                    # Registrar aviso
                    await conn.execute(
                        "INSERT INTO metas_avisos (user_id, tipo, data) VALUES ($1, 'quarta', $2)",
                        str(user_id), agora_db()
                    )
                    
                    # Buscar canal da meta
                    canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(user_id))
                    if canal_id:
                        canal = bot.get_channel(int(canal_id))
                        if canal:
                            user = await pegar_usuario(user_id)
                            embed = discord.Embed(
                                title="⚠️ AVISO DE META SEMANAL",
                                description=f"{user.mention} **atenção!**",
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
                            return True
    return False


async def verificar_meta_atrasada(user_id, data_criacao):
    """Verifica se a meta está atrasada e envia aviso"""
    hoje = agora()
    dias_sem_meta = (hoje - data_criacao).days
    
    if dias_sem_meta >= 14:  # 2 semanas sem meta
        async with get_db().acquire() as conn:
            ja_avisado = await conn.fetchval(
                "SELECT 1 FROM metas_avisos WHERE user_id = $1 AND tipo = 'remocao' AND data > NOW() - INTERVAL '7 days'",
                str(user_id)
            )
            
            if not ja_avisado:
                await conn.execute(
                    "INSERT INTO metas_avisos (user_id, tipo, data) VALUES ($1, 'remocao', $2)",
                    str(user_id), agora_db()
                )
                
                canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(user_id))
                if canal_id:
                    canal = bot.get_channel(int(canal_id))
                    if canal:
                        user = await pegar_usuario(user_id)
                        embed = discord.Embed(
                            title="🚨 URGENTE - META ATRASADA",
                            description=f"{user.mention} **sua meta está ATRASADA!**",
                            color=0xe74c3c
                        )
                        embed.add_field(
                            name="⏰ Você está há mais de 2 semanas sem fechar uma meta!",
                            value=(
                                "⚠️ **Isso pode resultar na sua REMOÇÃO da facção!**\n\n"
                                "📌 **Entre em contato com a gerência URGENTEMENTE!**"
                            ),
                            inline=False
                        )
                        embed.set_footer(text="Meta semanal • Vida Rasa")
                        await canal.send(embed=embed)
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
# ==================== QUERIES DE ENTREGAS =================
# =========================================================

async def salvar_entrega_parcelada(
    pedido_original: int,
    total_entregas: int,
    pt_por_entrega: int,
    sub_por_entrega: int,
    vendedor_id: str,
    organizacao: str,
    observacoes: str,
    canal_id: str
):
    """Salva uma venda parcelada no banco"""
    async with get_db().acquire() as conn:
        proxima = agora() + timedelta(days=1)
        proxima = proxima.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # USANDO A FUNÇÃO QUE JÁ EXISTE NO SEU CÓDIGO
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
            proxima_naive,
            canal_id
        )


async def buscar_entregas_pendentes():
    """Busca entregas que precisam ser criadas"""
    async with get_db().acquire() as conn:
        return await conn.fetch(
            """
            SELECT * FROM entregas_parceladas
            WHERE ativo = true
            AND proxima_entrega <= NOW()
            ORDER BY proxima_entrega ASC
            """
        )


async def atualizar_entrega_parcelada(
    entrega_id: int,
    entrega_atual: int,
    mensagem_id: str,
    proxima_entrega = None
):
    """Atualiza o progresso de uma entrega parcelada"""
    async with get_db().acquire() as conn:
        if proxima_entrega is None:
            proxima_entrega = agora() + timedelta(days=1)
            proxima_entrega = proxima_entrega.replace(hour=0, minute=0, second=0, microsecond=0)
        
        # USANDO A FUNÇÃO QUE JÁ EXISTE NO SEU CÓDIGO
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

async def finalizar_entregas(entrega_id: int):
    """Marca as entregas como concluídas"""
    async with get_db().acquire() as conn:
        await conn.execute(
            "UPDATE entregas_parceladas SET ativo = false WHERE id = $1",
            entrega_id
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
    """Verifica se um canal da Kick está ao vivo - VERSÃO MELHORADA"""
    try:
        # Verificar cache primeiro
        cache_key = f"kick_{canal}"
        agora_ts = time_module.time()
        
        if cache_key in cache_lives:
            dados, timestamp = cache_lives[cache_key]
            if agora_ts - timestamp < CACHE_LIVES_TTL:
                print(f"📦 Cache Kick: {canal} = {dados[0]}")
                return dados
        
        # =========================================================
        # ================= TENTATIVA 1: API OFICIAL ==============
        # =========================================================
        
        url_api = f"https://kick.com/api/v2/channels/{canal}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/json",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8"
        }
        
        try:
            async with http_session.get(url_api, headers=headers, timeout=10) as r:
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
                        
                        resultado = (True, titulo, jogo, thumbnail)
                        cache_lives[cache_key] = (resultado, agora_ts)
                        print(f"✅ Kick API: {canal} está AO VIVO!")
                        return resultado
                    else:
                        print(f"📴 Kick API: {canal} está OFFLINE")
                else:
                    print(f"⚠️ Kick API status {r.status}, tentando fallback")
        except asyncio.TimeoutError:
            print(f"⏰ Timeout na API Kick para {canal}, tentando fallback")
        except Exception as e:
            print(f"⚠️ Erro na API Kick para {canal}: {e}, tentando fallback")
        
        # =========================================================
        # ================= TENTATIVA 2: SCRAPING HTML ============
        # =========================================================
        
        try:
            url_page = f"https://kick.com/{canal}"
            headers_html = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            }
            
            async with http_session.get(url_page, headers=headers_html, timeout=15) as r:
                if r.status == 200:
                    html = await r.text()
                    
                    # Múltiplos padrões de busca
                    padroes = [
                        r'"isLive":\s*true',
                        r'"livestream":\s*\{',
                        r'"live":\s*true',
                        r'isLive:true'
                    ]
                    
                    ao_vivo = False
                    for padrao in padroes:
                        if re.search(padrao, html, re.IGNORECASE):
                            ao_vivo = True
                            break
                    
                    if ao_vivo:
                        # Tentar extrair título
                        titulo_match = re.search(r'"sessionTitle":"([^"]+)"', html)
                        titulo = titulo_match.group(1) if titulo_match else f"Live na Kick - {canal}"
                        
                        # Tentar extrair thumbnail
                        thumb_match = re.search(r'"thumbnail":"([^"]+)"', html)
                        thumbnail = thumb_match.group(1) if thumb_match else None
                        
                        # Tentar extrair jogo
                        jogo_match = re.search(r'"category":"([^"]+)"', html)
                        jogo = jogo_match.group(1) if jogo_match else None
                        
                        resultado = (True, titulo, jogo, thumbnail)
                        cache_lives[cache_key] = (resultado, agora_ts)
                        print(f"✅ Kick HTML: {canal} está AO VIVO!")
                        return resultado
                    else:
                        print(f"📴 Kick HTML: {canal} está OFFLINE")
                else:
                    print(f"❌ Kick HTML: status {r.status} para {canal}")
        except asyncio.TimeoutError:
            print(f"⏰ Timeout no HTML Kick para {canal}")
        except Exception as e:
            print(f"⚠️ Erro no HTML Kick para {canal}: {e}")
        
        # =========================================================
        # ================= TENTATIVA 3: FALLBACK FINAL ===========
        # =========================================================
        
        # Se todas as tentativas falharem, considerar OFFLINE
        resultado = (False, None, None, None)
        cache_lives[cache_key] = (resultado, agora_ts)
        print(f"📴 Kick: {canal} - OFFLINE (fallback final)")
        return resultado
        
    except Exception as e:
        print(f"❌ Erro ao verificar Kick para {canal}: {e}")
        import traceback
        traceback.print_exc()
        return False, None, None, None

async def checar_tiktok(username):
    """Verifica se um canal do TikTok está ao vivo - VERSÃO MELHORADA"""
    try:
        username = username.lower().replace("@", "").strip()
        
        # Verificar cache primeiro
        cache_key = f"tiktok_{username}"
        agora_ts = time_module.time()
        
        if cache_key in cache_lives:
            dados, timestamp = cache_lives[cache_key]
            if agora_ts - timestamp < CACHE_LIVES_TTL:
                print(f"📦 Cache TikTok: {username} = {dados[0]}")
                return dados
        
        # =========================================================
        # ================= TENTATIVA 1: API TIKTOK ===============
        # =========================================================
        
        # TikTok tem API pública limitada, mas vamos tentar
        try:
            url_api = f"https://www.tiktok.com/api/v1/live/detail/?aid=1988&room_id=&uniqueId={username}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "application/json",
            }
            
            async with http_session.get(url_api, headers=headers, timeout=10) as r:
                if r.status == 200:
                    data = await r.json()
                    if data.get("status_code") == 0 and data.get("data", {}).get("room_id"):
                        titulo = data.get("data", {}).get("title", f"Live no TikTok - @{username}")
                        jogo = "TikTok"
                        thumbnail = data.get("data", {}).get("cover_url")
                        resultado = (True, titulo, jogo, thumbnail)
                        cache_lives[cache_key] = (resultado, agora_ts)
                        print(f"✅ TikTok API: {username} está AO VIVO!")
                        return resultado
        except Exception as e:
            print(f"⚠️ Erro na API TikTok para {username}: {e}")
        
        # =========================================================
        # ================= TENTATIVA 2: SCRAPING =================
        # =========================================================
        
        try:
            url = f"https://www.tiktok.com/@{username}"
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            }
            
            async with http_session.get(url, headers=headers, timeout=15) as response:
                if response.status != 200:
                    print(f"⚠️ TikTok status {response.status} para {username}")
                    resultado = (False, None, None, None)
                    cache_lives[cache_key] = (resultado, agora_ts)
                    return resultado
                
                html = await response.text()
                
                # Múltiplos padrões de busca
                padroes = [
                    r'"isLive":\s*true',
                    r'"liveStatus":\s*1',
                    r'"room_id":"\d+"',
                    r'"status":\s*1',
                ]
                
                ao_vivo = False
                for padrao in padroes:
                    if re.search(padrao, html, re.IGNORECASE):
                        ao_vivo = True
                        break
                
                if ao_vivo:
                    titulo_match = re.search(r'"title":"([^"]+)"', html)
                    titulo = titulo_match.group(1) if titulo_match else f"Live no TikTok - @{username}"
                    
                    resultado = (True, titulo, "TikTok", None)
                    cache_lives[cache_key] = (resultado, agora_ts)
                    print(f"✅ TikTok HTML: {username} está AO VIVO!")
                    return resultado
                else:
                    print(f"📴 TikTok HTML: {username} está OFFLINE")
        
        except asyncio.TimeoutError:
            print(f"⏰ Timeout no TikTok para {username}")
        except Exception as e:
            print(f"⚠️ Erro no TikTok para {username}: {e}")
        
        # =========================================================
        # ================= FALLBACK FINAL ========================
        # =========================================================
        
        resultado = (False, None, None, None)
        cache_lives[cache_key] = (resultado, agora_ts)
        return resultado
        
    except Exception as e:
        print(f"❌ Erro ao verificar TikTok para {username}: {e}")
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
# ==================== CACHE DE LIVES =====================
# =========================================================

cache_lives = {}
cache_lives_timestamp = 0
CACHE_LIVES_TTL = 120  # 2 minutos de cache

# =========================================================
# ============ CLASSE REGISTRO (MODAL E VIEWS) ============
# =========================================================

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
        
        # =========================================================
        # ================= FORMATAR NOME =========================
        # =========================================================
        
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
        
        nome_formatado = capitalizar_nome(self.nome.value)
        vulgo_formatado = capitalizar_nome(self.vulgo.value) if self.vulgo.value else None
        
        # =========================================================
        # ================= ATUALIZAR NICK ========================
        # =========================================================
        
        if vulgo_formatado:
            # 🔥 FORMATO: Passaporte Nome | Vulgo (sem " - ")
            nick = f"{self.passaporte.value} {nome_formatado} | {vulgo_formatado}"
        else:
            nick = f"{self.passaporte.value} {nome_formatado}"
        
        # 🔥 GARANTIR QUE NÃO ULTRAPASSA 32 CARACTERES
        if len(nick) > 32:
            # Se passar, cortar o nome
            nome_cortado = nome_formatado[:20]
            if vulgo_formatado:
                nick = f"{self.passaporte.value} {nome_cortado} | {vulgo_formatado}"
            else:
                nick = f"{self.passaporte.value} {nome_cortado}"
            
            # Se ainda passar, cortar mais
            if len(nick) > 32:
                nick = nick[:32]
        
        await membro.edit(nick=nick)
        
        # =========================================================
        # ================= ABRIR SELECT ==========================
        # =========================================================
        
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
            discord.SelectOption(
                label="Membro da 442",
                description="Se tornar membro da facção",
                emoji="🕴️"
            ),
            discord.SelectOption(
                label="Amigo da 442",
                description="Apenas para resenha ou reunião",
                emoji="🤝"
            )
        ]
        super().__init__(
            placeholder="Escolha o tipo de acesso",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        membro = interaction.user
        
        # Cargos
        agregado = guild.get_role(AGREGADO_ROLE_ID)
        amigos = guild.get_role(1309121290241704046)
        convidado = guild.get_role(CONVIDADO_ROLE_ID)
        em_registro = guild.get_role(EM_REGISTRO_ROLE_ID)
        
        escolha = self.values[0]
        
        # Remover cargo "Em Registro"
        if em_registro:
            await membro.remove_roles(em_registro)
            print(f"✅ Cargo 'Em Registro' removido de {membro.name}")
        
        # Adicionar cargo selecionado
        if escolha == "Membro da 442":
            if agregado:
                await membro.add_roles(agregado)
                print(f"✅ Cargo 'Agregado' adicionado para {membro.name}")
        elif escolha == "Amigo da 442":
            if amigos:
                await membro.add_roles(amigos)
                print(f"✅ Cargo 'Amigos' adicionado para {membro.name}")
        
        # Remover cargo convidado (se tiver)
        if convidado:
            await membro.remove_roles(convidado)
        
        # =========================================================
        # ================= ENVIAR EMBED PARA O HISTÓRICO =========
        # =========================================================
        
        # 🔥 CANAL DE HISTÓRICO (LOG)
        canal_log = guild.get_channel(CANAL_LOG_REGISTRO_ID)
        
        if canal_log:
            
            embed = discord.Embed(
                title="🎉 NOVO MEMBRO REGISTRADO!",
                description=f"**{membro.mention}** acabou de se registrar na **Vida Rasa**!",
                color=0x2ecc71,
                timestamp=agora()
            )
            
            # Thumbnail do usuário
            if membro.display_avatar:
                embed.set_thumbnail(url=membro.display_avatar.url)
            
            embed.set_author(
                name=membro.display_name,
                icon_url=membro.display_avatar.url if membro.display_avatar else None
            )
            
            # Informações do Membro
            informacoes = (
                f"**📋 Passaporte:** `{self.passaporte}`\n"
                f"**👤 Nome:** {self.nome}\n"
                f"**🏷️ Vulgo:** {self.vulgo or '❌ Não informado'}\n"
                f"**📱 Telefone:** {self.telefone}\n"
                f"**👤 Indicado por:** {self.indicado or '❌ Não informado'}\n"
                f"**🎯 Tipo:** {escolha}"
            )
            
            embed.add_field(
                name="📋 INFORMAÇÕES DO MEMBRO",
                value=informacoes,
                inline=False
            )
            
            # Status
            embed.add_field(
                name="📌 STATUS",
                value=(
                    f"✅ **Registro concluído**\n"
                    f"🔹 Cargo atribuído: **{escolha}**\n"
                    f"🆔 ID: `{membro.id}`"
                ),
                inline=False
            )
            
            # Cargo atribuído
            if escolha == "Membro da 442":
                cargo_emoji = "🕴️"
            else:
                cargo_emoji = "🤝"
            
            embed.add_field(
                name="🎯 CARGO ATRIBUÍDO",
                value=f"{cargo_emoji} **{escolha}**",
                inline=False
            )
            
            # Footer
            embed.set_footer(
                text=f"Registro realizado com sucesso • Sistema Automático",
                icon_url=bot.user.display_avatar.url if bot.user.display_avatar else None
            )
            
            # 🔥 ENVIAR PARA O CANAL DE HISTÓRICO
            await canal_log.send(embed=embed)
            print(f"✅ Embed de registro enviado para o canal de histórico")
        
        else:
            print(f"❌ Canal de histórico NÃO ENCONTRADO! ID: {CANAL_LOG_REGISTRO_ID}")
        
        # =========================================================
        # ================= RESPOSTA AO USUÁRIO ===================
        # =========================================================
        
        await interaction.response.send_message(
            f"✅ **Registro concluído com sucesso!**\n\n"
            f"📋 Você foi registrado como: **{escolha}**\n"
            f"👤 Nome: {self.nome}\n"
            f"🏷️ Vulgo: {self.vulgo or 'Não informado'}\n"
            f"📱 Telefone: {self.telefone}",
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
    
    # =========================================================
    # ================= MANTER PAINEL DE META NO FINAL ========
    # =========================================================
    
    canal = message.channel
    
    # Verificar se o canal é uma sala de meta
    if isinstance(canal, discord.TextChannel):
        for uid, dados in list(metas_cache.items()):
            if dados["canal_id"] == canal.id:
                try:
                    await asyncio.sleep(2)  # Esperar a mensagem ser enviada
                    await fixar_painel_meta_no_final(int(uid))
                except Exception as e:
                    print(f"Erro ao fixar painel: {e}")
                break

    # =========================================================
    # ================= LAVAGEM (já existia) ==================
    # =========================================================
    
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
    # ================= CONTROLE DE BAÚ =======================
    # =========================================================
    
    # Controle de baú - manter painéis no final
    await on_message_controle(message)
    
    await bot.process_commands(message)

# =========================================================
# ==================== EVENTO DE MEMBRO (METAS) ===========
# =========================================================

@bot.event
@bot.event
async def on_member_update(before, after):
    """Quando o membro muda de cargo, atualiza a categoria da meta"""
    if after.bot:
        return
    
    # Verificar se o membro ganhou cargo de agregado
    tinha_agregado = any(r.id == AGREGADO_ROLE_ID for r in before.roles)
    tem_agregado = any(r.id == AGREGADO_ROLE_ID for r in after.roles)
    
    if not tinha_agregado and tem_agregado:
        await asyncio.sleep(2)
        print(f"🔵 {after.name} ganhou cargo de Agregado, criando sala...")
        
        # 🔥 VERIFICAR SE JÁ TEM META (MESMO SEM SALA)
        async with get_db().acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(after.id))
        
        # Se não tem meta, criar
        if not meta:
            sala = await criar_sala_meta(after)
            if sala:
                print(f"✅ Sala criada para {after.name}")
            else:
                print(f"❌ Erro ao criar sala para {after.name}")
        else:
            # Se tem meta, verificar se o canal existe
            canal = after.guild.get_channel(meta["canal_id"])
            if not canal:
                # Canal não existe, recriar
                sala = await criar_sala_meta(after)
                if sala:
                    print(f"✅ Sala recriada para {after.name}")
            else:
                # Só atualizar o painel
                await atualizar_embed_meta(after.id)
                print(f"📊 Painel atualizado para {after.name}")
        
        return
    
    # Se tem meta, verificar se precisa atualizar a categoria
    if str(after.id) in metas_cache:
        await atualizar_categoria_meta(after)
        
@bot.command(name="forcar_painel_meta")
async def cmd_forcar_painel_meta(ctx, member: discord.Member = None):
    """Força a criação/atualização do painel de meta de um membro (ADM apenas)"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    if not member:
        member = ctx.author
    
    await ctx.send(f"🔄 Forçando criação do painel para {member.display_name}...")
    
    try:
        # Verificar se o membro tem meta
        if str(member.id) not in metas_cache:
            # Criar sala
            sala = await criar_sala_meta(member)
            if sala:
                await ctx.send(f"✅ Sala e painel criados para {member.display_name}!")
            else:
                await ctx.send(f"❌ Erro ao criar sala para {member.display_name}!")
        else:
            # Forçar atualização do painel
            await atualizar_embed_meta(member.id)
            await ctx.send(f"✅ Painel atualizado para {member.display_name}!")
            
    except Exception as e:
        await ctx.send(f"❌ Erro: {e}")
        print(f"Erro ao criar painel para {member.display_name}: {e}")

@bot.event
async def on_member_join(member):
    """Quando um membro entra no servidor, dá o cargo 'Em Registro'"""
    if member.bot:
        return
    
    try:
        cargo_em_registro = member.guild.get_role(EM_REGISTRO_ROLE_ID)
        if cargo_em_registro:
            await member.add_roles(cargo_em_registro)
            print(f"✅ Cargo 'Em Registro' adicionado para {member.name}")
    except Exception as e:
        print(f"❌ Erro ao adicionar cargo 'Em Registro' para {member.name}: {e}")


@bot.event
async def on_guild_channel_delete(channel):
    """Quando um canal é deletado, remove do cache e banco se for uma sala de meta"""
    # Verificar se é uma sala de meta
    for uid, dados in list(metas_cache.items()):
        if dados["canal_id"] == channel.id:
            print(f"🗑️ Canal de meta deletado: {channel.name} (Usuário: {uid})")
            
            # Remover do cache
            metas_cache.pop(uid)
            
            # Remover do banco
            try:
                async with get_db().acquire() as conn:
                    await conn.execute("DELETE FROM metas WHERE user_id = $1", uid)
                print(f"✅ Meta do usuário {uid} removida do banco")
            except Exception as e:
                print(f"❌ Erro ao remover meta do banco: {e}")
            break

@bot.command(name="resetar_meta")
async def cmd_resetar_meta(ctx, member: discord.Member = None):
    """Reseta a meta de um usuário (remove do banco) - ADM apenas"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    if not member:
        await ctx.send("❌ Use: !resetar_meta @membro")
        return
    
    await ctx.send(f"🔄 Resetando meta de {member.display_name}...")
    
    try:
        # Remover do banco
        async with get_db().acquire() as conn:
            await conn.execute("DELETE FROM metas WHERE user_id = $1", str(member.id))
        
        # Remover do cache
        if str(member.id) in metas_cache:
            del metas_cache[str(member.id)]
        
        # Remover cargo e recolocar para forçar recriação
        cargo_agregado = ctx.guild.get_role(AGREGADO_ROLE_ID)
        if cargo_agregado and cargo_agregado in member.roles:
            await member.remove_roles(cargo_agregado)
            await asyncio.sleep(1)
            await member.add_roles(cargo_agregado)
            await ctx.send(f"✅ Cargo removido e readicionado para forçar recriação!")
        
        await ctx.send(f"✅ Meta de {member.display_name} resetada! A sala será recriada em breve.")
        
    except Exception as e:
        await ctx.send(f"❌ Erro: {e}")
        print(f"Erro ao resetar meta: {e}")


# =========================================================
# ==================== SISTEMA DE METAS ====================
# =========================================================

async def criar_sala_meta(member: discord.Member):
    """Cria a sala de meta para o membro com o painel"""
    guild = member.guild
    nome_canal = f"📁-{member.display_name}".lower()
    
    print(f"🏗️ Criando sala para {member.display_name}...")
    
    # 🔥 PRIMEIRO: VERIFICAR SE O MEMBRO JÁ TEM META NO BANCO
    async with get_db().acquire() as conn:
        meta_existente = await conn.fetchrow(
            "SELECT * FROM metas WHERE user_id = $1",
            str(member.id)
        )
    
    # Se já tem meta no banco, só atualizar o painel
    if meta_existente:
        canal_id = int(meta_existente["canal_id"])
        canal_existe = guild.get_channel(canal_id)
        
        if canal_existe:
            print(f"📁 Meta já existe para {member.display_name}, atualizando painel...")
            metas_cache[str(member.id)] = {
                "canal_id": canal_id,
                "dinheiro": meta_existente["dinheiro"],
                "polvora": meta_existente["polvora"],
                "acao": meta_existente["acao"],
                "dinheiro_acoes": meta_existente.get("dinheiro_acoes") or 0
            }
            await atualizar_embed_meta(member.id)
            return canal_existe
        else:
            # Canal não existe mais, remover meta do banco
            await conn.execute("DELETE FROM metas WHERE user_id = $1", str(member.id))
            if str(member.id) in metas_cache:
                del metas_cache[str(member.id)]
    
    # 🔥 VERIFICAR SE O MEMBRO JÁ TEM UMA SALA (MESMO SEM META NO BANCO)
    for canal in guild.text_channels:
        if member.display_name.lower() in canal.name.lower() and "📁" in canal.name:
            print(f"📁 Sala encontrada para {member.display_name}: {canal.name}")
            
            # 🔥 SALVAR A META NO BANCO AGORA!
            await salvar_meta_db(member.id, canal.id, 0, 0, 0)
            
            metas_cache[str(member.id)] = {
                "canal_id": canal.id,
                "dinheiro": 0,
                "polvora": 0,
                "acao": None,
                "dinheiro_acoes": 0
            }
            
            await atualizar_embed_meta(member.id)
            print(f"✅ Meta salva no banco para {member.display_name} usando sala existente!")
            return canal
    
    # Se não encontrou sala, criar uma nova
    categoria_id = obter_categoria_meta(member)
    if not categoria_id:
        print(f"❌ Nenhuma categoria encontrada para {member.display_name}")
        return None
    
    categoria = guild.get_channel(categoria_id)
    if not categoria:
        print(f"❌ Categoria {categoria_id} não encontrada")
        return None
    
    nick = member.display_name.lower().replace(" ", "-")
    nome_canal = f"📁・{nick}"
    
    overwrites = {
        guild.default_role: discord.PermissionOverwrite(view_channel=False),
        member: discord.PermissionOverwrite(view_channel=True, send_messages=True),
    }
    
    gerente = guild.get_role(CARGO_GERENTE_ID)
    if gerente:
        overwrites[gerente] = discord.PermissionOverwrite(view_channel=True)
    
    gerente_geral = guild.get_role(CARGO_GERENTE_GERAL_ID)
    if gerente_geral:
        overwrites[gerente_geral] = discord.PermissionOverwrite(view_channel=True)
    
    print(f"📁 Criando canal: {nome_canal} na categoria {categoria.name}")
    
    canal = await guild.create_text_channel(
        nome_canal,
        category=categoria,
        overwrites=overwrites
    )
    
    print(f"✅ Canal criado: {canal.name} (ID: {canal.id})")
    
    # 🔥 SALVAR A META NO BANCO IMEDIATAMENTE
    await salvar_meta_db(member.id, canal.id, 0, 0, 0)
    
    metas_cache[str(member.id)] = {
        "canal_id": canal.id,
        "dinheiro": 0,
        "polvora": 0,
        "acao": None,
        "dinheiro_acoes": 0
    }
    
    await asyncio.sleep(1)
    await atualizar_embed_meta(member.id)
    
    print(f"📊 Sala e painel criados para {member.display_name}")
    return canal


async def zerar_todas_metas():
    """Zera todas as metas (dinheiro e pólvora)"""
    async with get_db().acquire() as conn:
        await conn.execute(
            """
            UPDATE metas SET 
                dinheiro = 0, 
                dinheiro_acoes = 0, 
                polvora = 0
            """
        )
        rows = await conn.fetch("SELECT user_id, canal_id FROM metas")
        return rows


class ZerarMetasView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)
    
    @discord.ui.button(
        label="⚠️ ZERAR TODAS AS METAS",
        style=discord.ButtonStyle.danger,
        custom_id="zerar_metas_btn",
        emoji="⚠️"
    )
    async def zerar_metas(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem zerar todas as metas!", ephemeral=True)
            return
        
        view = ConfirmarZerarView()
        await interaction.response.send_message(
            "⚠️ **ATENÇÃO!** Você está prestes a zerar TODAS as metas.\n\n"
            "Isso vai resetar o dinheiro e pólvora de TODOS os membros.\n"
            "**Esta ação não pode ser desfeita!**\n\n"
            "Clique em **'Sim, zerar tudo'** para confirmar.",
            view=view,
            ephemeral=True
        )


class ConfirmarZerarView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=30)
    
    @discord.ui.button(
        label="✅ Sim, zerar tudo",
        style=discord.ButtonStyle.danger,
        emoji="✅"
    )
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
            
            await interaction.followup.send(
                f"✅ **Todas as metas foram zeradas com sucesso!**\n\n"
                f"📊 {atualizadas} metas resetadas.",
                ephemeral=True
            )
            
            canal_gerencia = interaction.guild.get_channel(CANAL_GERENCIA_ID)
            if canal_gerencia:
                embed = discord.Embed(
                    title="⚠️ METAS ZERADAS",
                    description=f"Todas as metas foram resetadas por {interaction.user.mention}",
                    color=0xe74c3c,
                    timestamp=agora()
                )
                await canal_gerencia.send(embed=embed)
            
        except Exception as e:
            await interaction.followup.send(f"❌ Erro ao zerar metas: {e}", ephemeral=True)
    
    @discord.ui.button(
        label="❌ Cancelar",
        style=discord.ButtonStyle.secondary,
        emoji="❌"
    )
    async def cancelar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ Operação cancelada.", ephemeral=True)


async def atualizar_categoria_meta(member):
    """Atualiza a categoria da sala do membro baseada no cargo atual"""
    try:
        if str(member.id) not in metas_cache:
            print(f"⚠️ {member.name} não tem meta no cache")
            return
        
        dados = metas_cache[str(member.id)]
        canal = member.guild.get_channel(dados["canal_id"])
        
        if not canal:
            print(f"⚠️ Canal da meta de {member.name} não encontrado")
            return
        
        nova_categoria_id = obter_categoria_meta(member)
        
        if not nova_categoria_id:
            print(f"⚠️ Nenhuma categoria encontrada para {member.name}")
            return
        
        nova_categoria = member.guild.get_channel(nova_categoria_id)
        
        if not nova_categoria:
            print(f"⚠️ Categoria {nova_categoria_id} não encontrada")
            return
        
        if canal.category_id == nova_categoria_id:
            print(f"✅ {member.name} já está na categoria correta")
            return
        
        await canal.edit(category=nova_categoria)
        print(f"📁 Categoria de {member.name} movida para {nova_categoria.name}")
        await atualizar_embed_meta(member.id)
        
    except Exception as e:
        print(f"❌ Erro ao atualizar categoria de {member.name}: {e}")
        import traceback
        traceback.print_exc()


async def depositar_na_meta(user_id, valor, motivo):
    """Deposita dinheiro na meta do usuário"""
    async with get_db().acquire() as conn:
        meta = await conn.fetchrow("SELECT dinheiro, dinheiro_acoes FROM metas WHERE user_id = $1", str(user_id))
        
        if meta:
            if "Ação" in motivo:
                novo_acoes = (meta["dinheiro_acoes"] or 0) + valor
                await conn.execute(
                    "UPDATE metas SET dinheiro_acoes = $1 WHERE user_id = $2",
                    novo_acoes, str(user_id)
                )
            else:
                novo_valor = meta["dinheiro"] + valor
                await conn.execute(
                    "UPDATE metas SET dinheiro = $1 WHERE user_id = $2",
                    novo_valor, str(user_id)
                )
            
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
                    if "Ação" in motivo:
                        await conn.execute(
                            "UPDATE metas SET dinheiro_acoes = $1 WHERE user_id = $2",
                            valor, str(user_id)
                        )
                    else:
                        await conn.execute(
                            "UPDATE metas SET dinheiro = $1 WHERE user_id = $2",
                            valor, str(user_id)
                        )
                    return True
            return False


async def adicionar_polvora_meta(user_id, quantidade):
    """Adiciona pólvora à meta do usuário"""
    async with get_db().acquire() as conn:
        meta = await conn.fetchrow("SELECT polvora FROM metas WHERE user_id = $1", str(user_id))
        if meta:
            novo_valor = meta["polvora"] + quantidade
            await conn.execute("UPDATE metas SET polvora = $1 WHERE user_id = $2", novo_valor, str(user_id))
            return True
        return False


async def adicionar_dinheiro_meta(user_id, valor):
    """Adiciona dinheiro à meta do usuário"""
    async with get_db().acquire() as conn:
        meta = await conn.fetchrow("SELECT dinheiro FROM metas WHERE user_id = $1", str(user_id))
        if meta:
            novo_valor = meta["dinheiro"] + valor
            await conn.execute("UPDATE metas SET dinheiro = $1 WHERE user_id = $2", novo_valor, str(user_id))
            return True
        return False


async def fechar_meta(user_id, data_inicio, data_fim):
    """Fecha uma meta e registra no histórico"""
    async with get_db().acquire() as conn:
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
            str(user_id), 
            meta["dinheiro"], 
            meta["polvora"], 
            acao,
            meta.get("dinheiro_acoes") or 0,
            data_inicio, 
            data_fim, 
            agora_db()
        )
        
        await conn.execute(
            "UPDATE metas SET dinheiro = 0, polvora = 0, dinheiro_acoes = 0 WHERE user_id = $1",
            str(user_id)
        )
        
        return {
            "dinheiro": meta["dinheiro"],
            "polvora": meta["polvora"],
            "acao": acao,
            "dinheiro_acoes": meta.get("dinheiro_acoes") or 0
        }


async def buscar_historico_metas(data_inicio, data_fim):
    """Busca histórico de metas fechadas no período"""
    async with get_db().acquire() as conn:
        return await conn.fetch(
            """
            SELECT * FROM metas_historico 
            WHERE data_fechamento BETWEEN $1 AND $2
            ORDER BY data_fechamento DESC
            """,
            data_inicio, data_fim
        )


async def fechar_todas_metas(data_inicio, data_fim):
    """Fecha todas as metas ativas e retorna o relatório"""
    async with get_db().acquire() as conn:
        metas = await conn.fetch("SELECT * FROM metas")
        
        if not metas:
            return None, []
        
        relatorio = []
        
        for meta in metas:
            user_id = meta["user_id"]
            dinheiro = meta["dinheiro"] or 0
            polvora = meta["polvora"] or 0
            acao = meta["acao"] or "N/A"
            dinheiro_acoes = meta.get("dinheiro_acoes") or 0
            
            await conn.execute(
                """
                INSERT INTO metas_historico (user_id, dinheiro, polvora, acao, dinheiro_acoes, data_inicio, data_fim, data_fechamento)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                """,
                user_id, 
                dinheiro, 
                polvora, 
                acao,
                dinheiro_acoes,
                data_inicio, 
                data_fim, 
                agora_db()
            )
            
            await conn.execute(
                "UPDATE metas SET dinheiro = 0, polvora = 0, dinheiro_acoes = 0 WHERE user_id = $1",
                user_id
            )
            
            relatorio.append({
                "user_id": user_id,
                "dinheiro": dinheiro,
                "polvora": polvora,
                "acao": acao,
                "dinheiro_acoes": dinheiro_acoes,
                "total": dinheiro + dinheiro_acoes
            })
        
        # Buscar membros que NÃO têm meta
        guild = bot.get_guild(GUILD_ID)
        membros_sem_meta = []
        
        if guild:
            cargos_meta = [
                AGREGADO_ROLE_ID,
                CARGO_MEMBRO_ID,
                CARGO_SOLDADO_ID,
                CARGO_01_ID,
                CARGO_02_ID,
                CARGO_GERENTE_ID,
                CARGO_GERENTE_GERAL_ID,
                CARGO_RESP_METAS_ID,
                CARGO_RESP_ACAO_ID,
                CARGO_RESP_VENDAS_ID,
                CARGO_RESP_PRODUCAO_ID
            ]
            
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


# =========================================================
# ==================== MODAIS DE METAS ====================
# =========================================================

class AdicionarPolvoraModal(discord.ui.Modal, title="💣 Adicionar Pólvora"):
    quantidade = discord.ui.TextInput(
        label="Quantidade de Pólvora",
        placeholder="Digite a quantidade (ex: 100)",
        required=True
    )
    
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
        
        # 🔥 VERIFICAR SE O USUÁRIO TEM META
        async with get_db().acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(self.user_id))
        
        if not meta:
            # 🔥 CRIAR A META AUTOMATICAMENTE
            guild = interaction.guild
            member = guild.get_member(int(self.user_id))
            if member:
                await criar_sala_meta(member)
                await asyncio.sleep(1)
                await carregar_metas_cache()
                await interaction.response.send_message(
                    "✅ **Meta criada automaticamente!**\n"
                    "💡 Tente novamente agora.",
                    ephemeral=True
                )
                return
            else:
                await interaction.response.send_message(
                    "❌ **Meta não encontrada!**\n\n"
                    "💡 Clique em '➕ Criar Minha Sala' no canal de solicitar sala.",
                    ephemeral=True
                )
                return
        
        sucesso = await adicionar_polvora_meta(self.user_id, qtd)
        
        if not sucesso:
            await interaction.response.send_message("❌ Erro ao adicionar pólvora!", ephemeral=True)
            return
        
        await carregar_metas_cache()
        await atualizar_embed_meta(self.user_id)
        
        await interaction.response.send_message(
            f"✅ **{fmt_num(qtd)} pólvora(s) adicionada(s) à meta!**",
            ephemeral=True
        )


class AdicionarDinheiroModal(discord.ui.Modal, title="💰 Adicionar Dinheiro Sujo"):
    quantidade = discord.ui.TextInput(
        label="Valor do Dinheiro Sujo",
        placeholder="Digite o valor (ex: 5000)",
        required=True
    )
    
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
        
        # 🔥 VERIFICAR SE O USUÁRIO TEM META
        async with get_db().acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(self.user_id))
        
        if not meta:
            # 🔥 CRIAR A META AUTOMATICAMENTE
            guild = interaction.guild
            member = guild.get_member(int(self.user_id))
            if member:
                await criar_sala_meta(member)
                await asyncio.sleep(1)
                await carregar_metas_cache()
                await interaction.response.send_message(
                    "✅ **Meta criada automaticamente!**\n"
                    "💡 Tente novamente agora.",
                    ephemeral=True
                )
                return
            else:
                await interaction.response.send_message(
                    "❌ **Meta não encontrada!**\n\n"
                    "💡 Clique em '➕ Criar Minha Sala' no canal de solicitar sala.",
                    ephemeral=True
                )
                return
        
        sucesso = await adicionar_dinheiro_meta(self.user_id, valor)
        
        if not sucesso:
            await interaction.response.send_message("❌ Erro ao adicionar dinheiro!", ephemeral=True)
            return
        
        await carregar_metas_cache()
        await atualizar_embed_meta(self.user_id)
        
        await interaction.response.send_message(
            f"✅ **{formatar_dinheiro(valor)} adicionado à meta!**",
            ephemeral=True
        )


class FecharMetaModal(discord.ui.Modal, title="🔒 Fechar Meta"):
    data_inicio = discord.ui.TextInput(
        label="📅 Data de INÍCIO da meta",
        placeholder="Ex: 01/06/2026",
        required=True
    )
    data_fim = discord.ui.TextInput(
        label="📅 Data de FIM da meta",
        placeholder="Ex: 30/06/2026",
        required=True
    )
    
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
        
        embed = discord.Embed(
            title="🔒 META FECHADA",
            description=f"👤 <@{self.user_id}>",
            color=0xe74c3c
        )
        
        embed.add_field(
            name="💰 Dinheiro Sujo",
            value=formatar_dinheiro(resultado["dinheiro"]),
            inline=True
        )
        
        embed.add_field(
            name="🎯 Dinheiro Ações",
            value=formatar_dinheiro(resultado["dinheiro_acoes"]),
            inline=True
        )
        
        embed.add_field(
            name="💣 Pólvora",
            value=f"{fmt_num(resultado['polvora'])} unidades",
            inline=True
        )
        
        embed.add_field(
            name="📅 Período",
            value=f"{self.data_inicio.value} até {self.data_fim.value}",
            inline=False
        )
        
        embed.set_footer(text=f"Ação: {resultado['acao'] or 'N/A'}")
        
        canal = interaction.guild.get_channel(RESULTADOS_METAS_ID)
        if canal:
            await canal.send(embed=embed)
        
        await atualizar_embed_meta(self.user_id)
        
        await interaction.followup.send(
            f"✅ **Meta fechada com sucesso!**\n\n"
            f"💰 Dinheiro: {formatar_dinheiro(resultado['dinheiro'])}\n"
            f"🎯 Ações: {formatar_dinheiro(resultado['dinheiro_acoes'])}\n"
            f"💣 Pólvora: {fmt_num(resultado['polvora'])} unidades",
            ephemeral=True
        )


class RelatorioMetasModal(discord.ui.Modal, title="📊 Relatório de Metas"):
    data_inicio = discord.ui.TextInput(
        label="📅 Data INÍCIO",
        placeholder="Ex: 01/06/2026",
        required=True
    )
    data_fim = discord.ui.TextInput(
        label="📅 Data FIM",
        placeholder="Ex: 30/06/2026",
        required=True
    )
    
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
        
        historico = await buscar_historico_metas(inicio_dt, fim_dt)
        
        if not historico:
            await interaction.followup.send(
                f"📭 Nenhuma meta fechada no período **{self.data_inicio.value}** a **{self.data_fim.value}**.",
                ephemeral=True
            )
            return
        
        total_dinheiro = sum(r["dinheiro"] for r in historico)
        total_polvora = sum(r["polvora"] for r in historico)
        total_acoes = sum(r.get("dinheiro_acoes") or 0 for r in historico)
        
        embed = discord.Embed(
            title="📊 RELATÓRIO DE METAS",
            description=f"📅 **Período:** {self.data_inicio.value} até {self.data_fim.value}",
            color=0x2ecc71
        )
        
        embed.add_field(
            name="💰 TOTAL DINHEIRO SUJO",
            value=formatar_dinheiro(total_dinheiro),
            inline=True
        )
        
        embed.add_field(
            name="🎯 TOTAL DINHEIRO AÇÕES",
            value=formatar_dinheiro(total_acoes),
            inline=True
        )
        
        embed.add_field(
            name="💣 TOTAL PÓLVORA",
            value=f"{fmt_num(total_polvora)} unidades",
            inline=True
        )
        
        lista = ""
        for item in historico[:15]:
            user = await pegar_usuario(int(item["user_id"]))
            nome = user.display_name if user else f"ID: {item['user_id']}"
            total = item["dinheiro"] + (item.get("dinheiro_acoes") or 0)
            lista += f"👤 {nome} - 💰 {formatar_dinheiro(total)} - 💣 {fmt_num(item['polvora'])}\n"
        
        if len(historico) > 15:
            lista += f"\n*... e mais {len(historico) - 15} registros*"
        
        embed.add_field(name="📋 METAS FECHADAS", value=lista, inline=False)
        embed.set_footer(text=f"Total: {len(historico)} metas fechadas")
        
        canal = interaction.guild.get_channel(RESULTADOS_METAS_ID)
        if canal:
            await canal.send(embed=embed)
            await interaction.followup.send(
                f"✅ Relatório enviado no canal <#{RESULTADOS_METAS_ID}>!",
                ephemeral=True
            )
        else:
            await interaction.followup.send(embed=embed, ephemeral=True)


# =========================================================
# ==================== PAINEL DE METAS ====================
# =========================================================

class MetaView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=None)
        self.user_id = user_id
    
    @discord.ui.button(
        label="💣 Adicionar Pólvora",
        style=discord.ButtonStyle.primary,
        custom_id="meta_adicionar_polvora",
        emoji="💣"
    )
    async def adicionar_polvora(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 🔥 VERIFICAR SE É O DONO DA SALA
        is_dono = str(interaction.user.id) == str(self.user_id)
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_dono and not is_gerente:
            await interaction.response.send_message("❌ Apenas o dono da sala ou gerentes podem adicionar pólvora!", ephemeral=True)
            return
        
        # 🔥 VERIFICAR SE O USUÁRIO TEM META
        async with get_db().acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(self.user_id))
        
        if not meta:
            # 🔥 CRIAR A META AUTOMATICAMENTE
            guild = interaction.guild
            member = guild.get_member(int(self.user_id))
            if member:
                await criar_sala_meta(member)
                await asyncio.sleep(1)
                await carregar_metas_cache()
                await interaction.response.send_message(
                    "✅ **Meta criada automaticamente!**\n"
                    "💡 Tente novamente agora.",
                    ephemeral=True
                )
                return
            else:
                await interaction.response.send_message(
                    "❌ **Meta não encontrada!**\n\n"
                    "💡 Clique em '➕ Criar Minha Sala' no canal de solicitar sala.",
                    ephemeral=True
                )
                return
        
        await interaction.response.send_modal(AdicionarPolvoraModal(self.user_id))
    
    @discord.ui.button(
        label="💰 Adicionar Dinheiro Sujo",
        style=discord.ButtonStyle.success,
        custom_id="meta_adicionar_dinheiro",
        emoji="💰"
    )
    async def adicionar_dinheiro(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 🔥 VERIFICAR SE É O DONO DA SALA
        is_dono = str(interaction.user.id) == str(self.user_id)
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_dono and not is_gerente:
            await interaction.response.send_message("❌ Apenas o dono da sala ou gerentes podem adicionar dinheiro!", ephemeral=True)
            return
        
        # 🔥 VERIFICAR SE O USUÁRIO TEM META
        async with get_db().acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(self.user_id))
        
        if not meta:
            # 🔥 CRIAR A META AUTOMATICAMENTE
            guild = interaction.guild
            member = guild.get_member(int(self.user_id))
            if member:
                await criar_sala_meta(member)
                await asyncio.sleep(1)
                await carregar_metas_cache()
                await interaction.response.send_message(
                    "✅ **Meta criada automaticamente!**\n"
                    "💡 Tente novamente agora.",
                    ephemeral=True
                )
                return
            else:
                await interaction.response.send_message(
                    "❌ **Meta não encontrada!**\n\n"
                    "💡 Clique em '➕ Criar Minha Sala' no canal de solicitar sala.",
                    ephemeral=True
                )
                return
        
        await interaction.response.send_modal(AdicionarDinheiroModal(self.user_id))
    
    @discord.ui.button(
        label="🔒 Fechar Meta",
        style=discord.ButtonStyle.danger,
        custom_id="meta_fechar",
        emoji="🔒"
    )
    async def fechar_meta(self, interaction: discord.Interaction, button: discord.ui.Button):
        # 🔥 VERIFICAR SE É O DONO DA SALA
        is_dono = str(interaction.user.id) == str(self.user_id)
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_dono and not is_gerente:
            await interaction.response.send_message("❌ Apenas o dono da sala ou gerentes podem fechar a meta!", ephemeral=True)
            return
        
        await interaction.response.send_modal(FecharMetaModal(self.user_id))


class RelatorioMetasButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="📊 Gerar Relatório de Metas",
            style=discord.ButtonStyle.success,
            custom_id="relatorio_metas_btn",
            emoji="📊"
        )
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(RelatorioMetasModal())


class FecharTodasMetasButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="🔒 Fechar Todas as Metas (Semanal)",
            style=discord.ButtonStyle.danger,
            custom_id="fechar_todas_metas_btn",
            emoji="🔒"
        )
    
    async def callback(self, interaction: discord.Interaction):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem fechar todas as metas!", ephemeral=True)
            return
        
        await interaction.response.send_modal(FecharTodasMetasModal())


class FecharTodasMetasModal(discord.ui.Modal, title="🔒 Fechar Metas Semanais"):
    data_inicio = discord.ui.TextInput(
        label="📅 Data de INÍCIO da semana",
        placeholder="Ex: 01/07/2026",
        required=True
    )
    data_fim = discord.ui.TextInput(
        label="📅 Data de FIM da semana",
        placeholder="Ex: 07/07/2026",
        required=True
    )
    
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
        
        try:
            relatorio, membros_sem_meta = await fechar_todas_metas(inicio, fim)
            
            if not relatorio and not membros_sem_meta:
                await interaction.followup.send("📭 Nenhuma meta para fechar.", ephemeral=True)
                return
            
            total_dinheiro = sum(r["dinheiro"] for r in relatorio)
            total_polvora = sum(r["polvora"] for r in relatorio)
            total_dinheiro_acoes = sum(r["dinheiro_acoes"] for r in relatorio)
            total_geral = sum(r["total"] for r in relatorio)
            
            embed = discord.Embed(
                title="📊 RELATÓRIO SEMANAL - METAS FECHADAS",
                description=f"📅 **Período:** {self.data_inicio.value} até {self.data_fim.value}",
                color=0x2ecc71,
                timestamp=agora()
            )
            
            embed.add_field(
                name="📊 RESUMO GERAL",
                value=(
                    f"💰 **Dinheiro Sujo (Meta):** {formatar_dinheiro(total_dinheiro)}\n"
                    f"🎯 **Dinheiro de Ações:** {formatar_dinheiro(total_dinheiro_acoes)}\n"
                    f"💣 **Pólvora:** {fmt_num(total_polvora)} unidades\n"
                    f"📦 **Total Geral:** {formatar_dinheiro(total_geral)}\n"
                    f"👥 **Membros com meta:** {len(relatorio)}\n"
                    f"✅ **Pagaram:** {len([r for r in relatorio if r['total'] > 0])}\n"
                    f"❌ **Não pagaram:** {len([r for r in relatorio if r['total'] == 0])}"
                ),
                inline=False
            )
            
            if relatorio:
                pagaram = [r for r in relatorio if r["total"] > 0]
                nao_pagaram = [r for r in relatorio if r["total"] == 0]
                pagaram_ordenado = sorted(pagaram, key=lambda x: x["total"], reverse=True)
                
                if pagaram_ordenado:
                    lista_pagaram = ""
                    for i, item in enumerate(pagaram_ordenado, 1):
                        user = await pegar_usuario(int(item["user_id"]))
                        nome = user.display_name if user else f"ID: {item['user_id']}"
                        
                        lista_pagaram += (
                            f"**{i}.** {nome}\n"
                            f"   💰 Meta: {formatar_dinheiro(item['dinheiro'])}\n"
                            f"   🎯 Ações: {formatar_dinheiro(item['dinheiro_acoes'])}\n"
                            f"   💣 Pólvora: {fmt_num(item['polvora'])}\n"
                            f"   📦 **Total:** {formatar_dinheiro(item['total'])}\n"
                        )
                    
                    if len(pagaram_ordenado) <= 10:
                        embed.add_field(
                            name=f"✅ QUEM PAGOU ({len(pagaram)} membros)",
                            value=lista_pagaram[:1024],
                            inline=False
                        )
                    else:
                        partes = [pagaram_ordenado[i:i+10] for i in range(0, len(pagaram_ordenado), 10)]
                        for idx, parte in enumerate(partes, 1):
                            lista = ""
                            for i, item in enumerate(parte, 1 + (idx-1)*10):
                                user = await pegar_usuario(int(item["user_id"]))
                                nome = user.display_name if user else f"ID: {item['user_id']}"
                                lista += (
                                    f"**{i}.** {nome}\n"
                                    f"   💰 {formatar_dinheiro(item['total'])}\n"
                                )
                            embed.add_field(
                                name=f"✅ QUEM PAGOU ({len(parte)} membros) - Parte {idx}",
                                value=lista[:1024],
                                inline=False
                            )
                
                if nao_pagaram:
                    lista_nao_pagaram = ""
                    for i, item in enumerate(nao_pagaram, 1):
                        user = await pegar_usuario(int(item["user_id"]))
                        nome = user.display_name if user else f"ID: {item['user_id']}"
                        lista_nao_pagaram += f"**{i}.** {nome} - ❌ **ZERADO**\n"
                    
                    embed.add_field(
                        name=f"❌ QUEM NÃO PAGOU ({len(nao_pagaram)} membros)",
                        value=lista_nao_pagaram[:1024] if lista_nao_pagaram else "Nenhum",
                        inline=False
                    )
            
            if membros_sem_meta:
                lista_sem_meta = ""
                for i, item in enumerate(membros_sem_meta[:20], 1):
                    lista_sem_meta += f"**{i}.** {item['menção']} - ❌ **SEM META**\n"
                
                if len(membros_sem_meta) > 20:
                    lista_sem_meta += f"\n*... e mais {len(membros_sem_meta) - 20} membros*"
                
                embed.add_field(
                    name=f"⚠️ MEMBROS SEM META ({len(membros_sem_meta)} membros)",
                    value=lista_sem_meta[:1024] if lista_sem_meta else "Nenhum",
                    inline=False
                )
            
            if pagaram_ordenado and len(pagaram_ordenado) >= 3:
                top3 = pagaram_ordenado[:3]
                ranking = ""
                emojis = ["🥇", "🥈", "🥉"]
                
                for i, item in enumerate(top3):
                    user = await pegar_usuario(int(item["user_id"]))
                    nome = user.display_name if user else f"ID: {item['user_id']}"
                    ranking += f"{emojis[i]} {nome} - {formatar_dinheiro(item['total'])}\n"
                
                embed.add_field(
                    name="🏆 TOP 3 QUE MAIS PAGARAM",
                    value=ranking,
                    inline=False
                )
            
            embed.set_footer(text=f"Relatório gerado por {interaction.user.display_name}")
            
            canal_resultados = interaction.guild.get_channel(RESULTADOS_METAS_ID)
            if canal_resultados:
                await canal_resultados.send(embed=embed)
                await interaction.followup.send(
                    f"✅ **Relatório enviado no canal <#{RESULTADOS_METAS_ID}>!**",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(embed=embed, ephemeral=True)
            
            for uid in metas_cache.keys():
                await atualizar_embed_meta(int(uid))
                await asyncio.sleep(0.3)
            
            print(f"📊 Relatório semanal gerado por {interaction.user.name}")
            
        except Exception as e:
            print(f"❌ Erro ao fechar metas: {e}")
            import traceback
            traceback.print_exc()
            await interaction.followup.send(f"❌ Erro ao fechar metas: {e}", ephemeral=True)


class ZerarMetasButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="⚠️ Zerar Todas as Metas",
            style=discord.ButtonStyle.danger,
            custom_id="zerar_metas_btn_painel",
            emoji="⚠️"
        )
    
    async def callback(self, interaction: discord.Interaction):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem zerar todas as metas!", ephemeral=True)
            return
        
        view = ConfirmarZerarView()
        await interaction.response.send_message(
            "⚠️ **ATENÇÃO!** Você está prestes a zerar TODAS as metas.\n\n"
            "Isso vai resetar o dinheiro e pólvora de TODOS os membros.\n"
            "**Esta ação não pode ser desfeita!**\n\n"
            "Clique em **'✅ Sim, zerar tudo'** para confirmar.",
            view=view,
            ephemeral=True
        )

# =========================================================
# ==================== BOTÃO FECHAR METAS AUTOMÁTICO ======
# =========================================================

class FecharMetasAutomaticoButton(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="🔒 Fechar Metas (Automático - Semana Anterior)",
            style=discord.ButtonStyle.success,
            custom_id="fechar_metas_automatico_btn",
            emoji="🔒"
        )
    
    async def callback(self, interaction: discord.Interaction):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem fechar todas as metas!", ephemeral=True)
            return
        
        # CALCULAR A SEMANA ANTERIOR AUTOMATICAMENTE
        data_inicio, data_fim = calcular_semana_anterior()
        
        data_inicio_str = data_inicio.strftime("%d/%m/%Y")
        data_fim_str = data_fim.strftime("%d/%m/%Y")
        
        # Confirmar com o usuário
        view = ConfirmarFechamentoAutomaticoView(data_inicio, data_fim, data_inicio_str, data_fim_str)
        
        embed = discord.Embed(
            title="🔒 FECHAR METAS - SEMANA ANTERIOR",
            description=(
                f"📅 **Período a ser fechado:**\n"
                f"**{data_inicio_str}** a **{data_fim_str}**\n\n"
                f"⚠️ **ATENÇÃO:** Esta ação irá:\n"
                f"• Fechar TODAS as metas deste período\n"
                f"• Gerar o relatório completo\n"
                f"• Resetar as metas dos membros\n\n"
                f"🔄 **Esta semana é calculada automaticamente!**\n"
                f"📌 Sempre a semana anterior (Segunda a Domingo)"
            ),
            color=0xe67e22
        )
        
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# =========================================================
# ==================== CONFIRMAÇÃO FECHAMENTO AUTOMÁTICO ==
# =========================================================

class ConfirmarFechamentoAutomaticoView(discord.ui.View):
    def __init__(self, data_inicio, data_fim, data_inicio_str, data_fim_str):
        super().__init__(timeout=60)
        self.data_inicio = data_inicio
        self.data_fim = data_fim
        self.data_inicio_str = data_inicio_str
        self.data_fim_str = data_fim_str
    
    @discord.ui.button(
        label="✅ Confirmar Fechamento",
        style=discord.ButtonStyle.danger,
        custom_id="confirmar_fechamento_auto",
        emoji="✅"
    )
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        try:
            # 🔥 CORREÇÃO: Converter para datas sem timezone (NAIVE) para o banco
            data_inicio_naive = self.data_inicio.replace(tzinfo=None)
            data_fim_naive = self.data_fim.replace(tzinfo=None)
            
            # USAR A MESMA FUNÇÃO QUE JÁ EXISTE NO SEU CÓDIGO
            relatorio, membros_sem_meta = await fechar_todas_metas(
                data_inicio_naive,
                data_fim_naive
            )
            
            if not relatorio and not membros_sem_meta:
                await interaction.followup.send("📭 Nenhuma meta para fechar.", ephemeral=True)
                return
            
            total_dinheiro = sum(r["dinheiro"] for r in relatorio)
            total_polvora = sum(r["polvora"] for r in relatorio)
            total_dinheiro_acoes = sum(r["dinheiro_acoes"] for r in relatorio)
            total_geral = sum(r["total"] for r in relatorio)
            
            embed = discord.Embed(
                title="📊 RELATÓRIO SEMANAL - METAS FECHADAS",
                description=f"📅 **Período:** {self.data_inicio_str} até {self.data_fim_str}",
                color=0x2ecc71,
                timestamp=agora()
            )
            
            embed.add_field(
                name="📊 RESUMO GERAL",
                value=(
                    f"💰 **Dinheiro Sujo (Meta):** {formatar_dinheiro(total_dinheiro)}\n"
                    f"🎯 **Dinheiro de Ações:** {formatar_dinheiro(total_dinheiro_acoes)}\n"
                    f"💣 **Pólvora:** {fmt_num(total_polvora)} unidades\n"
                    f"📦 **Total Geral:** {formatar_dinheiro(total_geral)}\n"
                    f"👥 **Membros com meta:** {len(relatorio)}\n"
                    f"✅ **Pagaram:** {len([r for r in relatorio if r['total'] > 0])}\n"
                    f"❌ **Não pagaram:** {len([r for r in relatorio if r['total'] == 0])}"
                ),
                inline=False
            )
            
            if relatorio:
                pagaram = [r for r in relatorio if r["total"] > 0]
                nao_pagaram = [r for r in relatorio if r["total"] == 0]
                pagaram_ordenado = sorted(pagaram, key=lambda x: x["total"], reverse=True)
                
                if pagaram_ordenado:
                    lista_pagaram = ""
                    for i, item in enumerate(pagaram_ordenado[:10], 1):
                        user = await pegar_usuario(int(item["user_id"]))
                        nome = user.display_name if user else f"ID: {item['user_id']}"
                        lista_pagaram += f"**{i}.** {nome} - 💰 {formatar_dinheiro(item['total'])}\n"
                    
                    embed.add_field(
                        name=f"✅ QUEM PAGOU ({len(pagaram)} membros)",
                        value=lista_pagaram[:1024],
                        inline=False
                    )
                
                if nao_pagaram:
                    lista_nao_pagaram = ""
                    for i, item in enumerate(nao_pagaram[:10], 1):
                        user = await pegar_usuario(int(item["user_id"]))
                        nome = user.display_name if user else f"ID: {item['user_id']}"
                        lista_nao_pagaram += f"**{i}.** {nome} - ❌ **ZERADO**\n"
                    
                    embed.add_field(
                        name=f"❌ QUEM NÃO PAGOU ({len(nao_pagaram)} membros)",
                        value=lista_nao_pagaram[:1024],
                        inline=False
                    )
            
            if membros_sem_meta:
                lista_sem_meta = ""
                for i, item in enumerate(membros_sem_meta[:10], 1):
                    lista_sem_meta += f"**{i}.** {item['menção']} - ❌ **SEM META**\n"
                
                embed.add_field(
                    name=f"⚠️ MEMBROS SEM META ({len(membros_sem_meta)} membros)",
                    value=lista_sem_meta[:1024],
                    inline=False
                )
            
            embed.set_footer(text=f"Relatório gerado por {interaction.user.display_name} • Fechamento Automático")
            
            canal_resultados = interaction.guild.get_channel(RESULTADOS_METAS_ID)
            if canal_resultados:
                await canal_resultados.send(embed=embed)
                await interaction.followup.send(
                    f"✅ **Relatório enviado no canal <#{RESULTADOS_METAS_ID}>!**\n"
                    f"📅 Período: {self.data_inicio_str} a {self.data_fim_str}",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(embed=embed, ephemeral=True)
            
            # Atualizar painéis de metas
            for uid in metas_cache.keys():
                await atualizar_embed_meta(int(uid))
                await asyncio.sleep(0.3)
            
            print(f"📊 Relatório semanal automático gerado por {interaction.user.name}")
            
        except Exception as e:
            print(f"❌ Erro ao fechar metas automático: {e}")
            import traceback
            traceback.print_exc()
            await interaction.followup.send(f"❌ Erro ao fechar metas: {e}", ephemeral=True)
    
    @discord.ui.button(
        label="❌ Cancelar",
        style=discord.ButtonStyle.secondary,
        custom_id="cancelar_fechamento_auto",
        emoji="❌"
    )
    async def cancelar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ Operação cancelada.", ephemeral=True)


# =========================================================
# ==================== FUNÇÕES DE METAS ====================
# =========================================================

async def carregar_metas_cache():
    """Recarrega o cache de metas do banco"""
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
        print(f"📊 Cache de metas recarregado: {len(metas_cache)} metas")
        return True
    except Exception as e:
        print(f"❌ Erro ao recarregar cache de metas: {e}")
        return False


async def atualizar_embed_meta(user_id):
    """Atualiza o embed da meta de um usuário"""
    try:
        print(f"🔄 Atualizando meta de {user_id}...")
        
        if str(user_id) not in metas_cache:
            print(f"⚠️ Usuário {user_id} não está no cache, recarregando...")
            await carregar_metas_cache()
            
            if str(user_id) not in metas_cache:
                print(f"❌ Usuário {user_id} não tem meta no cache")
                guild = bot.get_guild(GUILD_ID)
                member = guild.get_member(int(user_id))
                if member:
                    await criar_sala_meta(member)
                    await carregar_metas_cache()
                return
        
        dados = metas_cache[str(user_id)]
        canal = bot.get_channel(dados["canal_id"])
        
        if not canal:
            print(f"❌ Canal da meta de {user_id} não encontrado (ID: {dados['canal_id']})")
            if str(user_id) in metas_cache:
                del metas_cache[str(user_id)]
            async with get_db().acquire() as conn:
                await conn.execute("DELETE FROM metas WHERE user_id = $1", str(user_id))
            print(f"🗑️ Meta removida do banco para {user_id} (canal não existe)")
            return
        
        print(f"✅ Canal encontrado: {canal.name} (ID: {canal.id})")
        
        async with get_db().acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(user_id))
        
        if not meta:
            print(f"❌ Meta de {user_id} não encontrada no banco, criando...")
            await salvar_meta_db(user_id, canal.id, 0, 0, 0)
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(user_id))
            if not meta:
                print(f"❌ Falha ao criar meta para {user_id}")
                return
            metas_cache[str(user_id)] = {
                "canal_id": canal.id,
                "dinheiro": 0,
                "polvora": 0,
                "acao": None,
                "dinheiro_acoes": 0
            }
            print(f"✅ Meta criada para {user_id}")
        
        user = await pegar_usuario(user_id)
        nome = user.display_name if user else str(user_id)
        
        dinheiro_meta = meta["dinheiro"] or 0
        dinheiro_acoes = meta.get("dinheiro_acoes") or 0
        polvora = meta["polvora"] or 0
        
        acao = meta.get("acao")
        if acao is None:
            acao = "Nenhuma"
        else:
            acao = str(acao)
        
        embed = discord.Embed(
            title=f"📊 META DE {nome.upper()}",
            color=0x3498db,
            timestamp=agora()
        )
        
        embed.add_field(
            name="💰 DINHEIRO SUJO (Meta)",
            value=formatar_dinheiro(dinheiro_meta),
            inline=True
        )
        
        embed.add_field(
            name="🎯 DINHEIRO DE AÇÕES",
            value=formatar_dinheiro(dinheiro_acoes),
            inline=True
        )
        
        embed.add_field(
            name="💣 PÓLVORA",
            value=f"{fmt_num(polvora)} unidades" if polvora > 0 else "0 unidades",
            inline=True
        )
        
        embed.add_field(
            name="🎯 AÇÃO ATUAL",
            value=acao,
            inline=False
        )
        
        embed.add_field(
            name="📌 COMO USAR",
            value=(
                "**💣 Adicionar Pólvora** - Registre pólvora da meta\n"
                "**💰 Adicionar Dinheiro Sujo** - Registre dinheiro da meta\n"
            ),
            inline=False
        )
        
        embed.set_footer(text=f"ID: {user_id}")
        
        mensagens_deletadas = 0
        async for msg in canal.history(limit=30):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    mensagens_deletadas += 1
                    await asyncio.sleep(0.3)
                except:
                    pass
        
        if mensagens_deletadas > 0:
            print(f"🗑️ {mensagens_deletadas} mensagens antigas deletadas de {canal.name}")
        
        await canal.send(embed=embed, view=MetaView(user_id))
        print(f"✅ Painel de meta criado para {nome} em {canal.name}")
        
        total = dinheiro_meta + dinheiro_acoes
        await verificar_meta_concluida(user_id, total)
        
    except Exception as e:
        print(f"❌ Erro ao atualizar embed da meta: {e}")
        import traceback
        traceback.print_exc()


async def enviar_painel_relatorio_metas():
    """Envia o painel para gerar relatório de metas e zerar metas"""
    canal = bot.get_channel(1521495685092999279)
    
    if not canal:
        print(f"❌ Canal de relatório de metas não encontrado: 1521495685092999279")
        return
    
    embed = discord.Embed(
        title="📊 GERENCIAMENTO DE METAS",
        description=(
            "**Gerencie as metas de todos os membros.**\n\n"
            "📌 **Opções disponíveis:**\n"
            "• 📊 **Gerar Relatório** - Relatório de metas fechadas (individual)\n"
            "• 🔒 **Fechar Metas Semanais** - Fecha TODAS as metas (com datas)\n"
            "• 🔒 **Fechar Metas (Automático)** - Fecha a semana anterior automaticamente\n"
            "• ⚠️ **Zerar Metas** - Resetar TODAS as metas (cuidado!)\n\n"
            "📋 **O relatório semanal mostra:**\n"
            "• Quem pagou e quanto\n"
            "• Quem NÃO pagou\n"
            "• Membros sem meta\n"
            "• Totais gerais"
        ),
        color=0x2ecc71
    )
    
    embed.add_field(
        name="📌 COMO USAR - FECHAR METAS (AUTOMÁTICO)",
        value=(
            "**Clique no botão verde e confirme:**\n"
            "• O sistema calcula a SEMANA ANTERIOR (Segunda a Domingo)\n"
            "• Fecha todas as metas do período\n"
            "• Gera o relatório automaticamente\n\n"
            "**Exemplo:**\n"
            "• Se fechar hoje (20/07/2026) → Fecha 13/07 a 19/07\n"
            "• Se fechar amanhã (21/07/2026) → Fecha 13/07 a 19/07\n"
            "• Sempre a SEMANA ANTERIOR completa!"
        ),
        inline=False
    )
    
    embed.add_field(
        name="📌 COMO USAR - FECHAR METAS (MANUAL)",
        value=(
            "**Fechar Metas Semanais (Manual):**\n"
            "• Informe a data de INÍCIO da semana\n"
            "• Informe a data de FIM da semana\n"
            "• O sistema vai fechar todas as metas e gerar relatório\n\n"
            "**Exemplo:**\n"
            "• Data INÍCIO: `01/07/2026`\n"
            "• Data FIM: `07/07/2026`"
        ),
        inline=False
    )
    
    view = discord.ui.View(timeout=None)
    view.add_item(RelatorioMetasButton())
    view.add_item(FecharTodasMetasButton())
    view.add_item(FecharMetasAutomaticoButton())  # ← NOVO BOTÃO
    view.add_item(ZerarMetasButton())
    
    await enviar_ou_atualizar_painel(
        "painel_relatorio_metas",
        1521495685092999279,
        embed,
        view
    )
    
    print("📊 Painel de gerenciamento de metas enviado")


# =========================================================
# ==================== COMANDOS DE METAS ===================
# =========================================================

@bot.command(name="forcar_meta")
async def cmd_forcar_meta(ctx, member: discord.Member = None):
    """Força a criação da meta no banco para um membro que já tem sala (ADM apenas)"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    if not member:
        member = ctx.author
    
    await ctx.send(f"🔄 Forçando criação da meta para {member.display_name}...")
    
    try:
        # Verificar se o membro já tem meta no banco
        async with get_db().acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(member.id))
        
        if meta:
            await ctx.send(f"✅ {member.display_name} já tem meta no banco! (ID: {meta['canal_id']})")
            await atualizar_embed_meta(member.id)
            return
        
        # Procurar uma sala existente
        sala_encontrada = None
        for canal in ctx.guild.text_channels:
            if member.display_name.lower() in canal.name.lower() and "📁" in canal.name:
                sala_encontrada = canal
                break
        
        if sala_encontrada:
            # Salvar meta no banco
            await salvar_meta_db(member.id, sala_encontrada.id, 0, 0, 0)
            
            metas_cache[str(member.id)] = {
                "canal_id": sala_encontrada.id,
                "dinheiro": 0,
                "polvora": 0,
                "acao": None,
                "dinheiro_acoes": 0
            }
            
            await atualizar_embed_meta(member.id)
            await ctx.send(f"✅ Meta criada para {member.display_name} usando sala existente: {sala_encontrada.mention}")
        else:
            # Criar sala
            sala = await criar_sala_meta(member)
            if sala:
                await ctx.send(f"✅ Sala e meta criadas para {member.display_name}!")
            else:
                await ctx.send(f"❌ Erro ao criar meta para {member.display_name}!")
        
    except Exception as e:
        await ctx.send(f"❌ Erro: {e}")
        print(f"Erro ao forçar meta: {e}")


@bot.command(name="atualizar_paineis_metas")
async def cmd_atualizar_paineis_metas(ctx):
    """Atualiza os painéis de metas em todas as salas existentes (ADM apenas)"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    await ctx.send("🔄 Atualizando painéis de metas em todas as salas...")
    
    try:
        metas = await carregar_metas_db()
        
        if not metas:
            await ctx.send("📭 Nenhuma meta encontrada.")
            return
        
        atualizadas = 0
        erros = 0
        
        for meta in metas:
            user_id = int(meta["user_id"])
            canal_id = int(meta["canal_id"])
            
            canal = ctx.guild.get_channel(canal_id)
            if not canal:
                print(f"❌ Canal {canal_id} não encontrado para usuário {user_id}")
                erros += 1
                continue
            
            try:
                await atualizar_embed_meta(user_id)
                atualizadas += 1
                await asyncio.sleep(1)
                print(f"✅ Painel atualizado para {canal.name}")
            except Exception as e:
                print(f"❌ Erro ao atualizar {canal.name}: {e}")
                erros += 1
        
        await ctx.send(
            f"✅ **{atualizadas} painéis atualizados!**\n"
            f"❌ **{erros} erros** (verifique o console)"
        )
        
    except Exception as e:
        await ctx.send(f"❌ Erro: {e}")
        print(f"Erro ao atualizar painéis: {e}")


@bot.command(name="criar_painel_meta")
async def cmd_criar_painel_meta(ctx, member: discord.Member = None):
    """Cria/atualiza o painel de meta de um membro específico (ADM apenas)"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    if not member:
        member = ctx.author
    
    await ctx.send(f"🔄 Criando painel de meta para {member.display_name}...")
    
    try:
        if str(member.id) not in metas_cache:
            await criar_sala_meta(member)
            await ctx.send(f"✅ Sala e painel criados para {member.display_name}!")
        else:
            await atualizar_embed_meta(member.id)
            await ctx.send(f"✅ Painel atualizado para {member.display_name}!")
            
    except Exception as e:
        await ctx.send(f"❌ Erro: {e}")
        print(f"Erro ao criar painel para {member.display_name}: {e}")


@bot.command(name="verificar_paineis_metas")
async def cmd_verificar_paineis_metas(ctx):
    """Verifica quais salas de meta têm painel (ADM apenas)"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    await ctx.send("🔍 Verificando salas de metas...")
    
    try:
        metas = await carregar_metas_db()
        
        if not metas:
            await ctx.send("📭 Nenhuma meta encontrada.")
            return
        
        com_painel = []
        sem_painel = []
        canais_nao_encontrados = []
        
        for meta in metas:
            user_id = int(meta["user_id"])
            canal_id = int(meta["canal_id"])
            
            canal = ctx.guild.get_channel(canal_id)
            if not canal:
                canais_nao_encontrados.append(f"Usuário {user_id} - Canal {canal_id}")
                continue
            
            tem_painel = False
            async for msg in canal.history(limit=30):
                if msg.author == bot.user and msg.embeds:
                    if msg.embeds[0].title and "META DE" in msg.embeds[0].title.upper():
                        tem_painel = True
                        break
            
            user = await pegar_usuario(user_id)
            nome = user.display_name if user else str(user_id)
            
            if tem_painel:
                com_painel.append(f"✅ {nome} - {canal.mention}")
            else:
                sem_painel.append(f"❌ {nome} - {canal.mention}")
        
        embed = discord.Embed(
            title="📊 VERIFICAÇÃO DE PAINÉIS DE METAS",
            color=0x3498db,
            timestamp=agora()
        )
        
        total = len(com_painel) + len(sem_painel) + len(canais_nao_encontrados)
        embed.add_field(
            name="📊 RESUMO",
            value=f"**Total de metas:** {total}\n"
                  f"✅ **Com painel:** {len(com_painel)}\n"
                  f"❌ **Sem painel:** {len(sem_painel)}\n"
                  f"⚠️ **Canais não encontrados:** {len(canais_nao_encontrados)}",
            inline=False
        )
        
        if com_painel:
            embed.add_field(
                name="✅ COM PAINEL",
                value="\n".join(com_painel[:20]) if com_painel else "Nenhum",
                inline=False
            )
        
        if sem_painel:
            embed.add_field(
                name="❌ SEM PAINEL",
                value="\n".join(sem_painel[:20]) if sem_painel else "Nenhum",
                inline=False
            )
        
        if canais_nao_encontrados:
            embed.add_field(
                name="⚠️ CANAIS NÃO ENCONTRADOS",
                value="\n".join(canais_nao_encontrados[:10]) if canais_nao_encontrados else "Nenhum",
                inline=False
            )
        
        embed.set_footer(text="Use !atualizar_paineis_metas para corrigir os que estão sem painel")
        
        await ctx.send(embed=embed)
        
    except Exception as e:
        await ctx.send(f"❌ Erro: {e}")
        print(f"Erro ao verificar painéis: {e}")


@bot.command(name="historico_metas")
async def cmd_historico_metas(ctx, data_inicio: str = None, data_fim: str = None):
    """Mostra o histórico de metas fechadas de um período (ADM apenas)"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    if not data_inicio or not data_fim:
        hoje = agora()
        dias_para_segunda = hoje.weekday()
        inicio_semana = hoje - timedelta(days=dias_para_segunda + 7)
        fim_semana = inicio_semana + timedelta(days=6)
        data_inicio = inicio_semana.strftime("%d/%m/%Y")
        data_fim = fim_semana.strftime("%d/%m/%Y")
        await ctx.send(f"📅 Buscando histórico da semana passada: {data_inicio} a {data_fim}")
    
    try:
        inicio = datetime.strptime(data_inicio, "%d/%m/%Y")
        fim = datetime.strptime(data_fim, "%d/%m/%Y")
        inicio_dt = inicio.replace(hour=0, minute=0, second=0)
        fim_dt = fim.replace(hour=23, minute=59, second=59)
    except ValueError:
        await ctx.send("❌ Formato de data inválido! Use DD/MM/AAAA")
        return
    
    historico = await buscar_historico_metas(inicio_dt, fim_dt)
    
    if not historico:
        await ctx.send(f"📭 Nenhuma meta fechada no período **{data_inicio}** a **{data_fim}**.")
        return
    
    total_dinheiro = sum(r["dinheiro"] for r in historico)
    total_acoes = sum(r.get("dinheiro_acoes") or 0 for r in historico)
    total_polvora = sum(r["polvora"] for r in historico)
    
    embed = discord.Embed(
        title="📊 HISTÓRICO DE METAS",
        description=f"📅 **Período:** {data_inicio} até {data_fim}",
        color=0x3498db
    )
    
    embed.add_field(
        name="💰 TOTAL DINHEIRO SUJO",
        value=formatar_dinheiro(total_dinheiro),
        inline=True
    )
    
    embed.add_field(
        name="🎯 TOTAL DINHEIRO AÇÕES",
        value=formatar_dinheiro(total_acoes),
        inline=True
    )
    
    embed.add_field(
        name="💣 TOTAL PÓLVORA",
        value=f"{fmt_num(total_polvora)} unidades",
        inline=True
    )
    
    lista = ""
    for item in historico[:20]:
        user = await pegar_usuario(int(item["user_id"]))
        nome = user.display_name if user else f"ID: {item['user_id']}"
        total = item["dinheiro"] + (item.get("dinheiro_acoes") or 0)
        lista += f"👤 {nome} - 💰 {formatar_dinheiro(total)} - 💣 {fmt_num(item['polvora'])}\n"
    
    if len(historico) > 20:
        lista += f"\n*... e mais {len(historico) - 20} registros*"
    
    embed.add_field(name="📋 METAS FECHADAS", value=lista, inline=False)
    embed.set_footer(text=f"Total: {len(historico)} metas fechadas")
    
    await ctx.send(embed=embed)


async def fixar_painel_meta_no_final(user_id):
    """Garante que o painel de meta seja a última mensagem do canal"""
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
            print(f"📌 Painel de meta recolocado no final para {user_id}")
        except Exception as e:
            print(f"Erro ao recolocar painel: {e}")
        
    except Exception as e:
        print(f"❌ Erro ao fixar painel: {e}")
        import traceback
        traceback.print_exc()


async def verificar_meta_concluida(user_id, valor_total):
    """Verifica se a meta atingiu R$ 300.000 e envia aviso"""
    if valor_total >= 300000:
        async with get_db().acquire() as conn:
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
                            description=f"{user.mention} **parabéns!** Sua meta semanal de **R$ 300.000,00** foi atingida! 🎉",
                            color=0x2ecc71
                        )
                        embed.add_field(
                            name="💰 Total atingido",
                            value=formatar_dinheiro(valor_total),
                            inline=True
                        )
                        embed.add_field(
                            name="📅 Data",
                            value=agora().strftime('%d/%m/%Y %H:%M'),
                            inline=True
                        )
                        await canal.send(embed=embed)
                        return True
        return False
    return False


async def verificar_avisos_quarta():
    """Verifica na quarta-feira se membros não fizeram depósitos"""
    hoje = agora()
    
    if hoje.weekday() != 2:
        return
    
    async with get_db().acquire() as conn:
        metas = await conn.fetch("SELECT * FROM metas")
        
        for meta in metas:
            user_id = meta["user_id"]
            dinheiro = meta["dinheiro"] or 0
            dinheiro_acoes = meta.get("dinheiro_acoes") or 0
            total = dinheiro + dinheiro_acoes
            
            if total == 0:
                ja_avisado = await conn.fetchval(
                    "SELECT 1 FROM metas_avisos WHERE user_id = $1 AND tipo = 'quarta' AND data::date = $2",
                    str(user_id), hoje.date()
                )
                
                if not ja_avisado:
                    await conn.execute(
                        "INSERT INTO metas_avisos (user_id, tipo, data) VALUES ($1, 'quarta', $2)",
                        str(user_id), agora_db()
                    )
                    
                    canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(user_id))
                    if canal_id:
                        canal = bot.get_channel(int(canal_id))
                        if canal:
                            user = await pegar_usuario(user_id)
                            embed = discord.Embed(
                                title="⚠️ AVISO DE META SEMANAL",
                                description=f"{user.mention} **atenção!**",
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
                            return True
    return False

# =========================================================
# ==================== FIM DO SISTEMA DE METAS ============
# =========================================================

# =========================================================
# ==================== SISTEMA DE VENDAS ===================
# =========================================================

class StatusView(discord.ui.View):
    def __init__(self, disabled: bool = False, entrega_id: int = None, total_entregas: int = 1):
        super().__init__(timeout=None)
        self.entrega_id = entrega_id
        self.total_entregas = total_entregas
        self.entrega_ja_entregue = False
        self.proxima_criada = False
        
        # Se for venda parcelada, habilitar botão "Criar Próxima Entrega"
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
        if self.entrega_ja_entregue:
            await interaction.response.send_message(
                "⚠️ **Esta entrega já foi marcada como entregue!**",
                ephemeral=True
            )
            return

        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        if self.pedido_cancelado(linhas):
            await interaction.response.send_message("⚠️ Este pedido foi cancelado.", ephemeral=True)
            return

        if self.entrega_ja_foi_entregue(linhas):
            await interaction.response.send_message(
                "⚠️ **Esta entrega já foi entregue!**",
                ephemeral=True
            )
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

        self.entrega_ja_entregue = True

        # Desabilitar APENAS o botão Entregue
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
                    print("Erro envio baú:", e)

        await enviar_painel_vendas()
        await enviar_painel_fabricacao()

    @discord.ui.button(label="📦 Criar Próxima Entrega", style=discord.ButtonStyle.primary, custom_id="criar_proxima_entrega", disabled=False)
    async def criar_proxima(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Cria a próxima entrega - SEMPRE DISPONÍVEL para vendas parceladas"""
        
        if not self.entrega_id:
            await interaction.response.send_message(
                "❌ Esta venda não tem entregas parceladas.",
                ephemeral=True
            )
            return

        if self.proxima_criada:
            await interaction.response.send_message(
                "⚠️ **A próxima entrega já foi criada!**",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        try:
            async with get_db().acquire() as conn:
                entrega = await conn.fetchrow(
                    "SELECT * FROM entregas_parceladas WHERE id = $1 AND ativo = true",
                    self.entrega_id
                )

            if not entrega:
                await interaction.followup.send(
                    "❌ **Entrega não encontrada no banco de dados!**",
                    ephemeral=True
                )
                return

            entrega_atual = entrega["entrega_atual"]
            total_entregas = entrega["total_entregas"]

            if entrega_atual >= total_entregas:
                await interaction.followup.send(
                    f"✅ **Todas as {total_entregas} entregas já foram concluídas!**",
                    ephemeral=True
                )
                self.proxima_criada = True
                for child in self.children:
                    if child.custom_id == "criar_proxima_entrega":
                        child.disabled = True
                        child.label = "✅ Todas criadas"
                await interaction.message.edit(view=self)
                return

            proxima_entrega_num = entrega_atual + 1
            pedido_original = entrega["pedido_original"]

            # Buscar lista de entregas
            import json
            async with get_db().acquire() as conn2:
                detalhes = await conn2.fetchrow(
                    "SELECT entregas_json FROM entregas_detalhes WHERE entrega_id = $1",
                    self.entrega_id
                )

            if detalhes and detalhes["entregas_json"]:
                entregas_lista = json.loads(detalhes["entregas_json"])
            else:
                # Fallback
                async with get_db().acquire() as conn3:
                    primeira = await conn3.fetchrow(
                        "SELECT pt_por_entrega, sub_por_entrega FROM entregas_parceladas WHERE pedido_original = $1 ORDER BY id ASC LIMIT 1",
                        pedido_original
                    )
                
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

            # Pegar próxima entrega
            idx = proxima_entrega_num - 1
            if idx >= len(entregas_lista):
                await interaction.followup.send(
                    f"❌ **Erro: Entrega {proxima_entrega_num} não encontrada!**",
                    ephemeral=True
                )
                return

            entrega_data = entregas_lista[idx]
            pt_entrega = entrega_data["pt"]
            sub_entrega = entrega_data["sub"]

            if pt_entrega == 0 and sub_entrega == 0:
                await interaction.followup.send(
                    f"✅ **Todas as entregas foram concluídas!**",
                    ephemeral=True
                )
                self.proxima_criada = True
                for child in self.children:
                    if child.custom_id == "criar_proxima_entrega":
                        child.disabled = True
                        child.label = "✅ Todas criadas"
                await interaction.message.edit(view=self)
                return

            # Criar embed da próxima entrega
            vendedor_id = entrega["vendedor_id"]
            organizacao = entrega["organizacao"]
            observacoes = entrega["observacoes"]
            canal_id = int(entrega["canal_id"])

            canal = bot.get_channel(canal_id)
            if not canal:
                await interaction.followup.send(
                    f"❌ **Canal {canal_id} não encontrado!**",
                    ephemeral=True
                )
                return

            config = ORGANIZACOES_CONFIG.get(organizacao, {"emoji": "🏷️", "cor": 0x1e3a8a})
            grupo = await buscar_grupo_por_organizacao(organizacao)

            titulo_embed = f"📦 ENTREGA {proxima_entrega_num}/{total_entregas} • Pedido #{pedido_original:04d}"
            descricao = f"**🔴 ATENÇÃO! Esta venda tem {total_entregas} entregas no total!**\n📦 **Esta entrega contém:** PT {fmt_num(pt_entrega)} + SUB {fmt_num(sub_entrega)} munições"

            embed_novo = discord.Embed(
                title=titulo_embed,
                description=descricao,
                color=config["cor"]
            )

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
            valor_formatado = formatar_dinheiro(valor_entrega)

            embed_novo.add_field(name="💰 Valor (esta entrega)", value=f"**{valor_formatado}**", inline=False)

            embed_novo.add_field(
                name="📋 STATUS DAS ENTREGAS",
                value=(
                    f"**Total de entregas:** {total_entregas}\n"
                    f"**Entrega atual:** {proxima_entrega_num}/{total_entregas}\n"
                    f"**Próxima entrega:** 🔒 Aguardando esta ser ENTREGUE"
                ),
                inline=False
            )

            embed_novo.add_field(name="📌 Status", value="📦 A Entregar\n⏳ Pagamento pendente", inline=False)

            if observacoes:
                embed_novo.add_field(name="📝 Observações", value=observacoes, inline=False)

            if grupo:
                embed_novo.add_field(
                    name="📊 INTEGRAÇÃO COM GRUPO",
                    value=f"✅ Compra registrada automaticamente no grupo **{organizacao}**",
                    inline=False
                )

            embed_novo.set_footer(
                text=f"🛡 Sistema de Encomendas • VDR 442 • Entrega {proxima_entrega_num}/{total_entregas} • ID: {self.entrega_id}"
            )

            # Enviar nova entrega com botão habilitado
            msg = await canal.send(embed=embed_novo, view=StatusView(entrega_id=self.entrega_id, total_entregas=total_entregas))

            # Atualizar banco
            await atualizar_entrega_parcelada(
                entrega_id=self.entrega_id,
                entrega_atual=proxima_entrega_num,
                mensagem_id=str(msg.id),
                proxima_entrega=None
            )

            self.proxima_criada = True
            for child in self.children:
                if child.custom_id == "criar_proxima_entrega":
                    child.disabled = True
                    child.label = f"✅ Próxima criada ({proxima_entrega_num}/{total_entregas})"

            await interaction.message.edit(view=self)

            await interaction.followup.send(
                f"✅ **Entrega {proxima_entrega_num}/{total_entregas} criada com sucesso!**\n"
                f"📦 Conteúdo: PT {fmt_num(pt_entrega)} + SUB {fmt_num(sub_entrega)} munições",
                ephemeral=True
            )

            await enviar_painel_vendas()
            await enviar_painel_fabricacao()

        except Exception as e:
            print(f"❌ Erro ao criar próxima entrega: {e}")
            import traceback
            traceback.print_exc()
            await interaction.followup.send(
                f"❌ **Erro ao criar próxima entrega:** {str(e)}",
                ephemeral=True
            )

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

        # Verificar se já foi entregue para reverter estoque
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
                        print("Erro envio baú reversão:", e)

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
        placeholder="Digite a quantidade de munição PT (ex: 24000)",
        required=True
    )
    qtd_sub = discord.ui.TextInput(
        label="Quantidade SUB",
        placeholder="Digite a quantidade de munição SUB (ex: 16000)",
        required=True
    )
    total_entregas = discord.ui.TextInput(
        label="Número de entregas",
        placeholder="Ex: 2, 3, 4... (padrão: 1)",
        required=False
    )
    observacoes = discord.ui.TextInput(
        label="Observações",
        style=discord.TextStyle.paragraph,
        required=False,
        placeholder="Observações adicionais sobre a venda"
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            pt = int(self.qtd_pt.value.strip()) if self.qtd_pt.value.strip() else 0
            sub = int(self.qtd_sub.value.strip()) if self.qtd_sub.value.strip() else 0
            
            if pt < 0 or sub < 0:
                raise ValueError
                
            if pt == 0 and sub == 0:
                await interaction.response.send_message(
                    "❌ Você precisa informar pelo menos PT ou SUB!",
                    ephemeral=True
                )
                return
                
        except ValueError:
            await interaction.response.send_message(
                "❌ Valores inválidos. Digite apenas números positivos.",
                ephemeral=True
            )
            return
        
        # Número de entregas (padrão: 1)
        try:
            total_entregas = int(self.total_entregas.value.strip()) if self.total_entregas.value else 1
            if total_entregas < 1:
                total_entregas = 1
        except:
            total_entregas = 1
        
        org_nome = self.organizacao.value.strip().upper()
        config = ORGANIZACOES_CONFIG.get(
            org_nome,
            {"emoji": "🏷️", "cor": 0x1e3a8a}
        )
        
        numero_pedido = await proximo_pedido()
        
        LIMITE_DIARIO = 8000
        
        # =========================================================
        # ================= CRIAR LISTA DE ENTREGAS ===============
        # =========================================================
        
        # Calcular quantas entregas de PT
        if pt == 0:
            entregas_pt = 0
        else:
            entregas_pt = (pt + LIMITE_DIARIO - 1) // LIMITE_DIARIO
        
        # Calcular quantas entregas de SUB
        if sub == 0:
            entregas_sub = 0
        else:
            entregas_sub = (sub + LIMITE_DIARIO - 1) // LIMITE_DIARIO
        
        # Total de entregas é o MAIOR
        num_entregas = max(entregas_pt, entregas_sub)
        if num_entregas == 0:
            num_entregas = 1
        
        # Se o usuário especificou um número diferente, usar o maior
        if total_entregas > num_entregas:
            num_entregas = total_entregas
        
        # Construir lista de entregas com valores REAIS
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
            
            entregas_lista.append({
                "pt": pt_entrega,
                "sub": sub_entrega
            })
        
        # =========================================================
        # ================= SALVAR O TOTAL REAL ===================
        # =========================================================
        
        # SALVAR A LISTA COMPLETA DE ENTREGAS NO BANCO
        import json
        entregas_json = json.dumps(entregas_lista)
        
        pacotes_pt_total = pt // 50
        pacotes_sub_total = sub // 50
        
        total = (pt * 50) + (sub * 90)
        
        await salvar_venda_db(
            str(interaction.user.id),
            total,
            numero_pedido
        )
        
        valor_formatado = formatar_dinheiro(total)
        
        # =========================================================
        # ================= INTEGRAÇÃO COM GRUPOS =================
        # =========================================================
        
        grupo = await buscar_grupo_por_organizacao(org_nome)
        
        if grupo:
            if pacotes_pt_total > 0:
                valor_pt = pacotes_pt_total * 50
                await registrar_compra_grupo_db(grupo["grupo_id"], "PT", pacotes_pt_total, valor_pt)
                print(f"📦 PT registrado no grupo {org_nome}: {pacotes_pt_total} pacotes")
            
            if pacotes_sub_total > 0:
                valor_sub = pacotes_sub_total * 90
                await registrar_compra_grupo_db(grupo["grupo_id"], "SUB", pacotes_sub_total, valor_sub)
                print(f"📦 SUB registrado no grupo {org_nome}: {pacotes_sub_total} pacotes")
            
            await enviar_embed_grupo(grupo["grupo_id"])
            print(f"✅ Venda integrada com grupo: {org_nome}")
        
        # =========================================================
        # ================= CRIAR ENTREGAS ========================
        # =========================================================
        
        if num_entregas > 1:
            # Venda parcelada - salvar a PRIMEIRA entrega como referência
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
            
            # Salvar a lista completa de entregas
            async with get_db().acquire() as conn:
                await conn.execute(
                    "INSERT INTO entregas_detalhes (entrega_id, entregas_json) VALUES ($1, $2) ON CONFLICT (entrega_id) DO UPDATE SET entregas_json = $2",
                    entrega_id, entregas_json
                )
            
            # Primeira entrega
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
            
            msg_resposta = (
                f"✅ **Venda parcelada registrada!**\n\n"
                f"📦 **Pedido #{numero_pedido:04d}**\n"
                f"🏷 **Organização:** {org_nome}\n"
                f"📦 **Total PT:** {fmt_num(pt)} munições\n"
                f"📦 **Total SUB:** {fmt_num(sub)} munições\n"
                f"💰 **Total:** R$ {valor_formatado}\n\n"
                f"📋 **Entregas ({num_entregas} no total):**\n{resumo_entregas}\n"
                f"✅ **Entrega 1/{num_entregas} criada!**\n"
                f"📌 Próxima entrega: Clique em 'Criar Próxima Entrega' após entregar"
            )
            
            if grupo:
                msg_resposta += f"\n📊 **Grupo integrado:** ✅ {org_nome}"
            else:
                msg_resposta += f"\n📊 **Grupo integrado:** ❌ Nenhum grupo encontrado"
            
            await interaction.response.send_message(msg_resposta, ephemeral=True)
            
            await enviar_painel_vendas()
            await enviar_painel_fabricacao()
            return
        
        # =========================================================
        # ================= ENTREGA ÚNICA =========================
        # =========================================================
        
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
        
        msg_resposta = (
            f"✅ **Venda registrada!**\n\n"
            f"📦 **Pedido #{numero_pedido:04d}**\n"
            f"🏷 **Organização:** {org_nome}\n"
            f"🔫 **PT:** {fmt_num(pt)} munições\n"
            f"🔫 **SUB:** {fmt_num(sub)} munições\n"
            f"💰 **Total:** R$ {valor_formatado}"
        )
        
        if grupo:
            msg_resposta += f"\n📊 **Grupo integrado:** ✅ {org_nome}"
        else:
            msg_resposta += f"\n📊 **Grupo integrado:** ❌ Nenhum grupo encontrado"
        
        await interaction.response.send_message(msg_resposta, ephemeral=True)
        
        await enviar_painel_vendas()
        await enviar_painel_fabricacao()
        
# =========================================================
# ==================== FUNÇÃO PARA CRIAR EMBED DE ENTREGA =
# =========================================================

async def criar_embed_entrega(
    interaction: discord.Interaction,
    pedido_numero: int,
    entrega_atual: int,
    total_entregas: int,
    pt: int,
    sub: int,
    org_nome: str,
    config: dict,
    observacoes: str,
    entrega_id: int = None,
    vendedor_id: str = None,
    grupo: dict = None,
    entregas_lista: list = None
):
    """Cria um embed de entrega (única ou parcelada)"""
    
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
    
    embed = discord.Embed(
        title=titulo,
        description=descricao,
        color=config["cor"]
    )
    
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
        
        embed.add_field(
            name="🚨 RESUMO DAS ENTREGAS",
            value=resumo,
            inline=False
        )
    elif total_entregas > 1:
        embed.add_field(
            name="🚨 QUANTAS ENTREGAS?",
            value=f"**{total_entregas} ENTREGAS NO TOTAL**\n📌 Esta é a entrega **{entrega_atual}/{total_entregas}**\n🔄 Próxima entrega: Será criada quando esta for **ENTREGUE**",
            inline=False
        )
    
    embed.add_field(
        name="👤 Vendedor",
        value=f"<@{vendedor_id or interaction.user.id}>",
        inline=False
    )
    
    embed.add_field(
        name="🏷 Organização",
        value=f"{config['emoji']} {org_nome}",
        inline=False
    )
    
    embed.add_field(
        name="🔫 PT",
        value=f"{fmt_num(pt)} munições\n📦 {pacotes_pt} pacotes",
        inline=True
    )
    
    embed.add_field(
        name="🔫 SUB",
        value=f"{fmt_num(sub)} munições\n📦 {pacotes_sub} pacotes",
        inline=True
    )
    
    valor_entrega = (pt * 50) + (sub * 90)
    valor_formatado = (
        f"{valor_entrega:,.2f}"
        .replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
    )
    
    embed.add_field(
        name="💰 Valor (esta entrega)",
        value=f"**R$ {valor_formatado}**",
        inline=False
    )
    
    if total_entregas > 1:
        embed.add_field(
            name="📋 STATUS DAS ENTREGAS",
            value=(
                f"**Total de entregas:** {total_entregas}\n"
                f"**Entrega atual:** {entrega_atual}/{total_entregas}\n"
                f"**Próxima entrega:** 🔒 Aguardando esta ser ENTREGUE"
            ),
            inline=False
        )
    
    embed.add_field(
        name="📌 Status",
        value="📦 A Entregar\n⏳ Pagamento pendente",
        inline=False
    )
    
    if observacoes:
        embed.add_field(
            name="📝 Observações",
            value=observacoes,
            inline=False
        )
    
    if grupo:
        embed.add_field(
            name="📊 INTEGRAÇÃO COM GRUPO",
            value=f"✅ Compra registrada automaticamente no grupo **{org_nome}**",
            inline=False
        )
    
    if entrega_id:
        embed.set_footer(
            text=f"🛡 Sistema de Encomendas • VDR 442 • Entrega {entrega_atual}/{total_entregas} • ID: {entrega_id}"
        )
    else:
        embed.set_footer(
            text=f"🛡 Sistema de Encomendas • VDR 442 • Entrega {entrega_atual}/{total_entregas}"
        )
    
    msg = await canal.send(embed=embed, view=StatusView(entrega_id=entrega_id))
    
    if entrega_id:
        await atualizar_entrega_parcelada(
            entrega_id=entrega_id,
            entrega_atual=entrega_atual,
            mensagem_id=str(msg.id),
            proxima_entrega=None
        )
    
    return msg

# =========================================================
# ==================== CRIAR PRÓXIMA ENTREGA ==============
# =========================================================

async def criar_proxima_entrega(entrega_id: int, guild: discord.Guild):
    """Cria a próxima entrega quando a atual for entregue"""
    try:
        async with get_db().acquire() as conn:
            entrega = await conn.fetchrow(
                "SELECT * FROM entregas_parceladas WHERE id = $1 AND ativo = true",
                entrega_id
            )
        
        if not entrega:
            print(f"❌ Entrega {entrega_id} não encontrada")
            return
        
        entrega_atual = entrega["entrega_atual"]
        total_entregas = entrega["total_entregas"]
        
        # SE JÁ CHEGOU NA ÚLTIMA, FINALIZAR
        if entrega_atual >= total_entregas:
            await finalizar_entregas(entrega_id)
            print(f"✅ Todas as {total_entregas} entregas foram concluídas!")
            return
        
        # PRÓXIMA ENTREGA
        proxima_entrega_num = entrega_atual + 1
        
        print(f"🔄 Criando próxima entrega {proxima_entrega_num}/{total_entregas}")
        
        pedido_original = entrega["pedido_original"]
        pt_total = entrega["pt_por_entrega"] * total_entregas  # PT total original
        sub_total = entrega["sub_por_entrega"] * total_entregas  # SUB total original
        vendedor_id = entrega["vendedor_id"]
        organizacao = entrega["organizacao"]
        observacoes = entrega["observacoes"]
        canal_id = int(entrega["canal_id"])
        
        canal = bot.get_channel(canal_id)
        if not canal:
            print(f"❌ Canal {canal_id} não encontrado")
            return
        
        config = ORGANIZACOES_CONFIG.get(
            organizacao,
            {"emoji": "🏷️", "cor": 0x1e3a8a}
        )
        
        grupo = await buscar_grupo_por_organizacao(organizacao)
        
        # =========================================================
        # ================= CALCULAR QUANTAS ENTREGAS =============
        # =========================================================
        
        LIMITE_DIARIO = 8000
        
        # Calcular entregas de PT
        if pt_total == 0:
            entregas_pt = 0
        else:
            entregas_pt = (pt_total + LIMITE_DIARIO - 1) // LIMITE_DIARIO  # Arredonda pra cima
        
        # Calcular entregas de SUB
        if sub_total == 0:
            entregas_sub = 0
        else:
            entregas_sub = (sub_total + LIMITE_DIARIO - 1) // LIMITE_DIARIO  # Arredonda pra cima
        
        # Total de entregas é o MAIOR entre os dois
        total_entregas_calc = max(entregas_pt, entregas_sub)
        
        # =========================================================
        # ================= CONSTRUIR LISTA DE ENTREGAS ===========
        # =========================================================
        
        entregas_lista = []
        
        for i in range(total_entregas_calc):
            entrega_num = i + 1
            
            # =========================================================
            # ================= CALCULAR PT ===========================
            # =========================================================
            
            if pt_total > 0:
                if entrega_num == total_entregas_calc:
                    # ÚLTIMA ENTREGA: o que sobrou
                    pt_entrega = pt_total - (pt_por_entrega * (total_entregas_calc - 1))
                else:
                    # ENTREGAS NORMAIS: 8.000 (ou o que faltar)
                    pt_entrega = min(LIMITE_DIARIO, pt_total - (pt_por_entrega * i))
            else:
                pt_entrega = 0
            
            # =========================================================
            # ================= CALCULAR SUB ==========================
            # =========================================================
            
            if sub_total > 0:
                if entrega_num == total_entregas_calc:
                    # ÚLTIMA ENTREGA: o que sobrou
                    sub_entrega = sub_total - (sub_por_entrega * (total_entregas_calc - 1))
                else:
                    # ENTREGAS NORMAIS: 8.000 (ou o que faltar)
                    sub_entrega = min(LIMITE_DIARIO, sub_total - (sub_por_entrega * i))
            else:
                sub_entrega = 0
            
            entregas_lista.append({
                "pt": pt_entrega,
                "sub": sub_entrega
            })
        
        # =========================================================
        # ================= PEGAR ESTA ENTREGA =====================
        # =========================================================
        
        entrega_atual_data = entregas_lista[proxima_entrega_num - 1]
        pt_entrega = entrega_atual_data["pt"]
        sub_entrega = entrega_atual_data["sub"]
        
        # =========================================================
        # ================= CRIAR EMBED ===========================
        # =========================================================
        
        titulo = f"📦 ENTREGA {proxima_entrega_num}/{total_entregas} • Pedido #{pedido_original:04d}"
        descricao = f"**🔴 ATENÇÃO! Esta venda tem {total_entregas} entregas no total!**\n📦 **Esta entrega contém:** PT {fmt_num(pt_entrega)} + SUB {fmt_num(sub_entrega)} munições"
        
        embed = discord.Embed(
            title=titulo,
            description=descricao,
            color=config["cor"]
        )
        
        resumo = ""
        for i, e in enumerate(entregas_lista, 1):
            if i < proxima_entrega_num:
                status = "✅"
            elif i == proxima_entrega_num:
                status = "🔴"
            else:
                status = "⏳"
            resumo += f"{status} Entrega {i}/{total_entregas}: PT {fmt_num(e['pt'])} + SUB {fmt_num(e['sub'])} munições\n"
        
        embed.add_field(
            name="🚨 RESUMO DAS ENTREGAS",
            value=resumo,
            inline=False
        )
        
        embed.add_field(
            name="👤 Vendedor",
            value=f"<@{vendedor_id}>",
            inline=False
        )
        
        embed.add_field(
            name="🏷 Organização",
            value=f"{config['emoji']} {organizacao}",
            inline=False
        )
        
        pacotes_pt = pt_entrega // 50
        pacotes_sub = sub_entrega // 50
        
        embed.add_field(
            name="🔫 PT",
            value=f"{fmt_num(pt_entrega)} munições\n📦 {pacotes_pt} pacotes",
            inline=True
        )
        
        embed.add_field(
            name="🔫 SUB",
            value=f"{fmt_num(sub_entrega)} munições\n📦 {pacotes_sub} pacotes",
            inline=True
        )
        
        valor_entrega = (pt_entrega * 50) + (sub_entrega * 90)
        valor_formatado = (
            f"{valor_entrega:,.2f}"
            .replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )
        
        embed.add_field(
            name="💰 Valor (esta entrega)",
            value=f"**R$ {valor_formatado}**",
            inline=False
        )
        
        embed.add_field(
            name="📋 STATUS DAS ENTREGAS",
            value=(
                f"**Total de entregas:** {total_entregas}\n"
                f"**Entrega atual:** {proxima_entrega_num}/{total_entregas}\n"
                f"**Próxima entrega:** 🔒 Aguardando esta ser ENTREGUE"
            ),
            inline=False
        )
        
        embed.add_field(
            name="📌 Status",
            value="📦 A Entregar\n⏳ Pagamento pendente",
            inline=False
        )
        
        if observacoes:
            embed.add_field(
                name="📝 Observações",
                value=observacoes,
                inline=False
            )
        
        if grupo:
            embed.add_field(
                name="📊 INTEGRAÇÃO COM GRUPO",
                value=f"✅ Compra registrada automaticamente no grupo **{organizacao}**",
                inline=False
            )
        
        embed.set_footer(
            text=f"🛡 Sistema de Encomendas • VDR 442 • Entrega {proxima_entrega_num}/{total_entregas} • ID: {entrega_id}"
        )
        
        # =========================================================
        # ================= ENVIAR MENSAGEM =======================
        # =========================================================
        
        msg = await canal.send(embed=embed, view=StatusView(entrega_id=entrega_id))
        
        # =========================================================
        # ================= ATUALIZAR BANCO =======================
        # =========================================================
        
        await atualizar_entrega_parcelada(
            entrega_id=entrega_id,
            entrega_atual=proxima_entrega_num,
            mensagem_id=str(msg.id),
            proxima_entrega=None
        )
        
        print(f"✅ Entrega {proxima_entrega_num}/{total_entregas} criada para pedido #{pedido_original}")
        
                
    except Exception as e:
        print(f"❌ Erro ao criar próxima entrega: {e}")
        import traceback
        traceback.print_exc()

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

# =========================================================
# ==================== HEARTBEAT PRODUÇÕES ================
# =========================================================

async def verificar_heartbeat_producoes():
    """Verifica se todas as produções estão sendo atualizadas"""
    try:
        async with get_db().acquire() as conn:
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
            
            # Verificar se já passou do fim
            if agora_br >= fim:
                # Finalizar produção
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
        
        # Verificar cada produção ativa
        for pid, fim in producoes_ativas.items():
            # Verificar se a task está rodando
            if pid not in producoes_tasks or producoes_tasks[pid].done():
                print(f"🔧 HEARTBEAT: Task morta para {pid}, recriando...")
                if pid in producoes_tasks:
                    del producoes_tasks[pid]
                task = asyncio.create_task(acompanhar_producao(pid))
                producoes_tasks[pid] = task
                print(f"✅ HEARTBEAT: Task recriada para {pid}")
            
            # Verificar se a mensagem ainda existe
            prod = await carregar_producao(pid)
            if prod:
                canal = bot.get_channel(prod["canal_id"])
                if canal:
                    try:
                        msg = await canal.fetch_message(prod["msg_id"])
                    except discord.NotFound:
                        print(f"⚠️ HEARTBEAT: Mensagem perdida para {pid}, recriando...")
                        desc = await gerar_desc_producao(prod)
                        embed = discord.Embed(title="🏭 Produção", description=desc, color=0x3498db)
                        view = None if prod.get("segunda_task_confirmada") else SegundaTaskView(pid)
                        msg = await canal.send(embed=embed, view=view)
                        prod["msg_id"] = msg.id
                        await salvar_producao(pid, prod)
                        print(f"✅ HEARTBEAT: Mensagem recriada para {pid}")
        
        print(f"💚 HEARTBEAT: {len(producoes_ativas)} produções ativas verificadas")
        
    except Exception as e:
        print(f"❌ Erro no heartbeat: {e}")
        import traceback
        traceback.print_exc()
        
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

        # =========================================================
        # ================= ATUALIZAR PAINÉIS =====================
        # =========================================================
        await enviar_painel_fabricacao()
        await enviar_painel_vendas()


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

        # =========================================================
        # ================= ATUALIZAR PAINEL ======================
        # =========================================================
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

        # =========================================================
        # ================= ATUALIZAR PAINEL ======================
        # =========================================================
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
    quantidade = discord.ui.TextInput(
        label="Quantidade de Pólvora",
        placeholder="Digite apenas a quantidade (ex: 100)",
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value.strip())
            if qtd <= 0:
                raise ValueError
        except:
            await interaction.response.send_message(
                "❌ Quantidade inválida! Digite um número positivo.",
                ephemeral=True
            )
            return
        
        valor = qtd * PRECO_POLVORA
        
        # Salvar no banco
        try:
            await salvar_polvora_db(
                interaction.user.id,
                qtd,
                valor
            )
        except Exception as e:
            print(f"❌ Erro ao salvar pólvora: {e}")
            await interaction.response.send_message(
                f"❌ Erro ao salvar: {e}",
                ephemeral=True
            )
            return
        
        canal = interaction.guild.get_channel(CANAL_REGISTRO_POLVORA_ID)
        
        if not canal:
            await interaction.response.send_message(
                "❌ Canal de registro não encontrado!",
                ephemeral=True
            )
            return
        
        valor_formatado = formatar_dinheiro(valor)
        
        embed = discord.Embed(
            title="🧨 Registro de Pólvora",
            color=0xe67e22,
            timestamp=agora()
        )
        
        embed.add_field(
            name="Registrado por",
            value=interaction.user.mention,
            inline=False
        )
        
        embed.add_field(
            name="Quantidade",
            value=f"{fmt_num(qtd)} unidades",
            inline=True
        )
        
        embed.add_field(
            name="Valor total",
            value=f"**{valor_formatado}**",
            inline=True
        )
        
        embed.set_footer(text=f"R$ {PRECO_POLVORA:.2f} por unidade")
        
        await canal.send(embed=embed)
        
        await interaction.response.send_message(
            f"✅ **Registro feito com sucesso!**\n\n"
            f"📦 Quantidade: {fmt_num(qtd)} unidades\n"
            f"💰 Valor: {valor_formatado}",
            ephemeral=True
        )
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
    dias = discord.ui.TextInput(
        label="Quantos dias de aluguel?",
        placeholder="Digite o número de dias (ex: 15, 30, 7)",
        required=True
    )
    
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
        
        # =========================================================
        # ================= VERIFICAR SE ESTÁ ALUGADO =============
        # =========================================================
        
        # 🔥 CORREÇÃO: Verificar se está alugado e se ainda não expirou
        if self.galpao in alugueis_ativos:
            dados = alugueis_ativos[self.galpao]
            # Se ainda não expirou, não pode alugar
            if agora() < dados["fim"]:
                await interaction.followup.send(
                    f"❌ **{self.galpao}** já está alugado!\n"
                    f"👤 Alugado por: <@{dados['user_id']}>\n"
                    f"⏰ Vence em: {dados['fim'].strftime('%d/%m/%Y às %H:%M')}",
                    ephemeral=True
                )
                return
        
        # =========================================================
        # ================= SALVAR ALUGUEL =========================
        # =========================================================
        
        if self.editando:
            # Editando aluguel existente
            await renovar_aluguel_db(self.galpao, self.user_id, data_fim.replace(tzinfo=None), dias_aluguel)
            
            alugueis_ativos[self.galpao] = {
                "user_id": self.user_id,
                "fim": data_fim,
                "dias": dias_aluguel
            }
            
            await interaction.followup.send(
                f"✅ **{self.galpao}** atualizado com sucesso!\n"
                f"👤 **Alugado por:** <@{self.user_id}>\n"
                f"📅 **Dias:** {dias_aluguel} dia(s)\n"
                f"⏰ **Vence em:** {data_fim.strftime('%d/%m/%Y às %H:%M')}",
                ephemeral=True
            )
        else:
            # 🔥 CORREÇÃO: Se já expirou, permite realugar (remove o antigo)
            if self.galpao in alugueis_ativos:
                # Remove o aluguel expirado
                del alugueis_ativos[self.galpao]
                await desativar_aluguel_db(self.galpao)
                print(f"🗑️ Aluguel expirado removido para realugar: {self.galpao}")
            
            # Salvar novo aluguel
            await salvar_aluguel_db(self.galpao, interaction.user.id, data_fim.replace(tzinfo=None), dias_aluguel)
            
            alugueis_ativos[self.galpao] = {
                "user_id": interaction.user.id,
                "fim": data_fim,
                "dias": dias_aluguel
            }
            
            await interaction.followup.send(
                f"✅ **{self.galpao}** alugado com sucesso!\n"
                f"👤 **Alugado por:** {interaction.user.mention}\n"
                f"📅 **Dias:** {dias_aluguel} dia(s)\n"
                f"⏰ **Vence em:** {data_fim.strftime('%d/%m/%Y às %H:%M')}",
                ephemeral=True
            )
        
        # Atualizar o painel
        await atualizar_painel_alugueis()


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
    """Verifica e remove aluguéis expirados (SEM ENVIAR MENSAGENS)"""
    agora_br = agora()
    expirou = False
    
    for galpao, dados in list(alugueis_ativos.items()):
        if agora_br >= dados["fim"]:
            await desativar_aluguel_db(galpao)
            expirou = True
            print(f"⏰ Aluguel expirado: {galpao}")
            
            # 🔥 REMOVIDO: Não envia mais mensagem no canal
            # canal = bot.get_channel(CANAL_FABRICACAO_ID)
            # if canal:
            #     await canal.send(f"⚠️ **{galpao}** - O aluguel EXPIRou!")
    
    if expirou:
        await atualizar_painel_alugueis()



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

@bot.command(name="limpar_cache_lives")
async def cmd_limpar_cache_lives(ctx):
    """Limpa o cache de lives manualmente"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    global cache_lives
    cache_lives.clear()
    await ctx.send("✅ Cache de lives limpo!")

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


async def sincronizar_paineis_estoque(guild):
    """Sincroniza o estoque entre os painéis de vendas e fabricação"""
    try:
        # Atualizar painel de vendas
        await enviar_painel_vendas()
        
        # Atualizar painel de fabricação
        await enviar_painel_fabricacao()
        
        print("🔄 Estoques sincronizados entre os painéis")
    except Exception as e:
        print(f"❌ Erro ao sincronizar painéis: {e}")

async def enviar_painel_vendas():
    """Envia/atualiza o painel de vendas com estoque atualizado"""
    canal = bot.get_channel(CANAL_VENDAS_ID)
    if not canal:
        print("❌ Canal de vendas não encontrado")
        return
    
    estoque = await carregar_estoque()
    
    embed = discord.Embed(
        title="🛒 Painel de Vendas",
        description="Escolha uma opção abaixo.\n\n⚠️ **ATENÇÃO:** Antes de entregar um pedido, verifique se há ESTOQUE disponível!",
        color=0x2ecc71
    )
    
    embed.add_field(
        name="📦 ESTOQUE DISPONÍVEL",
        value=f"🔫 PT: **{fmt_num(estoque['PT'])}** pacotes\n🔫 SUB: **{fmt_num(estoque['SUB'])}** pacotes",
        inline=False
    )
    
    embed.set_footer(text=f"🔄 Atualizado em {agora().strftime('%d/%m/%Y %H:%M:%S')}")
    
    # View com botão de atualizar
    view = CalculadoraView()
    
    await enviar_ou_atualizar_painel("painel_vendas", CANAL_VENDAS_ID, embed, view)
    print("🛒 Painel de vendas atualizado")


async def enviar_painel_fabricacao():
    """Envia/atualiza o painel de fabricação com estoque atualizado"""
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    if not canal:
        print("❌ Canal de fabricação não encontrado")
        return
    
    estoque_municoes = await carregar_estoque()
    estoque_insumos = await carregar_estoque_insumos()
    
    embed = discord.Embed(
        title="🏭 PAINEL DE FABRICAÇÃO",
        description="**Gerencie a produção e estoque:**",
        color=0x2ecc71
    )
    
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
    
    # Usar a mesma view com botão de atualizar
    view = FabricacaoView()
    
    # Deletar mensagens antigas do painel
    async for msg in canal.history(limit=20):
        if msg.author == bot.user and msg.embeds and msg.embeds[0].title == "🏭 PAINEL DE FABRICAÇÃO":
            try:
                await msg.delete()
            except:
                pass
    
    await canal.send(embed=embed, view=view)
    print("🏭 Painel de fabricação atualizado")
    
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
    
    embed = discord.Embed(
        title="💣 Registro de Pólvora",
        description=(
            "**Clique no botão abaixo para registrar a compra de pólvora.**\n\n"
            "📌 **Informe apenas a quantidade comprada.**\n"
            "💰 O valor será calculado automaticamente (R$ 80 por unidade)."
        ),
        color=0xe67e22
    )
    
    await enviar_ou_atualizar_painel(
        "painel_polvora",
        CANAL_CALCULO_POLVORA_ID,
        embed,
        PolvoraView()
    )
    
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
        await interaction.response.defer(ephemeral=True)
        
        # 🔥 VERIFICAR SE JÁ TEM META
        async with get_db().acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(interaction.user.id))
        
        if meta:
            canal = interaction.guild.get_channel(meta["canal_id"])
            if canal:
                await interaction.followup.send(f"✅ Você já possui uma sala! {canal.mention}", ephemeral=True)
                await atualizar_embed_meta(interaction.user.id)
                return
            else:
                # Canal não existe, remover meta e recriar
                await conn.execute("DELETE FROM metas WHERE user_id = $1", str(interaction.user.id))
                if str(interaction.user.id) in metas_cache:
                    del metas_cache[str(interaction.user.id)]
        
        # 🔥 VERIFICAR SE JÁ TEM UMA SALA (MESMO SEM META)
        for canal in interaction.guild.text_channels:
            if interaction.user.display_name.lower() in canal.name.lower() and "📁" in canal.name:
                # Salvar meta no banco
                await salvar_meta_db(interaction.user.id, canal.id, 0, 0, 0)
                metas_cache[str(interaction.user.id)] = {
                    "canal_id": canal.id,
                    "dinheiro": 0,
                    "polvora": 0,
                    "acao": None,
                    "dinheiro_acoes": 0
                }
                await atualizar_embed_meta(interaction.user.id)
                await interaction.followup.send(f"✅ Sala encontrada e meta criada! {canal.mention}", ephemeral=True)
                return
        
        # Criar nova sala
        await criar_sala_meta(interaction.user)
        await interaction.followup.send("✅ Sua sala foi criada com sucesso!", ephemeral=True)

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
    """Envia o painel com contagem regressiva dos aluguéis"""
    
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    if not canal:
        print("❌ Canal de fabricação não encontrado")
        return
    
    await carregar_alugueis_db()
    
    embed = discord.Embed(
        title="🏭💰 CONTROLE DE ALUGUEL - GALPÕES",
        description="**Contagem regressiva atualizada automaticamente**",
        color=0x3498db
    )
    
    view = AluguelView()
    
    # ===== GALPÃO NORTE =====
    if "GALPÕES NORTE" in alugueis_ativos:
        dados = alugueis_ativos["GALPÕES NORTE"]
        data_fim = dados["fim"]
        
        # 🔥 VERIFICAR SE JÁ EXPIR0U
        if agora() >= data_fim:
            # Se expirou, mostrar como disponível
            embed.add_field(
                name="🏭 GALPÕES NORTE",
                value=(
                    f"**STATUS:** 🟢 DISPONÍVEL\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"**💰 Clique no botão abaixo para alugar**\n"
                    f"**📅 Aluguel mínimo:** 1 dia"
                ),
                inline=False
            )
            # Remover do cache ativo
            del alugueis_ativos["GALPÕES NORTE"]
        else:
            barra_prog = calcular_barra_progresso(data_fim, dados["dias"])
            embed.add_field(
                name="🏭 GALPÕES NORTE",
                value=(
                    f"**STATUS:** 🔵 ALUGADO\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"**👤 Alugado Por:** <@{dados['user_id']}>\n"
                    f"**📅 Dias alugados:** {dados['dias']} dia(s)\n"
                    f"**⏰ Vence em:** <t:{int(data_fim.timestamp())}:F>\n"
                    f"**🕐 TEMPO RESTANTE:**\n"
                    f"{formatar_tempo_detalhado(data_fim)}\n"
                    f"{barra_prog}"
                ),
                inline=False
            )
            view.adicionar_botoes_edicao("GALPÕES NORTE", dados['user_id'], dados['dias'])
    else:
        embed.add_field(
            name="🏭 GALPÕES NORTE",
            value=(
                f"**STATUS:** 🟢 DISPONÍVEL\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"**💰 Clique no botão abaixo para alugar**\n"
                f"**📅 Aluguel mínimo:** 1 dia"
            ),
            inline=False
        )
    
    # ===== GALPÃO SUL =====
    if "GALPÕES SUL" in alugueis_ativos:
        dados = alugueis_ativos["GALPÕES SUL"]
        data_fim = dados["fim"]
        
        # 🔥 VERIFICAR SE JÁ EXPIR0U
        if agora() >= data_fim:
            # Se expirou, mostrar como disponível
            embed.add_field(
                name="🏭 GALPÕES SUL",
                value=(
                    f"**STATUS:** 🟢 DISPONÍVEL\n"
                    f"━━━━━━━━━━━━━━━━━━━━\n"
                    f"**💰 Clique no botão abaixo para alugar**\n"
                    f"**📅 Aluguel mínimo:** 1 dia"
                ),
                inline=False
            )
            # Remover do cache ativo
            del alugueis_ativos["GALPÕES SUL"]
        else:
            barra_prog = calcular_barra_progresso(data_fim, dados["dias"])
            embed.add_field(
                name="🏭 GALPÕES SUL",
                value=(
                    f"**STATUS:** 🔵 ALUGADO\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"**👤 Alugado Por:** <@{dados['user_id']}>\n"
                    f"**📅 Dias alugados:** {dados['dias']} dia(s)\n"
                    f"**⏰ Vence em:** <t:{int(data_fim.timestamp())}:F>\n"
                    f"**🕐 TEMPO RESTANTE:**\n"
                    f"{formatar_tempo_detalhado(data_fim)}\n"
                    f"{barra_prog}"
                ),
                inline=False
            )
            view.adicionar_botoes_edicao("GALPÕES SUL", dados['user_id'], dados['dias'])
    else:
        embed.add_field(
            name="🏭 GALPÕES SUL",
            value=(
                f"**STATUS:** 🟢 DISPONÍVEL\n"
                f"━━━━━━━━━━━━━━━━━━━━\n"
                f"**💰 Clique no botão abaixo para alugar**\n"
                f"**📅 Aluguel mínimo:** 1 dia"
            ),
            inline=False
        )
    
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

@tasks.loop(hours=1)
async def verificar_avisos_meta():
    """Verifica avisos de meta a cada hora"""
    try:
        await verificar_avisos_quarta()
    except Exception as e:
        print(f"❌ Erro ao verificar avisos de meta: {e}")

# =========================================================
# ==================== LOOP HEARTBEAT =====================
# =========================================================

@tasks.loop(minutes=2)
async def heartbeat_producoes():
    """Loop de heartbeat para verificar produções a cada 2 minutos"""
    await verificar_heartbeat_producoes()

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
# ==================== LIMPAR CACHE LIVES =================
# =========================================================

@tasks.loop(minutes=10)
async def limpar_cache_lives():
    """Limpa o cache de lives a cada 10 minutos"""
    global cache_lives
    agora_ts = time_module.time()
    
    # Remover entradas mais antigas que o TTL
    keys_to_remove = []
    for key, (_, timestamp) in cache_lives.items():
        if agora_ts - timestamp > CACHE_LIVES_TTL:
            keys_to_remove.append(key)
    
    for key in keys_to_remove:
        del cache_lives[key]
    
    if keys_to_remove:
        print(f"🧹 Cache de lives limpo: {len(keys_to_remove)} entradas removidas")

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


# =========================================================
# ==================== MODAIS DE GRUPOS ====================
# =========================================================

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
    def __init__(self, grupo_id, nome_org):
        super().__init__(timeout=60)
        self.grupo_id = grupo_id
        self.nome_org = nome_org
    
    @discord.ui.button(
        label="✅ Sim, Excluir",
        style=discord.ButtonStyle.danger,
        custom_id="confirmar_excluir",
        emoji="✅"
    )
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        try:
            if not self.grupo_id:
                await interaction.followup.send("❌ ID do grupo não encontrado!", ephemeral=True)
                return
            
            await desativar_grupo_db(self.grupo_id)
            
            canal = interaction.guild.get_channel(CANAL_GRUPOS_ID)
            if canal:
                async for msg in canal.history(limit=50):
                    if msg.author == interaction.client.user and msg.embeds:
                        for embed in msg.embeds:
                            if embed.footer and self.grupo_id in embed.footer.text:
                                try:
                                    await msg.delete()
                                    await interaction.followup.send(
                                        f"✅ **Grupo {self.nome_org} excluído com sucesso!**",
                                        ephemeral=True
                                    )
                                    return
                                except:
                                    pass
            
            await interaction.followup.send(
                f"✅ **Grupo {self.nome_org} excluído do banco de dados!**",
                ephemeral=True
            )
            
        except Exception as e:
            print(f"❌ Erro ao excluir grupo: {e}")
            await interaction.followup.send(f"❌ Erro ao excluir: {e}", ephemeral=True)
    
    @discord.ui.button(
        label="❌ Cancelar",
        style=discord.ButtonStyle.secondary,
        custom_id="cancelar_excluir",
        emoji="❌"
    )
    async def cancelar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("❌ Operação cancelada.", ephemeral=True)


# =========================================================
# ==================== VIEWS DE GRUPOS ====================
# =========================================================

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
        try:
            is_admin = interaction.user.guild_permissions.administrator
            is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
            
            if not is_admin and not is_gerente:
                await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem editar grupos!", ephemeral=True)
                return
            
            if not self.grupo_id:
                await interaction.response.send_message("❌ ID do grupo não encontrado!", ephemeral=True)
                return
            
            dados = await carregar_grupo_db(self.grupo_id)
            if not dados:
                await interaction.response.send_message(f"❌ Grupo não encontrado! ID: {self.grupo_id}", ephemeral=True)
                return
            
            await interaction.response.send_modal(EditarGrupoModal(self.grupo_id, dados))
            
        except Exception as e:
            print(f"❌ Erro ao editar grupo: {e}")
            try:
                await interaction.response.send_message(f"❌ Erro: {e}", ephemeral=True)
            except:
                pass
    
    @discord.ui.button(
        label="🔄 Atualizar",
        style=discord.ButtonStyle.secondary,
        custom_id="atualizar_grupo",
        emoji="🔄"
    )
    async def atualizar_grupo(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        try:
            if not self.grupo_id:
                await interaction.followup.send("❌ ID do grupo não encontrado!", ephemeral=True)
                return
            
            await enviar_embed_grupo(self.grupo_id)
            await interaction.followup.send(
                f"✅ **Grupo {self.nome_org} atualizado com sucesso!**",
                ephemeral=True
            )
        except Exception as e:
            print(f"❌ Erro ao atualizar grupo: {e}")
            await interaction.followup.send(
                f"❌ Erro ao atualizar: {e}",
                ephemeral=True
            )
    
    @discord.ui.button(
        label="🗑️ Excluir Grupo",
        style=discord.ButtonStyle.danger,
        custom_id="excluir_grupo",
        emoji="🗑️"
    )
    async def excluir_grupo(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            is_admin = interaction.user.guild_permissions.administrator
            is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
            
            if not is_admin and not is_gerente:
                await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem excluir grupos!", ephemeral=True)
                return
            
            if not self.grupo_id:
                await interaction.response.send_message("❌ ID do grupo não encontrado!", ephemeral=True)
                return
            
            view = ConfirmarExcluirView(self.grupo_id, self.nome_org)
            await interaction.response.send_message(
                f"⚠️ **Tem certeza que deseja excluir o grupo {self.nome_org}?**\n"
                f"Esta ação não pode ser desfeita!",
                view=view,
                ephemeral=True
            )
            
        except Exception as e:
            print(f"❌ Erro ao excluir grupo: {e}")
            try:
                await interaction.response.send_message(f"❌ Erro: {e}", ephemeral=True)
            except:
                pass


# =========================================================
# ==================== VIEWS DE GRUPOS ====================
# =========================================================

class RegistrarGrupoView(discord.ui.View):
    """View com botão para registrar grupo e gerar relatório"""
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(
        label="📋 Registrar Novo Grupo",
        style=discord.ButtonStyle.success,
        custom_id="registrar_grupo_btn_simples",
        emoji="📋"
    )
    async def registrar_grupo(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            is_admin = interaction.user.guild_permissions.administrator
            is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
            
            if not is_admin and not is_gerente:
                await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem registrar grupos!", ephemeral=True)
                return
            
            await interaction.response.send_modal(RegistrarGrupoModal())
            
        except Exception as e:
            print(f"❌ Erro ao abrir modal de registro: {e}")
            try:
                await interaction.response.send_message(f"❌ Erro: {e}", ephemeral=True)
            except:
                pass
    
    @discord.ui.button(
        label="📊 Relatório de Grupos",
        style=discord.ButtonStyle.primary,
        custom_id="relatorio_grupos_btn",
        emoji="📊"
    )
    async def relatorio_grupos(self, interaction: discord.Interaction, button: discord.ui.Button):
        """Gera um relatório completo de todos os grupos cadastrados"""
        try:
            is_admin = interaction.user.guild_permissions.administrator
            is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
            
            if not is_admin and not is_gerente:
                await interaction.response.send_message("❌ Apenas ADM ou Gerentes podem gerar relatórios!", ephemeral=True)
                return
            
            await interaction.response.defer(ephemeral=True)
            
            grupos = await carregar_grupos_db()
            
            if not grupos:
                await interaction.followup.send("📭 Nenhum grupo cadastrado.", ephemeral=True)
                return
            
            import io
            
            relatorio = []
            relatorio.append("=" * 80)
            relatorio.append("RELATÓRIO COMPLETO DE GRUPOS CADASTRADOS")
            relatorio.append("=" * 80)
            relatorio.append(f"Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}")
            relatorio.append(f"Total de grupos: {len(grupos)}")
            relatorio.append("=" * 80)
            relatorio.append("")
            
            for i, grupo in enumerate(grupos, 1):
                compras = await carregar_compras_grupo_db(grupo["grupo_id"])
                total_pt = compras.get("PT", {}).get("quantidade", 0)
                total_sub = compras.get("SUB", {}).get("quantidade", 0)
                
                relatorio.append(f"{'=' * 80}")
                relatorio.append(f"GRUPO #{i}")
                relatorio.append(f"{'=' * 80}")
                relatorio.append(f"ID: {grupo['grupo_id']}")
                relatorio.append(f"Organização: {grupo['nome_org'].upper()}")
                relatorio.append(f"")
                relatorio.append(f"--- INFORMAÇÕES DO LÍDER ---")
                relatorio.append(f"Nome: {grupo['lider_nome']}")
                relatorio.append(f"Telefone: {grupo['lider_telefone']}")
                relatorio.append(f"")
                relatorio.append(f"--- INFORMAÇÕES DO BRAÇO ---")
                relatorio.append(f"Nome: {grupo['braco_nome'] or 'Não informado'}")
                relatorio.append(f"Telefone: {grupo['braco_telefone'] or 'Não informado'}")
                relatorio.append(f"")
                relatorio.append(f"--- PRODUTO ---")
                relatorio.append(f"Produto fornecido: {grupo['produto']}")
                relatorio.append(f"")
                relatorio.append(f"--- HISTÓRICO DE COMPRAS ---")
                relatorio.append(f"PT: {fmt_num(total_pt)} pacotes")
                relatorio.append(f"SUB: {fmt_num(total_sub)} pacotes")
                relatorio.append(f"Total: {fmt_num(total_pt + total_sub)} pacotes")
                relatorio.append(f"")
                relatorio.append(f"--- DATAS ---")
                relatorio.append(f"Criado em: {grupo['data_criacao'].strftime('%d/%m/%Y %H:%M')}")
                if grupo.get('data_atualizacao'):
                    relatorio.append(f"Atualizado em: {grupo['data_atualizacao'].strftime('%d/%m/%Y %H:%M')}")
                relatorio.append(f"Status: ATIVO")
                relatorio.append("")
            
            relatorio.append("=" * 80)
            relatorio.append("FIM DO RELATÓRIO")
            relatorio.append("=" * 80)
            
            texto = "\n".join(relatorio)
            
            await interaction.followup.send(
                "📊 **Relatório de Grupos**",
                file=discord.File(io.StringIO(texto), filename=f"relatorio_grupos_{agora().strftime('%d%m%Y_%H%M')}.txt"),
                ephemeral=True
            )
            
            embed = discord.Embed(
                title="📊 RELATÓRIO DE GRUPOS",
                description=f"**Total de grupos:** {len(grupos)}",
                color=0x2ecc71
            )
            
            lista = ""
            for i, grupo in enumerate(grupos[:20], 1):
                compras = await carregar_compras_grupo_db(grupo["grupo_id"])
                total_pt = compras.get("PT", {}).get("quantidade", 0)
                total_sub = compras.get("SUB", {}).get("quantidade", 0)
                lista += f"**{i}.** {grupo['nome_org'].upper()} - PT: {fmt_num(total_pt)} | SUB: {fmt_num(total_sub)}\n"
            
            if len(grupos) > 20:
                lista += f"\n*... e mais {len(grupos) - 20} grupos (veja o arquivo completo)*"
            
            embed.add_field(name="📋 LISTA DE GRUPOS", value=lista, inline=False)
            embed.set_footer(text=f"Relatório gerado em {agora().strftime('%d/%m/%Y %H:%M')}")
            
            await interaction.followup.send(embed=embed, ephemeral=True)
            
        except Exception as e:
            print(f"❌ Erro ao gerar relatório de grupos: {e}")
            import traceback
            traceback.print_exc()
            await interaction.followup.send(f"❌ Erro ao gerar relatório: {e}", ephemeral=True)


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
    
    # =========================================================
    # ================= PROCURAR MENSAGEM EXISTENTE ===========
    # =========================================================
    
    try:
        async for msg in canal.history(limit=50):
            if msg.author == bot.user and msg.embeds:
                for embed_msg in msg.embeds:
                    if embed_msg.footer and grupo_id in embed_msg.footer.text:
                        try:
                            view = GrupoView(grupo_id, nome_org)
                            await msg.edit(embed=embed, view=view)
                            print(f"✅ Grupo {nome_org} atualizado")
                            return
                        except Exception as e:
                            print(f"Erro ao atualizar embed: {e}")
    except Exception as e:
        print(f"Erro ao buscar mensagem: {e}")
    
    # Se não encontrou mensagem, criar nova
    view = GrupoView(grupo_id, nome_org)
    await canal.send(embed=embed, view=view)
    print(f"✅ Grupo {nome_org} criado")


# =========================================================
# ==================== FUNÇÕES DE GRUPOS ===================
# =========================================================

async def enviar_painel_registro_grupos():
    """Envia o painel com botão para registrar grupos e gerar relatório"""
    
    canal = bot.get_channel(CANAL_REGISTRO_GRUPOS_ID)
    if not canal:
        print(f"❌ Canal de registro de grupos não encontrado: {CANAL_REGISTRO_GRUPOS_ID}")
        return
    
    async for msg in canal.history(limit=20):
        if msg.author == bot.user and msg.embeds:
            if msg.embeds[0].title == "📋 REGISTRO DE GRUPOS":
                try:
                    embed = discord.Embed(
                        title="📋 REGISTRO DE GRUPOS",
                        description=(
                            "**Gerencie seus grupos de clientes aqui.**\n\n"
                            "📌 **Opções disponíveis:**\n"
                            "• 📋 **Registrar Novo Grupo** - Cadastre um novo grupo\n"
                            "• 📊 **Relatório de Grupos** - Gere um relatório completo para impressão\n\n"
                            "📋 **O relatório inclui:**\n"
                            "• Nome da Organização\n"
                            "• Líder (Nome e Telefone)\n"
                            "• Braço (Nome e Telefone)\n"
                            "• Produto fornecido\n"
                            "• Histórico de compras (PT e SUB)\n"
                            "• Datas de criação e atualização"
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
                    
                    await msg.edit(embed=embed, view=RegistrarGrupoView())
                    print(f"📋 Painel de registro de grupos atualizado")
                    return
                except Exception as e:
                    print(f"Erro ao atualizar painel: {e}")
                    try:
                        await msg.delete()
                    except:
                        pass
                    break
    
    embed = discord.Embed(
        title="📋 REGISTRO DE GRUPOS",
        description=(
            "**Gerencie seus grupos de clientes aqui.**\n\n"
            "📌 **Opções disponíveis:**\n"
            "• 📋 **Registrar Novo Grupo** - Cadastre um novo grupo\n"
            "• 📊 **Relatório de Grupos** - Gere um relatório completo para impressão\n\n"
            "📋 **O relatório inclui:**\n"
            "• Nome da Organização\n"
            "• Líder (Nome e Telefone)\n"
            "• Braço (Nome e Telefone)\n"
            "• Produto fornecido\n"
            "• Histórico de compras (PT e SUB)\n"
            "• Datas de criação e atualização"
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
    
    await canal.send(embed=embed, view=RegistrarGrupoView())
    print(f"📋 Painel de registro de grupos enviado")


# =========================================================
# ==================== FUNÇÕES DE GRUPOS ===================
# =========================================================

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
# ================ 14. SISTEMA DE MENSAGENS DE VENDAS =====
# =========================================================

# ================ 14.1 VARIÁVEL DE CONTROLE =============
mensagens_em_andamento = set()

# ================ 14.2 VIEW DO MENU DE MENSAGENS ========
class MenuMensagensView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📝 Gerar Mensagem de Venda", style=discord.ButtonStyle.primary, custom_id="gerar_mensagem_venda", emoji="📝")
    async def gerar_mensagem(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="📝 MENU DE MENSAGENS DE VENDA",
            description="**Selecione o tipo de mensagem que deseja gerar:**\n\n"
                       "📌 **Opções disponíveis:**\n"
                       "• 📦 Pedido Pronto\n"
                       "• ❌ Pedido Cancelado\n"
                       "• ✅ Pedido Finalizado\n"
                       "• 💰 Pendência de Pagamento\n"
                       "• ⚠️ Pagamento Pendente\n\n"
                       "🔹 **Você precisará informar:**\n"
                       "• O valor (quando aplicável)\n"
                       "• Seu passaporte e nome (para a chave PIX)",
            color=0x3498db
        )
        embed.set_footer(text="Clique no botão correspondente à mensagem que deseja gerar")
        
        view = SelecionarMensagemView()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# ================ 14.3 VIEW DE SELEÇÃO DE MENSAGEM ======
class SelecionarMensagemView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)
        
        # Botão 1 - Pedido Pronto (SUCESSO = verde)
        self.add_item(discord.ui.Button(
            label="📦 Pedido Pronto",
            style=discord.ButtonStyle.success,
            custom_id="msg_pedido_pronto",
            emoji="📦",
            row=0
        ))
        
        # Botão 2 - Pedido Cancelado (DANGER = vermelho)
        self.add_item(discord.ui.Button(
            label="❌ Pedido Cancelado",
            style=discord.ButtonStyle.danger,
            custom_id="msg_pedido_cancelado",
            emoji="❌",
            row=0
        ))
        
        # Botão 3 - Pedido Finalizado (SUCESSO = verde)
        self.add_item(discord.ui.Button(
            label="✅ Pedido Finalizado",
            style=discord.ButtonStyle.success,
            custom_id="msg_pedido_finalizado",
            emoji="✅",
            row=0
        ))
        
        # Botão 4 - Pendência de Pagamento (PRIMARY = azul) - CORRIGIDO!
        self.add_item(discord.ui.Button(
            label="💰 Pendência de Pagamento",
            style=discord.ButtonStyle.primary,  # ← AQUI ERA O ERRO!
            custom_id="msg_pendencia_pagamento",
            emoji="💰",
            row=1
        ))
        
        # Botão 5 - Pagamento Pendente (PRIMARY = azul)
        self.add_item(discord.ui.Button(
            label="⚠️ Pagamento Pendente",
            style=discord.ButtonStyle.primary,
            custom_id="msg_pagamento_pendente",
            emoji="⚠️",
            row=1
        ))
        
        # Botão 6 - Fechar (SECONDARY = cinza)
        self.add_item(discord.ui.Button(
            label="❌ Fechar",
            style=discord.ButtonStyle.secondary,
            custom_id="fechar_mensagens",
            emoji="❌",
            row=1
        ))
    
    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        custom_id = interaction.data.get("custom_id", "")
        
        if custom_id == "fechar_mensagens":
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
            await interaction.response.send_message(
                "⚠️ Você já tem uma mensagem em andamento! Finalize ou cancele a anterior.",
                ephemeral=True
            )
            return
        modal = MensagemPedidoProntoModal(interaction.user)
        await interaction.response.send_modal(modal)
    
    async def handle_pedido_cancelado(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await interaction.response.send_message(
                "⚠️ Você já tem uma mensagem em andamento! Finalize ou cancele a anterior.",
                ephemeral=True
            )
            return
        modal = MensagemPedidoCanceladoModal()
        await interaction.response.send_modal(modal)
    
    async def handle_pedido_finalizado(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await interaction.response.send_message(
                "⚠️ Você já tem uma mensagem em andamento! Finalize ou cancele a anterior.",
                ephemeral=True
            )
            return
        modal = MensagemPedidoFinalizadoModal()
        await interaction.response.send_modal(modal)
    
    async def handle_pendencia_pagamento(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await interaction.response.send_message(
                "⚠️ Você já tem uma mensagem em andamento! Finalize ou cancele a anterior.",
                ephemeral=True
            )
            return
        modal = MensagemPendenciaPagamentoModal()
        await interaction.response.send_modal(modal)
    
    async def handle_pagamento_pendente(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await interaction.response.send_message(
                "⚠️ Você já tem uma mensagem em andamento! Finalize ou cancele a anterior.",
                ephemeral=True
            )
            return
        modal = MensagemPagamentoPendenteModal(interaction.user)
        await interaction.response.send_modal(modal)

    
    async def handle_pedido_cancelado(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await interaction.response.send_message(
                "⚠️ Você já tem uma mensagem em andamento! Finalize ou cancele a anterior.",
                ephemeral=True
            )
            return
        modal = MensagemPedidoCanceladoModal()
        await interaction.response.send_modal(modal)
    
    async def handle_pedido_finalizado(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await interaction.response.send_message(
                "⚠️ Você já tem uma mensagem em andamento! Finalize ou cancele a anterior.",
                ephemeral=True
            )
            return
        modal = MensagemPedidoFinalizadoModal()
        await interaction.response.send_modal(modal)
    
    async def handle_pendencia_pagamento(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await interaction.response.send_message(
                "⚠️ Você já tem uma mensagem em andamento! Finalize ou cancele a anterior.",
                ephemeral=True
            )
            return
        modal = MensagemPendenciaPagamentoModal()
        await interaction.response.send_modal(modal)
    
    async def handle_pagamento_pendente(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await interaction.response.send_message(
                "⚠️ Você já tem uma mensagem em andamento! Finalize ou cancele a anterior.",
                ephemeral=True
            )
            return
        modal = MensagemPagamentoPendenteModal(interaction.user)
        await interaction.response.send_modal(modal)

# ================ 14.4 MODAIS DAS MENSAGENS =============

class MensagemPedidoProntoModal(discord.ui.Modal, title="📦 Pedido Pronto"):
    def __init__(self, usuario):
        super().__init__()
        self.usuario = usuario
        mensagens_em_andamento.add(usuario.id)
    
    valor = discord.ui.TextInput(
        label="💰 Valor da encomenda (opcional)",
        placeholder="Ex: 50000 ou deixe em branco",
        required=False,
        max_length=50
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        mensagens_em_andamento.discard(interaction.user.id)
        
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

@everyone

@everyone 
{passaporte} - {nome} — {agora().strftime('%d/%m/%Y %H:%M')}"""

        if valor_texto:
            mensagem += f"\n💰 Valor: {valor_texto}"

        embed = discord.Embed(
            title="📋 MENSAGEM GERADA - PEDIDO PRONTO",
            description="**Copie a mensagem abaixo e cole no canal desejado:**",
            color=0x2ecc71
        )
        embed.add_field(
            name="📝 MENSAGEM",
            value=f"```\n{mensagem}\n```",
            inline=False
        )
        embed.add_field(
            name="📌 DETALHES",
            value=f"👤 Gerado por: {interaction.user.mention}\n"
                  f"📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}",
            inline=False
        )
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class MensagemPedidoCanceladoModal(discord.ui.Modal, title="❌ Pedido Cancelado"):
    valor = discord.ui.TextInput(
        label="💰 Valor da encomenda (opcional)",
        placeholder="Ex: 50000 ou deixe em branco",
        required=False,
        max_length=50
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        mensagens_em_andamento.discard(interaction.user.id)
        
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

@everyone

@everyone 
{passaporte} - {nome} — {agora().strftime('%d/%m/%Y %H:%M')}
{valor_texto}"""

        embed = discord.Embed(
            title="📋 MENSAGEM GERADA - PEDIDO CANCELADO",
            description="**Copie a mensagem abaixo e cole no canal desejado:**",
            color=0xe74c3c
        )
        embed.add_field(
            name="📝 MENSAGEM",
            value=f"```\n{mensagem}\n```",
            inline=False
        )
        embed.add_field(
            name="📌 DETALHES",
            value=f"👤 Gerado por: {interaction.user.mention}\n"
                  f"📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}",
            inline=False
        )
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class MensagemPedidoFinalizadoModal(discord.ui.Modal, title="✅ Pedido Finalizado"):
    valor = discord.ui.TextInput(
        label="💰 Valor da encomenda (opcional)",
        placeholder="Ex: 50000 ou deixe em branco",
        required=False,
        max_length=50
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        mensagens_em_andamento.discard(interaction.user.id)
        
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

@everyone

@everyone 
{interaction.user.display_name} — {agora().strftime('%d/%m/%Y %H:%M')}
{valor_texto}"""

        embed = discord.Embed(
            title="📋 MENSAGEM GERADA - PEDIDO FINALIZADO",
            description="**Copie a mensagem abaixo e cole no canal desejado:**",
            color=0x2ecc71
        )
        embed.add_field(
            name="📝 MENSAGEM",
            value=f"```\n{mensagem}\n```",
            inline=False
        )
        embed.add_field(
            name="📌 DETALHES",
            value=f"👤 Gerado por: {interaction.user.mention}\n"
                  f"📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}",
            inline=False
        )
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class MensagemPendenciaPagamentoModal(discord.ui.Modal, title="💰 Pendência de Pagamento"):
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
        mensagens_em_andamento.discard(interaction.user.id)
        
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
        embed.add_field(
            name="📝 MENSAGEM",
            value=f"```\n{mensagem}\n```",
            inline=False
        )
        embed.add_field(
            name="📌 DETALHES",
            value=f"👤 Gerado por: {interaction.user.mention}\n"
                  f"💰 Valor: R$ {valor_texto}\n"
                  f"📱 Chave PIX: {chave_pix}\n"
                  f"📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}",
            inline=False
        )
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class MensagemPagamentoPendenteModal(discord.ui.Modal, title="⚠️ Pagamento Pendente"):
    def __init__(self, usuario):
        super().__init__()
        self.usuario = usuario
        mensagens_em_andamento.add(usuario.id)
    
    valor = discord.ui.TextInput(
        label="💰 Valor pendente",
        placeholder="Ex: 50000",
        required=True,
        max_length=50
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        mensagens_em_andamento.discard(interaction.user.id)
        
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

@everyone

@everyone 
{chave_pix} — {agora().strftime('%H:%M')}"""

        embed = discord.Embed(
            title="📋 MENSAGEM GERADA - PAGAMENTO PENDENTE",
            description="**Copie a mensagem abaixo e cole no canal desejado:**",
            color=0xe67e22
        )
        embed.add_field(
            name="📝 MENSAGEM",
            value=f"```\n{mensagem}\n```",
            inline=False
        )
        embed.add_field(
            name="📌 DETALHES",
            value=f"👤 Gerado por: {interaction.user.mention}\n"
                  f"💰 Valor: R$ {valor_texto}\n"
                  f"📱 Chave PIX: {chave_pix}\n"
                  f"📅 Data: {agora().strftime('%d/%m/%Y %H:%M:%S')}",
            inline=False
        )
        embed.set_footer(text="Clique em 'Copiar' para copiar a mensagem")
        
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# ================ 14.5 VIEW PARA COPIAR MENSAGEM ========
class CopiarMensagemView(discord.ui.View):
    def __init__(self, mensagem):
        super().__init__(timeout=120)
        self.mensagem = mensagem
    
    @discord.ui.button(label="📋 Copiar Mensagem", style=discord.ButtonStyle.success, custom_id="copiar_mensagem", emoji="📋")
    async def copiar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            f"✅ **Mensagem copiada!**\n\nUse `Ctrl+C` para copiar a mensagem abaixo:\n\n```\n{self.mensagem}\n```",
            ephemeral=True
        )
    
    @discord.ui.button(label="❌ Fechar", style=discord.ButtonStyle.secondary, custom_id="fechar_copiar", emoji="❌")
    async def fechar(self, interaction: discord.Interaction, button: discord.ui.Button):
        try:
            await interaction.message.delete()
        except:
            pass

# ================ 14.6 PAINEL DO MENU DE MENSAGENS ======
# =========================================================

async def enviar_painel_mensagens():
    """Envia o painel do menu de mensagens para o canal de textos vendas"""
    canal = bot.get_channel(CANAL_TEXTOS_VENDAS_ID)
    if not canal:
        print("❌ Canal de textos vendas não encontrado")
        return
    
    embed = discord.Embed(
        title="📝 GERADOR DE MENSAGENS DE VENDA",
        description="**Clique no botão abaixo para abrir o menu de mensagens.**\n\n"
                   "📌 **Mensagens disponíveis:**\n"
                   "• 📦 Pedido Pronto\n"
                   "• ❌ Pedido Cancelado\n"
                   "• ✅ Pedido Finalizado\n"
                   "• 💰 Pendência de Pagamento\n"
                   "• ⚠️ Pagamento Pendente\n\n"
                   "🔹 **Como funciona:**\n"
                   "1. Selecione o tipo de mensagem\n"
                   "2. Preencha os campos solicitados\n"
                   "3. A mensagem será gerada automaticamente\n"
                   "4. Copie e cole no canal desejado",
        color=0x3498db
    )
    embed.add_field(
        name="📌 DICA",
        value="• O passaporte é extraído automaticamente do seu apelido no servidor\n"
              "• Certifique-se de ter seu apelido no formato: `PASSAPORTE - NOME`",
        inline=False
    )
    embed.set_footer(text="Sistema de Mensagens • VDR 442")
    
    # Verificar se já existe uma mensagem do painel
    async for msg in canal.history(limit=20):
        if msg.author == bot.user and msg.embeds and msg.embeds[0].title == "📝 GERADOR DE MENSAGENS DE VENDA":
            try:
                await msg.edit(embed=embed, view=MenuMensagensView())
                print("📝 Painel de mensagens atualizado")
                return
            except:
                pass
    
    await canal.send(embed=embed, view=MenuMensagensView())
    print("📝 Painel de mensagens enviado")

# =========================================================
# ================ 15. SISTEMA DE CONTROLE DE BAÚ ========
# =========================================================

# ================ 15.1 IDS DOS CANAIS ===================
CANAL_CONTROLE_ARMAS_ID = 1528938987362848972
CANAL_ARMAS_ENTROU_ID = 1500983878045798430
CANAL_ARMAS_SAIU_ID = 1500983930533187734
CANAL_ARMAS_PERDEU_ID = 1500984222104686693
CANAL_BAU_ENTROU_ID = 1337358932158578719
CANAL_BAU_SAIU_ID = 1337358898784632882

# ================ 15.2 CONFIGURAÇÕES ====================
ITENS_DISPONIVEIS = [
    # 🔫 PISTOLAS
    "🔫 FIVE SEVEN",
    "🔫 GLOCK",
    
    # 🔫 SUBMETRALHADORAS
    "🔫 TRIZZ",
    "🔫 AUG",
    "🔫 EVO",
    "🔫 TEC",
    "🔫 GLOCK RAJADA",
    "🔫 MPX",
    "🔫 UZI",
    "🔫 AKA COMPACT",
    "🔫 SKORPION",
    
    # 🔫 FUZIS
    "🔫 SIG SAUER",
    "🔫 M4",
    "🔫 PARAFAL",
    "🔫 AK 103",
    "🔫 AK 74",
    "🔫 G36",
    "🔫 L95",
    "🔫 TAR",
    "🔫 QBZ",
    "🔫 SCAR",
    "🔫 W110",
    
    # 💉 SAÚDE
    "🩸 VITACORE",
    "💉 ADRENALINA",
    "🚑 PULSEIRA",
    
    # 🧨 EXPLOSIVOS E EQUIPAMENTOS
    "🧨 C4",
    "👕 COLETE",
    "🧪 ALCOOL GEL",
    "🎭 CAPUZ",
    "🔑 ALGEMA",
    "📵 BLOCK DE CELULAR",
    "📍 RASTREADOR DE CELULAR",
    
    # 🧰 KITS DE REPARO
    "🧰 KIT REPAROS COMUM",
    "🧰 KIT REPAROS RARO",
    "🧰 KIT REPARO ÉPICO",
    "🧰 KIT REPARO LENDARIO",
    "🔧 KIT SABOTAGEM PNEU",
    "🔧 KIT SABOTAGEM RODA",
    "🔧 KIT SABOTAGEM EXPLOSÃO",
    
    # 🍬 MUNIÇÕES
    "🍬 MUNIÇÃO PT",
    "🍬 MUNIÇÃO SUB",
    "🍬 MUNIÇÃO FUZIL",
    "🍬 MUNIÇÃO SNIPER",
    "💣 GRANADA",
    "🔥 MOLOTOV",
    
    # 🚀 VEÍCULOS E ACESSÓRIOS
    "🚀 NITRO",
    "🚨 BLOCK DE CARRO",
    "🔒 TRAVA",
    "🚧 PLACA"
]

# ================ 15.3 MAPEAMENTO DE ALIASES ============
ALIASES = {
    # 🔫 PISTOLAS
    "five": "FIVE SEVEN",
    "five seven": "FIVE SEVEN",
    "glock": "GLOCK",
    
    # 🔫 SUBMETRALHADORAS
    "trizz": "TRIZZ",
    "aug": "AUG",
    "evo": "EVO",
    "tec": "TEC",
    "glock rajada": "GLOCK RAJADA",
    "mpx": "MPX",
    "uzi": "UZI",
    "aka": "AKA COMPACT",
    "aka compact": "AKA COMPACT",
    "skorpion": "SKORPION",
    
    # 🔫 FUZIS
    "sig": "SIG SAUER",
    "sig sauer": "SIG SAUER",
    "m4": "M4",
    "parafal": "PARAFAL",
    "ak": "AK 103",
    "ak 103": "AK 103",
    "ak 74": "AK 74",
    "g36": "G36",
    "l95": "L95",
    "tar": "TAR",
    "qbz": "QBZ",
    "scar": "SCAR",
    "w110": "W110",
    
    # 💉 SAÚDE
    "vitacore": "VITACORE",
    "adrenalina": "ADRENALINA",
    "pulseira": "PULSEIRA",
    
    # 🧨 EXPLOSIVOS E EQUIPAMENTOS
    "c4": "C4",
    "colete": "COLETE",
    "alcool": "ALCOOL GEL",
    "alcool gel": "ALCOOL GEL",
    "capuz": "CAPUZ",
    "algema": "ALGEMA",
    "block celular": "BLOCK DE CELULAR",
    "block": "BLOCK DE CELULAR",
    "rastreador": "RASTREADOR DE CELULAR",
    "rastreador de celular": "RASTREADOR DE CELULAR",
    
    # 🧰 KITS DE REPARO
    "kit comum": "KIT REPAROS COMUM",
    "kit raro": "KIT REPAROS RARO",
    "kit épico": "KIT REPARO ÉPICO",
    "kit epico": "KIT REPARO ÉPICO",
    "kit lendario": "KIT REPARO LENDARIO",
    "sabotagem pneu": "KIT SABOTAGEM PNEU",
    "sabotagem roda": "KIT SABOTAGEM RODA",
    "sabotagem explosão": "KIT SABOTAGEM EXPLOSÃO",
    "sabotagem explosao": "KIT SABOTAGEM EXPLOSÃO",
    
    # 🍬 MUNIÇÕES
    "munição pt": "MUNIÇÃO PT",
    "municao pt": "MUNIÇÃO PT",
    "pt": "MUNIÇÃO PT",
    "munição sub": "MUNIÇÃO SUB",
    "municao sub": "MUNIÇÃO SUB",
    "sub": "MUNIÇÃO SUB",
    "munição fuzil": "MUNIÇÃO FUZIL",
    "municao fuzil": "MUNIÇÃO FUZIL",
    "fuzil": "MUNIÇÃO FUZIL",
    "munição sniper": "MUNIÇÃO SNIPER",
    "municao sniper": "MUNIÇÃO SNIPER",
    "sniper": "MUNIÇÃO SNIPER",
    "granada": "GRANADA",
    "molotov": "MOLOTOV",
    
    # 🚀 VEÍCULOS E ACESSÓRIOS
    "nitro": "NITRO",
    "block carro": "BLOCK DE CARRO",
    "trava": "TRAVA",
    "placa": "PLACA"
}

ITENS_COM_OPCOES = {
    "ak": ["AK 103", "AK 74"],
    "block": ["BLOCK DE CELULAR", "BLOCK DE CARRO"],
    "glock": ["GLOCK", "GLOCK RAJADA"]
}

# ================ 15.4 FUNÇÃO DE NORMALIZAÇÃO ===========
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

# ================ 15.5 BANCO DE DADOS ===================
async def criar_tabelas_controle():
    async with get_db().acquire() as conn:
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
        
        for item in ITENS_DISPONIVEIS:
            item_nome = item.split(" ", 1)[1] if " " in item else item
            await conn.execute("""
                INSERT INTO bau_estoque (item_nome, quantidade)
                VALUES ($1, 0)
                ON CONFLICT (item_nome) DO NOTHING
            """, item_nome.upper())

# ================ 15.6 FUNÇÕES DE ARMAS =================
async def registrar_arma(tipo, arma_nome, quantidade, responsavel, observacao=""):
    async with get_db().acquire() as conn:
        await conn.execute("""
            INSERT INTO armas_controle (tipo, arma_nome, quantidade, responsavel, observacao)
            VALUES ($1, $2, $3, $4, $5)
        """, tipo, arma_nome.upper(), quantidade, responsavel.upper(), observacao.upper() if observacao else "")
        
        if tipo == "saiu":
            await conn.execute("""
                INSERT INTO armas_emprestadas (arma_nome, quantidade, responsavel, observacao)
                VALUES ($1, $2, $3, $4)
            """, arma_nome.upper(), quantidade, responsavel.upper(), observacao.upper() if observacao else "")
            
async def buscar_armas_emprestadas():
    async with get_db().acquire() as conn:
        return await conn.fetch("""
            SELECT * FROM armas_emprestadas 
            WHERE ativo = true 
            ORDER BY data_retirada DESC
        """)

async def remover_arma_emprestada(arma_nome, responsavel):
    async with get_db().acquire() as conn:
        await conn.execute("""
            UPDATE armas_emprestadas 
            SET ativo = false, data_prevista_devolucao = NOW()
            WHERE LOWER(arma_nome) = LOWER($1) AND LOWER(responsavel) = LOWER($2) AND ativo = true
        """, arma_nome, responsavel)
        
# ================ 15.7 FUNÇÕES DE BAÚ ===================
async def registrar_item_bau(item_nome, quantidade, tipo_movimento, responsavel, observacao=""):
    async with get_db().acquire() as conn:
        await conn.execute("""
            INSERT INTO bau_itens (item_nome, quantidade, tipo_movimento, responsavel, observacao)
            VALUES ($1, $2, $3, $4, $5)
        """, item_nome.upper(), quantidade, tipo_movimento, responsavel.upper(), observacao.upper() if observacao else "")
        
        if tipo_movimento == "entrada":
            await conn.execute("""
                INSERT INTO bau_estoque (item_nome, quantidade)
                VALUES ($1, $2)
                ON CONFLICT (item_nome) DO UPDATE SET 
                    quantidade = bau_estoque.quantidade + $2,
                    ultima_atualizacao = NOW()
            """, item_nome.upper(), quantidade)
        elif tipo_movimento == "saida":
            await conn.execute("""
                UPDATE bau_estoque 
                SET quantidade = quantidade - $2, ultima_atualizacao = NOW()
                WHERE LOWER(item_nome) = LOWER($1)
            """, item_nome, quantidade)

async def buscar_estoque_bau():
    async with get_db().acquire() as conn:
        return await conn.fetch("SELECT * FROM bau_estoque ORDER BY item_nome")

# ================ 15.8 VIEW DE SELEÇÃO DE OPÇÕES =========
class SelecionarOpcaoView(discord.ui.View):
    def __init__(self, opcoes, callback_data):
        super().__init__(timeout=60)
        self.opcoes = opcoes
        self.callback_data = callback_data
        
        for opcao in opcoes:
            emoji = "📦"
            for item in ITENS_DISPONIVEIS:
                if opcao in item:
                    emoji = item.split(" ")[0] if " " in item else "📦"
                    break
            
            self.add_item(discord.ui.Button(
                label=opcao,
                style=discord.ButtonStyle.primary,
                custom_id=f"opcao_{opcao.replace(' ', '_')}",
                emoji=emoji
            ))
        
        self.add_item(discord.ui.Button(
            label="❌ CANCELAR",
            style=discord.ButtonStyle.danger,
            custom_id="cancelar_opcao",
            emoji="❌"
        ))
    
    async def interaction_check(self, interaction: discord.Interaction):
        custom_id = interaction.data.get("custom_id", "")
        
        if custom_id == "cancelar_opcao":
            await interaction.response.send_message("❌ OPERAÇÃO CANCELADA!", ephemeral=True)
            try:
                await interaction.message.delete()
            except:
                pass
            return False
        
        if custom_id.startswith("opcao_"):
            opcao_escolhida = custom_id.replace("opcao_", "").replace("_", " ")
            await interaction.response.defer()
            await self.processar_escolha(interaction, opcao_escolhida)
            return False
        
        return True
    
    async def processar_escolha(self, interaction: discord.Interaction, opcao_escolhida):
        dados = self.callback_data
        modal_class = dados.get("modal_class")
        canal_destino = dados.get("canal_destino")
        
        if modal_class == "arma_entrou":
            modal = RegistrarArmaEntrouModalComOpcao(opcao_escolhida, canal_destino)
        elif modal_class == "arma_saiu":
            modal = RegistrarArmaSaiuModalComOpcao(opcao_escolhida, canal_destino)
        elif modal_class == "arma_perdeu":
            modal = RegistrarArmaPerdeuModalComOpcao(opcao_escolhida)
        elif modal_class == "bau":
            modal = RegistrarItemBaúModalComOpcao(opcao_escolhida, dados.get("tipo_movimento"))
        elif modal_class == "manual":
            modal = AdicionarArmaManualModalComOpcao(opcao_escolhida)
        else:
            await interaction.followup.send("❌ ERRO AO PROCESSAR!", ephemeral=True)
            return
        
        await interaction.followup.send(f"✅ OPÇÃO SELECIONADA: **{opcao_escolhida}**", ephemeral=True)
        await interaction.followup.send_modal(modal)
        
        try:
            await interaction.message.delete()
        except:
            pass

# ================ 15.9 PAINEL CONTROLE ARMAS ============
class ControleArmasView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="🔄 ATUALIZAR", style=discord.ButtonStyle.secondary, custom_id="armas_atualizar", emoji="🔄")
    async def atualizar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await enviar_painel_armas()
        await interaction.followup.send("✅ PAINEL ATUALIZADO!", ephemeral=True)
    
    @discord.ui.button(label="➕ ADICIONAR MANUAL", style=discord.ButtonStyle.primary, custom_id="armas_adicionar_manual", emoji="➕")
    async def adicionar_manual(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        is_admin = interaction.user.guild_permissions.administrator
        
        if not is_gerente and not is_admin:
            await interaction.response.send_message(
                "❌ **APENAS GERENTES E ADM PODEM USAR ESTE RECURSO!**",
                ephemeral=True
            )
            return
        
        await interaction.response.send_modal(AdicionarArmaManualModal())

async def enviar_painel_armas():
    canal = bot.get_channel(CANAL_CONTROLE_ARMAS_ID)
    if not canal:
        print("❌ Canal de controle de armas não encontrado")
        return
    
    armas_emprestadas = await buscar_armas_emprestadas()
    
    embed = discord.Embed(
        title="🔫 CONTROLE DE ARMAS",
        description="**MEMBROS COM ARMAS EMPRESTADAS**\n\n📌 **FORMATO:** RESPONSÁVEL | ARMA | TEMPO",
        color=0x34495e,
        timestamp=agora()
    )
    
    if armas_emprestadas:
        texto = ""
        for arma in armas_emprestadas:
            data = arma["data_retirada"]
            if data.tzinfo is None:
                data = data.replace(tzinfo=BRASIL)
            dias = (agora() - data).days
            
            if dias > 7:
                cor = "🔴"
            elif dias > 3:
                cor = "🟡"
            else:
                cor = "🟢"
            
            texto += f"{cor} **{arma['responsavel']}** | {arma['arma_nome']} | {dias} DIA(S)\n"
        
        embed.add_field(name="📋 ARMAS EMPRESTADAS", value=texto, inline=False)
        embed.add_field(
            name="📊 TOTAL",
            value=f"🔫 {len(armas_emprestadas)} EMPRÉSTIMOS ATIVOS",
            inline=False
        )
    else:
        embed.add_field(name="📋 ARMAS EMPRESTADAS", value="✅ NENHUMA ARMA EMPRESTADA NO MOMENTO", inline=False)
    
    embed.set_footer(text="🟢 < 3 DIAS | 🟡 3-7 DIAS | 🔴 > 7 DIAS")
    
    try:
        async for msg in canal.history(limit=200):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    await asyncio.sleep(0.2)
                except:
                    pass
        
        await asyncio.sleep(0.5)
        await canal.send(embed=embed, view=ControleArmasView())
        print(f"📌 Painel de armas enviado como última mensagem")
        
    except Exception as e:
        print(f"❌ Erro ao enviar painel de armas: {e}")

# ================ 15.10 MODAL ADICIONAR MANUAL ===========
class AdicionarArmaManualModal(discord.ui.Modal, title="➕ ADICIONAR ARMA MANUAL"):
    arma_nome = discord.ui.TextInput(
        label="🔫 NOME DA ARMA (OU APELIDO)",
        placeholder="EX: FUZIL, M4, SIG, AK, GLOCK",
        required=True
    )
    
    quantidade = discord.ui.TextInput(
        label="📦 QUANTIDADE",
        placeholder="DIGITE A QUANTIDADE",
        required=True
    )
    
    observacao = discord.ui.TextInput(
        label="📝 OBSERVAÇÃO",
        style=discord.TextStyle.paragraph,
        required=False,
        placeholder="EX: MOTIVO, LOCAL, ETC"
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value.strip())
            if qtd <= 0:
                raise ValueError
        except:
            await interaction.response.send_message("❌ QUANTIDADE INVÁLIDA!", ephemeral=True)
            return
        
        arma_digitada = self.arma_nome.value.strip()
        opcoes = verificar_opcoes(arma_digitada)
        
        if opcoes and len(opcoes) > 1:
            callback_data = {
                "tipo": "manual",
                "modal_class": "manual"
            }
            view = SelecionarOpcaoView(opcoes, callback_data)
            
            await interaction.response.send_message(
                f"⚠️ **'{arma_digitada.upper()}' TEM MÚLTIPLAS OPÇÕES!**\n\n"
                f"**SELECIONE UMA DAS OPÇÕES ABAIXO:**",
                view=view,
                ephemeral=True
            )
            return
        
        arma_normalizada = normalizar_nome(arma_digitada)
        observacao = self.observacao.value.strip().upper() if self.observacao.value else ""
        
        # 🔥 RESPONSÁVEL = QUEM ESTÁ FAZENDO A AÇÃO
        responsavel = interaction.user.display_name.upper()
        
        await registrar_arma(
            "saiu",
            arma_normalizada,
            qtd,
            responsavel,
            f"MANUAL - {observacao}" if observacao else "MANUAL"
        )
        
        canal = interaction.guild.get_channel(CANAL_ARMAS_SAIU_ID)
        if canal:
            embed = discord.Embed(
                title="➕ ADIÇÃO MANUAL DE ARMA",
                color=0x9b59b6,
                timestamp=agora()
            )
            embed.add_field(name="👤 RESPONSÁVEL", value=f"**{responsavel}**", inline=True)
            embed.add_field(name="🔫 ARMA", value=f"**{arma_normalizada}**", inline=True)
            embed.add_field(name="📦 QUANTIDADE", value=f"**{fmt_num(qtd)}**", inline=True)
            if observacao:
                embed.add_field(name="📝 OBSERVAÇÃO", value=observacao, inline=False)
            embed.add_field(name="📌 ADICIONADO POR", value=interaction.user.mention, inline=False)
            embed.set_footer(text=f"ADIÇÃO MANUAL • {responsavel}")
            
            await canal.send(embed=embed)
        
        await interaction.response.send_message(
            f"✅ **ARMA ADICIONADA MANUALMENTE COM SUCESSO!**\n"
            f"👤 RESPONSÁVEL: {responsavel}\n"
            f"🔫 ARMA: {arma_normalizada}\n"
            f"📦 QUANTIDADE: {fmt_num(qtd)}",
            ephemeral=True
        )
        
        await asyncio.sleep(2)
        await enviar_painel_armas()

class AdicionarArmaManualModalComOpcao(discord.ui.Modal, title="➕ ADICIONAR ARMA MANUAL"):
    def __init__(self, arma_escolhida):
        super().__init__()
        self.arma_escolhida = arma_escolhida
    
    quantidade = discord.ui.TextInput(
        label="📦 QUANTIDADE",
        placeholder="DIGITE A QUANTIDADE",
        required=True
    )
    
    observacao = discord.ui.TextInput(
        label="📝 OBSERVAÇÃO",
        style=discord.TextStyle.paragraph,
        required=False,
        placeholder="EX: MOTIVO, LOCAL, ETC"
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value.strip())
            if qtd <= 0:
                raise ValueError
        except:
            await interaction.response.send_message("❌ QUANTIDADE INVÁLIDA!", ephemeral=True)
            return
        
        arma_normalizada = self.arma_escolhida.upper()
        observacao = self.observacao.value.strip().upper() if self.observacao.value else ""
        
        # 🔥 RESPONSÁVEL = QUEM ESTÁ FAZENDO A AÇÃO
        responsavel = interaction.user.display_name.upper()
        
        await registrar_arma(
            "saiu",
            arma_normalizada,
            qtd,
            responsavel,
            f"MANUAL - {observacao}" if observacao else "MANUAL"
        )
        
        canal = interaction.guild.get_channel(CANAL_ARMAS_SAIU_ID)
        if canal:
            embed = discord.Embed(
                title="➕ ADIÇÃO MANUAL DE ARMA",
                color=0x9b59b6,
                timestamp=agora()
            )
            embed.add_field(name="👤 RESPONSÁVEL", value=f"**{responsavel}**", inline=True)
            embed.add_field(name="🔫 ARMA", value=f"**{arma_normalizada}**", inline=True)
            embed.add_field(name="📦 QUANTIDADE", value=f"**{fmt_num(qtd)}**", inline=True)
            if observacao:
                embed.add_field(name="📝 OBSERVAÇÃO", value=observacao, inline=False)
            embed.add_field(name="📌 ADICIONADO POR", value=interaction.user.mention, inline=False)
            embed.set_footer(text=f"ADIÇÃO MANUAL • {responsavel}")
            
            await canal.send(embed=embed)
        
        await interaction.response.send_message(
            f"✅ **ARMA ADICIONADA MANUALMENTE COM SUCESSO!**\n"
            f"👤 RESPONSÁVEL: {responsavel}\n"
            f"🔫 ARMA: {arma_normalizada}\n"
            f"📦 QUANTIDADE: {fmt_num(qtd)}",
            ephemeral=True
        )
        
        await asyncio.sleep(2)
        await enviar_painel_armas()

# ================ 15.11 PAINEL ARMAS ENTROU ==============
class ArmasEntrouView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📥 REGISTRAR ENTRADA", style=discord.ButtonStyle.success, custom_id="armas_entrou_btn", emoji="📥")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarArmaEntrouModal())

async def enviar_painel_armas_entrou():
    canal = bot.get_channel(CANAL_ARMAS_ENTROU_ID)
    if not canal:
        print("❌ Canal armas entrou não encontrado")
        return
    
    embed = discord.Embed(
        title="📥 ARMAS - REGISTRO DE ENTRADA",
        description="**CLIQUE NO BOTÃO ABAIXO PARA REGISTRAR ENTRADA DE ARMAS**\n\n"
                   "📌 **INFORMAÇÕES:**\n"
                   "• NOME DA ARMA (PODE USAR APELIDO: SIG, M4, AK, ETC)\n"
                   "• QUANTIDADE DE MUNIÇÃO\n"
                   "• MOTIVO\n\n"
                   "👤 **QUEM REGISTRA É MARCADO AUTOMATICAMENTE**",
        color=0x2ecc71
    )
    
    try:
        async for msg in canal.history(limit=200):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    await asyncio.sleep(0.2)
                except:
                    pass
        
        await asyncio.sleep(0.5)
        await canal.send(embed=embed, view=ArmasEntrouView())
        print(f"📌 Painel de armas entrou enviado como última mensagem")
        
    except Exception as e:
        print(f"❌ Erro ao enviar painel de armas entrou: {e}")

class RegistrarArmaEntrouModal(discord.ui.Modal, title="📥 ENTRADA DE ARMAS"):
    arma_nome = discord.ui.TextInput(
        label="🔫 NOME DA ARMA (OU APELIDO)",
        placeholder="EX: FUZIL, M4, SIG, AK, GLOCK",
        required=True
    )
    
    quantidade = discord.ui.TextInput(
        label="📦 QUANTIDADE DE MUNIÇÃO",
        placeholder="DIGITE A QUANTIDADE",
        required=True
    )
    
    motivo = discord.ui.TextInput(
        label="📝 MOTIVO",
        placeholder="EX: DEVOLUÇÃO, COMPRA, ETC",
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value.strip())
            if qtd <= 0:
                raise ValueError
        except:
            await interaction.response.send_message("❌ QUANTIDADE INVÁLIDA!", ephemeral=True)
            return
        
        arma_digitada = self.arma_nome.value.strip()
        opcoes = verificar_opcoes(arma_digitada)
        
        if opcoes and len(opcoes) > 1:
            callback_data = {
                "tipo": "entrada",
                "modal_class": "arma_entrou",
                "canal_destino": CANAL_ARMAS_ENTROU_ID
            }
            view = SelecionarOpcaoView(opcoes, callback_data)
            
            await interaction.response.send_message(
                f"⚠️ **'{arma_digitada.upper()}' TEM MÚLTIPLAS OPÇÕES!**\n\n"
                f"**SELECIONE UMA DAS OPÇÕES ABAIXO:**",
                view=view,
                ephemeral=True
            )
            return
        
        arma_normalizada = normalizar_nome(arma_digitada)
        motivo = self.motivo.value.strip().upper()
        
        # 🔥 RESPONSÁVEL = QUEM ESTÁ FAZENDO A AÇÃO
        responsavel = interaction.user.display_name.upper()
        
        await registrar_arma(
            "entrou",
            arma_normalizada,
            qtd,
            responsavel,
            motivo
        )
        
        canal = interaction.guild.get_channel(CANAL_ARMAS_ENTROU_ID)
        if canal:
            embed = discord.Embed(
                title="📥 ENTRADA DE ARMA",
                color=0x2ecc71,
                timestamp=agora()
            )
            embed.add_field(name="👤 RESPONSÁVEL", value=f"**{responsavel}**", inline=True)
            embed.add_field(name="🔫 ARMA", value=f"**{arma_normalizada}**", inline=True)
            embed.add_field(name="📦 MUNIÇÃO", value=f"**{fmt_num(qtd)}**", inline=True)
            embed.add_field(name="📝 MOTIVO", value=motivo, inline=False)
            embed.add_field(name="📌 REGISTRADO POR", value=interaction.user.mention, inline=False)
            embed.set_footer(text=f"REGISTRADO POR {responsavel}")
            
            await canal.send(embed=embed)
        
        await interaction.response.send_message(
            f"✅ **ENTRADA REGISTRADA COM SUCESSO!**\n"
            f"👤 RESPONSÁVEL: {responsavel}\n"
            f"🔫 ARMA: {arma_normalizada}\n"
            f"📦 MUNIÇÃO: {fmt_num(qtd)}",
            ephemeral=True
        )
        
        await asyncio.sleep(2)
        await enviar_painel_armas_entrou()
        await enviar_painel_armas()

class RegistrarArmaEntrouModalComOpcao(discord.ui.Modal, title="📥 ENTRADA DE ARMAS"):
    def __init__(self, arma_escolhida, canal_destino):
        super().__init__()
        self.arma_escolhida = arma_escolhida
        self.canal_destino = canal_destino
    
    quantidade = discord.ui.TextInput(
        label="📦 QUANTIDADE DE MUNIÇÃO",
        placeholder="DIGITE A QUANTIDADE",
        required=True
    )
    
    motivo = discord.ui.TextInput(
        label="📝 MOTIVO",
        placeholder="EX: DEVOLUÇÃO, COMPRA, ETC",
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value.strip())
            if qtd <= 0:
                raise ValueError
        except:
            await interaction.response.send_message("❌ QUANTIDADE INVÁLIDA!", ephemeral=True)
            return
        
        arma_normalizada = self.arma_escolhida.upper()
        motivo = self.motivo.value.strip().upper()
        
        # 🔥 RESPONSÁVEL = QUEM ESTÁ FAZENDO A AÇÃO
        responsavel = interaction.user.display_name.upper()
        
        await registrar_arma(
            "entrou",
            arma_normalizada,
            qtd,
            responsavel,
            motivo
        )
        
        canal = interaction.guild.get_channel(self.canal_destino)
        if canal:
            embed = discord.Embed(
                title="📥 ENTRADA DE ARMA",
                color=0x2ecc71,
                timestamp=agora()
            )
            embed.add_field(name="👤 RESPONSÁVEL", value=f"**{responsavel}**", inline=True)
            embed.add_field(name="🔫 ARMA", value=f"**{arma_normalizada}**", inline=True)
            embed.add_field(name="📦 MUNIÇÃO", value=f"**{fmt_num(qtd)}**", inline=True)
            embed.add_field(name="📝 MOTIVO", value=motivo, inline=False)
            embed.add_field(name="📌 REGISTRADO POR", value=interaction.user.mention, inline=False)
            embed.set_footer(text=f"REGISTRADO POR {responsavel}")
            
            await canal.send(embed=embed)
        
        await interaction.response.send_message(
            f"✅ **ENTRADA REGISTRADA COM SUCESSO!**\n"
            f"👤 RESPONSÁVEL: {responsavel}\n"
            f"🔫 ARMA: {arma_normalizada}\n"
            f"📦 MUNIÇÃO: {fmt_num(qtd)}",
            ephemeral=True
        )
        
        await asyncio.sleep(2)
        await enviar_painel_armas_entrou()
        await enviar_painel_armas()

# ================ 15.12 PAINEL ARMAS SAIU ================
class ArmasSaiuView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📤 REGISTRAR SAÍDA", style=discord.ButtonStyle.primary, custom_id="armas_saiu_btn", emoji="📤")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarArmaSaiuModal())

async def enviar_painel_armas_saiu():
    canal = bot.get_channel(CANAL_ARMAS_SAIU_ID)
    if not canal:
        print("❌ Canal armas saiu não encontrado")
        return
    
    embed = discord.Embed(
        title="📤 ARMAS - REGISTRO DE SAÍDA",
        description="**CLIQUE NO BOTÃO ABAIXO PARA REGISTRAR SAÍDA DE ARMAS**\n\n"
                   "📌 **INFORMAÇÕES:**\n"
                   "• NOME DA ARMA (PODE USAR APELIDO: SIG, M4, AK, ETC)\n"
                   "• QUANTIDADE DE MUNIÇÃO\n"
                   "• MOTIVO\n\n"
                   "👤 **QUEM REGISTRA É MARCADO AUTOMATICAMENTE**",
        color=0x3498db
    )
    
    try:
        async for msg in canal.history(limit=200):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    await asyncio.sleep(0.2)
                except:
                    pass
        
        await asyncio.sleep(0.5)
        await canal.send(embed=embed, view=ArmasSaiuView())
        print(f"📌 Painel de armas saiu enviado como última mensagem")
        
    except Exception as e:
        print(f"❌ Erro ao enviar painel de armas saiu: {e}")

class RegistrarArmaSaiuModal(discord.ui.Modal, title="📤 SAÍDA DE ARMAS"):
    arma_nome = discord.ui.TextInput(
        label="🔫 NOME DA ARMA (OU APELIDO)",
        placeholder="EX: FUZIL, M4, SIG, AK, GLOCK",
        required=True
    )
    
    quantidade = discord.ui.TextInput(
        label="📦 QUANTIDADE DE MUNIÇÃO",
        placeholder="DIGITE A QUANTIDADE",
        required=True
    )
    
    motivo = discord.ui.TextInput(
        label="📝 MOTIVO",
        placeholder="EX: MISSÃO, PATRULHA, ETC",
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value.strip())
            if qtd <= 0:
                raise ValueError
        except:
            await interaction.response.send_message("❌ QUANTIDADE INVÁLIDA!", ephemeral=True)
            return
        
        arma_digitada = self.arma_nome.value.strip()
        opcoes = verificar_opcoes(arma_digitada)
        
        if opcoes and len(opcoes) > 1:
            callback_data = {
                "tipo": "saida",
                "modal_class": "arma_saiu",
                "canal_destino": CANAL_ARMAS_SAIU_ID
            }
            view = SelecionarOpcaoView(opcoes, callback_data)
            
            await interaction.response.send_message(
                f"⚠️ **'{arma_digitada.upper()}' TEM MÚLTIPLAS OPÇÕES!**\n\n"
                f"**SELECIONE UMA DAS OPÇÕES ABAIXO:**",
                view=view,
                ephemeral=True
            )
            return
        
        arma_normalizada = normalizar_nome(arma_digitada)
        motivo = self.motivo.value.strip().upper()
        
        # 🔥 RESPONSÁVEL = QUEM ESTÁ FAZENDO A AÇÃO
        responsavel = interaction.user.display_name.upper()
        
        await registrar_arma(
            "saiu",
            arma_normalizada,
            qtd,
            responsavel,
            motivo
        )
        
        canal = interaction.guild.get_channel(CANAL_ARMAS_SAIU_ID)
        if canal:
            embed = discord.Embed(
                title="📤 SAÍDA DE ARMA",
                color=0x3498db,
                timestamp=agora()
            )
            embed.add_field(name="👤 RESPONSÁVEL", value=f"**{responsavel}**", inline=True)
            embed.add_field(name="🔫 ARMA", value=f"**{arma_normalizada}**", inline=True)
            embed.add_field(name="📦 MUNIÇÃO", value=f"**{fmt_num(qtd)}**", inline=True)
            embed.add_field(name="📝 MOTIVO", value=motivo, inline=False)
            embed.add_field(name="📌 REGISTRADO POR", value=interaction.user.mention, inline=False)
            embed.set_footer(text=f"REGISTRADO POR {responsavel}")
            
            await canal.send(embed=embed)
        
        await interaction.response.send_message(
            f"✅ **SAÍDA REGISTRADA COM SUCESSO!**\n"
            f"👤 RESPONSÁVEL: {responsavel}\n"
            f"🔫 ARMA: {arma_normalizada}\n"
            f"📦 MUNIÇÃO: {fmt_num(qtd)}",
            ephemeral=True
        )
        
        await asyncio.sleep(2)
        await enviar_painel_armas_saiu()
        await enviar_painel_armas()

class RegistrarArmaSaiuModalComOpcao(discord.ui.Modal, title="📤 SAÍDA DE ARMAS"):
    def __init__(self, arma_escolhida, canal_destino):
        super().__init__()
        self.arma_escolhida = arma_escolhida
        self.canal_destino = canal_destino
    
    quantidade = discord.ui.TextInput(
        label="📦 QUANTIDADE DE MUNIÇÃO",
        placeholder="DIGITE A QUANTIDADE",
        required=True
    )
    
    motivo = discord.ui.TextInput(
        label="📝 MOTIVO",
        placeholder="EX: MISSÃO, PATRULHA, ETC",
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value.strip())
            if qtd <= 0:
                raise ValueError
        except:
            await interaction.response.send_message("❌ QUANTIDADE INVÁLIDA!", ephemeral=True)
            return
        
        arma_normalizada = self.arma_escolhida.upper()
        motivo = self.motivo.value.strip().upper()
        
        # 🔥 RESPONSÁVEL = QUEM ESTÁ FAZENDO A AÇÃO
        responsavel = interaction.user.display_name.upper()
        
        await registrar_arma(
            "saiu",
            arma_normalizada,
            qtd,
            responsavel,
            motivo
        )
        
        canal = interaction.guild.get_channel(self.canal_destino)
        if canal:
            embed = discord.Embed(
                title="📤 SAÍDA DE ARMA",
                color=0x3498db,
                timestamp=agora()
            )
            embed.add_field(name="👤 RESPONSÁVEL", value=f"**{responsavel}**", inline=True)
            embed.add_field(name="🔫 ARMA", value=f"**{arma_normalizada}**", inline=True)
            embed.add_field(name="📦 MUNIÇÃO", value=f"**{fmt_num(qtd)}**", inline=True)
            embed.add_field(name="📝 MOTIVO", value=motivo, inline=False)
            embed.add_field(name="📌 REGISTRADO POR", value=interaction.user.mention, inline=False)
            embed.set_footer(text=f"REGISTRADO POR {responsavel}")
            
            await canal.send(embed=embed)
        
        await interaction.response.send_message(
            f"✅ **SAÍDA REGISTRADA COM SUCESSO!**\n"
            f"👤 RESPONSÁVEL: {responsavel}\n"
            f"🔫 ARMA: {arma_normalizada}\n"
            f"📦 MUNIÇÃO: {fmt_num(qtd)}",
            ephemeral=True
        )
        
        await asyncio.sleep(2)
        await enviar_painel_armas_saiu()
        await enviar_painel_armas()

# ================ 15.13 PAINEL ARMAS PERDEU ==============
class ArmasPerdeuView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="💀 REGISTRAR PERDA", style=discord.ButtonStyle.danger, custom_id="armas_perdeu_btn", emoji="💀")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarArmaPerdeuModal())

async def enviar_painel_armas_perdeu():
    canal = bot.get_channel(CANAL_ARMAS_PERDEU_ID)
    if not canal:
        print("❌ Canal armas perdeu não encontrado")
        return
    
    embed = discord.Embed(
        title="💀 ARMAS - REGISTRO DE PERDA",
        description="**CLIQUE NO BOTÃO ABAIXO PARA REGISTRAR PERDA DE ARMAS**\n\n"
                   "📌 **INFORMAÇÕES:**\n"
                   "• NOME DA ARMA (PODE USAR APELIDO: SIG, M4, AK, ETC)\n"
                   "• PERDEU COMO:\n"
                   "• OBSERVAÇÃO\n\n"
                   "👤 **QUEM REGISTRA É MARCADO AUTOMATICAMENTE**",
        color=0xe74c3c
    )
    
    try:
        async for msg in canal.history(limit=200):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    await asyncio.sleep(0.2)
                except:
                    pass
        
        await asyncio.sleep(0.5)
        await canal.send(embed=embed, view=ArmasPerdeuView())
        print(f"📌 Painel de armas perdeu enviado como última mensagem")
        
    except Exception as e:
        print(f"❌ Erro ao enviar painel de armas perdeu: {e}")

class RegistrarArmaPerdeuModal(discord.ui.Modal, title="💀 PERDA DE ARMAS"):
    arma_nome = discord.ui.TextInput(
        label="🔫 NOME DA ARMA (OU APELIDO)",
        placeholder="EX: FUZIL, M4, SIG, AK, GLOCK",
        required=True
    )
    
    perdeu_como = discord.ui.TextInput(
        label="💀 PERDEU COMO:",
        placeholder="EX: MORREU, PERDEU NO MATO, ETC",
        required=True
    )
    
    observacao = discord.ui.TextInput(
        label="📝 OBSERVAÇÃO",
        style=discord.TextStyle.paragraph,
        required=False,
        placeholder="DETALHES ADICIONAIS SOBRE A PERDA"
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        arma_digitada = self.arma_nome.value.strip()
        opcoes = verificar_opcoes(arma_digitada)
        
        if opcoes and len(opcoes) > 1:
            callback_data = {
                "tipo": "perda",
                "modal_class": "arma_perdeu"
            }
            view = SelecionarOpcaoView(opcoes, callback_data)
            
            await interaction.response.send_message(
                f"⚠️ **'{arma_digitada.upper()}' TEM MÚLTIPLAS OPÇÕES!**\n\n"
                f"**SELECIONE UMA DAS OPÇÕES ABAIXO:**",
                view=view,
                ephemeral=True
            )
            return
        
        arma_normalizada = normalizar_nome(arma_digitada)
        perdeu_como = self.perdeu_como.value.strip().upper()
        observacao = self.observacao.value.strip().upper() if self.observacao.value else ""
        
        # 🔥 RESPONSÁVEL = QUEM ESTÁ FAZENDO A AÇÃO
        responsavel = interaction.user.display_name.upper()
        
        await registrar_arma(
            "perdeu",
            arma_normalizada,
            1,
            responsavel,
            observacao
        )
        
        canal = interaction.guild.get_channel(CANAL_ARMAS_PERDEU_ID)
        if canal:
            embed = discord.Embed(
                title="💀 PERDA DE ARMA",
                color=0xe74c3c,
                timestamp=agora()
            )
            embed.add_field(name="🔫 ARMA", value=f"**{arma_normalizada}**", inline=True)
            embed.add_field(name="💀 PERDEU COMO:", value=f"**{perdeu_como}**", inline=True)
            if observacao:
                embed.add_field(name="📝 OBSERVAÇÃO", value=observacao, inline=False)
            embed.add_field(name="📌 REGISTRADO POR", value=interaction.user.mention, inline=False)
            embed.set_footer(text=f"REGISTRADO POR {responsavel}")
            
            await canal.send(embed=embed)
        
        await interaction.response.send_message(
            f"✅ **PERDA REGISTRADA COM SUCESSO!**\n"
            f"🔫 ARMA: {arma_normalizada}\n"
            f"💀 PERDEU COMO: {perdeu_como}",
            ephemeral=True
        )
        
        await asyncio.sleep(2)
        await enviar_painel_armas_perdeu()
        await enviar_painel_armas()

class RegistrarArmaPerdeuModalComOpcao(discord.ui.Modal, title="💀 PERDA DE ARMAS"):
    def __init__(self, arma_escolhida):
        super().__init__()
        self.arma_escolhida = arma_escolhida
    
    perdeu_como = discord.ui.TextInput(
        label="💀 PERDEU COMO:",
        placeholder="EX: MORREU, PERDEU NO MATO, ETC",
        required=True
    )
    
    observacao = discord.ui.TextInput(
        label="📝 OBSERVAÇÃO",
        style=discord.TextStyle.paragraph,
        required=False,
        placeholder="DETALHES ADICIONAIS SOBRE A PERDA"
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        arma_normalizada = self.arma_escolhida.upper()
        perdeu_como = self.perdeu_como.value.strip().upper()
        observacao = self.observacao.value.strip().upper() if self.observacao.value else ""
        
        # 🔥 RESPONSÁVEL = QUEM ESTÁ FAZENDO A AÇÃO
        responsavel = interaction.user.display_name.upper()
        
        await registrar_arma(
            "perdeu",
            arma_normalizada,
            1,
            responsavel,
            observacao
        )
        
        canal = interaction.guild.get_channel(CANAL_ARMAS_PERDEU_ID)
        if canal:
            embed = discord.Embed(
                title="💀 PERDA DE ARMA",
                color=0xe74c3c,
                timestamp=agora()
            )
            embed.add_field(name="🔫 ARMA", value=f"**{arma_normalizada}**", inline=True)
            embed.add_field(name="💀 PERDEU COMO:", value=f"**{perdeu_como}**", inline=True)
            if observacao:
                embed.add_field(name="📝 OBSERVAÇÃO", value=observacao, inline=False)
            embed.add_field(name="📌 REGISTRADO POR", value=interaction.user.mention, inline=False)
            embed.set_footer(text=f"REGISTRADO POR {responsavel}")
            
            await canal.send(embed=embed)
        
        await interaction.response.send_message(
            f"✅ **PERDA REGISTRADA COM SUCESSO!**\n"
            f"🔫 ARMA: {arma_normalizada}\n"
            f"💀 PERDEU COMO: {perdeu_como}",
            ephemeral=True
        )
        
        await asyncio.sleep(2)
        await enviar_painel_armas_perdeu()
        await enviar_painel_armas()

# ================ 15.14 PAINEL BAÚ ENTROU ================
class BauEntrouView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📥 REGISTRAR ENTRADA", style=discord.ButtonStyle.success, custom_id="bau_entrou_btn", emoji="📥")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarItemBaúModal("entrada"))

async def enviar_painel_bau_entrou():
    canal = bot.get_channel(CANAL_BAU_ENTROU_ID)
    if not canal:
        print("❌ Canal baú entrou não encontrado")
        return
    
    estoque = await buscar_estoque_bau()
    
    embed = discord.Embed(
        title="📥 BAÚ - ESTOQUE ATUAL",
        description="**ITENS DISPONÍVEIS NO BAÚ**",
        color=0x2ecc71,
        timestamp=agora()
    )
    
    texto = ""
    for item in estoque:
        if item["quantidade"] > 0:
            emoji = "📦"
            for item_completo in ITENS_DISPONIVEIS:
                if item["item_nome"] in item_completo:
                    emoji = item_completo.split(" ")[0] if " " in item_completo else "📦"
                    break
            texto += f"{emoji} **{item['item_nome']}:** {fmt_num(item['quantidade'])}\n"
    
    if texto:
        embed.add_field(name="📋 ITENS NO BAÚ", value=texto, inline=False)
    else:
        embed.add_field(name="📋 ITENS NO BAÚ", value="📭 BAÚ VAZIO", inline=False)
    
    embed.add_field(
        name="📌 COMO USAR",
        value="**CLIQUE NO BOTÃO ABAIXO PARA REGISTRAR ENTRADA DE ITENS**\n\n"
              "💡 **DICA:** VOCÊ PODE USAR APELIDOS!\n"
              "EX: 'kit comum' → KIT REPAROS COMUM\n"
              "EX: 'block' → VAI PERGUNTAR QUAL\n\n"
              "👤 **QUEM REGISTRA É MARCADO AUTOMATICAMENTE**",
        inline=False
    )
    embed.set_footer(text="CLIQUE EM 'REGISTRAR ENTRADA' PARA ADICIONAR ITENS")
    
    try:
        async for msg in canal.history(limit=200):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    await asyncio.sleep(0.2)
                except:
                    pass
        
        await asyncio.sleep(0.5)
        await canal.send(embed=embed, view=BauEntrouView())
        print(f"📌 Painel de baú entrou enviado como última mensagem")
        
    except Exception as e:
        print(f"❌ Erro ao enviar painel de baú entrou: {e}")

# ================ 15.15 PAINEL BAÚ SAIU ==================
class BauSaiuView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(label="📤 REGISTRAR SAÍDA", style=discord.ButtonStyle.primary, custom_id="bau_saiu_btn", emoji="📤")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarItemBaúModal("saida"))

async def enviar_painel_bau_saiu():
    canal = bot.get_channel(CANAL_BAU_SAIU_ID)
    if not canal:
        print("❌ Canal baú saiu não encontrado")
        return
    
    estoque = await buscar_estoque_bau()
    
    embed = discord.Embed(
        title="📤 BAÚ - REGISTRO DE SAÍDA",
        description="**ITENS DISPONÍVEIS PARA RETIRADA**",
        color=0x3498db,
        timestamp=agora()
    )
    
    texto = ""
    for item in estoque:
        if item["quantidade"] > 0:
            emoji = "📦"
            for item_completo in ITENS_DISPONIVEIS:
                if item["item_nome"] in item_completo:
                    emoji = item_completo.split(" ")[0] if " " in item_completo else "📦"
                    break
            texto += f"{emoji} **{item['item_nome']}:** {fmt_num(item['quantidade'])}\n"
    
    if texto:
        embed.add_field(name="📋 ITENS DISPONÍVEIS", value=texto, inline=False)
    else:
        embed.add_field(name="📋 ITENS DISPONÍVEIS", value="📭 BAÚ VAZIO", inline=False)
    
    embed.add_field(
        name="📌 COMO USAR",
        value="**CLIQUE NO BOTÃO ABAIXO PARA REGISTRAR SAÍDA DE ITENS**\n\n"
              "💡 **DICA:** VOCÊ PODE USAR APELIDOS!\n"
              "EX: 'kit raro' → KIT REPAROS RARO\n"
              "EX: 'block' → VAI PERGUNTAR QUAL\n\n"
              "👤 **QUEM REGISTRA É MARCADO AUTOMATICAMENTE**",
        inline=False
    )
    embed.set_footer(text="CLIQUE EM 'REGISTRAR SAÍDA' PARA RETIRAR ITENS")
    
    try:
        async for msg in canal.history(limit=200):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    await asyncio.sleep(0.2)
                except:
                    pass
        
        await asyncio.sleep(0.5)
        await canal.send(embed=embed, view=BauSaiuView())
        print(f"📌 Painel de baú saiu enviado como última mensagem")
        
    except Exception as e:
        print(f"❌ Erro ao enviar painel de baú saiu: {e}")

# ================ 15.16 MODAL DE ITEM DO BAÚ ============
class RegistrarItemBaúModal(discord.ui.Modal):
    def __init__(self, tipo_movimento):
        super().__init__(title=f"📦 REGISTRAR ITEM - {tipo_movimento.upper()}")
        self.tipo_movimento = tipo_movimento
        
        titulos = {
            "entrada": "📥 ENTRADA DE ITEM",
            "saida": "📤 SAÍDA DE ITEM"
        }
        self.title = titulos.get(tipo_movimento, "REGISTRAR ITEM")
    
    item_nome = discord.ui.TextInput(
        label="📦 NOME DO ITEM (OU APELIDO)",
        placeholder="EX: COLETE, KIT COMUM, BLOCK, MUNIÇÃO PT",
        required=True
    )
    
    quantidade = discord.ui.TextInput(
        label="📦 QUANTIDADE",
        placeholder="DIGITE A QUANTIDADE",
        required=True
    )
    
    observacao = discord.ui.TextInput(
        label="📝 OBSERVAÇÃO",
        style=discord.TextStyle.paragraph,
        required=False,
        placeholder="EX: MOTIVO, LOCAL, ETC"
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value.strip())
            if qtd <= 0:
                raise ValueError
        except:
            await interaction.response.send_message("❌ QUANTIDADE INVÁLIDA!", ephemeral=True)
            return
        
        item_digitado = self.item_nome.value.strip()
        opcoes = verificar_opcoes(item_digitado)
        
        if opcoes and len(opcoes) > 1:
            callback_data = {
                "tipo": self.tipo_movimento,
                "modal_class": "bau",
                "tipo_movimento": self.tipo_movimento
            }
            view = SelecionarOpcaoView(opcoes, callback_data)
            
            await interaction.response.send_message(
                f"⚠️ **'{item_digitado.upper()}' TEM MÚLTIPLAS OPÇÕES!**\n\n"
                f"**SELECIONE UMA DAS OPÇÕES ABAIXO:**",
                view=view,
                ephemeral=True
            )
            return
        
        item_normalizado = normalizar_nome(item_digitado)
        
        # 🔥 RESPONSÁVEL = QUEM ESTÁ FAZENDO A AÇÃO
        responsavel = interaction.user.display_name.upper()
        
        await registrar_item_bau(
            item_normalizado,
            qtd,
            self.tipo_movimento,
            responsavel,
            self.observacao.value.strip().upper() if self.observacao.value else ""
        )
        
        canal_destino = CANAL_BAU_ENTROU_ID if self.tipo_movimento == "entrada" else CANAL_BAU_SAIU_ID
        canal = interaction.guild.get_channel(canal_destino)
        
        if canal:
            emojis = {"entrada": "📥", "saida": "📤"}
            cores = {"entrada": 0x2ecc71, "saida": 0x3498db}
            titulos = {"entrada": "ENTRADA", "saida": "SAÍDA"}
            
            embed = discord.Embed(
                title=f"{emojis[self.tipo_movimento]} ITEM DO BAÚ - {titulos[self.tipo_movimento]}",
                description=f"**{item_normalizado}**",
                color=cores[self.tipo_movimento],
                timestamp=agora()
            )
            embed.add_field(name="📦 QUANTIDADE", value=f"**{fmt_num(qtd)}**", inline=True)
            embed.add_field(name="👤 RESPONSÁVEL", value=f"**{responsavel}**", inline=True)
            if self.observacao.value:
                embed.add_field(name="📝 OBS", value=self.observacao.value.strip().upper(), inline=False)
            embed.add_field(name="📌 REGISTRADO POR", value=interaction.user.mention, inline=False)
            embed.set_footer(text=f"REGISTRADO POR {responsavel}")
            
            await canal.send(embed=embed)
        
        await interaction.response.send_message(
            f"✅ **ITEM REGISTRADO COM SUCESSO!**\n"
            f"📦 ITEM: {item_normalizado}\n"
            f"📦 QUANTIDADE: {fmt_num(qtd)}\n"
            f"📌 TIPO: {self.tipo_movimento.upper()}\n"
            f"👤 RESPONSÁVEL: {responsavel}",
            ephemeral=True
        )
        
        await asyncio.sleep(2)
        await enviar_painel_bau_entrou()
        await enviar_painel_bau_saiu()
        await enviar_painel_armas()

class RegistrarItemBaúModalComOpcao(discord.ui.Modal, title="📦 REGISTRAR ITEM"):
    def __init__(self, item_escolhido, tipo_movimento):
        super().__init__()
        self.item_escolhido = item_escolhido
        self.tipo_movimento = tipo_movimento
        
        titulos = {
            "entrada": "📥 ENTRADA DE ITEM",
            "saida": "📤 SAÍDA DE ITEM"
        }
        self.title = titulos.get(tipo_movimento, "REGISTRAR ITEM")
    
    quantidade = discord.ui.TextInput(
        label="📦 QUANTIDADE",
        placeholder="DIGITE A QUANTIDADE",
        required=True
    )
    
    observacao = discord.ui.TextInput(
        label="📝 OBSERVAÇÃO",
        style=discord.TextStyle.paragraph,
        required=False,
        placeholder="EX: MOTIVO, LOCAL, ETC"
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value.strip())
            if qtd <= 0:
                raise ValueError
        except:
            await interaction.response.send_message("❌ QUANTIDADE INVÁLIDA!", ephemeral=True)
            return
        
        item_normalizado = self.item_escolhido.upper()
        
        # 🔥 RESPONSÁVEL = QUEM ESTÁ FAZENDO A AÇÃO
        responsavel = interaction.user.display_name.upper()
        
        await registrar_item_bau(
            item_normalizado,
            qtd,
            self.tipo_movimento,
            responsavel,
            self.observacao.value.strip().upper() if self.observacao.value else ""
        )
        
        canal_destino = CANAL_BAU_ENTROU_ID if self.tipo_movimento == "entrada" else CANAL_BAU_SAIU_ID
        canal = interaction.guild.get_channel(canal_destino)
        
        if canal:
            emojis = {"entrada": "📥", "saida": "📤"}
            cores = {"entrada": 0x2ecc71, "saida": 0x3498db}
            titulos = {"entrada": "ENTRADA", "saida": "SAÍDA"}
            
            embed = discord.Embed(
                title=f"{emojis[self.tipo_movimento]} ITEM DO BAÚ - {titulos[self.tipo_movimento]}",
                description=f"**{item_normalizado}**",
                color=cores[self.tipo_movimento],
                timestamp=agora()
            )
            embed.add_field(name="📦 QUANTIDADE", value=f"**{fmt_num(qtd)}**", inline=True)
            embed.add_field(name="👤 RESPONSÁVEL", value=f"**{responsavel}**", inline=True)
            if self.observacao.value:
                embed.add_field(name="📝 OBS", value=self.observacao.value.strip().upper(), inline=False)
            embed.add_field(name="📌 REGISTRADO POR", value=interaction.user.mention, inline=False)
            embed.set_footer(text=f"REGISTRADO POR {responsavel}")
            
            await canal.send(embed=embed)
        
        await interaction.response.send_message(
            f"✅ **ITEM REGISTRADO COM SUCESSO!**\n"
            f"📦 ITEM: {item_normalizado}\n"
            f"📦 QUANTIDADE: {fmt_num(qtd)}\n"
            f"📌 TIPO: {self.tipo_movimento.upper()}\n"
            f"👤 RESPONSÁVEL: {responsavel}",
            ephemeral=True
        )
        
        await asyncio.sleep(2)
        await enviar_painel_bau_entrou()
        await enviar_painel_bau_saiu()
        await enviar_painel_armas()

# ================ 15.17 FUNÇÃO PARA VERIFICAR MENSAGENS ================

async def on_message_controle(message: discord.Message):
    if message.author.bot:
        return
    
    canais_controle = [
        CANAL_CONTROLE_ARMAS_ID,
        CANAL_ARMAS_ENTROU_ID,
        CANAL_ARMAS_SAIU_ID,
        CANAL_ARMAS_PERDEU_ID,
        CANAL_BAU_ENTROU_ID,
        CANAL_BAU_SAIU_ID
    ]
    
    if message.channel.id in canais_controle:
        await asyncio.sleep(3)
        
        canal = message.channel
        try:
            async for msg in canal.history(limit=200):
                if msg.author == bot.user:
                    try:
                        await msg.delete()
                        await asyncio.sleep(0.2)
                    except:
                        pass
            
            await asyncio.sleep(0.5)
            
            if canal.id == CANAL_CONTROLE_ARMAS_ID:
                await enviar_painel_armas()
            elif canal.id == CANAL_ARMAS_ENTROU_ID:
                await enviar_painel_armas_entrou()
            elif canal.id == CANAL_ARMAS_SAIU_ID:
                await enviar_painel_armas_saiu()
            elif canal.id == CANAL_ARMAS_PERDEU_ID:
                await enviar_painel_armas_perdeu()
            elif canal.id == CANAL_BAU_ENTROU_ID:
                await enviar_painel_bau_entrou()
            elif canal.id == CANAL_BAU_SAIU_ID:
                await enviar_painel_bau_saiu()
                
            print(f"📌 Painel recolocado no final do canal {canal.name}")
            
        except Exception as e:
            print(f"❌ Erro ao recolocar painel: {e}")

# ================ 15.18 FUNÇÃO PRINCIPAL ================

async def enviar_painel_controle():
    await criar_tabelas_controle()
    
    await enviar_painel_armas()
    await enviar_painel_armas_entrou()
    await enviar_painel_armas_saiu()
    await enviar_painel_armas_perdeu()
    await enviar_painel_bau_entrou()
    await enviar_painel_bau_saiu()
    
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
        bot.add_view(MetaView(0))
        bot.add_view(GrupoView("placeholder", "placeholder"))
        bot.add_view(ConfirmarExcluirView("placeholder", "placeholder"))
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

    # Iniciar loop de verificação de avisos de meta
    try:
        if not verificar_avisos_meta.is_running():
            verificar_avisos_meta.start()
            print("🔔 Loop de avisos de meta iniciado")
    except Exception as e:
        print(f"Erro ao iniciar loop de avisos: {e}")

    # =========================================================
    # ================= GRUPOS - SOMENTE O PAINEL =============
    # =========================================================
    
    # Enviar painel de grupos (apenas o botão de registro)
    try:
        await enviar_painel_registro_grupos()
    except Exception as e:
        print("Erro ao enviar painel registro grupos:", e)
    
    # NÃO RESTAURAR GRUPOS AUTOMATICAMENTE para evitar rate limit
    print("📋 Sistema de grupos carregado. Use o botão '🔄 Atualizar' no embed para atualizar cada grupo.")

    # Fixar painéis de metas no final de cada sala
    try:
        for uid in metas_cache.keys():
            await fixar_painel_meta_no_final(int(uid))
            await asyncio.sleep(0.5)
        print("📌 Painéis de metas fixados no final")
    except Exception as e:
        print(f"Erro ao fixar painéis: {e}")
    
    # =========================================================
    # ================= FIM GRUPOS ============================
    # =========================================================
    
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
            enviar_painel_relatorio_metas(),
            enviar_painel_mensagens(),
            enviar_painel_controle(),
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
