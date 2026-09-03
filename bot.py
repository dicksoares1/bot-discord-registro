# =========================================================
# ==================== BOT VDR v.7 - 100% COMPLETO ========
# =========================================================
# TODOS OS SISTEMAS :
# =========================================================
# 1. CONFIGURAÇÕES GLOBAIS
# 2. BANCO DE DADOS
# 3. UTILITÁRIOS
# 4. SISTEMA DE RECEPÇÃO/REGISTRO
# 5. SISTEMA DE AVISOS
# 6. SISTEMA FINANCEIRO
# 7. SISTEMA DE AUSÊNCIA
# 8. SISTEMA DE LAVAGEM
# 9. SISTEMA DE LIVES
# 10. SISTEMA DE BAÚ
# 11. SISTEMA DE AÇÕES
# 12. SISTEMA DE VENDAS (COM TRANSFERÊNCIA)
# 13. SISTEMA DE PRODUÇÃO
# 14. SISTEMA DE METAS (SEM PÓLVORA)
# 15. SISTEMA DE GRUPOS
# 16. SISTEMA DE MENSAGENS (COMPLETO)
# 17. SISTEMA DE LOGS
# 18. TASKS E EVENTOS
# 19. COMANDOS
# 20. MAIN
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
import time as time_module
import logging
import logging.handlers
import psutil
import signal
import random
import io
from discord.ext import commands, tasks
from discord.utils import escape_markdown
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from functools import wraps
from collections import defaultdict


# =========================================================
# IMAGEM DO VDRZINHO (TIGRE)
# =========================================================
TIGRE_IMAGE_URL = "https://media.discordapp.net/attachments/1009160488015384587/1542148964415963238/00NXEiFT7gshlT856mZdY1_1787746949961_na1fn_L2hvbWUvdWJ1bnR1L3RpZ3JlX2ZhbnRhc21hX2Rhc19zb21icmFz.png?ex=6a902da4&is=6a8edc24&hm=8f689963e26f5a672d7cc9e30f57e0a5af9db9f02b08baa51533e53674453839&=&format=webp&quality=lossless&width=1024&height=1024"
# =========================================================
# ==================== VDRZINHO - IA MASCOTE ==============
# =========================================================
# COLOCAR LOGO APÓS AS IMPORTAÇÕES
# =========================================================

import json
from collections import defaultdict
from datetime import datetime, timedelta

class VDRzinhoIA:
    """
    VDRzinho - IA Mascote da VDR 442
    - Fala em primeira pessoa
    - Aprende com as ações dos usuários
    - Tem personalidade única
    - Fala de forma natural e engraçada
    """
    
    def __init__(self):
        self.nome = "VDRzinho"
        self.versao = "2.0"
        
        # =========================================================
        # MEMÓRIA DO VDRZINHO
        # =========================================================
        self.memoria = {
            "usuario": {},
            "cache": {}
        }
        
        # =========================================================
        # FRASES DO VDRZINHO (COM PERSONALIDADE)
        # =========================================================
        self.frases = {
            "meta_concluida": [
                "🎯 **UHUUL!** {nome} bateu a meta! HEHE, tá voando baixo! 🐯",
                "🎯 **CARALHO, {nome}!** Meta batida com estilo! Tô orgulhoso! 🐯",
                "🎯 **OLHA SÓ!** {nome} é fera demais! Mais uma meta no bolso! 🐯",
                "🎯 **{nome}** acabou de destruir a meta! HEHE, continuar assim! 🐯"
            ],
            "venda_feita": [
                "💰 **OLHA O DINHEIRO!** {nome} fez uma venda! HEHE, tá rico! 🐯",
                "💰 **CARAMBA, {nome}!** Mais uma venda! O tigre tá feliz! 🐯",
                "💰 **{nome}** é um monstro das vendas! HEHE, continua! 🐯",
                "💰 **VENDAS!** {nome} arrasou! Dinheiro entrando! 🐯"
            ],
            "erro_aconteceu": [
                "AFF... {nome}, isso não foi legal. Tenta de novo, guerreiro! 🐯",
                "😅 **{nome}**, acho que você errou. Acontece com os melhores! 🐯",
                "🤔 **{nome}**, bora com calma! Tenta mais uma vez. 🐯"
            ],
            "ausencia_registrada": [
                "📋 **{nome}** tá de folga! HEHE, descansa aí, guerreiro! 🐯",
                "📋 **{nome}** pediu ausência! Vou segurar as pontas aqui! 🐯",
                "📋 **OLHA!** {nome} vai dar um tempo. Volta logo! 🐯"
            ],
            "acao_feita": [
                "⚔️ **AÇÃO NA VEIA!** {nome} participou! Vocês são uns loucos! 🐯",
                "⚔️ **{nome}** na ação! HEHE, bora pra cima! 🐯",
                "⚔️ **CARALHO!** {nome} tá na ação! Isso é guerra! 🐯"
            ],
            "reset_feito": [
                "♻️ **RESETEI TUDO!** Nova semana, nova caçada! HEHE! 🐯",
                "♻️ **TUDO LIMPO!** {nome} mandou resetar! Bora começar de novo! 🐯"
            ],
            "bot_iniciando": [
                "🐯 **VDRZINHO ONLINE!** Quem precisa de ajuda hoje? HEHE! 🐯",
                "🐯 **ACORDEI!** HEHE, tô pronto pra caçar! 🐯",
                "🐯 **VOLTEI!** Saudades de vocês! Bora trabalhar! 🐯"
            ],
            "novo_membro": [
                "📋 **OLHA SÓ!** {nome} entrou na família! HEHE, bem-vindo! 🐯",
                "📋 **{nome}** é novo por aqui! HEHE, bora se divertir! 🐯"
            ],
            "lavagem_feita": [
                "🧼 **LAVAGEM FEITA!** {nome} lavou uma grana! HEHE, tá limpinho! 🐯",
                "🧼 **OLHA SÓ!** {nome} lavou dinheiro! O tigre aprova! 🐯"
            ],
            "live_iniciada": [
                "🎥 **LIVE COMEÇANDO!** {nome} tá ao vivo! Bora dar moral! 🐯",
                "🎥 **{nome}** ligou a live! HEHE, vou assistir! 🐯"
            ],
            "bau_entrada": [
                "📦 **OLHA SÓ!** {nome} colocou **{qtd} {item}** no baú! HEHE, tá organizando! 🐯",
                "📦 **{nome}** guardou **{qtd} {item}**! O baú tá ficando cheio! 🐯",
                "📦 **CARAMBA!** {nome} colocou **{qtd} {item}**! HEHE, que bonito! 🐯",
                "📦 **{nome}** é organizado! Guardou **{qtd} {item}** no baú! 🐯"
            ],
            "bau_saida": [
                "📤 **{nome}** pegou **{qtd} {item}** do baú! HEHE, precisa pra ação! 🐯",
                "📤 **OLHA SÓ!** {nome} tirou **{qtd} {item}**! Tá se preparando! 🐯",
                "📤 **{nome}** pegou **{qtd} {item}**! HEHE, bora pra guerra! 🐯",
                "📤 **CARAMBA!** {nome} retirou **{qtd} {item}** do baú! 🐯"
            ],
            "arma_entrada": [
                "🔫 **{nome}** guardou **{qtd} {item}** no arsenal! HEHE, tá armado! 🐯",
                "🔫 **OLHA SÓ!** {nome} colocou **{qtd} {item}**! O arsenal tá forte! 🐯",
                "🔫 **{nome}** adicionou **{qtd} {item}**! HEHE, guerra é logo ali! 🐯"
            ],
            "arma_saida": [
                "🔫 **{nome}** pegou **{qtd} {item}** do arsenal! HEHE, vai pra ação! 🐯",
                "🔫 **OLHA SÓ!** {nome} tirou **{qtd} {item}**! Tá preparado! 🐯",
                "🔫 **{nome}** retirou **{qtd} {item}**! HEHE, bora pro abate! 🐯"
            ],
            "agradecimento": [
                "🐯 **DE NADA, {nome}!** HEHE, tô aqui pra ajudar! 🐯",
                "🐯 **{nome}**, você é o melhor! HEHE, foi um prazer! 🐯"
            ],
            "alerta": [
                "⚠️ **{nome}**, presta atenção! Isso não parece certo! 🐯",
                "⚠️ **OLHA SÓ!** {nome}, tem algo errado aí! 🐯"
            ]
        }

    # =========================================================
    # FUNÇÃO PRINCIPAL: VDRZINHO FALA
    # =========================================================
    def falar(self, tipo, user_id=None, nome="", dados=None):
        if user_id:
            self.aprender(user_id, tipo, dados)
        
        frases = self.frases.get(tipo, ["🐯 **VDRZINHO AQUI!** HEHE! 🐯"])
        frase = random.choice(frases)
        
        # Substituir placeholders
        if "{nome}" in frase and nome:
            frase = frase.replace("{nome}", nome)
        
        if dados:
            if "{qtd}" in frase and "qtd" in dados:
                frase = frase.replace("{qtd}", str(dados["qtd"]))
            if "{item}" in frase and "item" in dados:
                frase = frase.replace("{item}", dados["item"])
        
        return frase

    # =========================================================
    # FUNÇÃO PARA APRENDER
    # =========================================================
    def aprender(self, user_id, tipo, dados=None):
        if user_id not in self.memoria["usuario"]:
            self.memoria["usuario"][user_id] = {
                "metas": 0, "vendas": 0, "erros": 0, "ausencias": 0,
                "acoes": 0, "ultima_acao": "", "ultima_data": agora_db(),
                "total_interacoes": 0
            }
        
        if tipo == "meta_concluida":
            self.memoria["usuario"][user_id]["metas"] += 1
        elif tipo == "venda_feita":
            self.memoria["usuario"][user_id]["vendas"] += 1
        elif tipo == "erro_aconteceu":
            self.memoria["usuario"][user_id]["erros"] += 1
        elif tipo == "ausencia_registrada":
            self.memoria["usuario"][user_id]["ausencias"] += 1
        elif tipo == "acao_feita":
            self.memoria["usuario"][user_id]["acoes"] += 1
        
        self.memoria["usuario"][user_id]["ultima_acao"] = tipo
        self.memoria["usuario"][user_id]["ultima_data"] = agora_db()
        self.memoria["usuario"][user_id]["total_interacoes"] += 1
        
        if user_id not in self.memoria["cache"]:
            self.memoria["cache"][user_id] = {"ultimas_acoes": [], "humor": "feliz"}
        self.memoria["cache"][user_id]["ultimas_acoes"].append(tipo)
        if len(self.memoria["cache"][user_id]["ultimas_acoes"]) > 10:
            self.memoria["cache"][user_id]["ultimas_acoes"].pop(0)

    # =========================================================
    # FUNÇÃO PARA CRIAR EMBED COM A RESPOSTA
    # =========================================================
    def embed_resposta(self, tipo, interaction=None, user_id=None, nome="", dados=None, cor=0xFF6B00):
        if not user_id and interaction:
            user_id = interaction.user.id
            nome = interaction.user.display_name
        
        if not nome and user_id:
            try:
                user = bot.get_user(int(user_id))
                if user:
                    nome = user.display_name or user.name
                else:
                    nome = str(user_id)
            except:
                nome = str(user_id)
        
        fala = self.falar(tipo, user_id, nome, dados)
        
        embed = discord.Embed(
            description=fala,
            color=cor,
            timestamp=agora()
        )
        
        embed.set_author(
            name="🐯 VDRzinho • IA da VDR 442",
            icon_url=bot.user.display_avatar.url if bot.user else None
        )
        
        embed.set_thumbnail(url=TIGRE_IMAGE_URL)
        
        if user_id and user_id in self.memoria["usuario"]:
            stats = self.memoria["usuario"][user_id]
            rodape = f"🧠 {stats['total_interacoes']} interações • {stats['metas']} metas • {stats['vendas']} vendas"
            embed.set_footer(text=rodape, icon_url=bot.user.display_avatar.url if bot.user else None)
        else:
            embed.set_footer(text="🐯 VDRzinho está sempre aprendendo!", icon_url=bot.user.display_avatar.url if bot.user else None)
        
        return embed

    # =========================================================
    # FUNÇÃO ESPECIAL PARA BAU (COM DETALHES)
    # =========================================================
    def embed_bau(self, tipo, nome, item, quantidade, interaction=None):
        """Cria resposta específica para o baú"""
        dados = {"qtd": quantidade, "item": item}
        tipo_embed = "bau_entrada" if tipo == "entrou" else "bau_saida"
        
        # Se for arma, usa o tipo específico
        if is_arma(item):
            tipo_embed = "arma_entrada" if tipo == "entrou" else "arma_saida"
        
        return self.embed_resposta(
            tipo=tipo_embed,
            interaction=interaction,
            nome=nome,
            dados=dados,
            cor=0xFF6B00
        )
    
    def get_stats(self, user_id):
        if user_id in self.memoria["usuario"]:
            return self.memoria["usuario"][user_id]
        return None

    def resetar_memoria(self, user_id):
        if user_id in self.memoria["usuario"]:
            self.memoria["usuario"][user_id] = {
                "metas": 0, "vendas": 0, "erros": 0, "ausencias": 0,
                "acoes": 0, "ultima_acao": "", "ultima_data": agora_db(),
                "total_interacoes": 0
            }
            return True
        return False

    async def salvar_memoria(self):
        pool = await get_pool()
        if not pool:
            return
        try:
            memoria_json = json.dumps(self.memoria["usuario"])
            async with pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO vdrzinho_memoria (id, memoria, data_atualizacao)
                    VALUES (1, $1, NOW())
                    ON CONFLICT (id) DO UPDATE SET memoria = $1, data_atualizacao = NOW()
                """, memoria_json)
        except Exception as e:
            logger.error(f"❌ Erro ao salvar memória do VDRzinho: {e}")

    async def carregar_memoria(self):
        pool = await get_pool()
        if not pool:
            return
        try:
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT memoria FROM vdrzinho_memoria WHERE id = 1")
                if row and row["memoria"]:
                    self.memoria["usuario"] = json.loads(row["memoria"])
                    logger.info("🧠 Memória do VDRzinho carregada!")
        except Exception as e:
            logger.error(f"❌ Erro ao carregar memória do VDRzinho: {e}")

# =========================================================
# CRIAR A INSTÂNCIA DO VDRZINHO
# =========================================================
vdrzinho = VDRzinhoIA()

# =========================================================
# 0.INICIALIZAÇÃO DO BOT
# =========================================================

bot = commands.Bot(command_prefix="!", intents=discord.Intents.all())

# =========================================================
# ==================== PARTE 1: CONFIGURAÇÕES GLOBAIS =====
# =========================================================

# =========================================================
# 1.1 LOGGER
# =========================================================
logging.basicConfig(level=logging.INFO, handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger('VDR_BOT_COMPLETO')

# =========================================================
# 1.2 TOKENS E VARIÁVEIS DE AMBIENTE
# =========================================================
TOKEN = os.environ.get("TOKEN")
if not TOKEN:
    logger.critical("❌ TOKEN não encontrado!")
    sys.exit(1)

DATABASE_URL = os.environ.get("DATABASE_URL") or os.environ.get("POSTGRES_URL")
if not DATABASE_URL:
    logger.critical("❌ DATABASE_URL não encontrada!")
    sys.exit(1)

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")

# =========================================================
# 1.3 CONSTANTES GLOBAIS
# =========================================================
BRASIL = ZoneInfo("America/Sao_Paulo")
GUILD_ID = 1229526644193099880
BASE_PATH = "/mnt/data"

PRECO_POLVORA = 80
PRECO_EMBALAGEM_POR_UNIDADE = 2000000 / 25000
TEMPO_BASE_NORTE = 65
TEMPO_BASE_SUL = 130
META_LIMITE = 300000

# =========================================================
# 1.4 IDs - CARGOS
# =========================================================
CARGO_GERENTE_ID = 1324499473296134154
CARGO_GERENTE_GERAL_ID = 1462804425163935796
CARGO_01_ID = 1258753233355014144
CARGO_02_ID = 1258753479082512394
CARGO_RESP_METAS_ID = 1337407399656423485
CARGO_RESP_ACAO_ID = 1337379517274259509
CARGO_RESP_P1_ID = 1537563287393402920
CARGO_RESP_VENDAS_ID = 1337379530586980352
CARGO_RESP_PRODUCAO_ID = 1337379524949573662
CARGO_SOLDADO_ID = 1537803858611281940
CARGO_MEMBRO_ID = 1422847198789369926
CARGO_AGREGADO_ID = 1422847202937536532
CARGO_AUSENTE_ID = 1337420032212336823
CONVIDADO_ROLE_ID = 1337382961456353342
EM_REGISTRO_ROLE_ID = 1337382961456353342
AGREGADO_ROLE_ID = 1422847202937536532

CARGOS_PERMITIDOS_REMOVER = [
    CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID,
    CARGO_01_ID, CARGO_02_ID
]

CARGOS_PERMITIDOS_ESCALACAO = [
    CARGO_AGREGADO_ID, CARGO_MEMBRO_ID, CARGO_SOLDADO_ID,
    CARGO_01_ID, CARGO_02_ID, CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID
]

# =========================================================
# 1.5 IDs - CATEGORIAS
# =========================================================
CATEGORIA_META_GERENTE_ID = 1337374002422743122
CATEGORIA_META_RESPONSAVEIS_ID = 1462810826992783422
CATEGORIA_META_SOLDADO_ID = 1537807041022664835
CATEGORIA_META_MEMBRO_ID = 1461335697209163900
CATEGORIA_META_AGREGADO_ID = 1461335748870541323

# =========================================================
# 1.6 IDs - CANAIS
# =========================================================
# SISTEMA DE RECEPÇÃO/REGISTRO
CANAL_REGISTRO_ID = 1229556030397218878
CANAL_LOG_REGISTRO_ID = 1462457604939841851
CANAL_BOAS_VINDAS = 1229526645111656562

# SISTEMA DE METAS
CANAL_SOLICITAR_SALA_ID = 1337374500366450741
RESULTADOS_METAS_ID = 1341403574483288125

# SISTEMA DE PRODUÇÃO
CANAL_FABRICACAO_ID = 1466421612566810634
CANAL_REGISTRO_GALPAO_ID = 1356174712337862819
CANAL_BAU_GALPAO_ID = 1448561598384963747
CANAL_BAU_GALPAO_SUL_ID = 1356174937764794521
CANAL_CALCULO_POLVORA_ID = 1462834441968943157
CANAL_REGISTRO_POLVORA_ID = 1448570795101261846

# SISTEMA DE VENDAS
CANAL_CALCULADORA_ID = 1460984821458272347
CANAL_ENCOMENDAS_ID = 1460980984811098294
CANAL_VENDAS_ID = 1460984821458272347
CANAL_TEXTOS_VENDAS_ID = 1499045083994001500

# SISTEMA DE AÇÕES
CANAL_ESCALACOES_ID = 1241406819545514064
CANAL_RELATORIO_ACOES_ID = 1477308788531921019

# SISTEMA DE LAVAGEM
CANAL_INICIAR_LAVAGEM_ID = 1467152989499293768
CANAL_LAVAGEM_MEMBROS_ID = 1467159346923311216
CANAL_RELATORIO_LAVAGEM_ID = 1467150805273546878

# SISTEMA DE LIVES
CANAL_CADASTRO_LIVE_ID = 1466464557215256790
CANAL_DIVULGACAO_LIVE_ID = 1243325102917943335

# SISTEMA DE AUSÊNCIA
CANAL_BOTAO_AUSENCIA_ID = 1491427870277374162
CANAL_REGISTRO_AUSENCIA_ID = 1313854772545196032
CANAL_GERENCIA_ID = 1237393478414241854

# SISTEMA DE GRUPOS
CANAL_GRUPOS_ID = 1448563544386961479

# SISTEMA FINANCEIRO
CANAL_RELATORIO_FINANCEIRO_ID = 1498664038559776768
CANAL_REGISTRAR_COMPRA_ID = 1498668853465448560
CANAL_COMPRAS_REGISTRADAS_ID = 1270467793363669053

# SISTEMA DE LOGS
CANAL_LOGS_GERAIS_ID = 1541438570705977564
CANAL_BAU_MEMBROS_ID = 1337358932158578719
CANAL_BAU_LOG_ID = 1337358898784632882
CANAL_ARMAS_ESTOQUE_ID = 1500983878045798430
CANAL_ARMAS_LOG_ID = 1500983930533187734

# SISTEMA DE AVISOS
CANAL_AVISOS_VIDA_RASA_ID = 1229526645342339075
CANAL_AVISOS_ACOES_ID = 1366528075621339227
CANAL_AVISOS_VENDAS_ID = 1448560922019758241
CANAL_AVISOS_METAS_ID = 1541794867267641404
CANAL_CRIAR_AVISOS_ID = 1541795328972562513

# =========================================================
# 1.7 CORES E ESTILOS
# =========================================================
class Cores:
    META = 0x1a1a2e
    VENDA = 0x0f3460
    PRODUCAO = 0x16213e
    ACAO = 0x533483
    GRUPO = 0x0a3d62
    LIVE = 0x9146FF
    AUSENCIA = 0xe67e22
    FINANCEIRO = 0x1abc9c
    SUCESSO = 0x00d2ff
    ERRO = 0xff4757
    AVISO = 0xffa502
    INFO = 0x2ed573
    DESTAQUE = 0xff6b81
    ROXO = 0x6c5ce7
    DOURADO = 0xf9ca24
    PRATA = 0xb2bec3
    BRANCO = 0xdfe6e9

class Emojis:
    META = "📊"
    VENDA = "🛒"
    PRODUCAO = "🏭"
    ACAO = "⚔️"
    GRUPO = "👥"
    LIVE = "🎥"
    AUSENCIA = "📋"
    FINANCEIRO = "💰"
    SUCESSO = "✅"
    ERRO = "❌"
    AVISO = "⚠️"
    INFO = "ℹ️"
    DESTAQUE = "⭐"
    CONFIG = "⚙️"
    USER = "👤"
    CALENDARIO = "📅"
    RELOGIO = "⏰"
    LOCAL = "📍"
    LINK = "🔗"
    ARQUIVO = "📁"
    ESTATISTICA = "📈"
    TROFEU = "🏆"
    MEDALHA = "🥇"
    FOGO = "🔥"
    CORACAO = "❤️"
    ESCUDO = "🛡️"

# =========================================================
# ==================== PARTE 2: BANCO DE DADOS ============
# =========================================================

# =========================================================
# 2.1 VARIÁVEIS GLOBAIS DO BANCO
# =========================================================
db = None
db_lock = asyncio.Lock()
db_reconnect_attempts = 0
MAX_DB_RECONNECT_ATTEMPTS = 10

# =========================================================
# 2.2 CONEXÃO COM BANCO DE DADOS
# =========================================================
async def conectar_db():
    global db, db_reconnect_attempts
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL não encontrada!")
        return None
    async with db_lock:
        if db and not db._closed:
            try:
                async with db.acquire() as conn:
                    await conn.fetchval("SELECT 1")
                db_reconnect_attempts = 0
                return db
            except:
                pass
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

def get_db():
    global db
    if db and not db._closed:
        return db
    return None

async def get_pool():
    pool = get_db()
    if pool:
        return pool
    logger.warning("⚠️ Pool do banco fechado! Reconectando...")
    return await conectar_db()

# =========================================================
# 2.3 INICIALIZAÇÃO DAS TABELAS
# =========================================================
async def inicializar_tabelas(pool):
    async with pool.acquire() as conn:
        # =========================================================
        # METAS (SEM PÓLVORA)
        # =========================================================
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS metas (
                user_id VARCHAR(30) PRIMARY KEY,
                canal_id VARCHAR(30),
                dinheiro BIGINT DEFAULT 0,
                acao TEXT,
                dinheiro_acoes BIGINT DEFAULT 0,
                saldo_excedente BIGINT DEFAULT 0
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS metas_historico (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                dinheiro BIGINT,
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

        # =========================================================
        # PRODUÇÃO
        # =========================================================
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
            CREATE TABLE IF NOT EXISTS polvoras (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30),
                quantidade INTEGER,
                valor INTEGER,
                data TEXT
            )
        """)
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

        # =========================================================
        # VENDAS
        # =========================================================
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

        # =========================================================
        # AÇÕES
        # =========================================================
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
        # =========================================================
        # PARTICIPANTES MANUAIS DAS AÇÕES
        # =========================================================
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS acoes_participantes_manuais (
                id SERIAL PRIMARY KEY,
                acao_id INTEGER,
                nome TEXT,
                data_criacao TIMESTAMP DEFAULT NOW()
            )
        """)

        # =========================================================
        # GRUPOS
        # =========================================================
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS grupos (
                grupo_id VARCHAR(50) PRIMARY KEY,
                nome_org TEXT,
                lider_nome TEXT,
                lider_telefone TEXT,
                braco_nome TEXT,
                braco_telefone TEXT,
                produto TEXT,
                tipo_org VARCHAR(30) DEFAULT 'PISTA SEM PAINEL',
                observacoes TEXT,
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

        # =========================================================
        # LIVES
        # =========================================================
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

        # =========================================================
        # AUSÊNCIAS
        # =========================================================
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

        # =========================================================
        # LAVAGEM
        # =========================================================
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

        # =========================================================
        # FINANCEIRO
        # =========================================================
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS compras (
                id SERIAL PRIMARY KEY,
                produto TEXT,
                valor INTEGER,
                comprado_por VARCHAR(30),
                data TIMESTAMP DEFAULT NOW()
            )
        """)

        # =========================================================
        # REGISTRO
        # =========================================================
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

        # =========================================================
        # BAÚ
        # =========================================================
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bau_estoque (
                id SERIAL PRIMARY KEY,
                item_nome VARCHAR(100) UNIQUE NOT NULL,
                quantidade INT DEFAULT 0,
                ultima_atualizacao TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS bau_movimentacoes (
                id SERIAL PRIMARY KEY,
                tipo VARCHAR(10) NOT NULL,
                item_nome VARCHAR(100) NOT NULL,
                quantidade INT NOT NULL,
                membro VARCHAR(100),
                observacao TEXT,
                data TIMESTAMP DEFAULT NOW()
            )
        """)

        # =========================================================
        # PAINÉIS
        # =========================================================
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS paineis (
                nome VARCHAR(50) PRIMARY KEY,
                canal_id VARCHAR(30),
                mensagem_id VARCHAR(30),
                data_atualizacao TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS botoes_persistentes (
                id SERIAL PRIMARY KEY,
                mensagem_id VARCHAR(30) NOT NULL,
                canal_id VARCHAR(30) NOT NULL,
                tipo VARCHAR(50) NOT NULL,
                dados JSONB,
                criado_em TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS vdrzinho_memoria (
                id INTEGER PRIMARY KEY DEFAULT 1,
                memoria JSONB,
                data_atualizacao TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # =========================================================
        # SISTEMA XLSPY - SUSPEITOS E VERIFICAÇÕES
        # =========================================================
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS suspeitos (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30) NOT NULL,
                motivo TEXT,
                adicionado_por VARCHAR(30),
                data_adicao TIMESTAMP DEFAULT NOW(),
                ativo BOOLEAN DEFAULT true
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS verificacoes (
                id SERIAL PRIMARY KEY,
                user_id VARCHAR(30) NOT NULL,
                verificador VARCHAR(30),
                resultado VARCHAR(20),
                data_verificacao TIMESTAMP DEFAULT NOW()
            )
        """)

    logger.info("✅ Todas as tabelas criadas/verificadas com sucesso!")

# =========================================================
# ==================== PARTE 3: UTILITÁRIOS ===============
# =========================================================

# =========================================================
# 3.1 FUNÇÕES DE DATA E HORA
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

# =========================================================
# 3.2 FUNÇÕES DE FORMATAÇÃO
# =========================================================
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
    if not valor or str(valor).strip() == "":
        return default
    if isinstance(valor, int):
        return valor
    try:
        if isinstance(valor, str):
            valor = valor.replace(".", "").replace(",", "")
        return int(valor)
    except (ValueError, TypeError):
        return default

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

# =========================================================
# 3.3 FUNÇÕES DE PLATAFORMA
# =========================================================
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

# =========================================================
# 3.4 FUNÇÕES DE PERMISSÃO
# =========================================================
def pode_remover_ausencia(member):
    if not member:
        return False
    return any(role.id in CARGOS_PERMITIDOS_REMOVER for role in member.roles)

def pode_gerenciar_lavagem(member):
    cargos_permitidos = [CARGO_GERENTE_ID, CARGO_01_ID, CARGO_02_ID, CARGO_GERENTE_GERAL_ID]
    return any(role.id in cargos_permitidos for role in member.roles)

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

# =========================================================
# 3.5 FUNÇÕES DE SEGURANÇA
# =========================================================
async def safe_request(func, *args, max_retries=3, **kwargs):
    for attempt in range(max_retries):
        try:
            return await func(*args, **kwargs)
        except discord.HTTPException as e:
            if e.status == 429:
                retry_after = 5
                if hasattr(e, 'response') and e.response:
                    retry_after = e.response.headers.get('Retry-After', 5)
                    try:
                        retry_after = float(retry_after)
                    except:
                        retry_after = 5
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

async def pegar_usuario(uid):
    if uid in user_cache:
        return user_cache[uid]
    try:
        user = await bot.fetch_user(uid)
        user_cache[uid] = user
        return user
    except:
        return None

async def pegar_apelido(user_id, guild=None):
    try:
        if guild:
            member = guild.get_member(int(user_id))
            if member:
                return member.display_name
        user = await bot.fetch_user(int(user_id))
        if user:
            return user.display_name or user.name
        return str(user_id)
    except:
        return str(user_id)

# =========================================================
# 3.6 FUNÇÕES DE CACHE
# =========================================================
class CacheManager:
    def __init__(self, default_ttl=300, max_size=100):
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

cache = CacheManager(default_ttl=300, max_size=100)

# =========================================================
# 3.7 VARIÁVEIS GLOBAIS
# =========================================================
http_session = None
user_cache = {}
edit_queue = asyncio.Queue()
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
bau_print_pendente = {}
armas_print_pendente = {}

# =========================================================
# ==================== PARTE 4: RECEPÇÃO/REGISTRO =========
# =========================================================

# =========================================================
# 4.1 FUNÇÕES DE REGISTRO
# =========================================================
async def salvar_registro_historico(user_id, user_name, passaporte, nome, vulgo, telefone, indicado, tipo):
    pool = await get_pool()
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
            """, str(user_id), user_name, passaporte, nome, vulgo, telefone, indicado, tipo, agora_db())
    except Exception as e:
        logger.error(f"❌ Erro ao salvar registro histórico: {e}")

async def verificar_registro_existente(user_id):
    pool = await get_pool()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            return await conn.fetchval("SELECT 1 FROM registros_historico WHERE user_id = $1", str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao verificar registro: {e}")
        return False

# =========================================================
# 4.2 MODAL DE REGISTRO
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
            view=view, ephemeral=True
        )

# =========================================================
# 4.3 SELECT DE TIPO DE REGISTRO
# =========================================================
class TipoRegistroSelect(discord.ui.Select):
    def __init__(self, nome, passaporte, vulgo, telefone, indicado):
        self.nome = nome
        self.passaporte = passaporte
        self.vulgo = vulgo
        self.telefone = telefone
        self.indicado = indicado
        options = [
            discord.SelectOption(label="Agregado", description="Se tornar membro da facção", emoji="🕴️")
        ]
        super().__init__(placeholder="Confirme seu registro", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        membro = interaction.user
        agregado = guild.get_role(AGREGADO_ROLE_ID)
        convidado = guild.get_role(CONVIDADO_ROLE_ID)
        em_registro = guild.get_role(EM_REGISTRO_ROLE_ID)
        
        if em_registro:
            try:
                await membro.remove_roles(em_registro)
            except Exception as e:
                logger.error(f"❌ Erro ao remover cargo 'Em Registro': {e}")
        
        # Adicionar cargo de Agregado
        if agregado:
            try:
                await membro.add_roles(agregado)
            except Exception as e:
                logger.error(f"❌ Erro ao adicionar cargo 'Agregado': {e}")
        
        if convidado:
            try:
                await membro.remove_roles(convidado)
            except:
                pass
        
        # Salvar no histórico
        await salvar_registro_historico(
            membro.id, membro.name, self.passaporte, self.nome,
            self.vulgo, self.telefone, self.indicado, "Agregado"
        )
        
        # =========================================================
        # CRIAR SALA DE META AUTOMATICAMENTE
        # =========================================================
        await criar_sala_meta(membro)
        
        # =========================================================
        # ENVIAR CONFIRMAÇÃO
        # =========================================================
        canal_log = interaction.guild.get_channel(CANAL_LOG_REGISTRO_ID)
        if canal_log:
            embed = discord.Embed(
                title="🎉 NOVO MEMBRO REGISTRADO!",
                description=f"**{membro.mention}** acabou de se registrar na **Vida Rasa**!",
                color=0x2ecc71, timestamp=agora()
            )
            if membro.display_avatar:
                embed.set_thumbnail(url=membro.display_avatar.url)
            informacoes = (
                f"**📋 Passaporte:** `{self.passaporte}`\n"
                f"**👤 Nome:** {self.nome}\n"
                f"**🏷️ Vulgo:** {self.vulgo or '❌ Não informado'}\n"
                f"**📱 Telefone:** {self.telefone}\n"
                f"**👤 Indicado por:** {self.indicado or '❌ Não informado'}\n"
                f"**🎯 Tipo:** Agregado"
            )
            embed.add_field(name="📋 INFORMAÇÕES DO MEMBRO", value=informacoes, inline=False)
            embed.add_field(name="📌 STATUS", value=f"✅ **Registro concluído**\n🔹 Cargo atribuído: **Agregado**\n🆔 ID: `{membro.id}`", inline=False)
            embed.add_field(name="🎯 CARGO ATRIBUÍDO", value="🕴️ **Agregado**", inline=False)
            embed.set_footer(text=f"Registro realizado com sucesso • Sistema Automático")
            try:
                await canal_log.send(embed=embed)
                await interaction.response.send_message(
                    f"✅ **Registro concluído com sucesso!**\n\n"
                    f"📋 Você foi registrado como: **Agregado**\n"
                    f"👤 Nome: {self.nome}\n"
                    f"📁 Sua sala de meta foi criada automaticamente!\n"
                    f"📨 Seu registro foi enviado para o histórico!",
                    ephemeral=True
                )
            except:
                await interaction.response.send_message(
                    f"✅ **Registro concluído com sucesso!**\n\n"
                    f"📋 Você foi registrado como: **Agregado**\n"
                    f"👤 Nome: {self.nome}\n"
                    f"📁 Sua sala de meta foi criada automaticamente!\n"
                    f"⚠️ **Mas houve um erro ao enviar para o histórico!**",
                    ephemeral=True
                )

# =========================================================
# 4.4 VIEWS DE REGISTRO
# =========================================================
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

# =========================================================
# 4.5 FUNÇÃO DE ENVIAR PAINEL DE REGISTRO
# =========================================================
async def enviar_painel_registro():
    canal = bot.get_channel(CANAL_REGISTRO_ID)
    if not canal:
        logger.error("❌ Canal registro não encontrado")
        return

    embed = discord.Embed(
        title="📋 ── REGISTRO VDR 442 ── 📋",
        description="🛡 Sistema de Admissão • Vida Rasa",
        color=0x1a1a2e,
        timestamp=agora()
    )
    embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
    embed.set_author(
        name="🛡 Vida Rasa 442 • Sistema de Registro",
        icon_url=bot.user.display_avatar.url if bot.user else None
    )
    embed.add_field(
        name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        value="",
        inline=False
    )
    embed.add_field(
        name="📌 ESCOLHA O TIPO DE ENTRADA",
        value=(
            "```yaml\n"
            "🕴️ AGREGADO  →  Para quem quer ser MEMBRO da facção\n"
            "              →  Terá acesso a todas as áreas\n"
            "              →  Participará de metas e ações\n"
            "              →  Obrigatório cumprir as regras\n"
            "\n"
            "🤝 AMIGO     →  Para fãs, visitantes ou convidados\n"
            "              →  Acesso apenas à áreas sociais\n"
            "              →  Não participa de metas/ ações\n"
            "              →  Apenas para resenha e conversa\n"
            "```"
        ),
        inline=False
    )
    embed.add_field(
        name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        value="",
        inline=False
    )
    embed.add_field(
        name="⚠️ ATENÇÃO",
        value=(
            "🔴 **Agregado** - Você será um membro oficial da **Vida Rasa 442**\n"
            "   • Terá obrigações como metas semanais\n"
            "   • Participará de ações e produções\n"
            "   • Respeitará as regras da facção\n\n"
            "🟢 **Amigo** - Você é bem-vindo para resenhar\n"
            "   • Apenas canais sociais liberados\n"
            "   • Sem obrigações de metas\n"
            "   • Pode ser promovido a Agregado depois"
        ),
        inline=False
    )
    embed.add_field(
        name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        value="",
        inline=False
    )
    embed.add_field(
        name="📋 COMO FUNCIONA",
        value=(
            "1️⃣ Clique em **'Fazer Registro'**\n"
            "2️⃣ Preencha seus dados (Passaporte, Nome, etc)\n"
            "3️⃣ Escolha entre **Agregado** ou **Amigo**\n"
            "4️⃣ Pronto! Você será liberado automaticamente"
        ),
        inline=False
    )
    embed.set_footer(
        text="🛡 Vida Rasa 442 • Sistema Automático de Registro",
        icon_url=bot.user.display_avatar.url if bot.user else None
    )

    view = RegistroView()
    await enviar_ou_atualizar_painel("painel_registro", CANAL_REGISTRO_ID, embed, view)
    logger.info("✅ Painel de registro criado")

# =========================================================
# ==================== PARTE 5: SISTEMA DE AVISOS =========
# =========================================================

# =========================================================
# 5.1 FUNÇÃO PARA CRIAR EMBED DE AVISO
# =========================================================
async def criar_embed_aviso_supremo(titulo, mensagem, cor_hex, canal_nome, tipo_aviso="📢"):
    tipos = {
        "urgente": {"cor": 0xe74c3c, "emoji": "🔴", "borda": "🔥"},
        "importante": {"cor": 0xf1c40f, "emoji": "⭐", "borda": "✨"},
        "informativo": {"cor": 0x3498db, "emoji": "ℹ️", "borda": "📌"},
        "sucesso": {"cor": 0x2ecc71, "emoji": "✅", "borda": "🎉"},
        "aviso": {"cor": 0xe67e22, "emoji": "⚠️", "borda": "📢"}
    }
    tipo_detectado = "informativo"
    titulo_lower = titulo.lower()
    if any(p in titulo_lower for p in ["urgente", "importante", "atenção", "perigo"]):
        tipo_detectado = "urgente"
    elif any(p in titulo_lower for p in ["sucesso", "concluído", "finalizado", "parabéns"]):
        tipo_detectado = "sucesso"
    elif any(p in titulo_lower for p in ["aviso", "atenção", "cuidado"]):
        tipo_detectado = "aviso"
    elif any(p in titulo_lower for p in ["info", "informação", "comunicado"]):
        tipo_detectado = "informativo"
    info = tipos.get(tipo_detectado, tipos["informativo"])
    cor_final = cor_hex if cor_hex else info["cor"]
    emoji_tipo = info["emoji"]
    borda = info["borda"]
    embed = discord.Embed(
        title=f"{emoji_tipo} {borda} {titulo.upper()} {borda} {emoji_tipo}",
        description=f"```yaml\n{mensagem}\n```",
        color=cor_final,
        timestamp=agora()
    )
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(name="📌 DESTINO", value=f"```yaml\n{canal_nome}\n```", inline=True)
    embed.add_field(name="📅 DATA E HORA", value=f"```yaml\n{agora().strftime('%d/%m/%Y %H:%M')}\n```", inline=True)
    embed.add_field(name="📋 STATUS", value=f"```yaml\n{('🔴 URGENTE' if tipo_detectado == 'urgente' else '📢 ATIVO')}\n```", inline=True)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.set_footer(text="🛡 Vida Rasa 442 • Sistema Oficial de Avisos", icon_url=bot.user.display_avatar.url if bot.user else None)
    embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
    return embed

# =========================================================
# 5.2 SELECT DE AVISOS
# =========================================================
class AvisosSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label="📢 Vida Rasa", description="Aviso geral para toda a facção", emoji="📢", value="vida_rasa"),
            discord.SelectOption(label="⚔️ Ações", description="Aviso sobre ações e escalações", emoji="⚔️", value="acoes"),
            discord.SelectOption(label="🛒 Vendas", description="Aviso sobre vendas e encomendas", emoji="🛒", value="vendas"),
            discord.SelectOption(label="🎯 Metas", description="Aviso sobre metas semanais", emoji="🎯", value="metas")
        ]
        super().__init__(placeholder="📌 Selecione o canal para o aviso...", min_values=1, max_values=1, options=options)

    async def callback(self, interaction: discord.Interaction):
        valor = self.values[0]
        canais = {
            "vida_rasa": CANAL_AVISOS_VIDA_RASA_ID,
            "acoes": CANAL_AVISOS_ACOES_ID,
            "vendas": CANAL_AVISOS_VENDAS_ID,
            "metas": CANAL_AVISOS_METAS_ID
        }
        nomes = {
            "vida_rasa": "📢 Vida Rasa",
            "acoes": "⚔️ Ações",
            "vendas": "🛒 Vendas",
            "metas": "🎯 Metas"
        }
        canal_id = canais.get(valor)
        nome_canal = nomes.get(valor, "Desconhecido")
        if not canal_id:
            await interaction.response.send_message("❌ Canal inválido!", ephemeral=True)
            return
        modal = AvisoModal(canal_id, nome_canal)
        await interaction.response.send_modal(modal)

# =========================================================
# 5.3 MODAL DE AVISO
# =========================================================
class AvisoModal(discord.ui.Modal, title="📢 Criar Aviso Supremo"):
    def __init__(self, canal_id, nome_canal):
        super().__init__(timeout=300)
        self.canal_id = canal_id
        self.nome_canal = nome_canal

    titulo = discord.ui.TextInput(label="📌 Título do Aviso", placeholder="Ex: ATENÇÃO REUNIÃO!", required=True, max_length=100)
    mensagem = discord.ui.TextInput(label="📝 Mensagem do Aviso", placeholder="Digite o conteúdo do aviso...", style=discord.TextStyle.paragraph, required=True, max_length=2000)
    cor = discord.ui.TextInput(label="🎨 Cor (opcional)", placeholder="verde, vermelho, amarelo, azul, roxo, laranja", required=False, max_length=20)
    tipo = discord.ui.TextInput(label="🏷️ Tipo (opcional)", placeholder="urgente, importante, aviso, sucesso, informativo", required=False, max_length=20)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            cores = {"verde": 0x2ecc71, "vermelho": 0xe74c3c, "amarelo": 0xf1c40f, "azul": 0x3498db, "roxo": 0x9b59b6, "laranja": 0xe67e22}
            cor_hex = cores.get(self.cor.value.lower().strip(), None)
            embed = await criar_embed_aviso_supremo(
                titulo=self.titulo.value,
                mensagem=self.mensagem.value,
                cor_hex=cor_hex,
                canal_nome=self.nome_canal
            )
            canal = interaction.guild.get_channel(self.canal_id)
            if canal:
                await canal.send(
                    content="🔔 @everyone **NOVO AVISO OFICIAL!**",
                    embed=embed,
                    allowed_mentions=discord.AllowedMentions(everyone=True)
                )
                canal_log = interaction.guild.get_channel(CANAL_CRIAR_AVISOS_ID)
                if canal_log:
                    embed_log = discord.Embed(
                        title="✅ AVISO CRIADO (ANÔNIMO)",
                        description=f"📢 **{self.titulo.value}**",
                        color=0x2ecc71,
                        timestamp=agora()
                    )
                    embed_log.add_field(name="👤 Enviado por (LOG)", value=interaction.user.mention, inline=True)
                    embed_log.add_field(name="📌 Canal", value=self.nome_canal, inline=True)
                    embed_log.add_field(name="📝 Conteúdo", value=self.mensagem.value[:500], inline=False)
                    embed_log.set_footer(text="🛡 Vida Rasa 442 • Log de Avisos")
                    await canal_log.send(embed=embed_log)
                await interaction.followup.send(f"✅ **Aviso supremo enviado com sucesso para {self.nome_canal}!**", ephemeral=True)
            else:
                await interaction.followup.send("❌ **Canal não encontrado!**", ephemeral=True)
        except Exception as e:
            logger.error(f"❌ Erro ao enviar aviso: {e}")
            await interaction.followup.send(f"❌ **Erro ao enviar aviso:** {str(e)[:100]}", ephemeral=True)

# =========================================================
# 5.4 VIEW DE AVISOS
# =========================================================
class AvisosView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(AvisosSelect())

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        tem_permissao = (
            interaction.user.guild_permissions.administrator or
            any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID, CARGO_01_ID, CARGO_02_ID] for r in interaction.user.roles)
        )
        if not tem_permissao:
            await interaction.response.send_message(
                "❌ **Apenas Gerentes, ADM, Cargo 01 e Cargo 02 podem criar avisos!**",
                ephemeral=True
            )
            return False
        return True

# =========================================================
# 5.5 FUNÇÃO DE ENVIAR PAINEL DE AVISOS
# =========================================================
async def enviar_painel_avisos():
    canal = bot.get_channel(CANAL_CRIAR_AVISOS_ID)
    if not canal:
        logger.error(f"❌ Canal de criação de avisos não encontrado! ID: {CANAL_CRIAR_AVISOS_ID}")
        return
    embed = discord.Embed(
        title="🌟 ── SISTEMA DE AVISOS SUPREMO ── 🌟",
        description="🔔 Crie avisos elegantes e profissionais para a facção",
        color=0x1a1a2e,
        timestamp=agora()
    )
    embed.set_author(name="🛡 Vida Rasa 442 • Sistema de Avisos", icon_url=bot.user.display_avatar.url if bot.user else None)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="📋 COMO USAR",
        value=(
            "```yaml\n"
            "1️⃣ Selecione o canal no menu suspenso\n"
            "2️⃣ Preencha o título do aviso\n"
            "3️⃣ Digite o conteúdo\n"
            "4️⃣ Escolha uma cor (opcional)\n"
            "5️⃣ Escolha o tipo (opcional)\n"
            "6️⃣ Clique em Enviar\n"
            "\n"
            "🎨 CORES: verde, vermelho, amarelo, azul, roxo, laranja\n"
            "🏷️ TIPOS: urgente, importante, aviso, sucesso, informativo\n"
            "🔒 ANÔNIMO: Ninguém saberá quem enviou!\n"
            "⚠️ APENAS: Gerentes, ADM, Cargo 01, Cargo 02\n"
            "```"
        ),
        inline=False
    )
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="📌 CANAIS DISPONÍVEIS",
        value=(
            "📢 **Vida Rasa** - Avisos gerais\n"
            "⚔️ **Ações** - Avisos de ações\n"
            "🛒 **Vendas** - Avisos de vendas\n"
            "🎯 **Metas** - Avisos de metas"
        ),
        inline=False
    )
    embed.set_footer(text="🛡 Vida Rasa 442 • Sistema de Avisos", icon_url=bot.user.display_avatar.url if bot.user else None)
    view = AvisosView()
    await enviar_ou_atualizar_painel("painel_avisos", CANAL_CRIAR_AVISOS_ID, embed, view)

# =========================================================
# ==================== PARTE 6: SISTEMA FINANCEIRO ========
# =========================================================

# =========================================================
# 6.1 FUNÇÕES DE BANCO DE DADOS - FINANCEIRO
# =========================================================
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

# =========================================================
# 6.2 MODAL DE REGISTRAR COMPRA
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
            await interaction.followup.send(
                f"✅ **Compra registrada com sucesso!**\n"
                f"📦 Produto: {produto}\n"
                f"💰 Valor: {formatar_dinheiro(valor_compra)}",
                ephemeral=True
            )
        else:
            await interaction.followup.send(
                f"✅ **Compra registrada com sucesso!**\n"
                f"📦 Produto: {produto}\n"
                f"💰 Valor: {formatar_dinheiro(valor_compra)}",
                ephemeral=True
            )

# =========================================================
# 6.3 VIEW DE REGISTRAR COMPRA
# =========================================================
class RegistrarCompraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📝 Registrar Nova Compra", style=discord.ButtonStyle.success, custom_id="registrar_compra_btn", emoji="💰")
    async def registrar_compra(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarCompraModal())

# =========================================================
# 6.4 MODAL DE RELATÓRIO FINANCEIRO
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
                polvora_row = await conn.fetchrow(
                    "SELECT COALESCE(SUM(polvora), 0) as total_polvora FROM producoes_finalizadas WHERE data >= $1 AND data <= $2",
                    inicio_dt, fim_dt
                )
                vendas_row = await conn.fetchrow(
                    "SELECT COALESCE(SUM(valor), 0) as total_vendas FROM vendas WHERE TO_DATE(data, 'DD/MM/YYYY') BETWEEN $1 AND $2",
                    inicio.date(), fim.date()
                )
                polvora_comprada_row = await conn.fetchrow(
                    "SELECT COALESCE(SUM(quantidade), 0) as total_quantidade, COALESCE(SUM(valor), 0) as total_valor FROM polvoras WHERE data::date BETWEEN $1::date AND $2::date",
                    inicio, fim
                )
                compras_row = None
                total_gasto_compras = 0
                lista_compras = []
                if incluir_compras:
                    compras_row = await conn.fetch(
                        "SELECT produto, valor, comprado_por, data FROM compras WHERE data >= $1 AND data <= $2 ORDER BY data DESC",
                        inicio_dt, fim_dt
                    )
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
            embed = discord.Embed(
                title="📊 RELATÓRIO FINANCEIRO",
                description=f"📅 **Período:** {self.data_inicio.value} até {self.data_fim.value}",
                color=0x1abc9c
            )
            embed.add_field(
                name="💣 PÓLVORA",
                value=(
                    f"**Utilizada na produção:** {fmt_num(total_polvora_gasta)} unidades\n"
                    f"**💰 Gasto com pólvora:** {formatar_dinheiro(total_gasto_polvora)}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"**Comprada no período:** {fmt_num(total_polvora_comprada)} unidades\n"
                    f"**💰 Gasto na compra:** {formatar_dinheiro(total_gasto_polvora_comprada)}"
                ),
                inline=False
            )
            if total_embalagens > 0:
                embed.add_field(
                    name="📦 EMBALAGENS",
                    value=(
                        f"**Quantidade comprada:** {fmt_num(total_embalagens)} unidades\n"
                        f"**💰 Gasto com embalagens:** {formatar_dinheiro(total_gasto_embalagens)}"
                    ),
                    inline=False
                )
            if incluir_compras and lista_compras:
                compras_texto = ""
                for compra in lista_compras[:10]:
                    data = compra["data"]
                    if data.tzinfo is None:
                        data = data.replace(tzinfo=BRASIL)
                    compras_texto += f"• {compra['produto']} - {formatar_dinheiro(compra['valor'])} - {data.strftime('%d/%m')}\n"
                if len(lista_compras) > 10:
                    compras_texto += f"\n*... e mais {len(lista_compras) - 10} compras*"
                embed.add_field(
                    name="📦 OUTRAS COMPRAS",
                    value=(
                        f"**Total gasto em outras compras:** {formatar_dinheiro(total_gasto_compras)}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"{compras_texto}"
                    ),
                    inline=False
                )
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
            embed.add_field(
                name="📊 RESUMO FINANCEIRO",
                value=(
                    f"**💰 Total de Vendas:** {formatar_dinheiro(total_vendas)}\n"
                    f"**💸 Total de Gastos:** {formatar_dinheiro(total_gastos)}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"{emoji_saldo} **SALDO:** {formatar_dinheiro(saldo)}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"**📋 DETALHAMENTO DOS GASTOS:**\n"
                    f"{detalhe_gastos}"
                ),
                inline=False
            )
            embed.set_footer(text=f"Relatório gerado em {agora().strftime('%d/%m/%Y às %H:%M')}")
            canal = interaction.guild.get_channel(CANAL_RELATORIO_FINANCEIRO_ID)
            if canal:
                await canal.send(embed=embed)
                await interaction.followup.send(f"✅ Relatório financeiro enviado!", ephemeral=True)
            else:
                await interaction.followup.send(embed=embed, ephemeral=True)
        except ValueError:
            await interaction.followup.send("❌ **Formato de data inválido!** Use DD/MM/AAAA", ephemeral=True)
        except Exception as e:
            logger.error(f"ERRO RELATORIO FINANCEIRO: {e}")
            await interaction.followup.send(f"❌ Erro ao gerar relatório: {str(e)}", ephemeral=True)

# =========================================================
# 6.5 VIEW DE RELATÓRIO FINANCEIRO
# =========================================================
class RelatorioFinanceiroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📊 Gerar Relatório Financeiro", style=discord.ButtonStyle.success, custom_id="relatorio_financeiro_btn", emoji="💰")
    async def gerar_relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RelatorioFinanceiroModal())

# =========================================================
# 6.6 FUNÇÕES DE ENVIAR PAINÉIS FINANCEIROS
# =========================================================
async def enviar_painel_registrar_compra():
    canal = bot.get_channel(CANAL_REGISTRAR_COMPRA_ID)
    if not canal:
        logger.error(f"❌ Canal de registrar compra não encontrado: {CANAL_REGISTRAR_COMPRA_ID}")
        return
    embed = discord.Embed(
        title="💰 REGISTRAR COMPRA",
        description=(
            "Clique no botão abaixo para registrar uma nova compra.\n\n"
            "📋 **Informações necessárias:**\n"
            "• 📦 Nome do produto\n"
            "• 💰 Valor da compra\n\n"
            "Após registrar, a compra aparecerá automaticamente no canal de registros."
        ),
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

async def enviar_painel_relatorio_financeiro():
    canal = bot.get_channel(CANAL_RELATORIO_FINANCEIRO_ID)
    if not canal:
        logger.error("❌ Canal de relatório financeiro não encontrado")
        return
    embed = discord.Embed(
        title="💰 RELATÓRIO FINANCEIRO",
        description=(
            "Clique no botão abaixo para gerar um relatório financeiro completo.\n\n"
            "📋 **O relatório inclui:**\n"
            "• 💣 Pólvora utilizada na produção\n"
            "• 💰 Gasto total com pólvora\n"
            "• 🛒 Total de vendas no período\n"
            "• 📦 Gasto com embalagens (opcional)\n"
            "• 📦 Outras compras registradas\n"
            "• 📊 Saldo final (vendas - gastos)\n\n"
            "📅 **Você pode escolher:**\n"
            "• Data inicial e final\n"
            "• Incluir ou não outras compras (SIM/NAO)"
        ),
        color=0x1abc9c
    )
    embed.add_field(
        name="📌 EXEMPLO DE PREENCHIMENTO",
        value="**Data inicial:** `01/04/2026`\n**Data final:** `30/04/2026`\n**Incluir compras:** `SIM` (ou `NAO`)",
        inline=False
    )
    embed.set_footer(text="Os valores são calculados automaticamente com base no banco de dados")
    await enviar_ou_atualizar_painel("painel_relatorio_financeiro", CANAL_RELATORIO_FINANCEIRO_ID, embed, RelatorioFinanceiroView())

# =========================================================
# ==================== PARTE 7: SISTEMA DE AUSÊNCIA =======
# =========================================================

# =========================================================
# 7.1 FUNÇÕES DE BANCO DE DADOS - AUSÊNCIA
# =========================================================
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

async def desativar_ausencia(user_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE ausencias SET ativo = false WHERE user_id = $1 AND ativo = true", str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao desativar ausência: {e}")

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

# =========================================================
# 7.2 MODAL DE AUSÊNCIA
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
                embed_alerta = discord.Embed(
                    title="⚠️ AUSÊNCIA PROLONGADA",
                    description=f"{interaction.user.mention} solicitou ausência de **{dias_ausencia} dias**!",
                    color=0xe74c3c
                )
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
            # =========================================================
            # VDRZINHO - Ausência registrada
            # =========================================================
            await canal_registro.send(embed=vdrzinho.embed_resposta(
                tipo="ausencia_registrada",
                interaction=interaction,
                nome=self.nome.value
            ))
            
            embed_ausencia = discord.Embed(
                title="📋 ── AUSÊNCIA REGISTRADA ── 📋",
                description=f"👤 {interaction.user.mention} está ausente!",
                color=0xe67e22,
                timestamp=agora()
            )
            embed_ausencia.set_thumbnail(url=interaction.user.display_avatar.url if interaction.user.display_avatar else None)
            embed_ausencia.set_author(name="🛡 Vida Rasa 442 • Sistema de Ausência", icon_url=bot.user.display_avatar.url if bot.user else None)
            embed_ausencia.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed_ausencia.add_field(name="👤 NOME", value=f"```yaml\n{self.nome.value}\n```", inline=True)
            embed_ausencia.add_field(name="⏳ TOTAL DE DIAS", value=f"```yaml\n{dias_ausencia} dia(s)\n```", inline=True)
            embed_ausencia.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed_ausencia.add_field(name="📅 PERÍODO", value=f"```yaml\n{self.data_inicio.value} a {self.data_fim.value}\n```", inline=False)
            embed_ausencia.add_field(name="📝 MOTIVO", value=f"```yaml\n{self.motivo.value}\n```", inline=False)
            if dias_ausencia >= 15:
                embed_ausencia.add_field(name="⚠️ ATENÇÃO", value="🔴 **Ausência prolongada!** Gerência notificada.", inline=False)
            embed_ausencia.set_footer(text=f"🛡 Vida Rasa 442 • Solicitado em {agora().strftime('%d/%m/%Y às %H:%M')}", icon_url=bot.user.display_avatar.url if bot.user else None)
            await canal_registro.send(embed=embed_ausencia)

# =========================================================
# 7.3 SELECT DE REMOVER AUSÊNCIA
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
        embed = discord.Embed(
            title="🔄 ── RETORNO REGISTRADO ── 🔄",
            description=f"👤 {member.mention if member else f'<@{user_id}>'} retornou!",
            color=0x2ecc71,
            timestamp=agora()
        )
        embed.set_thumbnail(url=member.display_avatar.url if member and member.display_avatar else None)
        embed.set_author(name="🛡 Vida Rasa 442 • Sistema de Ausência", icon_url=bot.user.display_avatar.url if bot.user else None)
        embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
        embed.add_field(name="👤 USUÁRIO", value=f"```yaml\n{member.display_name if member else f'ID: {user_id}'}\n```", inline=True)
        if dias_antecipados > 0:
            embed.add_field(name="📅 DIAS ANTECIPADOS", value=f"```yaml\n{dias_antecipados} dia(s) antes do previsto\n```", inline=True)
        embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
        embed.add_field(name="📌 STATUS", value="✅ **Cargo ausente removido.**\n🔄 Usuário pode solicitar nova ausência.", inline=False)
        embed.set_footer(text=f"🛡 Vida Rasa 442 • Retorno registrado em {agora().strftime('%d/%m/%Y %H:%M')}", icon_url=bot.user.display_avatar.url if bot.user else None)
        await interaction.response.edit_message(content=None, embed=embed, view=None)

# =========================================================
# 7.4 VIEWS DE AUSÊNCIA
# =========================================================
class RemoverAusenciaView(discord.ui.View):
    def __init__(self, ausencias):
        super().__init__(timeout=60)
        self.add_item(RemoverAusenciaSelect(ausencias))

class AusenciaUnificadoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📝 Solicitar Ausência", style=discord.ButtonStyle.primary, custom_id="ausencia_solicitar", emoji="📝")
    async def solicitar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AusenciaModal())

    @discord.ui.button(label="🔄 Remover Ausência", style=discord.ButtonStyle.primary, custom_id="ausencia_remover", emoji="🔄")
    async def remover(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pode_remover_ausencia(interaction.user):
            await interaction.response.send_message(
                "❌ Você não tem permissão para remover ausências!\n"
                "Apenas **Gerente, Cargo 01, Cargo 02 e Gerente Geral** podem usar este recurso.",
                ephemeral=True
            )
            return
        ausencias = await buscar_ausencias_ativas_db()
        if not ausencias:
            await interaction.response.send_message("📭 Nenhuma ausência ativa no momento.", ephemeral=True)
            return
        view = RemoverAusenciaView(ausencias)
        await interaction.response.send_message(
            "📋 Selecione o membro que **retornou antes do previsto**:\n"
            "O cargo ausente será removido imediatamente.",
            view=view,
            ephemeral=True
        )

# =========================================================
# 7.5 FUNÇÃO DE ENVIAR PAINEL DE AUSÊNCIA
# =========================================================
async def enviar_painel_ausencia():
    canal = bot.get_channel(CANAL_BOTAO_AUSENCIA_ID)
    if not canal:
        logger.error(f"❌ Canal do botão NÃO ENCONTRADO! ID: {CANAL_BOTAO_AUSENCIA_ID}")
        return
    embed = discord.Embed(
        title="📋 ── SISTEMA DE AUSÊNCIA ── 📋",
        description="🛡 VDR 442 • Gerenciamento de Ausências",
        color=0xe67e22,
        timestamp=agora()
    )
    embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
    embed.set_author(name="🛡 Vida Rasa 442 • Sistema de Ausência", icon_url=bot.user.display_avatar.url if bot.user else None)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="📝 SOLICITAR AUSÊNCIA",
        value=(
            "```yaml\n"
            "📌 Como usar:\n"
            "1️⃣ Digite seu nome completo\n"
            "2️⃣ Data de INÍCIO (ex: 10/04/2026)\n"
            "3️⃣ Data de RETORNO (ex: 15/04/2026)\n"
            "4️⃣ Digite o motivo\n"
            "\n"
            "✅ Você receberá o cargo 'Ausente'\n"
            "✅ Quando o período acabar, o cargo será removido\n"
            "```"
        ),
        inline=False
    )
    embed.add_field(
        name="⚠️ AUSÊNCIAS PROLONGADAS",
        value="🔴 **Ausências de 15 dias ou mais**\n   • Serão notificadas à gerência\n   • O membro deve ser removido do tablet",
        inline=False
    )
    embed.add_field(
        name="📅 EXEMPLO",
        value="```yaml\n📌 Data INÍCIO: 10/04/2026\n📌 Data RETORNO: 15/04/2026\n(contando todos os dias entre 10 e 15)\n```",
        inline=False
    )
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="🔄 REMOVER AUSÊNCIA (RETORNO ANTECIPADO)",
        value=(
            "```yaml\n"
            "📌 Clique no botão abaixo caso um membro tenha\n"
            "   retornado antes do previsto.\n"
            "\n"
            "⚠️ APENAS PARA:\n"
            "   • Gerente\n"
            "   • Cargo 01\n"
            "   • Cargo 02\n"
            "   • Gerente Geral\n"
            "\n"
            "📋 Como usar:\n"
            "1️⃣ Clique no botão\n"
            "2️⃣ Selecione o membro na lista\n"
            "3️⃣ Confirme a remoção\n"
            "\n"
            "✅ O cargo 'Ausente' será removido imediatamente\n"
            "```"
        ),
        inline=False
    )
    embed.set_footer(text="🛡 Vida Rasa 442 • Sistema de Ausência", icon_url=bot.user.display_avatar.url if bot.user else None)
    view = AusenciaUnificadoView()
    await enviar_ou_atualizar_painel("painel_ausencia", CANAL_BOTAO_AUSENCIA_ID, embed, view)
    logger.info("✅ Painel de ausência unificado criado")

# =========================================================
# ==================== PARTE 8: SISTEMA DE LAVAGEM ========
# =========================================================

# =========================================================
# 8.1 FUNÇÕES DE BANCO DE DADOS - LAVAGEM
# =========================================================
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

async def limpar_lavagens_db():
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM lavagens")
    except Exception as e:
        logger.error(f"❌ Erro ao limpar lavagens: {e}")

# =========================================================
# 8.2 MODAL DE LAVAGEM
# =========================================================
class LavagemModal(discord.ui.Modal, title="🧼 Iniciar Lavagem"):
    valor = discord.ui.TextInput(label="💰 Valor do dinheiro sujo", placeholder="Ex: 100000", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            valor_sujo = safe_int(self.valor.value)
            if valor_sujo <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ **Valor inválido!** Digite um número positivo.", ephemeral=True)
            return
        taxa = 20
        valor_retorno = int(valor_sujo * 0.8)
        embed_confirmacao = discord.Embed(
            title="🧼 ── LAVAGEM INICIADA ── 🧼",
            description="💰 Sistema Financeiro • VDR 442",
            color=0xf1c40f,
            timestamp=agora()
        )
        embed_confirmacao.add_field(
            name="📋 INFORMAÇÕES",
            value=(
                f"```yaml\n"
                f"💰 Valor sujo: {formatar_dinheiro(valor_sujo)}\n"
                f"📊 Taxa: {taxa}%\n"
                f"💵 Valor a repassar: {formatar_dinheiro(valor_retorno)}\n"
                f"📅 Data: {agora().strftime('%d/%m/%Y %H:%M')}\n"
                f"```"
            ),
            inline=False
        )
        embed_confirmacao.add_field(name="📎 PRÓXIMO PASSO", value="📸 **Envie o PRINT da tela** neste canal para finalizar a lavagem.", inline=False)
        embed_confirmacao.set_footer(text="🛡 Vida Rasa 442 • Sistema de Lavagem", icon_url=bot.user.display_avatar.url if bot.user else None)
        msg_info = await interaction.channel.send(content=f"{interaction.user.mention}", embed=embed_confirmacao)
        lavagens_pendentes[interaction.user.id] = {
            "sujo": valor_sujo,
            "retorno": valor_retorno,
            "taxa": taxa,
            "msg_info": msg_info
        }
        await interaction.followup.send(
            f"✅ **Lavagem iniciada com sucesso!**\n"
            f"💰 Valor: {formatar_dinheiro(valor_sujo)}\n"
            f"💵 Retorno: {formatar_dinheiro(valor_retorno)}\n"
            f"📎 Envie o print no canal.",
            ephemeral=True
        )

# =========================================================
# 8.3 VIEWS DE LAVAGEM
# =========================================================
class LavagemView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🧼 Iniciar Lavagem", style=discord.ButtonStyle.primary, custom_id="lavagem_iniciar", emoji="🧼")
    async def iniciar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(LavagemModal())

    @discord.ui.button(label="🧹 Limpar Sala", style=discord.ButtonStyle.danger, custom_id="lavagem_limpar", emoji="🧹")
    async def limpar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message("❌ Você não tem permissão para limpar a sala!", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        canal = interaction.guild.get_channel(CANAL_LAVAGEM_MEMBROS_ID)
        if not canal:
            await interaction.followup.send("❌ Canal de lavagem não encontrado!", ephemeral=True)
            return
        deletadas = 0
        async for msg in canal.history(limit=200):
            try:
                await msg.delete()
                deletadas += 1
                await asyncio.sleep(0.2)
            except:
                pass
        await limpar_lavagens_db()
        embed = discord.Embed(title="🧹 ── SALA LIMPA ── 🧹", description=f"✅ **{deletadas} mensagens deletadas!**", color=0x2ecc71, timestamp=agora())
        embed.set_footer(text=f"🛡 Vida Rasa 442 • Limpeza realizada por {interaction.user.display_name}", icon_url=bot.user.display_avatar.url if bot.user else None)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="📊 Gerar Relatório", style=discord.ButtonStyle.success, custom_id="lavagem_relatorio", emoji="📊")
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message("❌ Você não tem permissão para gerar relatório!", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        dados = await carregar_lavagens_db()
        if not dados:
            await interaction.followup.send("📭 Nenhuma lavagem registrada.", ephemeral=True)
            return
        canal = interaction.guild.get_channel(CANAL_RELATORIO_LAVAGEM_ID)
        if not canal:
            await interaction.followup.send("❌ Canal de relatório não encontrado!", ephemeral=True)
            return
        total_sujo = sum(item["valor"] for item in dados)
        total_repassado = sum(item["liquido"] for item in dados)
        total_lavagens = len(dados)
        embed_resumo = discord.Embed(
            title="🧼 ── RELATÓRIO DE LAVAGEM ── 🧼",
            description="💰 Sistema Financeiro • VDR 442",
            color=0x1abc9c,
            timestamp=agora()
        )
        embed_resumo.set_thumbnail(url=interaction.guild.icon.url if interaction.guild.icon else bot.user.display_avatar.url)
        embed_resumo.set_author(name="🛡 Vida Rasa 442 • Relatório de Lavagem", icon_url=bot.user.display_avatar.url if bot.user else None)
        embed_resumo.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
        embed_resumo.add_field(
            name="📊 RESUMO GERAL",
            value=(
                f"```yaml\n"
                f"📋 Total de lavagens: {total_lavagens}\n"
                f"💰 Total de dinheiro sujo: {formatar_dinheiro(total_sujo)}\n"
                f"💵 Total a repassar (80%): {formatar_dinheiro(total_repassado)}\n"
                f"📊 Taxa aplicada: 20%\n"
                f"📅 Gerado em: {agora().strftime('%d/%m/%Y %H:%M')}\n"
                f"```"
            ),
            inline=False
        )
        embed_resumo.set_footer(text=f"🛡 Vida Rasa 442 • Relatório gerado por {interaction.user.display_name}", icon_url=bot.user.display_avatar.url if bot.user else None)
        await canal.send(embed=embed_resumo)
        await asyncio.sleep(1.5)
        usuarios = {}
        for item in dados:
            uid = item["user_id"]
            if uid not in usuarios:
                usuarios[uid] = {"sujo": 0, "repassado": 0, "quantidade": 0}
            usuarios[uid]["sujo"] += item["valor"]
            usuarios[uid]["repassado"] += item["liquido"]
            usuarios[uid]["quantidade"] += 1
        usuarios_ordenados = sorted(usuarios.items(), key=lambda x: x[1]["repassado"], reverse=True)
        texto_resumo = "📋 RESUMO PARA REPASSES\n"
        texto_resumo += "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        for uid, dados_user in usuarios_ordenados:
            try:
                user = await bot.fetch_user(int(uid))
                if user:
                    member = interaction.guild.get_member(int(uid))
                    if member and member.display_name:
                        nome = member.display_name
                    else:
                        nome = user.display_name or user.name
                else:
                    nome = uid
            except:
                nome = uid
            texto_resumo += f"{nome} -> 💵 Repassar: {formatar_dinheiro(dados_user['repassado'])}\n"
        texto_resumo += "\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
        texto_resumo += f"💰 TOTAL A REPASSAR: {formatar_dinheiro(total_repassado)}"
        await canal.send(f"```yaml\n{texto_resumo}\n```")
        await interaction.followup.send(
            f"✅ **Relatório enviado com sucesso!**\n"
            f"📋 {total_lavagens} lavagens registradas\n"
            f"👥 {len(usuarios)} usuários envolvidos",
            ephemeral=True
        )

    @discord.ui.button(label="📩 Avisar TODOS no DM", style=discord.ButtonStyle.primary, custom_id="lavagem_dm", emoji="📩")
    async def avisar_todos(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message("❌ Você não tem permissão para enviar DMs!", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        dados = await carregar_lavagens_db()
        if not dados:
            await interaction.followup.send("📭 Nenhuma lavagem registrada.", ephemeral=True)
            return
        enviados = 0
        falhas = 0
        usuarios = {}
        for item in dados:
            uid = item["user_id"]
            if uid not in usuarios:
                usuarios[uid] = {"total_sujo": 0, "total_repassado": 0, "quantidade": 0}
            usuarios[uid]["total_sujo"] += item["valor"]
            usuarios[uid]["total_repassado"] += item["liquido"]
            usuarios[uid]["quantidade"] += 1
        for uid, dados_user in usuarios.items():
            try:
                user = await bot.fetch_user(int(uid))
                if user:
                    embed = discord.Embed(
                        title="🧼 ── DINHEIRO LAVADO ── 🧼",
                        description=f"💰 Sistema Financeiro • VDR 442",
                        color=0x2ecc71,
                        timestamp=agora()
                    )
                    embed.add_field(
                        name="📋 INFORMAÇÕES DA LAVAGEM",
                        value=(
                            f"```yaml\n"
                            f"📋 Total de lavagens: {dados_user['quantidade']}\n"
                            f"💰 Total de dinheiro sujo: {formatar_dinheiro(dados_user['total_sujo'])}\n"
                            f"💵 Total repassado (80%): {formatar_dinheiro(dados_user['total_repassado'])}\n"
                            f"📊 Taxa aplicada: 20%\n"
                            f"📅 Gerado em: {agora().strftime('%d/%m/%Y %H:%M')}\n"
                            f"```"
                        ),
                        inline=False
                    )
                    embed.set_footer(text="🛡 Vida Rasa 442 • Sistema de Lavagem", icon_url=bot.user.display_avatar.url if bot.user else None)
                    await user.send(embed=embed)
                    enviados += 1
            except:
                falhas += 1
            await asyncio.sleep(1.5)
        await interaction.followup.send(
            f"✅ **DM enviada para {enviados} membros.**\n"
            f"❌ Falhas: {falhas}",
            ephemeral=True
        )

# =========================================================
# 8.4 FUNÇÃO DE ENVIAR PAINEL DE LAVAGEM
# =========================================================
async def enviar_painel_lavagem():
    canal = bot.get_channel(CANAL_INICIAR_LAVAGEM_ID)
    if not canal:
        logger.error("❌ Canal de lavagem não encontrado")
        return
    dados = await carregar_lavagens_db()
    total_lavagens = len(dados)
    total_repassado = sum(item["liquido"] for item in dados) if dados else 0
    total_sujo = sum(item["valor"] for item in dados) if dados else 0
    embed = discord.Embed(
        title="🧼 ── LAVAGEM DE DINHEIRO ── 🧼",
        description="💰 Sistema Financeiro • VDR 442",
        color=0x1a1a2e,
        timestamp=agora()
    )
    embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
    embed.set_author(name="🛡 Vida Rasa 442 • Sistema de Lavagem", icon_url=bot.user.display_avatar.url if bot.user else None)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="📋 COMO FUNCIONA",
        value=(
            "```yaml\n"
            "1️⃣ Clique em 'Iniciar Lavagem'\n"
            "2️⃣ Informe o valor do dinheiro sujo\n"
            "3️⃣ Envie o PRINT da tela\n"
            "4️⃣ Aguarde a confirmação\n"
            "\n"
            "📊 TAXA: 20%\n"
            "💵 RETORNO: 80% do valor informado\n"
            "```"
        ),
        inline=False
    )
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="📊 ESTATÍSTICAS GERAIS",
        value=(
            f"```yaml\n"
            f"📋 Total de lavagens: {total_lavagens}\n"
            f"💰 Total de dinheiro sujo: {formatar_dinheiro(total_sujo)}\n"
            f"💵 Total repassado (80%): {formatar_dinheiro(total_repassado)}\n"
            f"📊 Taxa média: 20%\n"
            f"```"
        ),
        inline=False
    )
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="📌 OPÇÕES DISPONÍVEIS",
        value=(
            "🧼 **Iniciar Lavagem** - Comece uma nova lavagem\n"
            "🧹 **Limpar Sala** - Remove todas as mensagens (ADM)\n"
            "📊 **Gerar Relatório** - Lista todas as lavagens\n"
            "📩 **Avisar DM** - Envia notificação para todos"
        ),
        inline=False
    )
    embed.set_footer(text=f"🛡 Vida Rasa 442 • Atualizado em {agora().strftime('%d/%m/%Y %H:%M:%S')}", icon_url=bot.user.display_avatar.url if bot.user else None)
    view = LavagemView()
    await enviar_ou_atualizar_painel("painel_lavagem", CANAL_INICIAR_LAVAGEM_ID, embed, view)

# =========================================================
# 8.5 EVENTO ON_MESSAGE PARA LAVAGEM
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
            if not canal_destino:
                await message.reply("❌ Canal de destino não encontrado!")
                return
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
            embed = discord.Embed(title="🧼 Nova Lavagem", color=0x1abc9c, timestamp=agora())
            embed.add_field(name="👤 Membro", value=message.author.mention, inline=False)
            embed.add_field(name="💰 Valor sujo", value=formatar_dinheiro(valor_sujo), inline=True)
            embed.add_field(name="💵 Valor a repassar (80%)", value=formatar_dinheiro(valor_retorno), inline=True)
            embed.add_field(name="📊 Taxa", value=f"{taxa}%", inline=True)
            embed.set_image(url=f"attachment://{arquivo.filename}")
        # =========================================================
        # VDRZINHO - Lavagem registrada
        # =========================================================
            await canal_destino.send(embed=vdrzinho.embed_resposta(
                tipo="lavagem_feita",
                user_id=message.author.id,
                nome=message.author.display_name,
                dados={"valor": valor_sujo}
            ))
        
            await canal_destino.send(embed=embed, file=arquivo)
            try:
                await message.author.send(
                    f"✅ **Lavagem registrada!**\n\n"
                    f"💰 Valor sujo: {formatar_dinheiro(valor_sujo)}\n"
                    f"💵 Valor a repassar: {formatar_dinheiro(valor_retorno)}\n"
                    f"📊 Taxa: {taxa}%"
                )
            except:
                pass

# =========================================================
# ==================== PARTE 9: SISTEMA DE LIVES ==========
# =========================================================

# =========================================================
# 9.1 FUNÇÕES DE BANCO DE DADOS - LIVES
# =========================================================
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

async def salvar_live_db(user_id, link):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO lives (user_id, link, divulgado) VALUES ($1, $2, false)", str(user_id), link)
    except Exception as e:
        logger.error(f"❌ Erro ao salvar live: {e}")

async def atualizar_divulgado_db(link, valor):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE lives SET divulgado=$1 WHERE link=$2", valor, link)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar divulgado: {e}")

async def remover_live_db(user_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM lives WHERE user_id=$1", str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao remover live: {e}")

async def salvar_live_manual(user_id, user_name, plataforma, link, titulo, categoria):
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE lives_manual SET ativo = false WHERE user_id = $1 AND ativo = true", str(user_id))
            return await conn.fetchval(
                "INSERT INTO lives_manual (user_id, user_name, plataforma, link, titulo, categoria) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
                str(user_id), user_name, plataforma, link, titulo, categoria
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar live manual: {e}")
        return None

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

async def desativar_live_manual(live_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE lives_manual SET ativo = false WHERE id = $1", live_id)
    except Exception as e:
        logger.error(f"❌ Erro ao desativar live manual: {e}")

# =========================================================
# 9.2 FUNÇÕES DE TWITCH
# =========================================================
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
        headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {token}"
        }
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

# =========================================================
# 9.3 FUNÇÃO DE DIVULGAR LIVE
# =========================================================
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
        
        # =========================================================
        # VDRZINHO - Live iniciada
        # =========================================================
        await canal.send(embed=vdrzinho.embed_resposta(
            tipo="live_iniciada",
            user_id=user_id,
            nome=user.display_name if user else str(user_id)
        ))
        
        await safe_request(
            canal.send,
            content="@everyone 🔴 **LIVE INICIADA!**",
            embed=embed,
            allowed_mentions=discord.AllowedMentions(everyone=True)
        )
        return True
        
    except Exception as e:
        logger.error(f"❌ ERRO ao divulgar live: {e}")
        return False

# =========================================================
# 9.4 MODAIS DE LIVES
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
                await interaction.response.send_message(
                    f"❌ Você já cadastrou o canal **{novo_canal}** na plataforma **{plataforma}**!",
                    ephemeral=True
                )
                return
        await salvar_live_db(interaction.user.id, novo_link)
        embed = discord.Embed(title="✅ Live cadastrada!", description=f"{interaction.user.mention}\n📺 **{plataforma.upper()}** - {novo_link}", color=0x2ecc71)
        await interaction.response.send_message(embed=embed, ephemeral=True)

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
        resultado = await divulgar_live(
            user_id=self.user_id,
            link=link,
            titulo=titulo,
            jogo=jogo,
            thumbnail=None,
            plataforma=plataforma.lower()
        )
        if resultado:
            await interaction.response.send_message(
                f"✅ **LIVE PUBLICADA COM SUCESSO!**\n\n"
                f"📺 **Plataforma:** {plataforma}\n"
                f"🔗 **Link:** {link}\n"
                f"📝 **Título:** {titulo}\n"
                f"🎮 **Jogo:** {jogo}",
                ephemeral=True
            )
        else:
            await interaction.response.send_message("❌ **ERRO AO PUBLICAR LIVE!**", ephemeral=True)

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
        await interaction.response.send_modal(PublicarLiveManualModal(self.user_id, self.user_name))

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
            await interaction.response.send_message(
                "❌ **Você não tem uma live cadastrada!**\n"
                "Clique em 'Cadastrar/Atualizar Live' primeiro.",
                ephemeral=True
            )
            return
        plataforma = live["plataforma"].upper()
        link = live["link"]
        titulo = live["titulo"] or "Live ao vivo!"
        categoria = live["categoria"] or "GTA RP"
        cores = {"KICK": 0x53FC18, "TIKTOK": 0x000000, "YOUTUBE": 0xFF0000, "TWITCH": 0x9146FF}
        icones = {"KICK": "🟢", "TIKTOK": "📱", "YOUTUBE": "▶️", "TWITCH": "🟣"}
        color = cores.get(plataforma, 0x2ecc71)
        icone = icones.get(plataforma, "🔴")
        embed = discord.Embed(
            title=f"{icone} LIVE AO VIVO!",
            description=(
                f"👤 **Streamer:** {interaction.user.mention}\n"
                f"📺 **Plataforma:** {plataforma}\n"
                f"🎮 **Jogo:** {categoria}\n"
                f"📝 **Título:** {titulo}\n\n"
                f"🔗 **Assistir:** {link}"
            ),
            color=color,
            timestamp=agora()
        )
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
        await safe_request(
            canal_divulgacao.send,
            content=f"@everyone 🔴 **LIVE INICIADA!**",
            embed=embed,
            allowed_mentions=discord.AllowedMentions(everyone=True)
        )
        await desativar_live_manual(live["id"])
        await interaction.response.send_message(
            f"✅ **LIVE ANUNCIADA COM SUCESSO!**\n"
            f"📢 Anúncio enviado para <#{CANAL_DIVULGACAO_LIVE_ID}>",
            ephemeral=True
        )

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
        await interaction.response.send_message(
            "✅ **Live cancelada com sucesso!**\n"
            "Você pode cadastrar uma nova live quando quiser.",
            ephemeral=True
        )

# =========================================================
# 9.5 VIEWS DE LIVES
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

    @discord.ui.button(label="🎥 Minha Live", style=discord.ButtonStyle.primary, custom_id="minha_live_manual", emoji="🎥")
    async def minha_live(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = GerenciarLiveView(interaction.user.id, interaction.user.display_name)
        embed = discord.Embed(
            title="🎥 GERENCIAR MINHA LIVE",
            description=(
                "**📌 Como funciona:**\n\n"
                "1. Clique em **'Cadastrar/Atualizar Live'**\n"
                "2. Informe a plataforma (Kick, TikTok, etc)\n"
                "3. Cole o link da sua live\n"
                "4. Quando começar, clique em **'ANUNCIAR LIVE'**\n\n"
                "✅ **Plataformas suportadas:**\n"
                "• 🟢 Kick\n"
                "• 📱 TikTok\n"
                "• ▶️ YouTube\n"
                "• E qualquer outra!"
            ),
            color=0x3498db
        )
        embed.set_footer(text="Sistema de Lives • VDR")
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# =========================================================
# 9.6 FUNÇÃO DE ENVIAR PAINEL DE LIVES
# =========================================================
async def enviar_painel_lives():
    canal = bot.get_channel(CANAL_CADASTRO_LIVE_ID)
    if not canal:
        logger.error("❌ Canal cadastro live não encontrado")
        return
    embed = discord.Embed(
        title="🎥 SISTEMA DE LIVES",
        description=(
            "**Gerencie suas lives de forma simples e rápida!**\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "🟣 **TWITCH - AUTOMÁTICO**\n"
            "• Cadastre sua live **uma única vez**\n"
            "• Quando entrar ao vivo, o bot **anuncia automaticamente**\n"
            "• Você não precisa fazer mais nada!\n\n"
            "🟢 **KICK / TIKTOK / YOUTUBE - MANUAL**\n"
            "• **Toda vez** que for começar a live, publique manualmente\n"
            "• Preencha as informações e clique em 'Publicar Live'\n"
            "• O anúncio vai imediatamente para o canal de divulgação\n\n"
            "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
            "📢 **Todas as lives vão para:** <#1243325102917943335>\n"
            "⚠️ **Importante:** O link deve ser válido e acessível!"
        ),
        color=0x9146FF,
        timestamp=agora()
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
# 9.7 TASK DE VERIFICAR LIVES
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
# ==================== PARTE 10: SISTEMA DE BAÚ ===========
# =========================================================

# =========================================================
# 10.1 FUNÇÕES DE BANCO DE DADOS - BAÚ
# =========================================================
async def atualizar_bau_estoque(item_nome, quantidade, operacao="adicionar"):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            existente = await conn.fetchval("SELECT quantidade FROM bau_estoque WHERE item_nome = $1", item_nome)
            if existente is not None:
                if operacao == "adicionar":
                    nova_quantidade = existente + quantidade
                else:
                    nova_quantidade = existente - quantidade
                    if nova_quantidade < 0:
                        nova_quantidade = 0
                await conn.execute("UPDATE bau_estoque SET quantidade = $1, ultima_atualizacao = NOW() WHERE item_nome = $2", nova_quantidade, item_nome)
            else:
                if operacao == "remover":
                    return
                await conn.execute("INSERT INTO bau_estoque (item_nome, quantidade, ultima_atualizacao) VALUES ($1, $2, NOW())", item_nome, quantidade)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar estoque do baú: {e}")

async def registrar_movimentacao_bau(tipo, item_nome, quantidade, membro, observacao=None):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO bau_movimentacoes (tipo, item_nome, quantidade, membro, observacao, data) VALUES ($1, $2, $3, $4, $5, NOW())", tipo, item_nome, quantidade, membro, observacao)
    except Exception as e:
        logger.error(f"❌ Erro ao registrar movimentação: {e}")

async def carregar_bau_estoque():
    pool = await get_pool()
    if not pool:
        return {}
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT item_nome, quantidade FROM bau_estoque ORDER BY item_nome")
            estoque = {}
            for row in rows:
                estoque[row["item_nome"]] = row["quantidade"]
            return estoque
    except Exception as e:
        logger.error(f"❌ Erro ao carregar estoque do baú: {e}")
        return {}

# =========================================================
# 10.2 FUNÇÃO PARA DETECTAR ARMAS
# =========================================================
def is_arma(item_nome):
    item_lower = item_nome.lower()
    palavras_arma = [
        "fuzil", "glock", "shotgun", "m4", "ak47", "ak-47",
        "sniper", "pistola", "sig", "ak", "aug", "carabina",
        "rifle", "g3", "fal", "m16", "ar15", "revolver",
        "magnum", "uzi", "mp5", "p90", "escopeta", "metralhadora"
    ]
    for palavra in palavras_arma:
        if palavra in item_lower:
            return True
    return False

# =========================================================
# 10.3 FUNÇÃO PARA CRIAR EMBED DO BAÚ
# =========================================================
async def criar_embed_bau_estoque():
    embed = discord.Embed(
        title="📦 ── ESTOQUE DO BAÚ ── 📦",
        description="🔫 VDR 442 • Controle de Estoque Geral",
        color=0x1a1a2e,
        timestamp=agora()
    )
    embed.set_author(name="🛡 Vida Rasa 442 • Baú de Membros", icon_url=bot.user.display_avatar.url if bot.user else None)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    estoque = await carregar_bau_estoque()
    if estoque:
        texto_estoque = ""
        for item, qtd in estoque.items():
            if qtd > 0 and not is_arma(item):
                texto_estoque += f"🔹 {item}: {qtd} unidade(s)\n"
        if texto_estoque:
            embed.add_field(name="📊 ITENS NO BAÚ", value=f"```\n{texto_estoque}\n```", inline=False)
        else:
            embed.add_field(name="📊 ITENS NO BAÚ", value="```\n📭 Baú vazio\n```", inline=False)
    else:
        embed.add_field(name="📊 ITENS NO BAÚ", value="```\n📭 Baú vazio\n```", inline=False)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="📋 COMO USAR",
        value=(
            "```yaml\n"
            "📥 ENTRADA: Clique em 'Registrar Entrada'\n"
            "📤 SAÍDA: Clique em 'Registrar Saída'\n"
            "\n"
            "📌 EXEMPLO:\n"
            "placas: 100\n"
            "c4: 10\n"
            "kit medico: 5\n"
            "```"
        ),
        inline=False
    )
    embed.set_footer(text="🛡 Vida Rasa 442 • Sistema de Baú", icon_url=bot.user.display_avatar.url if bot.user else None)
    return embed

# =========================================================
# 10.4 FUNÇÃO PARA CRIAR EMBED DE ARMAS
# =========================================================
async def criar_embed_armas_estoque():
    embed = discord.Embed(
        title="🔫 ── ESTOQUE DE ARMAS ── 🔫",
        description="🔫 VDR 442 • Controle de Armas",
        color=0x1a1a2e,
        timestamp=agora()
    )
    embed.set_author(name="🛡 Vida Rasa 442 • Arsenal", icon_url=bot.user.display_avatar.url if bot.user else None)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    estoque = await carregar_bau_estoque()
    if estoque:
        texto_estoque = ""
        for item, qtd in estoque.items():
            if qtd > 0 and is_arma(item):
                texto_estoque += f"🔹 {item}: {qtd} unidade(s)\n"
        if texto_estoque:
            embed.add_field(name="📊 ARMAS NO ESTOQUE", value=f"```\n{texto_estoque}\n```", inline=False)
        else:
            embed.add_field(name="📊 ARMAS NO ESTOQUE", value="```\n📭 Nenhuma arma no estoque\n```", inline=False)
    else:
        embed.add_field(name="📊 ARMAS NO ESTOQUE", value="```\n📭 Nenhuma arma no estoque\n```", inline=False)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="📋 COMO USAR",
        value=(
            "```yaml\n"
            "🔫 ENTRADA: Clique em 'Registrar Armas Entrada'\n"
            "🔫 SAÍDA: Clique em 'Registrar Armas Saída'\n"
            "\n"
            "📌 EXEMPLO:\n"
            "Fuzil: 2\n"
            "Glock: 1\n"
            "Shotgun: 3\n"
            "G3: 10\n"
            "```"
        ),
        inline=False
    )
    embed.set_footer(text="🛡 Vida Rasa 442 • Arsenal", icon_url=bot.user.display_avatar.url if bot.user else None)
    return embed

# =========================================================
# 10.5 MODAIS DO BAÚ
# =========================================================
class BauModal(discord.ui.Modal):
    def __init__(self, tipo):
        self.tipo = tipo
        titulo = "📥 Registrar Entrada" if tipo == "entrou" else "📤 Registrar Saída"
        super().__init__(title=titulo)
        self.itens = discord.ui.TextInput(label="📦 Itens (item: quantidade)", placeholder="Ex: placas: 100\nc4: 10\nfuzil: 2", style=discord.TextStyle.paragraph, required=True, max_length=500)
        self.observacao = discord.ui.TextInput(label="📝 Observação (opcional)", placeholder="Ex: Para ação, Para estoque, etc", style=discord.TextStyle.paragraph, required=False, max_length=200)
        self.add_item(self.itens)
        self.add_item(self.observacao)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            nome_membro = interaction.user.display_name
            itens_dict = {}
            linhas = self.itens.value.strip().split('\n')
            for linha in linhas:
                if ':' in linha:
                    partes = linha.split(':', 1)
                    item = partes[0].strip()
                    try:
                        quantidade = int(partes[1].strip())
                    except:
                        quantidade = partes[1].strip()
                    itens_dict[item] = quantidade
            if not itens_dict:
                await interaction.followup.send("❌ **Nenhum item válido encontrado!** Use o formato: `Item: Quantidade`", ephemeral=True)
                return
            for item, quantidade in itens_dict.items():
                if self.tipo == "entrou":
                    await atualizar_bau_estoque(item, quantidade, "adicionar")
                else:
                    await atualizar_bau_estoque(item, quantidade, "remover")
                await registrar_movimentacao_bau(tipo=self.tipo, item_nome=item, quantidade=quantidade, membro=nome_membro, observacao=self.observacao.value if self.observacao.value else None)
            log_mensagens = []
            for item, quantidade in itens_dict.items():
                if self.tipo == "entrou":
                    log_mensagens.append(f"📥 **{nome_membro}** adicionou **{quantidade}** {item}.")
                else:
                    log_mensagens.append(f"📤 **{nome_membro}** pegou **{quantidade}** {item}.")
            texto_log = "\n".join(log_mensagens)
            canal_bau = interaction.guild.get_channel(CANAL_BAU_MEMBROS_ID)
            if canal_bau:
                embed = await criar_embed_bau_estoque()
                view = BauView()
                await enviar_ou_atualizar_painel_bau("painel_bau", CANAL_BAU_MEMBROS_ID, embed, view)
            if self.tipo == "entrou":
                canal_controle = interaction.guild.get_channel(CANAL_BAU_MEMBROS_ID)
                if canal_controle:
                    msg_pedido = await canal_controle.send(
                        f"📎 **{nome_membro}**, anexe o print da entrada aqui.\n"
                        f"📝 **Itens:** {', '.join([f'{item} ({qtd})' for item, qtd in itens_dict.items()])}"
                    )
                    bau_print_pendente[interaction.user.id] = {
                        "log": texto_log,
                        "canal_id": CANAL_BAU_LOG_ID,
                        "msg_pedido_id": msg_pedido.id
                    }
                    await interaction.followup.send(
                        f"✅ **Registro enviado!**\n📎 Agora anexe o print no canal **#bau-membros** para finalizar.",
                        ephemeral=True
                    )
                    return
            else:
                canal_log = interaction.guild.get_channel(CANAL_BAU_LOG_ID)
                if canal_log:
                    # =========================================================
                    # VDRZINHO - BAU COM DETALHES (PEGA O PRIMEIRO ITEM)
                    # =========================================================
                    primeiro_item = list(itens_dict.items())[0] if itens_dict else None
                    if primeiro_item:
                        item_nome, qtd = primeiro_item
                        if self.tipo == "entrou":
                            embed_vdr = vdrzinho.embed_bau("entrou", nome_membro, item_nome, qtd, interaction)
                        else:
                            embed_vdr = vdrzinho.embed_bau("saiu", nome_membro, item_nome, qtd, interaction)
                        await canal_log.send(embed=embed_vdr)
                    
                    await canal_log.send(texto_log)
                await interaction.followup.send(f"✅ **Registro de saída enviado com sucesso!**", ephemeral=True)
        except Exception as e:
            logger.error(f"❌ Erro no BauModal: {e}")
            await interaction.followup.send(f"❌ **Erro ao registrar:** {str(e)[:100]}", ephemeral=True)
            
class ArmasModal(discord.ui.Modal):
    def __init__(self, tipo):
        self.tipo = tipo
        titulo = "🔫 Registrar Armas Entrada" if tipo == "entrou" else "🔫 Registrar Armas Saída"
        super().__init__(title=titulo)
        self.itens = discord.ui.TextInput(label="🔫 Armas (arma: quantidade)", placeholder="Ex: Fuzil: 2\nGlock: 1\nG3: 10", style=discord.TextStyle.paragraph, required=True, max_length=500)
        self.observacao = discord.ui.TextInput(label="📝 Observação (opcional)", placeholder="Ex: Para ação, Para estoque, etc", style=discord.TextStyle.paragraph, required=False, max_length=200)
        self.add_item(self.itens)
        self.add_item(self.observacao)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            nome_membro = interaction.user.display_name
            itens_dict = {}
            linhas = self.itens.value.strip().split('\n')
            for linha in linhas:
                if ':' in linha:
                    partes = linha.split(':', 1)
                    item = partes[0].strip()
                    try:
                        quantidade = int(partes[1].strip())
                    except:
                        quantidade = partes[1].strip()
                    itens_dict[item] = quantidade
            if not itens_dict:
                await interaction.followup.send("❌ **Nenhuma arma válida encontrada!** Use o formato: `Arma: Quantidade`", ephemeral=True)
                return
            for item, quantidade in itens_dict.items():
                if self.tipo == "entrou":
                    await atualizar_bau_estoque(item, quantidade, "adicionar")
                else:
                    await atualizar_bau_estoque(item, quantidade, "remover")
                await registrar_movimentacao_bau(tipo=self.tipo, item_nome=item, quantidade=quantidade, membro=nome_membro, observacao=self.observacao.value if self.observacao.value else None)
            log_mensagens = []
            for item, quantidade in itens_dict.items():
                if self.tipo == "entrou":
                    log_mensagens.append(f"🔫 **{nome_membro}** adicionou **{quantidade}** {item}.")
                else:
                    log_mensagens.append(f"🔫 **{nome_membro}** pegou **{quantidade}** {item}.")
            texto_log = "\n".join(log_mensagens)
            canal_armas = interaction.guild.get_channel(CANAL_ARMAS_ESTOQUE_ID)
            if canal_armas:
                embed = await criar_embed_armas_estoque()
                view = ArmasView()
                await enviar_ou_atualizar_painel_bau("painel_armas", CANAL_ARMAS_ESTOQUE_ID, embed, view)
            if self.tipo == "entrou":
                canal_controle = interaction.guild.get_channel(CANAL_ARMAS_ESTOQUE_ID)
                if canal_controle:
                    msg_pedido = await canal_controle.send(
                        f"📎 **{nome_membro}**, anexe o print da entrada aqui.\n"
                        f"📝 **Itens:** {', '.join([f'{item} ({qtd})' for item, qtd in itens_dict.items()])}"
                    )
                    armas_print_pendente[interaction.user.id] = {
                        "log": texto_log,
                        "canal_id": CANAL_ARMAS_LOG_ID,
                        "msg_pedido_id": msg_pedido.id
                    }
                    await interaction.followup.send(
                        f"✅ **Registro de armas enviado!**\n📎 Agora anexe o print no canal **#armas-estoque** para finalizar.",
                        ephemeral=True
                    )
                    return
            else:
                canal_log = interaction.guild.get_channel(CANAL_ARMAS_LOG_ID)
                if canal_log:
                    # =========================================================
                    # VDRZINHO - ARMAS COM DETALHES
                    # =========================================================
                    primeiro_item = list(itens_dict.items())[0] if itens_dict else None
                    if primeiro_item:
                        item_nome, qtd = primeiro_item
                        if self.tipo == "entrou":
                            embed_vdr = vdrzinho.embed_bau("entrou", nome_membro, item_nome, qtd, interaction)
                        else:
                            embed_vdr = vdrzinho.embed_bau("saiu", nome_membro, item_nome, qtd, interaction)
                        await canal_log.send(embed=embed_vdr)
                    
                    await canal_log.send(texto_log)
                await interaction.followup.send(f"✅ **Registro de saída enviado com sucesso!**", ephemeral=True)
        except Exception as e:
            logger.error(f"❌ Erro no ArmasModal: {e}")
            await interaction.followup.send(f"❌ **Erro ao registrar:** {str(e)[:100]}", ephemeral=True)

# =========================================================
# 10.6 VIEWS DO BAÚ
# =========================================================
class BauView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📥 Registrar Entrada", style=discord.ButtonStyle.success, custom_id="bau_entrada_btn", emoji="📥")
    async def registrar_entrada(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = BauModal("entrou")
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="📤 Registrar Saída", style=discord.ButtonStyle.danger, custom_id="bau_saida_btn", emoji="📤")
    async def registrar_saida(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = BauModal("saiu")
        await interaction.response.send_modal(modal)

class ArmasView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🔫 Registrar Entrada", style=discord.ButtonStyle.success, custom_id="armas_entrada_btn", emoji="🔫")
    async def registrar_entrada(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ArmasModal("entrou")
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="🔫 Registrar Saída", style=discord.ButtonStyle.danger, custom_id="armas_saida_btn", emoji="🔫")
    async def registrar_saida(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = ArmasModal("saiu")
        await interaction.response.send_modal(modal)

# =========================================================
# 10.7 FUNÇÃO AUXILIAR PARA ATUALIZAR PAINEL DO BAÚ
# =========================================================
async def enviar_ou_atualizar_painel_bau(nome, canal_id, embed, view):
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
                        ultima_msg = None
                        async for m in canal.history(limit=1):
                            ultima_msg = m
                            break
                        if ultima_msg and ultima_msg.id != msg.id:
                            await msg.delete()
                            msg = None
                    if msg:
                        await msg.edit(embed=embed, view=view)
                        return
                except Exception as e:
                    logger.warning(f"⚠️ Erro ao atualizar painel {nome}: {e}")
            async for msg in canal.history(limit=50):
                if msg.author == bot.user:
                    if row and msg.id == row.get("mensagem_id"):
                        continue
                    try:
                        await msg.delete()
                        await asyncio.sleep(0.3)
                    except:
                        pass
            msg = await safe_request(canal.send, embed=embed, view=view)
            if msg:
                await conn.execute("INSERT INTO paineis (nome, canal_id, mensagem_id) VALUES ($1,$2,$3) ON CONFLICT (nome) DO UPDATE SET canal_id=$2, mensagem_id=$3", nome, str(canal_id), str(msg.id))
    except Exception as e:
        logger.error(f"❌ Erro crítico ao enviar painel {nome}: {e}")

# =========================================================
# 10.8 FUNÇÕES PARA ENVIAR PAINÉIS DO BAÚ
# =========================================================
async def enviar_painel_bau():
    canal = bot.get_channel(CANAL_BAU_MEMBROS_ID)
    if not canal:
        logger.error("❌ Canal BAU MEMBROS não encontrado!")
        return
    embed = await criar_embed_bau_estoque()
    view = BauView()
    await enviar_ou_atualizar_painel_bau("painel_bau", CANAL_BAU_MEMBROS_ID, embed, view)

async def enviar_painel_armas():
    canal = bot.get_channel(CANAL_ARMAS_ESTOQUE_ID)
    if not canal:
        logger.error("❌ Canal ARMAS ESTOQUE não encontrado!")
        return
    embed = await criar_embed_armas_estoque()
    view = ArmasView()
    await enviar_ou_atualizar_painel_bau("painel_armas", CANAL_ARMAS_ESTOQUE_ID, embed, view)

# =========================================================
# ==================== PARTE 11: SISTEMA DE AÇÕES =========
# =========================================================

# =========================================================
# 11.1 CONSTANTES DAS AÇÕES
# =========================================================
CATEGORIAS_ACOES = {
    "Fleeca": {"limite": 4, "emoji": "🏦", "acoes": ["Banco Fleeca - Rota 68", "Banco Fleeca - Chaves", "Banco Fleeca - Praia", "Banco Fleeca - Shopping"]},
    "Banco Central": {"limite": 1, "emoji": "🏛️", "acoes": ["Banco Central Com Refém", "Banco Central Sem Refém"]},
    "Joalheria": {"limite": 5, "emoji": "💎", "acoes": ["Joalheria"]},
    "Banco de Paleto": {"limite": 1, "emoji": "🏦", "acoes": ["Banco de Paleto"]},
    "Nióbio": {"limite": 1, "emoji": "⚗️", "acoes": ["Nióbio"]},
    "Lojas e Carros Fortes": {"limite": None, "emoji": "🏪", "acoes": ["Loja de Armas (Ammunation)", "Loja de Bebidas", "Loja de Departamento", "Mergulhador", "Grapeseed", "Companhia de Gás", "Life Invader", "Aeroporto de Sucata", "Carro Forte - Açougue", "Carro Forte - Faculdade", "Carro Forte - Grove Street"]},
    "Bahamas": {"limite": None, "emoji": "🏝️", "acoes": ["Banco Bahamas", "Burgueshot (Bahamas)", "Refinaria (Bahamas)", "Lan House - (Bahamas)", "Lan House - Jersey", "Lan House - Brooklyn", "Lan House - Manhattan", "Museu (Bahamas)"]},
    "Helicrash": {"limite": None, "emoji": "🚁", "acoes": ["🚁 Helicrash (13h)", "🚁 Helicrash (15h)", "🚁 Helicrash (22h)", "🚁 Helicrash (02h)"]}
}

ACAO_PARA_CATEGORIA = {}
for categoria, dados in CATEGORIAS_ACOES.items():
    for acao in dados["acoes"]:
        ACAO_PARA_CATEGORIA[acao] = categoria

ACOES_COMPLEXO = {}
ACOES_BAHAMAS = {}
ACOES_HELICRASH = {}
for categoria, dados in CATEGORIAS_ACOES.items():
    for acao in dados["acoes"]:
        if categoria == "Bahamas":
            ACOES_BAHAMAS[acao] = dados["limite"]
        elif categoria == "Helicrash":
            ACOES_HELICRASH[acao] = dados["limite"]
        else:
            ACOES_COMPLEXO[acao] = dados["limite"]

REGRAS_ACOES = {
    "Loja de Armas (Ammunation)": {"regras": ["👥 **Bandidos:** Obrigatório 2.", "🎯 **Com estande de tiro:** 0 fora.", "🎯 **Sem estande de tiro:** 1 fora.", "👮 **Máximo de policiais:** 3.", "🔫 **Armamento:** Todos de Pistola (exceto Magnum e Ap-Pistol).", "🤝 **Negociação:** Obrigatória.", "🚫 **Refém:** Proibido."]},
    "Loja de Bebidas": {"regras": ["👥 **Bandidos:** Obrigatório 3.", "👮 **Máximo de policiais:** 4.", "🔫 **Armamento:** Todos de Pistola (exceto Magnum e Ap-Pistol).", "🤝 **Negociação:** Obrigatória.", "🚫 **Refém:** Proibido.", "📌 **Obs:** Proibido o uso de 2 andares ou mais (teti chão)."]},
    "Loja de Departamento": {"regras": ["👥 **Bandidos:** Obrigatório 4 (máximo de 2 fora).", "👮 **Máximo de policiais:** 5.", "🚗 **Máximo de veículos:** 1 veículo, 4 rodas ou 2 motos.", "🔫 **Armamento:** Todos de Pistola (exceto Glock Rajada).", "🤝 **Negociação:** Obrigatória.", "👤 **Refém:** Opcional, máximo."]},
    "Mergulhador": {"regras": ["👥 **Bandidos:** Obrigatório 6.", "👮 **Máximo de policiais:** 8.", "🔫 **Armamento:** Todos de Pistola (exceto Magnum e Ap-Pistol).", "⚔️ **Negociação:** Inexistente, ação de confronto direto.", "🚫 **Refém:** Proibido.", "📌 **Obs:** Proibido o uso de 2 andares ou mais (teti chão)."]},
    "Grapeseed": {"regras": ["👥 **Bandidos:** Obrigatório 6.", "👮 **Máximo de policiais:** 7.", "🔫 **Armamento:** Todos de Pistola (exceto Magnum e Ap-Pistol).", "⚔️ **Negociação:** Inexistente, ação de confronto direto.", "🚫 **Refém:** Proibido.", "📌 **Obs:** Proibido o uso de 2 andares ou mais (teti chão)."]},
    "Companhia de Gás": {"regras": ["👥 **Bandidos:** Obrigatório 6.", "👮 **Máximo de policiais:** 8.", "🚗 **Máximo de veículos:** 3.", "🔫 **Armamento:** Todos de Pistola (exceto Magnum e Ap-Pistol).", "⚔️ **Negociação:** Inexistente, ação de confronto direto.", "🚫 **Refém:** Proibido.", "📌 1️⃣ Proibido subir em qualquer objeto/lugar durante a ação.", "📌 2️⃣ Proibido atirar contra policiais entrando no perímetro.", "📌 3️⃣ Todos os participantes devem estar dentro do perímetro para o embate começar."]},
    "Life Invader": {"regras": ["👥 **Bandidos:** Obrigatório 8.", "👮 **Máximo de policiais:** 10.", "🔫 **Armamento:** Todos de Pistola (exceto Magnum e Ap-Pistol).", "⚔️ **Negociação:** Inexistente, ação de confronto direto.", "🚫 **Refém:** Proibido.", "📌 Proibido subir em qualquer objeto/lugar durante a ação.", "📌 Proibido a utilização dos INTERIORES do perímetro (Life Invader, Cozinha/Piscina)."]},
    "Aeroporto de Sucata": {"regras": ["👥 **Máximo de bandidos:** 6.", "👮 **Máximo de policiais:** 8.", "🔫 **Armamento:** Obrigatório ter 6 pistolas.", "⚔️ **Negociação:** Inexistente, ação de confronto direto.", "🚫 **Refém:** Proibido."]},
    "Carro Forte - Açougue": {"regras": ["👥 **Bandidos:** Obrigatório 6.", "👮 **Máximo de policiais:** 8.", "🔫 **Armamento:** Mínimo SMG, obrigatório ter 2 RIFLES.", "⚔️ **Negociação:** Inexistente, ação de confronto direto.", "🚫 **Refém:** Proibido.", "📌 **Obs:** Proibido bugar head-glitch."]},
    "Carro Forte - Faculdade": {"regras": ["👥 **Bandidos:** Obrigatório 6.", "👮 **Máximo de policiais:** 8.", "🔫 **Armamento:** Mínimo SMG, obrigatório ter 2 RIFLES.", "⚔️ **Negociação:** Inexistente, ação de confronto direto.", "🚫 **Refém:** Proibido.", "📌 **Obs:** Proibido bugar head-glitch."]},
    "Carro Forte - Grove Street": {"regras": ["👮 **Máximo de policiais:** 8.", "🔫 **Armamento:** Mínimo SMG, obrigatório ter 2 RIFLES.", "⚔️ **Negociação:** Inexistente, ação de confronto direto.", "🚫 **Refém:** Proibido.", "📌 **Obs:** Proibido o uso de 2 andares ou mais (teti chão).", "📌 **Obs:** Helicóptero somente para visual, sem atirador."]},
    "Joalheria": {"regras": ["👥 **Bandidos:** Obrigatório 7 (máximo de 3 fora e 4 dentro).", "👮 **Máximo de policiais:** 9.", "🚗 **Máximo de veículos:** 3 (em caso de fuga).", "🔫 **Armamento:** No mínimo Submetralhadora.", "🤝 **Negociação:** Obrigatória.", "👤 **Refém:** Opcional, no máximo 3.", "📌 Proibido a utilização dos INTERIORES do perímetro (Prefeitura)."]},
    "Banco Fleeca - Rota 68": {"regras": ["👥 **Mínimo de bandidos:** 6 (mínimo de 3 dentro).", "👥 **Máximo de bandidos:** 8 (mínimo de 3 dentro).", "🚗 **Máximo de veículos:** 3.", "👮 **Máximo de policiais:** 9.", "🔫 **Armamento:** Mínimo submetralhadora, obrigatório ter 4 Rifles.", "🤝 **Negociação:** Obrigatória.", "👤 **Refém:** Opcional, no máximo 3.", "📌 Na fuga, só é permitido fazer o Fleeca Chaves."]},
    "Banco Fleeca - Chaves": {"regras": ["👥 **Mínimo de bandidos:** 6.", "👥 **Máximo de bandidos:** 8.", "🚗 **Máximo de veículos:** 3.", "👮 **Máximo de policiais:** 9.", "🔫 **Armamento:** Mínimo Submetralhadora.", "🤝 **Negociação:** Obrigatória.", "👤 **Refém:** Opcional, no máximo 3.", "📌 Regras de posicionamento: até 3 integrantes em locais altos/acessíveis no prédio e até 3 no interior do resort."]},
    "Banco Fleeca - Praia": {"regras": ["🔫 **Armamento:** Somente Submetralhadora.", "📌 Heli drone + teti chão.", "📌 Proibido interior da lojinha (cofre).", "📌 Proibido veículo dentro do perímetro.", "📌 Na casa de madeira fica limitado 3 bandidos.", "📌 Polícia não pode marcar saída.", "📌 Proibida a fuga."]},
    "Banco Fleeca - Shopping": {"regras": ["🔫 **Armamento:** Mínimo submetralhadora, obrigatório ter 4 Rifles.", "📌 Com atirador: máximo 4 bandidos em prédios.", "📌 Sem atirador: uso do interior do prédio proibido.", "📌 Limite máximo de pessoas no metrô: 3.", "📌 Proibida a fuga."]},
    "Banco de Paleto": {"regras": ["👥 **Bandidos:** Obrigatório 10.", "👮 **Máximo de policiais:** 12.", "🔫 **Armamento:** Todos de Rifle.", "⚔️ **Negociação:** Inexistente, ação de confronto direto.", "🚫 **Refém:** Proibido.", "📌 Os bandidos devem esperar o início da ação.", "📌 Ação inicia quando a polícia entrar no perímetro.", "📌 Helicóptero só poderá ter o piloto.", "📌 Máximo de 6 pessoas dentro do GALINHEIRO."]},
    "Banco Central Com Refém": {"regras": ["👥 **Bandidos:** Obrigatório 10.", "👥 **Bandidos fora:** Máximo 3 em prédios ou 5 no chão.", "🚗 **Máximo de veículos:** 4.", "👮 **Máximo de policiais:** 12.", "🔫 **Armamento:** Obrigatório RIFLE.", "🤝 **Negociação:** Obrigatória.", "👤 **Refém:** Permitido, máximo 4.", "📌 Reféns podem ser usados para tirar atiradores ou proibir reposicionamento com helicóptero.", "📌 Não pode ser os dois ao mesmo tempo.", "📌 Proibido o uso do interior do apartamento em frente ao POSTAL.", "📌 Obs: Proibido ter bandidos fora se a ação for na fuga."]},
    "Banco Central Sem Refém": {"regras": ["👥 **Bandidos:** Obrigatório 10.", "👥 **Bandidos fora:** Máximo 3 em prédios ou 5 no chão.", "🚗 **Máximo de veículos:** 3.", "👮 **Máximo de policiais:** 12.", "🔫 **Armamento:** Obrigatório RIFLE.", "🤝 **Negociação:** Obrigatória.", "🚫 **Refém:** Proibido.", "📌 Proibido o uso do interior do apartamento em frente ao POSTAL.", "📌 Obs: Proibido ter bandidos fora se a ação for na fuga."]},
    "Nióbio": {"regras": ["👥 **Bandidos:** Obrigatório 12 (sem limites fora).", "👮 **Máximo de policiais:** 18.", "🔫 **Armamento:** Obrigatório RIFLE.", "⚔️ **Negociação:** Inexistente.", "🚫 **Refém:** Proibido.", "📌 Proibido marcar a porta que dá acesso a água.", "📌 A parte da água só poderá ser acessada para entrar ou sair do túnel do NIÓBIO.", "📌 Limite de 4 bandidos entre o corredor que dá acesso a água e o quadrado do quebrado.", "📌 Máximo de 4 bandidos no fundo do nióbio."]},
    "🚁 Helicrash (13h)": {"regras": ["👥 **Máximo de participantes por facção/grupo:** 10.", "🚗 **Máximo de veículos por facção/grupo:** 2.", "🚫 **Proibido** o roubo de veículos durante o evento.", "👕 Todos os membros deverão OBRIGATORIAMENTE utilizar a roupa completa da sua facção/grupo.", "👥 Jogadores membros (setados) só poderão participar junto da sua própria facção/grupo.", "👤 Jogadores sem set podem formar grupos entre si, mas deverão usar uma roupa igual.", "💉 A reanimação é permitida somente após o término completo da ação.", "🚫 Proibido a utilização de GRANADEIRA."]},
    "🚁 Helicrash (15h)": {"regras": ["👥 **Máximo de participantes por facção/grupo:** 10.", "🚗 **Máximo de veículos por facção/grupo:** 2.", "🚫 **Proibido** o roubo de veículos durante o evento.", "👕 Todos os membros deverão OBRIGATORIAMENTE utilizar a roupa completa da sua facção/grupo.", "👥 Jogadores membros (setados) só poderão participar junto da sua própria facção/grupo.", "👤 Jogadores sem set podem formar grupos entre si, mas deverão usar uma roupa igual.", "💉 A reanimação é permitida somente após o término completo da ação.", "🚫 Proibido a utilização de GRANADEIRA."]},
    "🚁 Helicrash (22h)": {"regras": ["👥 **Máximo de participantes por facção/grupo:** 10.", "🚗 **Máximo de veículos por facção/grupo:** 2.", "🚫 **Proibido** o roubo de veículos durante o evento.", "👕 Todos os membros deverão OBRIGATORIAMENTE utilizar a roupa completa da sua facção/grupo.", "👥 Jogadores membros (setados) só poderão participar junto da sua própria facção/grupo.", "👤 Jogadores sem set podem formar grupos entre si, mas deverão usar uma roupa igual.", "💉 A reanimação é permitida somente após o término completo da ação.", "🚫 Proibido a utilização de GRANADEIRA."]},
    "🚁 Helicrash (02h)": {"regras": ["👥 **Máximo de participantes por facção/grupo:** 10.", "🚗 **Máximo de veículos por facção/grupo:** 2.", "🚫 **Proibido** o roubo de veículos durante o evento.", "👕 Todos os membros deverão OBRIGATORIAMENTE utilizar a roupa completa da sua facção/grupo.", "👥 Jogadores membros (setados) só poderão participar junto da sua própria facção/grupo.", "👤 Jogadores sem set podem formar grupos entre si, mas deverão usar uma roupa igual.", "💉 A reanimação é permitida somente após o término completo da ação.", "🚫 Proibido a utilização de GRANADEIRA."]},
    "Banco Bahamas": {"regras": ["👥 **Máximo de Bandidos:** 10.", "👮 **Máximo de Policiais:** 14.", "🔫 **Armamento:** Obrigatório RIFLE.", "🤝 **Negociação:** Obrigatória.", "👤 **Refém:** Opcional.", "📌 Proibido a utilização das estações de METRO (Subterraneo).", "📌 Limite de 6 pessoas no Salão.", "📌 Máximo de 4 bandidos na parte de baixo do Banco."], "is_bahamas": True},
    "Burgueshot (Bahamas)": {"regras": ["👥 **Mínimo de Bandidos:** 3.", "👥 **Máximo de Bandidos:** 5.", "👮 **Máximo de Policiais:** 5.", "🔫 **Armamento:** Mínimo pistola.", "🤝 **Negociação:** Obrigatória.", "👤 **Refém:** Opcional."], "is_bahamas": True},
    "Refinaria (Bahamas)": {"regras": ["👥 **Bandidos:** Obrigatório 6.", "👮 **Máximo de policiais:** 7.", "🔫 **Armamento:** Mínimo SMG.", "⚔️ **Negociação:** Inexistente, ação de confronto direto.", "🚫 **Refém:** Proibido.", "📌 Fica proibido o uso de atirador."], "is_bahamas": True},
    "Lan House - (Bahamas)": {"regras": ["👥 **Mínimo de Bandidos:** 6.", "👥 **Máximo de Bandidos:** 8.", "👮 **Máximo de Policiais:** 10.", "🔫 **Armamento:** Mínimo SMG.", "🤝 **Negociação:** Obrigatória.", "👤 **Refém:** Opcional.", "📌 Limite de 4 pessoas dentro da Lan House."], "is_bahamas": True},
    "Lan House - Jersey": {"regras": ["👥 **Mínimo de Bandidos:** 6.", "👥 **Máximo de Bandidos:** 8.", "👮 **Máximo de Policiais:** 10.", "🔫 **Armamento:** Mínimo SMG.", "🤝 **Negociação:** Obrigatória.", "👤 **Refém:** Opcional.", "📌 Limite de 4 pessoas dentro da Lan House."], "is_bahamas": True},
    "Lan House - Brooklyn": {"regras": ["👥 **Mínimo de Bandidos:** 6.", "👥 **Máximo de Bandidos:** 8.", "👮 **Máximo de Policiais:** 10.", "🔫 **Armamento:** Mínimo SMG.", "🤝 **Negociação:** Obrigatória.", "👤 **Refém:** Opcional.", "📌 Limite de 4 pessoas dentro da Lan House."], "is_bahamas": True},
    "Lan House - Manhattan": {"regras": ["👥 **Mínimo de Bandidos:** 6.", "👥 **Máximo de Bandidos:** 8.", "👮 **Máximo de Policiais:** 10.", "🔫 **Armamento:** Mínimo SMG.", "🤝 **Negociação:** Obrigatória.", "👤 **Refém:** Opcional.", "📌 Limite de 4 pessoas dentro da Lan House."], "is_bahamas": True},
    "Museu (Bahamas)": {
        "regras": [
            "👥 **Máximo de Bandidos:** 10.",
            "👮 **Máximo de Policiais:** 15.",
            "🔫 **Armamento:** Obrigatório RIFLE.",
            "⚔️ **Negociação:** Inexistente, ação de confronto direto com a polícia.",
            "🚫 **Refém:** Proibido reféns, o roubo é uma ação de confronto direto com a polícia.",
            "📌 **Obs:** Máximo de 5 bandidos no segundo andar."
        ],
        "is_bahamas": True
    }
}

# =========================================================
# 11.2 FUNÇÕES DE BANCO DE DADOS - AÇÕES
# =========================================================
async def salvar_acao_db(tipo, autor):
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            return await conn.fetchval("INSERT INTO acoes_semana (tipo, data, autor, status) VALUES ($1, $2, $3, 'aberta') RETURNING id", tipo, agora_db(), str(autor))
    except Exception as e:
        logger.error(f"❌ Erro ao salvar ação: {e}")
        return None

async def buscar_acoes_semana():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT tipo, COUNT(*) as qtd FROM acoes_semana WHERE status = 'concluida' AND (resultado = 'ganhou' OR resultado = 'perdeu' OR resultado = 'concluida') GROUP BY tipo")
    except Exception as e:
        logger.error(f"❌ Erro ao buscar ações: {e}")
        return []

async def participar_acao_db(acao_id, user_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO participantes_acoes (acao_id, user_id) VALUES ($1, $2)", acao_id, str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao participar ação: {e}")

async def remover_participante_db(acao_id, user_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM participantes_acoes WHERE acao_id = $1 AND user_id = $2", acao_id, str(user_id))
    except Exception as e:
        logger.error(f"❌ Erro ao remover participante: {e}")

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

async def cancelar_acao_db(acao_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE acoes_semana SET status='cancelada' WHERE id = $1", acao_id)
    except Exception as e:
        logger.error(f"❌ Erro ao cancelar ação: {e}")

async def concluir_acao_db(acao_id, resultado, valor=0):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE acoes_semana SET status='concluida', resultado=$1, valor=$2 WHERE id=$3", resultado, valor, acao_id)
    except Exception as e:
        logger.error(f"❌ Erro ao concluir ação: {e}")

# =========================================================
# 11.3 FUNÇÃO DE VERIFICAÇÃO DE LIMITE
# =========================================================
async def verificar_limite_categoria(acao_tipo):
    categoria = ACAO_PARA_CATEGORIA.get(acao_tipo)
    if not categoria:
        return True
    dados_categoria = CATEGORIAS_ACOES.get(categoria)
    if not dados_categoria:
        return True
    limite = dados_categoria["limite"]
    if limite is None:
        return True
    pool = await get_pool()
    if not pool:
        return True
    async with pool.acquire() as conn:
        acoes_da_categoria = dados_categoria["acoes"]
        placeholders = ",".join([f"${i+1}" for i in range(len(acoes_da_categoria))])
        query = f"SELECT COUNT(*) FROM acoes_semana WHERE tipo IN ({placeholders}) AND status = 'concluida' AND (resultado = 'ganhou' OR resultado = 'perdeu') AND data > NOW() - INTERVAL '7 days'"
        qtd = await conn.fetchval(query, *acoes_da_categoria)
        return qtd < limite

# =========================================================
# 11.4 VIEWS DE AÇÕES
# =========================================================
class FecharButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="❌ Fechar", style=discord.ButtonStyle.danger)

    async def callback(self, interaction: discord.Interaction):
        try:
            await interaction.message.delete()
        except discord.NotFound:
            pass

class SelecionarAcaoView(discord.ui.View):
    def __init__(self, acoes, titulo, emoji):
        super().__init__(timeout=60)
        options = []
        for nome, limite in acoes.items():
            categoria = ACAO_PARA_CATEGORIA.get(nome, "")
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
        pode_fazer = await verificar_limite_categoria(acao_tipo)
        if not pode_fazer:
            categoria = ACAO_PARA_CATEGORIA.get(acao_tipo, "Desconhecida")
            dados_categoria = CATEGORIAS_ACOES.get(categoria, {})
            limite = dados_categoria.get("limite", "?")
            await interaction.followup.send(f"❌ **Limite semanal da categoria {categoria} atingido!**\n📊 Limite: **{limite}** ação(ões) por semana\n📌 Ação: **{acao_tipo}**", ephemeral=True)
            return
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
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
        embed = discord.Embed(title=f"{emoji} ESCALAÇÃO - {acao_tipo}", color=cor, timestamp=agora())
        embed.add_field(name="📌 REGRAS DA AÇÃO", value="\n".join(regras), inline=False)
        if is_bahamas:
            embed.add_field(name="🏝️ REGRAS GERAIS - BAHAMAS", value="""━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
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
1️⃣1️⃣ **Disputa de blips:** Apenas 1 pessoa por facção pode puxar a ação.""", inline=False)
        if "Helicrash" in acao_tipo:
            horario = acao_tipo.split("(")[1].replace(")", "")
            embed.add_field(name="⏰ HORÁRIO", value=f"{horario} (horário de Brasília)", inline=False)
        categoria = ACAO_PARA_CATEGORIA.get(acao_tipo)
        if categoria:
            dados_categoria = CATEGORIAS_ACOES.get(categoria, {})
            limite = dados_categoria.get("limite")
            if limite is not None:
                async with pool.acquire() as conn:
                    acoes_da_categoria = dados_categoria["acoes"]
                    placeholders = ",".join([f"${i+1}" for i in range(len(acoes_da_categoria))])
                    query = f"SELECT COUNT(*) FROM acoes_semana WHERE tipo IN ({placeholders}) AND status = 'concluida' AND (resultado = 'ganhou' OR resultado = 'perdeu') AND data > NOW() - INTERVAL '7 days'"
                    qtd_feita = await conn.fetchval(query, *acoes_da_categoria)
                    restante = max(0, limite - qtd_feita)
                    embed.add_field(name=f"📊 LIMITE DA CATEGORIA {categoria.upper()}", value=f"{qtd_feita}/{limite} ações realizadas\n✅ Restam: {restante}", inline=False)
        embed.add_field(name="👥 PARTICIPANTES (0)", value="Nenhum participante ainda.\nClique no botão ✅ PARTICIPAR para se inscrever!", inline=False)
        embed.add_field(name="👤 CRIADO POR", value=interaction.user.mention, inline=True)
        embed.add_field(name="📅 DATA", value=agora().strftime('%d/%m/%Y %H:%M'), inline=True)
        embed.add_field(name="📝 COMO PARTICIPAR", value="✅ Clique em **'Participar'** para se inscrever na ação.\n📤 Quando a escalação estiver completa, o criador clica em **'Concluir'**.", inline=False)
        embed.set_footer(text=f"ID: {acao_id}")
        canal = interaction.guild.get_channel(CANAL_ESCALACOES_ID)
        if canal:
            view = AcaoView(acao_id, interaction.user.id)
            msg = await canal.send(embed=embed, view=view)
            await BotaoPersistente.salvar_botao(msg.id, canal.id, "acao", {"acao_id": acao_id, "criador_id": interaction.user.id})
            acoes_ativas[acao_id] = {"embed": embed, "criador_id": interaction.user.id}
            await interaction.followup.send(f"✅ Ação **{acao_tipo}** criada com sucesso!", ephemeral=True)
            try:
                await interaction.message.delete()
            except:
                pass
        else:
            await interaction.followup.send("❌ Canal de escalações não encontrado!", ephemeral=True)

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

        pool = await get_pool()
        if not pool:
            await interaction.response.send_message("❌ Banco de dados indisponível!", ephemeral=True)
            return

        async with pool.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.response.send_message("❌ Esta ação já foi concluída ou cancelada!", ephemeral=True)
                return

            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)

            # VERIFICAR SE É HELICRASH
            is_helicrash = "Helicrash" in acao["tipo"]

            if is_helicrash:
                # HELICRASH - Concluir direto sem modal
                await conn.execute("UPDATE acoes_semana SET status='concluida', resultado='concluida', valor=0 WHERE id=$1", self.acao_id)

                participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
                lista_participantes = "\n".join([f"<@{p['user_id']}>" for p in participantes]) if participantes else "Ninguém"

                embed_relatorio = discord.Embed(
                    title="🚁 RELATÓRIO DE HELICRASH",
                    description=f"**{acao['tipo']}**\n\n✅ Evento registrado com sucesso!",
                    color=0xe67e22
                )
                embed_relatorio.add_field(name="🏦 Evento", value=acao["tipo"], inline=False)
                embed_relatorio.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
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

            # =========================================================
            # AÇÕES NORMAIS - CRIAR RELATÓRIO COM BOTÃO DE ADICIONAR
            # =========================================================
            await conn.execute("UPDATE acoes_semana SET status='concluida' WHERE id=$1", self.acao_id)

            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)

            # Criar lista de participantes (só os que clicaram no botão)
            guild = interaction.guild
            lista_final = []
            for p in participantes:
                uid = p["user_id"]
                try:
                    member = guild.get_member(int(uid))
                    if member:
                        apelido = member.display_name
                        lista_final.append(f"👤 {apelido}")
                    else:
                        user = await bot.fetch_user(int(uid))
                        if user:
                            lista_final.append(f"👤 {user.display_name or user.name}")
                        else:
                            lista_final.append(f"👤 ID: {uid}")
                except:
                    lista_final.append(f"👤 ID: {uid}")

            if not lista_final:
                lista_final.append("Nenhum participante")

            participantes_texto = "\n".join(lista_final)

            # =========================================================
            # EMBED DO RELATÓRIO COM BOTÃO DE ADICIONAR
            # =========================================================
            embed_relatorio = discord.Embed(
                title="🚨 ── RELATÓRIO DE AÇÃO ── 🚨",
                description=f"⚔️ **{acao['tipo']}**",
                color=Cores.ACAO,
                timestamp=agora()
            )

            embed_relatorio.set_author(
                name="🛡 Vida Rasa 442 • Ações",
                icon_url=bot.user.display_avatar.url if bot.user else None
            )

            embed_relatorio.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)

            embed_relatorio.add_field(
                name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                value="",
                inline=False
            )

            embed_relatorio.add_field(
                name="🏦 AÇÃO",
                value=f"```yaml\n{acao['tipo']}\n```",
                inline=True
            )

            embed_relatorio.add_field(
                name="👤 CRIADA POR",
                value=f"```yaml\n{interaction.user.display_name}\n```",
                inline=True
            )

            embed_relatorio.add_field(
                name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                value="",
                inline=False
            )

            embed_relatorio.add_field(
                name="👥 PARTICIPANTES",
                value=f"```yaml\n{participantes_texto}\n```",
                inline=False
            )

            embed_relatorio.add_field(
                name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                value="",
                inline=False
            )

            embed_relatorio.add_field(
                name="🎯 STATUS",
                value="```yaml\n⏳ Aguardando resultado\n```",
                inline=False
            )

            embed_relatorio.set_footer(
                text=f"🛡 Vida Rasa 442 • ID: {self.acao_id} • {agora().strftime('%d/%m/%Y %H:%M')}",
                icon_url=bot.user.display_avatar.url if bot.user else None
            )

            # VIEW COM BOTÃO DE ADICIONAR PARTICIPANTES
            view = RelatorioAcaoView(self.acao_id)

            canal_relatorio = interaction.guild.get_channel(CANAL_RELATORIO_ACOES_ID)
            if canal_relatorio:
                msg = await canal_relatorio.send(embed=embed_relatorio, view=view)
                
                # Atualizar a mensagem original com o ID do relatório
                # para referência, mas não precisa salvar nada extra
                
                await interaction.message.delete()
                await interaction.followup.send(
                    f"✅ **Escalação concluída!**\n"
                    f"👥 {len(lista_final)} participantes registrados\n"
                    f"📌 Use o botão **➕ Adicionar Participantes** para incluir mais.",
                    ephemeral=True
                )
            else:
                await interaction.followup.send("❌ Canal de relatório não encontrado!", ephemeral=True)

            await enviar_painel_acoes(interaction.guild)

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
                    embed.set_field_at(i, name=f"👥 Participantes ({len(participantes) if participantes else 0})", value=lista_participantes, inline=False)
                    campo_atualizado = True
                    break
            if not campo_atualizado:
                embed.add_field(name=f"👥 Participantes ({len(participantes) if participantes else 0})", value=lista_participantes, inline=False)
            await mensagem.edit(embed=embed)
        except Exception as e:
            logger.error(f"❌ Erro ao atualizar embed: {e}")

class AdicionarParticipantesManualModal(discord.ui.Modal, title="📝 ADICIONAR PARTICIPANTES"):
    participantes = discord.ui.TextInput(
        label="👥 Nomes dos participantes (um por linha)",
        placeholder="Ex: Ruivo\nDreck\nLeon\nBatman",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=500
    )

    def __init__(self, acao_id, mensagem_original, acao):
        super().__init__(timeout=300)
        self.acao_id = acao_id
        self.mensagem_original = mensagem_original
        self.acao = acao

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return

        try:
            async with pool.acquire() as conn:
                # Atualizar status da ação para concluída
                await conn.execute("UPDATE acoes_semana SET status='concluida' WHERE id=$1", self.acao_id)

                # Buscar participantes que clicaram no botão
                participantes_botao = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
                ids_botao = [p["user_id"] for p in participantes_botao]

                # Processar nomes manuais
                nomes_manuais = []
                if self.participantes.value and self.participantes.value.strip():
                    nomes_manuais = [nome.strip() for nome in self.participantes.value.split('\n') if nome.strip()]
                    
                    # =========================================================
                    # SALVAR NOMES MANUAIS NO BANCO
                    # =========================================================
                    for nome in nomes_manuais:
                        await conn.execute(
                            "INSERT INTO acoes_participantes_manuais (acao_id, nome) VALUES ($1, $2)",
                            self.acao_id, nome
                        )

                # =========================================================
                # CRIAR LISTA DE PARTICIPANTES (BOTÃO + MANUAIS)
                # =========================================================
                lista_final = []

                # Adicionar os que clicaram no botão (com apelido)
                guild = interaction.guild
                for uid in ids_botao:
                    try:
                        member = guild.get_member(int(uid))
                        if member:
                            apelido = member.display_name
                            lista_final.append(f"👤 {apelido}")
                        else:
                            user = await bot.fetch_user(int(uid))
                            if user:
                                lista_final.append(f"👤 {user.display_name or user.name}")
                            else:
                                lista_final.append(f"👤 ID: {uid}")
                    except:
                        lista_final.append(f"👤 ID: {uid}")

                # Adicionar os nomes manuais
                for nome in nomes_manuais:
                    if nome:
                        lista_final.append(f"📝 {nome}")

                # Se não tiver ninguém
                if not lista_final:
                    lista_final.append("Nenhum participante")

                lista_participantes = "\n".join(lista_final)

                # =========================================================
                # CRIAR RELATÓRIO BONITO
                # =========================================================
                embed_relatorio = discord.Embed(
                    title="🚨 ── RELATÓRIO DE AÇÃO ── 🚨",
                    description=f"⚔️ **{self.acao['tipo']}**",
                    color=Cores.ACAO,
                    timestamp=agora()
                )

                embed_relatorio.set_author(
                    name="🛡 Vida Rasa 442 • Ações",
                    icon_url=bot.user.display_avatar.url if bot.user else None
                )

                embed_relatorio.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)

                embed_relatorio.add_field(
                    name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    value="",
                    inline=False
                )

                embed_relatorio.add_field(
                    name="🏦 AÇÃO",
                    value=f"```yaml\n{self.acao['tipo']}\n```",
                    inline=True
                )

                embed_relatorio.add_field(
                    name="👤 CRIADA POR",
                    value=f"```yaml\n{interaction.user.display_name}\n```",
                    inline=True
                )

                embed_relatorio.add_field(
                    name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    value="",
                    inline=False
                )

                embed_relatorio.add_field(
                    name="👥 PARTICIPANTES",
                    value=f"```yaml\n{lista_participantes}\n```",
                    inline=False
                )

                embed_relatorio.add_field(
                    name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                    value="",
                    inline=False
                )

                embed_relatorio.add_field(
                    name="🎯 STATUS",
                    value="```yaml\n⏳ Aguardando finalização\n```",
                    inline=False
                )

                embed_relatorio.set_footer(
                    text=f"🛡 Vida Rasa 442 • ID: {self.acao_id} • {agora().strftime('%d/%m/%Y %H:%M')}",
                    icon_url=bot.user.display_avatar.url if bot.user else None
                )

                canal_relatorio = interaction.guild.get_channel(CANAL_RELATORIO_ACOES_ID)
                if canal_relatorio:
                    msg = await canal_relatorio.send(embed=embed_relatorio, view=None)
                    await msg.edit(view=ResultadoAcaoView(self.acao_id, msg))

                    # Deletar mensagem da escalação
                    try:
                        await self.mensagem_original.delete()
                    except:
                        pass

                    await interaction.followup.send(
                        f"✅ **Escalação concluída!**\n"
                        f"👥 {len(lista_final)} participantes registrados",
                        ephemeral=True
                    )
                else:
                    await interaction.followup.send("❌ Canal de relatório não encontrado!", ephemeral=True)

                await enviar_painel_acoes(interaction.guild)

        except Exception as e:
            logger.error(f"❌ Erro ao concluir ação: {e}")
            await interaction.followup.send(f"❌ Erro ao concluir ação: {str(e)[:100]}", ephemeral=True)

class RelatorioAcaoView(discord.ui.View):
    def __init__(self, acao_id):
        super().__init__(timeout=None)
        self.acao_id = acao_id

    @discord.ui.button(label="➕ Adicionar Participantes", style=discord.ButtonStyle.primary, custom_id="relatorio_adicionar_participantes", emoji="➕", row=0)
    async def adicionar_participantes(self, interaction: discord.Interaction, button: discord.ui.Button):
        cargos_permitidos = [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID, CARGO_01_ID, CARGO_02_ID]
        is_gerente = any(r.id in cargos_permitidos for r in interaction.user.roles)
        is_admin = interaction.user.guild_permissions.administrator

        pool = await get_pool()
        if pool:
            async with pool.acquire() as conn:
                acao = await conn.fetchrow("SELECT autor FROM acoes_semana WHERE id=$1", self.acao_id)
                is_criador = str(interaction.user.id) == acao["autor"] if acao else False
        else:
            is_criador = False

        if not is_gerente and not is_admin and not is_criador:
            await interaction.response.send_message(
                "❌ **Apenas Gerentes, Cargo 01, Cargo 02, ADM ou o criador da ação podem adicionar participantes!**",
                ephemeral=True
            )
            return

        if pool:
            async with pool.acquire() as conn:
                acao = await conn.fetchrow("SELECT resultado FROM acoes_semana WHERE id=$1", self.acao_id)
                if acao and acao["resultado"] in ["ganhou", "perdeu"]:
                    await interaction.response.send_message(
                        "❌ **Esta ação já foi finalizada!** Não é possível adicionar mais participantes.",
                        ephemeral=True
                    )
                    return

        modal = AdicionarParticipantesRelatorioModal(self.acao_id, interaction.message)
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="🏆 Ganhou", style=discord.ButtonStyle.success, custom_id="relatorio_ganhou", emoji="🏆", row=1)
    async def ganhou(self, interaction: discord.Interaction, button: discord.ui.Button):
        cargos_permitidos = [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID, CARGO_01_ID, CARGO_02_ID]
        is_gerente = any(r.id in cargos_permitidos for r in interaction.user.roles)
        is_admin = interaction.user.guild_permissions.administrator

        pool = await get_pool()
        if pool:
            async with pool.acquire() as conn:
                acao = await conn.fetchrow("SELECT autor FROM acoes_semana WHERE id=$1", self.acao_id)
                is_criador = str(interaction.user.id) == acao["autor"] if acao else False
        else:
            is_criador = False

        if not is_gerente and not is_admin and not is_criador:
            await interaction.response.send_message(
                "❌ **Apenas Gerentes, Cargo 01, Cargo 02, ADM ou o criador da ação podem finalizar!**",
                ephemeral=True
            )
            return

        if pool:
            async with pool.acquire() as conn:
                acao = await conn.fetchrow("SELECT resultado FROM acoes_semana WHERE id=$1", self.acao_id)
                if acao and acao["resultado"] in ["ganhou", "perdeu"]:
                    await interaction.response.send_message(
                        "❌ **Esta ação já foi finalizada!**",
                        ephemeral=True
                    )
                    return

        await interaction.response.send_modal(ResultadoGanhouModal(self.acao_id, interaction.message))

    @discord.ui.button(label="💀 Perdeu", style=discord.ButtonStyle.danger, custom_id="relatorio_perdeu", emoji="💀", row=1)
    async def perdeu(self, interaction: discord.Interaction, button: discord.ui.Button):
        cargos_permitidos = [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID, CARGO_01_ID, CARGO_02_ID]
        is_gerente = any(r.id in cargos_permitidos for r in interaction.user.roles)
        is_admin = interaction.user.guild_permissions.administrator

        pool = await get_pool()
        if pool:
            async with pool.acquire() as conn:
                acao = await conn.fetchrow("SELECT autor FROM acoes_semana WHERE id=$1", self.acao_id)
                is_criador = str(interaction.user.id) == acao["autor"] if acao else False
        else:
            is_criador = False

        if not is_gerente and not is_admin and not is_criador:
            await interaction.response.send_message(
                "❌ **Apenas Gerentes, Cargo 01, Cargo 02, ADM ou o criador da ação podem finalizar!**",
                ephemeral=True
            )
            return

        if pool:
            async with pool.acquire() as conn:
                acao = await conn.fetchrow("SELECT resultado FROM acoes_semana WHERE id=$1", self.acao_id)
                if acao and acao["resultado"] in ["ganhou", "perdeu"]:
                    await interaction.response.send_message(
                        "❌ **Esta ação já foi finalizada!**",
                        ephemeral=True
                    )
                    return

        await interaction.response.send_modal(ResultadoPerdeuModal(self.acao_id, interaction.message))

class AdicionarParticipantesRelatorioModal(discord.ui.Modal, title="📝 ADICIONAR PARTICIPANTES"):
    participantes = discord.ui.TextInput(
        label="👥 Nomes dos participantes (um por linha)",
        placeholder="Ex: Ruivo\nDreck\nLeon\nBatman",
        style=discord.TextStyle.paragraph,
        required=False,
        max_length=500
    )

    def __init__(self, acao_id, mensagem_original):
        super().__init__(timeout=300)
        self.acao_id = acao_id
        self.mensagem_original = mensagem_original

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return

        try:
            async with pool.acquire() as conn:
                acao = await conn.fetchrow("SELECT resultado FROM acoes_semana WHERE id=$1", self.acao_id)
                if acao and acao["resultado"] in ["ganhou", "perdeu"]:
                    await interaction.followup.send(
                        "❌ **Esta ação já foi finalizada!** Não é possível adicionar mais participantes.",
                        ephemeral=True
                    )
                    return

                nomes_manuais = []
                if self.participantes.value and self.participantes.value.strip():
                    nomes_manuais = [nome.strip() for nome in self.participantes.value.split('\n') if nome.strip()]

                if not nomes_manuais:
                    await interaction.followup.send("❌ **Nenhum nome informado!**", ephemeral=True)
                    return

                for nome in nomes_manuais:
                    await conn.execute(
                        "INSERT INTO acoes_participantes_manuais (acao_id, nome) VALUES ($1, $2)",
                        self.acao_id, nome
                    )

                participantes_botao = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
                participantes_manuais = await conn.fetch("SELECT nome FROM acoes_participantes_manuais WHERE acao_id=$1", self.acao_id)
                acao_info = await conn.fetchrow("SELECT tipo FROM acoes_semana WHERE id=$1", self.acao_id)

            guild = interaction.guild
            lista_final = []

            for p in participantes_botao:
                uid = p["user_id"]
                try:
                    member = guild.get_member(int(uid))
                    if member:
                        lista_final.append(f"👤 {member.display_name}")
                    else:
                        user = await bot.fetch_user(int(uid))
                        if user:
                            lista_final.append(f"👤 {user.display_name or user.name}")
                        else:
                            lista_final.append(f"👤 ID: {uid}")
                except:
                    lista_final.append(f"👤 ID: {uid}")

            for p in participantes_manuais:
                if p["nome"]:
                    lista_final.append(f"📝 {p['nome']}")

            if not lista_final:
                lista_final.append("Nenhum participante")

            participantes_texto = "\n".join(lista_final)

            embed = self.mensagem_original.embeds[0]
            for i, field in enumerate(embed.fields):
                if field.name == "👥 PARTICIPANTES":
                    embed.set_field_at(i, name="👥 PARTICIPANTES", value=f"```yaml\n{participantes_texto}\n```", inline=False)
                    break

            embed.set_footer(
                text=f"🛡 Vida Rasa 442 • ID: {self.acao_id} • Atualizado em {agora().strftime('%d/%m/%Y %H:%M')}",
                icon_url=bot.user.display_avatar.url if bot.user else None
            )

            await self.mensagem_original.edit(embed=embed)

            await interaction.followup.send(
                f"✅ **{len(nomes_manuais)} participantes adicionados!**\n"
                f"👥 {len(lista_final)} participantes no total.",
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"❌ Erro ao adicionar participantes: {e}")
            await interaction.followup.send(f"❌ Erro ao adicionar participantes: {str(e)[:100]}", ephemeral=True)

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
        
        # ABRIR MODAL SÓ COM O VALOR (SEM DIVIDIR)
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

class ResultadoGanhouModal(discord.ui.Modal, title="🎉 Resultado - GANHOU"):
    dinheiro = discord.ui.TextInput(
        label="💰 Valor total ganho",
        placeholder="Ex: 50000",
        required=True
    )

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

            limite = ACOES_COMPLEXO.get(acao["tipo"]) or ACOES_BAHAMAS.get(acao["tipo"]) or ACOES_HELICRASH.get(acao["tipo"])
            if limite and limite is not None:
                qtd_feita = await conn.fetchval(
                    "SELECT COUNT(*) FROM acoes_semana WHERE tipo=$1 AND resultado='ganhou' AND id != $2",
                    acao["tipo"], self.acao_id
                )
                if qtd_feita >= limite:
                    await interaction.followup.send(
                        f"❌ Ação **{acao['tipo']}** já atingiu o limite semanal de **{limite}** vitória(s)!",
                        ephemeral=True
                    )
                    return

            await conn.execute(
                "UPDATE acoes_semana SET valor=$1, resultado='ganhou' WHERE id=$2",
                valor_total, self.acao_id
            )

            participantes_botao = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            participantes_manuais = await conn.fetch("SELECT nome FROM acoes_participantes_manuais WHERE acao_id=$1", self.acao_id)

        guild = interaction.guild
        lista_participantes = []

        for p in participantes_botao:
            uid = p["user_id"]
            try:
                member = guild.get_member(int(uid))
                if member:
                    lista_participantes.append(f"👤 {member.display_name}")
                else:
                    user = await bot.fetch_user(int(uid))
                    if user:
                        lista_participantes.append(f"👤 {user.display_name or user.name}")
                    else:
                        lista_participantes.append(f"👤 ID: {uid}")
            except:
                lista_participantes.append(f"👤 ID: {uid}")

        for p in participantes_manuais:
            if p["nome"]:
                lista_participantes.append(f"📝 {p['nome']}")

        if not lista_participantes:
            lista_participantes.append("Nenhum participante")

        participantes_texto = "\n".join(lista_participantes)

        embed = discord.Embed(
            title="🏆 ── RESULTADO DA AÇÃO ── 🏆",
            description=f"⚔️ **{acao['tipo']}**",
            color=Cores.SUCESSO,
            timestamp=agora()
        )

        embed.set_author(
            name="🛡 Vida Rasa 442 • Ações",
            icon_url=bot.user.display_avatar.url if bot.user else None
        )

        embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)

        embed.add_field(
            name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            value="",
            inline=False
        )

        embed.add_field(
            name="🏦 AÇÃO",
            value=f"```yaml\n{acao['tipo']}\n```",
            inline=True
        )

        embed.add_field(
            name="💰 TOTAL GANHO",
            value=f"```yaml\n{formatar_dinheiro(valor_total)}\n```",
            inline=True
        )

        embed.add_field(
            name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            value="",
            inline=False
        )

        embed.add_field(
            name="👥 PARTICIPANTES",
            value=f"```yaml\n{participantes_texto}\n```",
            inline=False
        )

        embed.add_field(
            name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            value="",
            inline=False
        )

        embed.add_field(
            name="📌 STATUS",
            value="```yaml\n✅ AÇÃO FINALIZADA\n🎉 RESULTADO: GANHOU\n```",
            inline=False
        )

        embed.set_footer(
            text=f"🛡 Vida Rasa 442 • ID: {self.acao_id} • Finalizada por {interaction.user.display_name} • {agora().strftime('%d/%m/%Y %H:%M')}",
            icon_url=bot.user.display_avatar.url if bot.user else None
        )

        await self.mensagem_original.edit(embed=embed, view=None)
        await enviar_painel_acoes(interaction.guild)

        await interaction.followup.send(
            f"✅ **Ação registrada como GANHA!**\n"
            f"💰 Valor: {formatar_dinheiro(valor_total)}\n"
            f"📌 **Valor vai para o caixa da facção.**",
            ephemeral=True
        )        
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
            await conn.execute(
                "UPDATE acoes_semana SET valor=0, resultado='perdeu' WHERE id=$1",
                self.acao_id
            )

            participantes_botao = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            participantes_manuais = await conn.fetch("SELECT nome FROM acoes_participantes_manuais WHERE acao_id=$1", self.acao_id)
            acao = await conn.fetchrow("SELECT tipo FROM acoes_semana WHERE id=$1", self.acao_id)

        guild = interaction.guild
        lista_participantes = []

        for p in participantes_botao:
            uid = p["user_id"]
            try:
                member = guild.get_member(int(uid))
                if member:
                    lista_participantes.append(f"👤 {member.display_name}")
                else:
                    user = await bot.fetch_user(int(uid))
                    if user:
                        lista_participantes.append(f"👤 {user.display_name or user.name}")
                    else:
                        lista_participantes.append(f"👤 ID: {uid}")
            except:
                lista_participantes.append(f"👤 ID: {uid}")

        for p in participantes_manuais:
            if p["nome"]:
                lista_participantes.append(f"📝 {p['nome']}")

        if not lista_participantes:
            lista_participantes.append("Nenhum participante")

        participantes_texto = "\n".join(lista_participantes)

        embed = discord.Embed(
            title="💀 ── RESULTADO DA AÇÃO ── 💀",
            description=f"⚔️ **{acao['tipo']}**",
            color=Cores.ERRO,
            timestamp=agora()
        )

        embed.set_author(
            name="🛡 Vida Rasa 442 • Ações",
            icon_url=bot.user.display_avatar.url if bot.user else None
        )

        embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)

        embed.add_field(
            name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            value="",
            inline=False
        )

        embed.add_field(
            name="🏦 AÇÃO",
            value=f"```yaml\n{acao['tipo']}\n```",
            inline=True
        )

        embed.add_field(
            name="💰 TOTAL",
            value=f"```yaml\nR$ 0,00\n```",
            inline=True
        )

        embed.add_field(
            name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            value="",
            inline=False
        )

        embed.add_field(
            name="👥 PARTICIPANTES",
            value=f"```yaml\n{participantes_texto}\n```",
            inline=False
        )

        embed.add_field(
            name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
            value="",
            inline=False
        )

        embed.add_field(
            name="📌 STATUS",
            value="```yaml\n✅ AÇÃO FINALIZADA\n💀 RESULTADO: PERDEU\n```",
            inline=False
        )

        embed.set_footer(
            text=f"🛡 Vida Rasa 442 • ID: {self.acao_id} • Finalizada por {interaction.user.display_name} • {agora().strftime('%d/%m/%Y %H:%M')}",
            icon_url=bot.user.display_avatar.url if bot.user else None
        )

        await self.mensagem_original.edit(embed=embed, view=None)
        await enviar_painel_acoes(interaction.guild)

        await interaction.followup.send(
            f"✅ **Ação registrada como PERDIDA!**\n"
            f"💀 Nenhum valor foi gerado.",
            ephemeral=True
        )
        
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

    @discord.ui.button(label="♻️ Resetar Ações", style=discord.ButtonStyle.danger, custom_id="acoes_reset", emoji="♻️", row=1)
    async def reset(self, interaction: discord.Interaction, button):
        # VERIFICAR PERMISSÃO: Gerente, Cargo 01, Cargo 02 ou ADM
        cargos_permitidos = [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID, CARGO_01_ID, CARGO_02_ID]
        is_gerente = any(r.id in cargos_permitidos for r in interaction.user.roles)
        is_admin = interaction.user.guild_permissions.administrator

        if not is_gerente and not is_admin:
            await interaction.response.send_message(
                "❌ **Apenas Gerentes, Cargo 01, Cargo 02 e ADM podem resetar as ações!**",
                ephemeral=True
            )
            return

        # PEDIR CONFIRMAÇÃO
        view = ConfirmarResetAcoesView()
        embed = discord.Embed(
            title="⚠️ RESETAR AÇÕES",
            description="**ATENÇÃO!** Esta ação irá **APAGAR TODAS AS AÇÕES** da semana atual.\n\n"
                        "📌 **O que será resetado:**\n"
                        "• Todas as ações em andamento\n"
                        "• Todas as ações concluídas\n"
                        "• Todos os participantes\n"
                        "• Todos os limites semanais\n\n"
                        "⚠️ **Esta ação é IRREVERSÍVEL!**\n\n"
                        "Clique em **✅ CONFIRMAR** para continuar.",
            color=0xe74c3c,
            timestamp=agora()
        )
        embed.set_footer(text="Vida Rasa 442 • Reset de Ações")
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class ConfirmarResetAcoesView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)

    @discord.ui.button(label="✅ CONFIRMAR RESET", style=discord.ButtonStyle.danger, custom_id="confirmar_reset_acoes", emoji="✅")
    async def confirmar(self, interaction: discord.Interaction, button):
        await interaction.response.defer(ephemeral=True)

        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return

        try:
            async with pool.acquire() as conn:
                # Resetar tudo
                await conn.execute("DELETE FROM participantes_acoes")
                await conn.execute("DELETE FROM acoes_semana")

            # Atualizar o painel
            await enviar_painel_acoes(interaction.guild)

            embed = discord.Embed(
                title="♻️ AÇÕES RESETADAS COM SUCESSO!",
                description="✅ **Todas as ações foram removidas do sistema.**\n\n"
                            "📌 **O que foi resetado:**\n"
                            "• Todas as ações em andamento ❌\n"
                            "• Todas as ações concluídas ❌\n"
                            "• Todos os participantes ❌\n"
                            "• Todos os limites semanais ❌\n\n"
                            "🔄 **O painel foi atualizado automaticamente.**",
                color=0x2ecc71,
                timestamp=agora()
            )
            embed.set_footer(text=f"Reset realizado por {interaction.user.display_name}")
            await interaction.followup.send(embed=embed, ephemeral=True)

        except Exception as e:
            logger.error(f"❌ Erro ao resetar ações: {e}")
            await interaction.followup.send(f"❌ **Erro ao resetar ações:** {str(e)[:100]}", ephemeral=True)

    @discord.ui.button(label="❌ CANCELAR", style=discord.ButtonStyle.secondary, custom_id="cancelar_reset_acoes", emoji="❌")
    async def cancelar(self, interaction: discord.Interaction, button):
        await interaction.response.send_message("❌ **Reset cancelado.**", ephemeral=True)
        try:
            await interaction.message.delete()
        except:
            pass

# =========================================================
# 11.5 FUNÇÕES DE RESTAURAR AÇÕES
# =========================================================
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
                            view = AcaoView(acao_id, criador_id)
                            await msg.edit(view=view)
                            contador += 1
                            await asyncio.sleep(1.0)
                    except:
                        pass
        logger.info(f"✅ {contador} ações restauradas com botões!")
    except Exception as e:
        logger.error(f"❌ Erro ao restaurar ações: {e}")

# =========================================================
# 11.6 FUNÇÃO DE ENVIAR PAINEL DE AÇÕES
# =========================================================
async def enviar_painel_acoes(guild):
    canal = guild.get_channel(CANAL_ESCALACOES_ID)
    if not canal:
        logger.error("❌ Canal ações não encontrado")
        return
    pool = await get_pool()
    if not pool:
        return
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT tipo, COUNT(*) as qtd FROM acoes_semana WHERE status = 'concluida' AND (resultado = 'ganhou' OR resultado = 'perdeu') AND data > NOW() - INTERVAL '7 days' GROUP BY tipo")
    feitas = {r["tipo"]: r["qtd"] for r in rows}
    descricao = "**📊 AÇÕES DA SEMANA - POR CATEGORIA**\n\n"
    total_geral_feitas = 0
    total_geral_meta = 0
    for categoria, dados in CATEGORIAS_ACOES.items():
        limite = dados["limite"]
        acoes = dados["acoes"]
        emoji = dados["emoji"]
        qtd_feita = sum(feitas.get(acao, 0) for acao in acoes)
        total_geral_feitas += qtd_feita
        if limite is None:
            descricao += f"**{emoji} {categoria}:** {qtd_feita} realizadas (ILIMITADO)\n"
        else:
            total_geral_meta += limite
            restante = max(0, limite - qtd_feita)
            status = "✅ COMPLETO" if qtd_feita >= limite else f"⏳ {restante} restantes"
            descricao += f"**{emoji} {categoria}:** {qtd_feita}/{limite} - {status}\n"
    if total_geral_meta > 0:
        porcentagem = int((total_geral_feitas / total_geral_meta) * 100)
        barra_progresso = "▓" * (porcentagem // 5) + "░" * (20 - (porcentagem // 5))
        descricao += f"\n**📊 PROGRESSO GERAL:** {porcentagem}% {barra_progresso}"
        descricao += f"\n{total_geral_feitas}/{total_geral_meta} ações realizadas"
    embed = discord.Embed(title="📊 AÇÕES DA SEMANA", description=descricao, color=0x2ecc71, timestamp=agora())
    embed.set_footer(text=f"Atualizado em {agora().strftime('%d/%m/%Y %H:%M')}")

    # VIEW COM BOTÃO DE RESET
    view = PainelAcoesView()  # ← AGORA TEM O BOTÃO DE RESET
    await enviar_ou_atualizar_painel("painel_acoes", CANAL_ESCALACOES_ID, embed, view)
# =========================================================
# ==================== PARTE 12: SISTEMA DE VENDAS ========
# =========================================================

# =========================================================
# 12.1 CONSTANTES DAS VENDAS
# =========================================================
ORGANIZACOES_CONFIG = {
    "VDR": {"emoji": "🕴️", "cor": 0x1e3a8a},
    "POLICIA": {"emoji": "👮", "cor": 0x3498db},
    "MAFIA": {"emoji": "🤵", "cor": 0x8e44ad},
    "BALAS": {"emoji": "🔫", "cor": 0xe67e22},
    "FAMILIA": {"emoji": "👨‍👩‍👧‍👦", "cor": 0x2ecc71}
}

# =========================================================
# 12.2 FUNÇÕES DE BANCO DE DADOS - VENDAS
# =========================================================
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

async def salvar_venda_db(vendedor_id, valor, pedido_numero):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO vendas (user_id, valor, data, pedido_numero) VALUES ($1, $2, $3, $4)", vendedor_id, valor, agora_db().strftime("%d/%m/%Y"), pedido_numero)
    except Exception as e:
        logger.error(f"❌ Erro ao salvar venda: {e}")

async def atualizar_valor_venda_db(pedido_numero, valor):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE vendas SET valor=$1 WHERE pedido_numero=$2", valor, pedido_numero)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar venda: {e}")

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
                "INSERT INTO entregas_parceladas (pedido_original, entrega_atual, total_entregas, pt_por_entrega, sub_por_entrega, vendedor_id, organizacao, observacoes, proxima_entrega, canal_id) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id",
                pedido_original, 1, total_entregas, pt_por_entrega, sub_por_entrega, vendedor_id, organizacao, observacoes, proxima_naive, canal_id
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar entrega parcelada: {e}")
        return None

async def buscar_entregas_pendentes():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT * FROM entregas_parceladas WHERE ativo = true AND proxima_entrega <= NOW() ORDER BY proxima_entrega ASC")
    except Exception as e:
        logger.error(f"❌ Erro ao buscar entregas pendentes: {e}")
        return []

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
            await conn.execute("UPDATE entregas_parceladas SET entrega_atual = $1, mensagem_ids = array_append(mensagem_ids, $2), proxima_entrega = $3 WHERE id = $4", entrega_atual, mensagem_id, proxima_naive, entrega_id)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar entrega parcelada: {e}")

async def finalizar_entregas(entrega_id):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE entregas_parceladas SET ativo = false WHERE id = $1", entrega_id)
    except Exception as e:
        logger.error(f"❌ Erro ao finalizar entregas: {e}")

async def salvar_entrega_detalhes(entrega_id, entregas_json):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO entregas_detalhes (entrega_id, entregas_json) VALUES ($1, $2) ON CONFLICT (entrega_id) DO UPDATE SET entregas_json = $2", entrega_id, entregas_json)
    except Exception as e:
        logger.error(f"❌ Erro ao salvar detalhes da entrega: {e}")

async def registrar_saida_estoque(pedido_numero, tipo, pacotes, retirado_por):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO saida_estoque (pedido_numero, tipo, pacotes, retirado_por, data) VALUES ($1, $2, $3, $4, NOW())", pedido_numero, tipo, pacotes, str(retirado_por))
            await atualizar_estoque(tipo, pacotes, "remover")
    except Exception as e:
        logger.error(f"❌ Erro ao registrar saída de estoque: {e}")

async def verificar_estoque_suficiente(tipo, pacotes_necessarios):
    estoque = await carregar_estoque()
    return estoque.get(tipo, 0) >= pacotes_necessarios

# =========================================================
# 12.3 MODAL DE CONFIRMAÇÃO DE TRANSFERÊNCIA
# =========================================================
class ConfirmarTransferenciaModal(discord.ui.Modal, title="📤 CONFIRMAR TRANSFERÊNCIA"):
    transferido_para = discord.ui.TextInput(
        label="📤 Transferido para:",
        placeholder="Digite o nome da pessoa (ex: Dreck ou Leon)",
        required=True,
        max_length=50
    )

    def __init__(self, interaction, mensagem_original, entrega_id, pedido_numero, valor, pt, sub):
        super().__init__(timeout=300)
        self.interaction = interaction
        self.mensagem_original = mensagem_original
        self.entrega_id = entrega_id
        self.pedido_numero = pedido_numero
        self.valor_total = valor
        self.pt = pt
        self.sub = sub

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            transferido_para = self.transferido_para.value.strip().upper()
            if not transferido_para:
                await interaction.followup.send("❌ **Nome inválido!** Digite o nome da pessoa que recebeu a transferência.", ephemeral=True)
                return

            embed = self.mensagem_original.embeds[0]
            embed.color = 0x2ecc71

            # =========================================================
            # ATUALIZAR STATUS DO PEDIDO
            # =========================================================
            for i, field in enumerate(embed.fields):
                if field.name == "📌 STATUS DO PEDIDO":
                    embed.set_field_at(
                        i,
                        name="📌 STATUS DO PEDIDO",
                        value=f"✅ **TRANSFERÊNCIA CONFIRMADA**\n📤 **Transferido para:** {transferido_para}\n👤 **Confirmado por:** {interaction.user.display_name}\n💰 **Valor:** {formatar_dinheiro(self.valor_total)}\n📅 **Data:** {agora().strftime('%d/%m/%Y %H:%M')}",
                        inline=False
                    )
                    break

            # =========================================================
            # ATUALIZAR TÍTULO
            # =========================================================
            if "ENTREGA" in embed.title:
                embed.title = f"✅ {embed.title} - TRANSFERÊNCIA CONFIRMADA"
            else:
                embed.title = f"✅ {embed.title} - TRANSFERÊNCIA CONFIRMADA"

            # =========================================================
            # REMOVER CAMPOS ANTIGOS E ADICIONAR TRANSFERÊNCIA
            # =========================================================
            indices_remover = []
            for i, field in enumerate(embed.fields):
                if field.name == "✅ VENDA FINALIZADA COM SUCESSO":
                    indices_remover.append(i)
                if field.name == "━━━━━━━━━━━━━━━━━━━━━━━━━━":
                    indices_remover.append(i)

            for i in sorted(indices_remover, reverse=True):
                embed.remove_field(i)

            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed.add_field(
                name="💰 TRANSFERÊNCIA REALIZADA",
                value=f"✅ **Pagamento enviado para:** {transferido_para}\n👤 **Confirmado por:** {interaction.user.display_name}\n💵 **Valor:** {formatar_dinheiro(self.valor_total)}",
                inline=False
            )
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="🔥 **Pedido encerrado no sistema**", inline=False)

            # =========================================================
            # VIEW DESABILITADA
            # =========================================================
            view = StatusView(
                disabled=True,
                entrega_id=self.entrega_id,
                total_entregas=1,
                entrega_atual=1,
                pago_ja_clicado=True,
                mensagem_original=self.mensagem_original,
                transferencia_confirmada=True,
                valor_total=self.valor_total,
                pt=self.pt,
                sub=self.sub,
                pedido_numero=self.pedido_numero,
                entregue_ja_clicado=True
            )

            await self.mensagem_original.edit(embed=embed, view=view)

            # =========================================================
            # LOG NO CANAL DE LOGS
            # =========================================================
            canal_log = interaction.guild.get_channel(CANAL_LOGS_GERAIS_ID)
            if canal_log:
                embed_log = discord.Embed(
                    title="📤 TRANSFERÊNCIA CONFIRMADA",
                    description=f"📦 **Pedido #{self.pedido_numero:04d}**",
                    color=0x2ecc71,
                    timestamp=agora()
                )
                embed_log.add_field(name="👤 Confirmado por", value=interaction.user.mention, inline=True)
                embed_log.add_field(name="📤 Transferido para", value=transferido_para, inline=True)
                embed_log.add_field(name="💰 Valor", value=formatar_dinheiro(self.valor_total), inline=True)
                embed_log.add_field(name="🔫 PT", value=f"{fmt_num(self.pt)} munições", inline=True)
                embed_log.add_field(name="🔫 SUB", value=f"{fmt_num(self.sub)} munições", inline=True)
                embed_log.set_footer(text=f"Transferência confirmada em {agora().strftime('%d/%m/%Y %H:%M')}")
                await canal_log.send(embed=embed_log)

            await interaction.followup.send(
                f"✅ **Transferência confirmada com sucesso!**\n"
                f"📤 Transferido para: **{transferido_para}**\n"
                f"👤 Confirmado por: **{interaction.user.display_name}**\n"
                f"💰 Valor: {formatar_dinheiro(self.valor_total)}\n"
                f"📦 Pedido #{self.pedido_numero:04d}",
                ephemeral=True
            )

        except Exception as e:
            logger.error(f"❌ Erro ao confirmar transferência: {e}")
            await interaction.followup.send(f"❌ **Erro ao confirmar transferência:** {str(e)[:100]}", ephemeral=True)
            
# =========================================================
# 12.4 FUNÇÃO DE CRIAR EMBED DE ENTREGA (COM BOTÃO DE TRANSFERÊNCIA)
# =========================================================
async def criar_embed_entrega(interaction, pedido_numero, entrega_atual, total_entregas, pt, sub, org_nome, config, observacoes, entrega_id=None, vendedor_id=None, grupo=None, entregas_lista=None):
    canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
    if not canal:
        await interaction.followup.send("❌ Canal de encomendas não encontrado!", ephemeral=True)
        return
    pacotes_pt = pt // 50
    pacotes_sub = sub // 50
    cor = config.get("cor", Cores.VENDA)
    emoji_org = config.get("emoji", "🏷️")
    vendedor_nome = "Desconhecido"
    if vendedor_id:
        try:
            user = await bot.fetch_user(int(vendedor_id))
            if user:
                guild = interaction.guild
                member = guild.get_member(int(vendedor_id))
                if member and member.display_name:
                    vendedor_nome = member.display_name
                else:
                    vendedor_nome = user.display_name or user.name
        except:
            vendedor_nome = str(vendedor_id)
    else:
        vendedor_nome = interaction.user.display_name
    if total_entregas > 1:
        titulo = f"📦 ENTREGA {entrega_atual}/{total_entregas} • Pedido #{pedido_numero:04d}"
        descricao = f"**🔴 ATENÇÃO! Esta venda tem {total_entregas} entregas no total!**\n📦 **Esta entrega contém:** PT {fmt_num(pt)} + SUB {fmt_num(sub)} munições"
    else:
        titulo = f"📦 NOVA ENCOMENDA • Pedido #{pedido_numero:04d}"
        descricao = "✅ Entrega única"
    embed = discord.Embed(title=titulo, description=descricao, color=cor, timestamp=agora())
    if org_nome == "VDR":
        embed.set_thumbnail(url="https://i.imgur.com/vdr_logo.png")
    elif org_nome == "POLICIA":
        embed.set_thumbnail(url="https://i.imgur.com/policia_logo.png")
    elif org_nome == "MAFIA":
        embed.set_thumbnail(url="https://i.imgur.com/mafia_logo.png")
    else:
        embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
    embed.set_author(name=f"{emoji_org} {org_nome} • Sistema de Encomendas", icon_url=bot.user.display_avatar.url if bot.user else None)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
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
        embed.add_field(name=f"{Emojis.ESTATISTICA} RESUMO DAS ENTREGAS", value=f"```yaml\n{resumo}\n```", inline=False)
        embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(name=f"{Emojis.USER} VENDEDOR", value=f"```yaml\n{vendedor_nome}\n```", inline=True)
    embed.add_field(name=f"{Emojis.LOCAL} ORGANIZAÇÃO", value=f"```yaml\n{emoji_org} {org_nome}\n```", inline=True)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(name=f"🔫 PT", value=f"```yaml\n{fmt_num(pt)} munições\n📦 {pacotes_pt} pacotes\n```", inline=True)
    embed.add_field(name=f"🔫 SUB", value=f"```yaml\n{fmt_num(sub)} munições\n📦 {pacotes_sub} pacotes\n```", inline=True)
    valor_total = (pt * 50) + (sub * 90)
    embed.add_field(name=f"{Emojis.FINANCEIRO} VALOR TOTAL", value=f"```yaml\n{formatar_dinheiro(valor_total)}\n```", inline=False)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    if total_entregas > 1:
        embed.add_field(name="📋 STATUS DAS ENTREGAS", value=f"```yaml\nTotal: {total_entregas} entregas\nAtual: {entrega_atual}/{total_entregas}\nPróxima: Aguardando esta ser ENTREGUE\n```", inline=False)
    embed.add_field(name="📌 STATUS DO PEDIDO", value="```yaml\n📦 A Entregar\n⏳ Pagamento pendente\n```", inline=False)
    if observacoes:
        embed.add_field(name=f"{Emojis.ARQUIVO} OBSERVAÇÕES", value=f"```yaml\n{observacoes}\n```", inline=False)
    if grupo:
        embed.add_field(name="📊 INTEGRAÇÃO COM GRUPO", value=f"```yaml\n✅ Compra registrada em {org_nome}\n```", inline=False)
    if entrega_id:
        embed.set_footer(text=f"🛡 Sistema de Encomendas • VDR 442 • Entrega {entrega_atual}/{total_entregas} • ID: {entrega_id}", icon_url=bot.user.display_avatar.url if bot.user else None)
    else:
        embed.set_footer(text=f"🛡 Sistema de Encomendas • VDR 442 • Entrega {entrega_atual}/{total_entregas}", icon_url=bot.user.display_avatar.url if bot.user else None)

    # VIEW COM BOTÃO DE TRANSFERÊNCIA
    view = StatusView(
        entrega_id=entrega_id,
        total_entregas=total_entregas,
        entrega_atual=entrega_atual,
        valor_total=valor_total,
        pt=pt,
        sub=sub,
        pedido_numero=pedido_numero
    )

    msg = await safe_request(canal.send, embed=embed, view=view)
    if msg and entrega_id:
        await BotaoPersistente.salvar_botao(msg.id, canal.id, "venda", {
            "entrega_id": entrega_id,
            "total_entregas": total_entregas,
            "entrega_atual": entrega_atual,
            "valor_total": valor_total,
            "pt": pt,
            "sub": sub,
            "pedido_numero": pedido_numero
        })
        await atualizar_entrega_parcelada(entrega_id, entrega_atual, str(msg.id), None)
    elif msg and not entrega_id:
        await BotaoPersistente.salvar_botao(msg.id, canal.id, "venda", {
            "entrega_id": None,
            "total_entregas": total_entregas,
            "entrega_atual": entrega_atual,
            "valor_total": valor_total,
            "pt": pt,
            "sub": sub,
            "pedido_numero": pedido_numero
        })
    return msg

# =========================================================
# 12.5 VIEW DE STATUS (COM BOTÃO DE TRANSFERÊNCIA)
# =========================================================
class StatusView(discord.ui.View):
    def __init__(self, disabled: bool = False, entrega_id: int = None, total_entregas: int = 1, entrega_atual: int = 1, pago_ja_clicado: bool = False, mensagem_original: discord.Message = None, transferencia_confirmada: bool = False, valor_total: int = 0, pt: int = 0, sub: int = 0, pedido_numero: int = 0, entregue_ja_clicado: bool = False):
        super().__init__(timeout=None)
        self.entrega_id = entrega_id
        self.total_entregas = total_entregas
        self.entrega_atual = entrega_atual
        self.entrega_ja_entregue = entregue_ja_clicado
        self.pago_ja_clicado = pago_ja_clicado
        self.mensagem_original = mensagem_original
        self.entrega_criada = False
        self.transferencia_confirmada = transferencia_confirmada
        self.valor_total = valor_total
        self.pt = pt
        self.sub = sub
        self.pedido_numero = pedido_numero

        # =========================================================
        # BOTÃO PAGO
        # =========================================================
        self.add_item(discord.ui.Button(
            label="💰 Pago",
            style=discord.ButtonStyle.primary,
            custom_id="status_pago_fixo",
            emoji="💰",
            disabled=self.pago_ja_clicado or self.transferencia_confirmada
        ))

        # =========================================================
        # BOTÃO ENTREGUE
        # =========================================================
        self.add_item(discord.ui.Button(
            label="✅ Entregue",
            style=discord.ButtonStyle.success,
            custom_id="status_entregue_fixo",
            emoji="✅",
            disabled=self.entrega_ja_entregue or self.transferencia_confirmada
        ))

        # =========================================================
        # BOTÃO EDITAR
        # =========================================================
        self.add_item(discord.ui.Button(
            label="✏️ Editar Venda",
            style=discord.ButtonStyle.primary,
            custom_id="editar_venda_fixo",
            emoji="✏️",
            disabled=self.transferencia_confirmada
        ))

        # =========================================================
        # BOTÃO CANCELAR
        # =========================================================
        self.add_item(discord.ui.Button(
            label="❌ Pedido cancelado",
            style=discord.ButtonStyle.danger,
            custom_id="status_cancelado_fixo",
            emoji="❌",
            disabled=self.transferencia_confirmada
        ))

        # =========================================================
        # BOTÃO TRANSFERÊNCIA - SÓ DESABILITA QUANDO CLICADO
        # =========================================================
        self.add_item(discord.ui.Button(
            label="📤 Confirmar Transferência",
            style=discord.ButtonStyle.success,
            custom_id="confirmar_transferencia_fixo",
            emoji="📤",
            disabled=self.transferencia_confirmada,
            row=1
        ))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        custom_id = interaction.data.get("custom_id", "")

        if custom_id == "confirmar_transferencia_fixo":
            if self.transferencia_confirmada:
                await interaction.response.send_message("⚠️ Esta transferência já foi confirmada!", ephemeral=True)
                return False
            if not self.entrega_id and self.pedido_numero == 0:
                await interaction.response.send_message("❌ Não foi possível identificar o pedido!", ephemeral=True)
                return False

            if self.valor_total == 0 or self.pt == 0:
                try:
                    pool = await get_pool()
                    if pool:
                        async with pool.acquire() as conn:
                            row = await conn.fetchrow("SELECT valor FROM vendas WHERE pedido_numero = $1", self.pedido_numero)
                            if row:
                                self.valor_total = row["valor"]
                except:
                    pass

            if self.valor_total == 0:
                await interaction.response.send_message("❌ Não foi possível encontrar o valor do pedido!", ephemeral=True)
                return False

            modal = ConfirmarTransferenciaModal(
                interaction=interaction,
                mensagem_original=interaction.message,
                entrega_id=self.entrega_id,
                pedido_numero=self.pedido_numero,
                valor=self.valor_total,
                pt=self.pt,
                sub=self.sub
            )
            await interaction.response.send_modal(modal)
            return False

        elif custom_id == "status_pago_fixo":
            if self.pago_ja_clicado:
                await interaction.response.send_message("⚠️ Este pedido já foi marcado como pago!", ephemeral=True)
                return False
            await interaction.response.defer()
            await self.pago(interaction, None)
            return False

        elif custom_id == "status_entregue_fixo":
            if self.entrega_ja_entregue:
                await interaction.response.send_message("⚠️ Esta entrega já foi marcada como entregue!", ephemeral=True)
                return False
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
            if field.name == "📌 Status" or field.name == "📌 STATUS DO PEDIDO":
                return i, field.value.split("\n")
        return None, ["📦 A entregar"]

    def set_status(self, embed, idx, linhas):
        if not linhas:
            linhas = ["📦 A entregar"]
        for i, field in enumerate(embed.fields):
            if field.name == "📌 STATUS DO PEDIDO":
                if "TRANSFERÊNCIA CONFIRMADA" in "\n".join(linhas):
                    novo_status = "✅ TRANSFERÊNCIA CONFIRMADA"
                elif "💰" in "\n".join(linhas) and "✅" in "\n".join(linhas):
                    novo_status = "✅ Pago e Entregue"
                elif "💰" in "\n".join(linhas):
                    novo_status = "💰 Pago"
                elif "✅" in "\n".join(linhas):
                    novo_status = "✅ Entregue"
                elif "❌" in "\n".join(linhas):
                    novo_status = "❌ Cancelado"
                else:
                    novo_status = "📦 A Entregar\n⏳ Pagamento pendente"
                embed.set_field_at(i, name="📌 STATUS DO PEDIDO", value=novo_status, inline=False)
                break
        if idx is None:
            embed.add_field(name="📌 Status", value="\n".join(linhas), inline=False)
            return embed
        try:
            embed.set_field_at(idx, name="📌 Status", value="\n".join(linhas), inline=False)
        except IndexError:
            embed.add_field(name="📌 Status", value="\n".join(linhas), inline=False)
        return embed

    def pedido_pago(self, linhas):
        return any(l.startswith("💰") for l in linhas)

    def pedido_cancelado(self, linhas):
        return any(l.startswith("❌") for l in linhas)

    def entrega_ja_foi_entregue(self, linhas):
        return any(l.startswith("✅") for l in linhas)

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
        pagador_apelido = await pegar_apelido(interaction.user.id, interaction.guild)

        linhas = [l for l in linhas if not l.startswith("⏳")]
        linhas = [l for l in linhas if not l.startswith("💰")]
        linhas.append(f"💰 Pago • Recebido por {pagador_apelido} • {agora_str}")

        embed = self.set_status(embed, idx, linhas)

        pago_foi_clicado = any(l.startswith("💰") for l in linhas)
        entregue_foi_clicado = any(l.startswith("✅") for l in linhas)
        finalizado = pago_foi_clicado and entregue_foi_clicado

        if finalizado:
            embed.color = 0x2ecc71
            embed.title = "🎉 VENDA CONCLUÍDA"

            for i, field in enumerate(embed.fields):
                if field.name == "📌 STATUS DO PEDIDO":
                    embed.set_field_at(i, name="📌 STATUS DO PEDIDO", value="✅ Pago e Entregue", inline=False)
                    break

            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed.add_field(name="✅ VENDA FINALIZADA COM SUCESSO", value="💰 **Pagamento recebido**\n📦 **Pedido entregue ao cliente**", inline=False)
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="🔥 **Pedido encerrado no sistema**", inline=False)

            await interaction.message.edit(embed=embed, view=StatusView(
                disabled=False,
                entrega_id=self.entrega_id,
                total_entregas=self.total_entregas,
                entrega_atual=self.entrega_atual,
                pago_ja_clicado=True,
                mensagem_original=interaction.message,
                transferencia_confirmada=False,
                valor_total=self.valor_total,
                pt=self.pt,
                sub=self.sub,
                pedido_numero=self.pedido_numero,
                entregue_ja_clicado=True
            ))

            await interaction.followup.send("✅ **Venda concluída com sucesso!**", ephemeral=True)

            if self.entrega_atual < self.total_entregas:
                await self.criar_proxima_entrega(interaction, embed, self.pedido_numero)

            await enviar_painel_vendas()
            await enviar_painel_fabricacao()
            return

        await interaction.message.edit(embed=embed, view=StatusView(
            disabled=False,
            entrega_id=self.entrega_id,
            total_entregas=self.total_entregas,
            entrega_atual=self.entrega_atual,
            pago_ja_clicado=True,
            mensagem_original=interaction.message,
            transferencia_confirmada=False,
            valor_total=self.valor_total,
            pt=self.pt,
            sub=self.sub,
            pedido_numero=self.pedido_numero,
            entregue_ja_clicado=self.entrega_ja_entregue
        ))

        await interaction.followup.send("✅ **Pagamento registrado!**", ephemeral=True)

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
                    f"❌ **ESTOQUE INSUFICIENTE!**\n\n🔫 PT: {pacotes_pt} pacotes necessários\n📦 Estoque atual: {estoque_atual['PT']} pacotes",
                    ephemeral=True
                )
                return

        if pacotes_sub > 0:
            estoque_suficiente = await verificar_estoque_suficiente("SUB", pacotes_sub)
            if not estoque_suficiente:
                estoque_atual = await carregar_estoque()
                await interaction.followup.send(
                    f"❌ **ESTOQUE INSUFICIENTE!**\n\n🔫 SUB: {pacotes_sub} pacotes necessários\n📦 Estoque atual: {estoque_atual['SUB']} pacotes",
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
        entregador_apelido = await pegar_apelido(interaction.user.id, interaction.guild)

        linhas = [l for l in linhas if not l.startswith("📦")]
        linhas = [l for l in linhas if not l.startswith("✅")]
        linhas.append(f"✅ Entregue por {entregador_apelido} • {agora_str}")

        embed = self.set_status(embed, idx, linhas)

        # =========================================================
        # ENVIA PARA O BAÚ DE PRODUÇÃO
        # =========================================================
        if pacotes_pt > 0 or pacotes_sub > 0:
            canal_bau = interaction.guild.get_channel(CANAL_BAU_GALPAO_SUL_ID)
            if canal_bau:
                try:
                    entregador_apelido_bau = await pegar_apelido(interaction.user.id, interaction.guild)
                    org_retirada = "VDR"
                    if self.entrega_id:
                        try:
                            pool = await get_pool()
                            if pool:
                                async with pool.acquire() as conn:
                                    row = await conn.fetchrow("SELECT organizacao FROM entregas_parceladas WHERE id = $1", self.entrega_id)
                                    if row:
                                        org_retirada = row["organizacao"]
                        except:
                            pass
                    itens = ""
                    if pacotes_pt > 0 and pacotes_sub > 0:
                        itens = f"PT: {pacotes_pt} pacotes / SUB: {pacotes_sub} pacotes"
                    elif pacotes_pt > 0:
                        itens = f"PT: {pacotes_pt} pacotes"
                    elif pacotes_sub > 0:
                        itens = f"SUB: {pacotes_sub} pacotes"
                    else:
                        itens = "Nenhum item retirado"
                    texto_bau = f"📦 ── SAÍDA DO BAÚ ── 📦\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n👤 RETIRADO POR: {entregador_apelido_bau}\n🏷️ PARA A ENTREGA DA ORG: {org_retirada}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📦 ITENS RETIRADOS: {itens}"
                    await canal_bau.send(f"```\n{texto_bau}\n```")
                except Exception as e:
                    logger.error(f"Erro envio baú: {e}")

        pago_foi_clicado = any(l.startswith("💰") for l in linhas)
        entregue_foi_clicado = any(l.startswith("✅") for l in linhas)
        finalizado = pago_foi_clicado and entregue_foi_clicado

        if finalizado:
            embed.color = 0x2ecc71
            embed.title = "🎉 VENDA CONCLUÍDA"

            for i, field in enumerate(embed.fields):
                if field.name == "📌 STATUS DO PEDIDO":
                    embed.set_field_at(i, name="📌 STATUS DO PEDIDO", value="✅ Pago e Entregue", inline=False)
                    break

            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed.add_field(name="✅ VENDA FINALIZADA COM SUCESSO", value="💰 **Pagamento recebido**\n📦 **Pedido entregue ao cliente**\n📊 **Estoque atualizado**", inline=False)
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="🔥 **Pedido encerrado no sistema**", inline=False)

            await interaction.message.edit(embed=embed, view=StatusView(
                disabled=False,
                entrega_id=self.entrega_id,
                total_entregas=self.total_entregas,
                entrega_atual=self.entrega_atual,
                pago_ja_clicado=True,
                mensagem_original=interaction.message,
                transferencia_confirmada=False,
                valor_total=self.valor_total,
                pt=self.pt,
                sub=self.sub,
                pedido_numero=self.pedido_numero,
                entregue_ja_clicado=True
            ))

            await interaction.followup.send("✅ **Venda concluída com sucesso!**", ephemeral=True)

            if self.entrega_atual < self.total_entregas:
                await self.criar_proxima_entrega(interaction, embed, pedido_numero)

            await enviar_painel_vendas()
            await enviar_painel_fabricacao()
            return

        await interaction.message.edit(embed=embed, view=StatusView(
            disabled=False,
            entrega_id=self.entrega_id,
            total_entregas=self.total_entregas,
            entrega_atual=self.entrega_atual,
            pago_ja_clicado=self.pago_ja_clicado,
            mensagem_original=interaction.message,
            transferencia_confirmada=False,
            valor_total=self.valor_total,
            pt=self.pt,
            sub=self.sub,
            pedido_numero=self.pedido_numero,
            entregue_ja_clicado=True
        ))

        await interaction.followup.send("✅ **Entrega registrada!**", ephemeral=True)

        await enviar_painel_vendas()
        await enviar_painel_fabricacao()

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
        if self.entrega_ja_entregue or self.pedido_pago(linhas):
            if pacotes_pt > 0:
                await atualizar_estoque("PT", pacotes_pt, "adicionar")
                logger.info(f"🔄 Estoque PT reabastecido: +{pacotes_pt} pacotes (Pedido #{pedido_numero})")
            if pacotes_sub > 0:
                await atualizar_estoque("SUB", pacotes_sub, "adicionar")
                logger.info(f"🔄 Estoque SUB reabastecido: +{pacotes_sub} pacotes (Pedido #{pedido_numero})")
            if self.entrega_ja_entregue and self.pedido_pago(linhas):
                status_anterior = "Pago e Entregue"
            elif self.pedido_pago(linhas):
                status_anterior = "Pago"
            elif self.entrega_ja_entregue:
                status_anterior = "Entregue"

        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        cancelador_apelido = await pegar_apelido(interaction.user.id, interaction.guild)

        canal_bau = interaction.guild.get_channel(CANAL_BAU_GALPAO_ID)
        if canal_bau:
            try:
                embed_bau = discord.Embed(title="🔄 PEDIDO CANCELADO - REVERSÃO DE ESTOQUE", color=0xe74c3c, timestamp=agora())
                embed_bau.add_field(name="📦 Pedido", value=f"#{pedido_numero:04d}", inline=True)
                embed_bau.add_field(name="👤 Cancelado por", value=cancelador_apelido, inline=True)
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

        linhas = [f"❌ Pedido cancelado por {cancelador_apelido} • {agora_str}"]
        if status_anterior:
            linhas.append(f"🔄 **ESTOQUE REVERTIDO** ({status_anterior})")

        embed = self.set_status(embed, idx, linhas)

        await interaction.message.edit(embed=embed, view=StatusView(
            disabled=False,
            entrega_id=self.entrega_id,
            total_entregas=self.total_entregas,
            entrega_atual=self.entrega_atual,
            pago_ja_clicado=True,
            mensagem_original=interaction.message,
            transferencia_confirmada=True,
            valor_total=self.valor_total,
            pt=self.pt,
            sub=self.sub,
            pedido_numero=self.pedido_numero,
            entregue_ja_clicado=True
        ))

        if self.entrega_id:
            await finalizar_entregas(self.entrega_id)

        await enviar_painel_vendas()
        await enviar_painel_fabricacao()

    def extrair_dados_venda(self, embed):
        dados = {"pt": 0, "sub": 0, "organizacao": "Desconhecida", "vendedor": "", "observacoes": ""}
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
            config = ORGANIZACOES_CONFIG.get(organizacao, {"emoji": "🏷️", "cor": 0x1a1a2e})
            await criar_embed_entrega(
                interaction=interaction,
                pedido_numero=pedido_original,
                entrega_atual=proxima_entrega_num,
                total_entregas=total_entregas,
                pt=pt_entrega,
                sub=sub_entrega,
                org_nome=organizacao,
                config=config,
                observacoes=observacoes,
                entrega_id=self.entrega_id,
                vendedor_id=vendedor_id,
                grupo=None,
                entregas_lista=entregas_lista
            )
            self.entrega_criada = True
            await interaction.followup.send(f"✅ **Entrega {proxima_entrega_num}/{total_entregas} criada automaticamente!**", ephemeral=True)
            await enviar_painel_vendas()
            await enviar_painel_fabricacao()
        except Exception as e:
            logger.error(f"❌ Erro ao criar próxima entrega automaticamente: {e}")
            await interaction.followup.send(f"❌ **Erro ao criar próxima entrega:** {str(e)}", ephemeral=True)
            # =========================================================
            # VIEW - TRANSFERÊNCIA CONTINUA ATIVO (transferencia_confirmada=False)
            # =========================================================
            await interaction.message.edit(embed=embed, view=StatusView(
                disabled=False,
                entrega_id=self.entrega_id,
                total_entregas=self.total_entregas,
                entrega_atual=self.entrega_atual,
                pago_ja_clicado=True,
                mensagem_original=interaction.message,
                transferencia_confirmada=False,  # ← TRANSFERÊNCIA ATIVO
                valor_total=self.valor_total,
                pt=self.pt,
                sub=self.sub,
                pedido_numero=self.pedido_numero,
                entregue_ja_clicado=True
            ))

            await interaction.followup.send("✅ **Venda concluída com sucesso!**", ephemeral=True)

            if self.entrega_atual < self.total_entregas:
                await self.criar_proxima_entrega(interaction, embed, self.pedido_numero)

            await enviar_painel_vendas()
            await enviar_painel_fabricacao()
            return

        # =========================================================
        # NÃO FINALIZADO - VIEW COM TRANSFERÊNCIA ATIVO
        # =========================================================
        await interaction.message.edit(embed=embed, view=StatusView(
            disabled=False,
            entrega_id=self.entrega_id,
            total_entregas=self.total_entregas,
            entrega_atual=self.entrega_atual,
            pago_ja_clicado=True,  # ← PAGO DESABILITADO
            mensagem_original=interaction.message,
            transferencia_confirmada=False,  # ← TRANSFERÊNCIA ATIVO
            valor_total=self.valor_total,
            pt=self.pt,
            sub=self.sub,
            pedido_numero=self.pedido_numero,
            entregue_ja_clicado=self.entrega_ja_entregue
        ))

        await interaction.followup.send("✅ **Pagamento registrado!**", ephemeral=True)

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
                    f"❌ **ESTOQUE INSUFICIENTE!**\n\n🔫 PT: {pacotes_pt} pacotes necessários\n📦 Estoque atual: {estoque_atual['PT']} pacotes",
                    ephemeral=True
                )
                return

        if pacotes_sub > 0:
            estoque_suficiente = await verificar_estoque_suficiente("SUB", pacotes_sub)
            if not estoque_suficiente:
                estoque_atual = await carregar_estoque()
                await interaction.followup.send(
                    f"❌ **ESTOQUE INSUFICIENTE!**\n\n🔫 SUB: {pacotes_sub} pacotes necessários\n📦 Estoque atual: {estoque_atual['SUB']} pacotes",
                    ephemeral=True
                )
                return

        self.entrega_ja_entregue = True

        titulo = embed.title
        pedido_numero = safe_int(titulo.split("#")[1]) if "#" in titulo else 0

        # =========================================================
        # REMOVER DO ESTOQUE
        # =========================================================
        if pacotes_pt > 0:
            await registrar_saida_estoque(pedido_numero, "PT", pacotes_pt, interaction.user.id)
            logger.info(f"🔫 Removido {pacotes_pt} pacotes PT do estoque (Pedido #{pedido_numero})")
        if pacotes_sub > 0:
            await registrar_saida_estoque(pedido_numero, "SUB", pacotes_sub, interaction.user.id)
            logger.info(f"🔫 Removido {pacotes_sub} pacotes SUB do estoque (Pedido #{pedido_numero})")

        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        entregador_apelido = await pegar_apelido(interaction.user.id, interaction.guild)

        linhas = [l for l in linhas if not l.startswith("📦")]
        linhas = [l for l in linhas if not l.startswith("✅")]
        linhas.append(f"✅ Entregue por {entregador_apelido} • {agora_str}")

        embed = self.set_status(embed, idx, linhas)

        # =========================================================
        # ENVIA PARA O BAÚ DE PRODUÇÃO
        # =========================================================
        if pacotes_pt > 0 or pacotes_sub > 0:
            canal_bau = interaction.guild.get_channel(CANAL_BAU_GALPAO_SUL_ID)
            if canal_bau:
                try:
                    entregador_apelido_bau = await pegar_apelido(interaction.user.id, interaction.guild)
                    org_retirada = "VDR"
                    if self.entrega_id:
                        try:
                            pool = await get_pool()
                            if pool:
                                async with pool.acquire() as conn:
                                    row = await conn.fetchrow("SELECT organizacao FROM entregas_parceladas WHERE id = $1", self.entrega_id)
                                    if row:
                                        org_retirada = row["organizacao"]
                        except:
                            pass
                    itens = ""
                    if pacotes_pt > 0 and pacotes_sub > 0:
                        itens = f"PT: {pacotes_pt} pacotes / SUB: {pacotes_sub} pacotes"
                    elif pacotes_pt > 0:
                        itens = f"PT: {pacotes_pt} pacotes"
                    elif pacotes_sub > 0:
                        itens = f"SUB: {pacotes_sub} pacotes"
                    else:
                        itens = "Nenhum item retirado"
                    texto_bau = f"📦 ── SAÍDA DO BAÚ ── 📦\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n👤 RETIRADO POR: {entregador_apelido_bau}\n🏷️ PARA A ENTREGA DA ORG: {org_retirada}\n━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n📦 ITENS RETIRADOS: {itens}"
                    await canal_bau.send(f"```\n{texto_bau}\n```")
                except Exception as e:
                    logger.error(f"Erro envio baú: {e}")

        pago_foi_clicado = any(l.startswith("💰") for l in linhas)
        entregue_foi_clicado = any(l.startswith("✅") for l in linhas)
        finalizado = pago_foi_clicado and entregue_foi_clicado

        if finalizado:
            embed.color = 0x2ecc71
            embed.title = "🎉 VENDA CONCLUÍDA"

            for i, field in enumerate(embed.fields):
                if field.name == "📌 STATUS DO PEDIDO":
                    embed.set_field_at(i, name="📌 STATUS DO PEDIDO", value="✅ Pago e Entregue", inline=False)
                    break

            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed.add_field(name="✅ VENDA FINALIZADA COM SUCESSO", value="💰 **Pagamento recebido**\n📦 **Pedido entregue ao cliente**\n📊 **Estoque atualizado**", inline=False)
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━", value="🔥 **Pedido encerrado no sistema**", inline=False)

            await interaction.message.edit(embed=embed, view=StatusView(
                disabled=False,
                entrega_id=self.entrega_id,
                total_entregas=self.total_entregas,
                entrega_atual=self.entrega_atual,
                pago_ja_clicado=True,
                mensagem_original=interaction.message,
                transferencia_confirmada=False,
                valor_total=self.valor_total,
                pt=self.pt,
                sub=self.sub,
                pedido_numero=self.pedido_numero,
                entregue_ja_clicado=True
            ))

            await interaction.followup.send("✅ **Venda concluída com sucesso!**", ephemeral=True)

            if self.entrega_atual < self.total_entregas:
                await self.criar_proxima_entrega(interaction, embed, pedido_numero)

            # =========================================================
            # FORÇAR ATUALIZAÇÃO DOS PAINÉIS
            # =========================================================
            await enviar_painel_vendas()
            await enviar_painel_fabricacao()
            return

        await interaction.message.edit(embed=embed, view=StatusView(
            disabled=False,
            entrega_id=self.entrega_id,
            total_entregas=self.total_entregas,
            entrega_atual=self.entrega_atual,
            pago_ja_clicado=self.pago_ja_clicado,
            mensagem_original=interaction.message,
            transferencia_confirmada=False,
            valor_total=self.valor_total,
            pt=self.pt,
            sub=self.sub,
            pedido_numero=self.pedido_numero,
            entregue_ja_clicado=True
        ))

        await interaction.followup.send("✅ **Entrega registrada!**", ephemeral=True)

        # =========================================================
        # FORÇAR ATUALIZAÇÃO DOS PAINÉIS
        # =========================================================
        await enviar_painel_vendas()
        await enviar_painel_fabricacao()

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
        if self.entrega_ja_entregue or self.pedido_pago(linhas):
            if pacotes_pt > 0:
                await atualizar_estoque("PT", pacotes_pt, "adicionar")
                logger.info(f"🔄 Estoque PT reabastecido: +{pacotes_pt} pacotes (Pedido #{pedido_numero})")
            if pacotes_sub > 0:
                await atualizar_estoque("SUB", pacotes_sub, "adicionar")
                logger.info(f"🔄 Estoque SUB reabastecido: +{pacotes_sub} pacotes (Pedido #{pedido_numero})")
            if self.entrega_ja_entregue and self.pedido_pago(linhas):
                status_anterior = "Pago e Entregue"
            elif self.pedido_pago(linhas):
                status_anterior = "Pago"
            elif self.entrega_ja_entregue:
                status_anterior = "Entregue"

        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        cancelador_apelido = await pegar_apelido(interaction.user.id, interaction.guild)

        canal_bau = interaction.guild.get_channel(CANAL_BAU_GALPAO_ID)
        if canal_bau:
            try:
                embed_bau = discord.Embed(title="🔄 PEDIDO CANCELADO - REVERSÃO DE ESTOQUE", color=0xe74c3c, timestamp=agora())
                embed_bau.add_field(name="📦 Pedido", value=f"#{pedido_numero:04d}", inline=True)
                embed_bau.add_field(name="👤 Cancelado por", value=cancelador_apelido, inline=True)
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

        linhas = [f"❌ Pedido cancelado por {cancelador_apelido} • {agora_str}"]
        if status_anterior:
            linhas.append(f"🔄 **ESTOQUE REVERTIDO** ({status_anterior})")

        embed = self.set_status(embed, idx, linhas)

        # =========================================================
        # CANCELADO - TRANSFERÊNCIA DESABILITADO
        # =========================================================
        await interaction.message.edit(embed=embed, view=StatusView(
            disabled=False,
            entrega_id=self.entrega_id,
            total_entregas=self.total_entregas,
            entrega_atual=self.entrega_atual,
            pago_ja_clicado=True,
            mensagem_original=interaction.message,
            transferencia_confirmada=True,  # ← TRANSFERÊNCIA DESABILITADO
            valor_total=self.valor_total,
            pt=self.pt,
            sub=self.sub,
            pedido_numero=self.pedido_numero,
            entregue_ja_clicado=True
        ))

        if self.entrega_id:
            await finalizar_entregas(self.entrega_id)

        await enviar_painel_vendas()
        await enviar_painel_fabricacao()

    def extrair_dados_venda(self, embed):
        dados = {"pt": 0, "sub": 0, "organizacao": "Desconhecida", "vendedor": "", "observacoes": ""}
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
            config = ORGANIZACOES_CONFIG.get(organizacao, {"emoji": "🏷️", "cor": 0x1a1a2e})
            await criar_embed_entrega(
                interaction=interaction,
                pedido_numero=pedido_original,
                entrega_atual=proxima_entrega_num,
                total_entregas=total_entregas,
                pt=pt_entrega,
                sub=sub_entrega,
                org_nome=organizacao,
                config=config,
                observacoes=observacoes,
                entrega_id=self.entrega_id,
                vendedor_id=vendedor_id,
                grupo=None,
                entregas_lista=entregas_lista
            )
            self.entrega_criada = True
            await interaction.followup.send(f"✅ **Entrega {proxima_entrega_num}/{total_entregas} criada automaticamente!**", ephemeral=True)
            await enviar_painel_vendas()
            await enviar_painel_fabricacao()
        except Exception as e:
            logger.error(f"❌ Erro ao criar próxima entrega automaticamente: {e}")
            await interaction.followup.send(f"❌ **Erro ao criar próxima entrega:** {str(e)}", ephemeral=True)
            
# =========================================================
# 12.6 MODAIS DE VENDAS
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

        # =========================================================
        # INTEGRAÇÃO COM GRUPOS
        # =========================================================
        grupo = await buscar_grupo_por_organizacao(org_nome)
        if grupo:
            if pacotes_pt_total > 0:
                valor_pt = pacotes_pt_total * 50 * 50  # pacotes × 50 munições × R$ 50
                await registrar_compra_grupo_db(grupo["grupo_id"], "PT", pacotes_pt_total, valor_pt)
            if pacotes_sub_total > 0:
                valor_sub = pacotes_sub_total * 50 * 90  # pacotes × 50 munições × R$ 90
                await registrar_compra_grupo_db(grupo["grupo_id"], "SUB", pacotes_sub_total, valor_sub)
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

class EditarVendaModal(discord.ui.Modal, title="✏️ Editar Venda"):
    def __init__(self, message):
        super().__init__(timeout=300)
        self.message = message
    qtd_pt = discord.ui.TextInput(label="🔫 Nova Quantidade PT", placeholder="Digite a nova quantidade de PT (deixe em branco para manter)", required=False, max_length=15)
    qtd_sub = discord.ui.TextInput(label="🔫 Nova Quantidade SUB", placeholder="Digite a nova quantidade de SUB (deixe em branco para manter)", required=False, max_length=15)
    organizacao = discord.ui.TextInput(label="🏷️ Nova Organização", placeholder="Digite a nova organização (deixe em branco para manter)", required=False, max_length=50)
    observacao = discord.ui.TextInput(label="📝 Nova Observação", placeholder="Digite a nova observação (deixe em branco para manter)", style=discord.TextStyle.paragraph, required=False, max_length=500)

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
            elif field.name == "💰 VALOR TOTAL":
                embed.set_field_at(i, name="💰 VALOR TOTAL", value=f"```yaml\n{valor_formatado}\n```", inline=False)
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
        embed_confirmacao = discord.Embed(title="✅ VENDA EDITADA!", description=f"📦 **Pedido #{pedido_numero:04d}**", color=0x2ecc71)
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
# 12.7 VIEWS DE VENDAS
# =========================================================
class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📝 Registrar Venda", style=discord.ButtonStyle.primary, custom_id="calc_registrar_venda", emoji="📝")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(VendaModal())

    @discord.ui.button(label="🔄 Atualizar Estoque", style=discord.ButtonStyle.secondary, custom_id="calc_atualizar_estoque", emoji="🔄", row=1)
    async def atualizar_estoque(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await enviar_painel_vendas()
        await interaction.followup.send("✅ Estoque atualizado!", ephemeral=True)

    @discord.ui.button(label="📅 Vendas por Período", style=discord.ButtonStyle.secondary, custom_id="calc_relatorio_vendas_periodo", emoji="📅", row=2)
    async def relatorio_periodo(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = RelatorioVendasPeriodoModal()
        await interaction.response.send_modal(modal)

    @discord.ui.button(label="📊 Relatório de Vendas", style=discord.ButtonStyle.success, custom_id="calc_relatorio_vendas", emoji="📊", row=1)
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        data_hoje = agora().strftime("%d/%m/%Y")
        
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT user_id, SUM(valor) as total, COUNT(*) as quantidade FROM vendas WHERE data = $1 GROUP BY user_id ORDER BY total DESC",
                data_hoje
            )
            total_geral = await conn.fetchval(
                "SELECT COALESCE(SUM(valor), 0) FROM vendas WHERE data = $1",
                data_hoje
            )
        
        if not rows:
            await interaction.followup.send(f"📭 Nenhuma venda registrada hoje.", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="📊 RELATÓRIO DE VENDAS - HOJE",
            description=f"📅 **Data:** {data_hoje}",
            color=Cores.VENDA,
            timestamp=agora()
        )
        embed.set_author(name="🛡 Vida Rasa 442 • Relatório de Vendas")
        
        texto = ""
        for i, row in enumerate(rows, 1):
            user = await pegar_usuario(int(row["user_id"]))
            nome = user.display_name if user else row["user_id"]
            texto += f"**{i}.** {nome}\n"
            texto += f"   💰 Vendas: **{formatar_dinheiro(row['total'])}**\n"
            texto += f"   📦 Pedidos: **{row['quantidade']}**\n\n"
        
        embed.add_field(name="👥 VENDEDORES", value=texto, inline=False)
        embed.add_field(name="💰 TOTAL GERAL", value=formatar_dinheiro(total_geral), inline=True)
        embed.add_field(name="📦 TOTAL DE PEDIDOS", value=sum(r["quantidade"] for r in rows), inline=True)
        embed.set_footer(text="Relatório gerado pelo sistema VDR")
        
        await interaction.followup.send(embed=embed, ephemeral=False)

    @discord.ui.button(label="📅 Relatório por Data", style=discord.ButtonStyle.secondary, custom_id="calc_relatorio_vendas_data", emoji="📅", row=2)
    async def relatorio_data(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = RelatorioVendasModal()
        await interaction.response.send_modal(modal)

class RelatorioVendasPeriodoModal(discord.ui.Modal, title="📅 RELATÓRIO DE VENDAS (PERÍODO)"):
    data_inicio = discord.ui.TextInput(
        label="📅 Data INÍCIO (DD/MM/AAAA)",
        placeholder="Ex: 01/08/2026",
        required=True
    )
    data_fim = discord.ui.TextInput(
        label="📅 Data FIM (DD/MM/AAAA)",
        placeholder="Ex: 31/08/2026",
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        data_inicio_str = self.data_inicio.value.strip()
        data_fim_str = self.data_fim.value.strip()
        
        try:
            data_inicio = datetime.strptime(data_inicio_str, "%d/%m/%Y")
            data_fim = datetime.strptime(data_fim_str, "%d/%m/%Y")
        except:
            await interaction.followup.send("❌ Formato inválido! Use DD/MM/AAAA", ephemeral=True)
            return
        
        if data_fim < data_inicio:
            await interaction.followup.send("❌ Data FIM deve ser depois da data INÍCIO!", ephemeral=True)
            return
        
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT user_id, SUM(valor) as total, COUNT(*) as quantidade
                FROM vendas 
                WHERE TO_DATE(data, 'DD/MM/YYYY') BETWEEN $1::date AND $2::date
                GROUP BY user_id 
                ORDER BY total DESC""",
                data_inicio, data_fim
            )
            total_geral = await conn.fetchval(
                "SELECT COALESCE(SUM(valor), 0) FROM vendas WHERE TO_DATE(data, 'DD/MM/YYYY') BETWEEN $1::date AND $2::date",
                data_inicio, data_fim
            )
        
        if not rows:
            await interaction.followup.send(f"📭 Nenhuma venda no período **{data_inicio_str}** a **{data_fim_str}**", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="📊 RELATÓRIO DE VENDAS (PERÍODO)",
            description=f"📅 **Período:** {data_inicio_str} a {data_fim_str}",
            color=Cores.VENDA,
            timestamp=agora()
        )
        embed.set_author(name="🛡 Vida Rasa 442 • Relatório de Vendas")
        
        texto = ""
        for i, row in enumerate(rows, 1):
            user = await pegar_usuario(int(row["user_id"]))
            nome = user.display_name if user else row["user_id"]
            texto += f"**{i}.** {nome}\n"
            texto += f"   💰 Vendas: **{formatar_dinheiro(row['total'])}**\n"
            texto += f"   📦 Pedidos: **{row['quantidade']}**\n\n"
        
        embed.add_field(name="👥 VENDEDORES", value=texto, inline=False)
        embed.add_field(name="💰 TOTAL GERAL", value=formatar_dinheiro(total_geral), inline=True)
        embed.add_field(name="📦 TOTAL DE PEDIDOS", value=sum(r["quantidade"] for r in rows), inline=True)
        embed.set_footer(text="Relatório gerado pelo sistema VDR")
        
        await interaction.followup.send(embed=embed, ephemeral=False)
# =========================================================
# 12.8 FUNÇÕES DE RESTAURAR VENDAS
# =========================================================
async def restaurar_botoes_vendas():
    try:
        canal = bot.get_channel(CANAL_ENCOMENDAS_ID)
        if not canal:
            logger.error("❌ Canal de encomendas não encontrado!")
            return
        contador_desabilitados = 0
        contador_concluidos = 0
        contador_cancelados = 0
        contador_pendentes = 0
        async for msg in canal.history(limit=500):
            if msg.author == bot.user and msg.embeds and len(msg.embeds) > 0:
                titulo = msg.embeds[0].title if msg.embeds[0].title else ""
                if "ENTREGA" in titulo.upper() or "ENCOMENDA" in titulo.upper() or "VENDA" in titulo.upper():
                    embed = msg.embeds[0]
                    pago = False
                    entregue = False
                    cancelado = False
                    concluida = False
                    transferencia_confirmada = False
                    if "VENDA CONCLUÍDA" in titulo.upper():
                        concluida = True
                        pago = True
                        entregue = True
                    for field in embed.fields:
                        if field.name == "📌 STATUS DO PEDIDO" or field.name == "📌 Status":
                            valor = field.value
                            if "TRANSFERÊNCIA CONFIRMADA" in valor:
                                transferencia_confirmada = True
                            if "💰" in valor or "Pago" in valor:
                                pago = True
                            if "✅" in valor or "Entregue" in valor:
                                entregue = True
                            if "❌" in valor or "cancelado" in valor.lower():
                                cancelado = True
                            break
                    if cancelado:
                        status = "CANCELADA"
                        contador_cancelados += 1
                        disabled = True
                        pago_ja_clicado = True
                    elif concluida or (pago and entregue):
                        status = "CONCLUÍDA"
                        contador_concluidos += 1
                        disabled = True
                        pago_ja_clicado = True
                    elif transferencia_confirmada:
                        status = "TRANSFERÊNCIA CONFIRMADA"
                        contador_concluidos += 1
                        disabled = True
                        pago_ja_clicado = True
                    elif pago:
                        status = "PAGO"
                        contador_pendentes += 1
                        disabled = False
                        pago_ja_clicado = True
                    elif entregue:
                        status = "ENTREGUE"
                        contador_pendentes += 1
                        disabled = False
                        pago_ja_clicado = False
                    else:
                        status = "PENDENTE"
                        contador_pendentes += 1
                        disabled = False
                        pago_ja_clicado = False
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
                    entrega_atual = 1
                    if embed.description:
                        if "entregas no total" in embed.description:
                            try:
                                total_entregas = safe_int(embed.description.split("tem")[1].split("entregas")[0].strip())
                            except:
                                pass
                    if "ENTREGA" in titulo:
                        try:
                            parte = titulo.split("ENTREGA")[1].strip().split("/")[0].strip()
                            entrega_atual = safe_int(parte)
                        except:
                            pass
                    valor_total = 0
                    pt = 0
                    sub = 0
                    pedido_numero = safe_int(titulo.split("#")[1]) if "#" in titulo else 0
                    for field in embed.fields:
                        if field.name == "💰 VALOR TOTAL" or field.name == "💰 Valor (esta entrega)" or field.name == "💰 Valor":
                            try:
                                valor_total = safe_int(field.value.replace("R$", "").replace(".", "").replace(",", "").strip())
                            except:
                                pass
                        if field.name == "🔫 PT":
                            try:
                                pt = int(field.value.split(" munições")[0].replace(".", "").replace(",", ""))
                            except:
                                pass
                        if field.name == "🔫 SUB":
                            try:
                                sub = int(field.value.split(" munições")[0].replace(".", "").replace(",", ""))
                            except:
                                pass
                    view = StatusView(
                        disabled=disabled,
                        entrega_id=entrega_id,
                        total_entregas=total_entregas,
                        entrega_atual=entrega_atual,
                        pago_ja_clicado=pago_ja_clicado,
                        mensagem_original=msg,
                        transferencia_confirmada=transferencia_confirmada,
                        valor_total=valor_total,
                        pt=pt,
                        sub=sub,
                        pedido_numero=pedido_numero
                    )
                    await safe_request(msg.edit, view=view)
                    contador_desabilitados += 1
                    await asyncio.sleep(0.5)
        logger.info(f"✅ {contador_desabilitados} mensagens de venda processadas!")
        logger.info(f"   🔒 {contador_concluidos} CONCLUÍDAS (desabilitadas)")
        logger.info(f"   🚫 {contador_cancelados} CANCELADAS (desabilitadas)")
        logger.info(f"   📦 {contador_pendentes} PENDENTES (ativas)")
    except Exception as e:
        logger.error(f"❌ Erro ao restaurar botões de vendas: {e}")

async def recriar_mensagens_vendas():
    try:
        canal = bot.get_channel(CANAL_ENCOMENDAS_ID)
        if not canal:
            logger.error("❌ Canal de encomendas não encontrado!")
            return
        contador_recriados = 0
        contador_ignorados = 0
        async for msg in canal.history(limit=500):
            if msg.author == bot.user and msg.embeds and len(msg.embeds) > 0:
                titulo = msg.embeds[0].title if msg.embeds[0].title else ""
                if "ENTREGA" in titulo.upper() or "ENCOMENDA" in titulo.upper() or "VENDA" in titulo.upper():
                    embed = msg.embeds[0]
                    concluida = False
                    cancelada = False
                    transferencia_confirmada = False
                    for field in embed.fields:
                        if field.name == "📌 STATUS DO PEDIDO" or field.name == "📌 Status":
                            valor = field.value
                            if "CONCLUÍDA" in valor.upper():
                                concluida = True
                            if "TRANSFERÊNCIA CONFIRMADA" in valor:
                                transferencia_confirmada = True
                            if "💰" in valor and "✅" in valor:
                                concluida = True
                            if "CANCELADO" in valor.upper() or "CANCELADA" in valor.upper():
                                cancelada = True
                            if "❌" in valor:
                                cancelada = True
                            break
                    if concluida or cancelada or transferencia_confirmada:
                        contador_ignorados += 1
                        continue
                    if msg.components:
                        contador_ignorados += 1
                        continue
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
                    view = StatusView(
                        disabled=False,
                        entrega_id=entrega_id,
                        total_entregas=total_entregas,
                        mensagem_original=msg
                    )
                    await safe_request(msg.edit, view=view)
                    contador_recriados += 1
                    await asyncio.sleep(0.5)
        logger.info(f"✅ {contador_recriados} mensagens de venda recriadas!")
        logger.info(f"⏭️ {contador_ignorados} vendas concluídas/canceladas (IGNORADAS)")
    except Exception as e:
        logger.error(f"❌ Erro ao recriar mensagens de vendas: {e}")

# =========================================================
# 12.9 FUNÇÃO DE ENVIAR PAINEL DE VENDAS
# =========================================================
async def enviar_painel_vendas():
    canal = bot.get_channel(CANAL_VENDAS_ID)
    if not canal:
        logger.error("❌ Canal de vendas não encontrado")
        return
    estoque = await carregar_estoque()
    embed = discord.Embed(
        title="💀 ── PAINEL DE VENDAS ── 💀",
        description="🛒 Sistema de Encomendas • VDR 442",
        color=0x1a1a2e,
        timestamp=agora()
    )
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(name="⚠️ ATENÇÃO", value="🔴 Antes de entregar um pedido, verifique o ESTOQUE disponível!", inline=False)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="📦 ESTOQUE DISPONÍVEL",
        value=(
            f"🔫 PT   →  **{fmt_num(estoque['PT'])}** pacotes  ({fmt_num(estoque['PT'] * 50)} munições)\n"
            f"🔫 SUB  →  **{fmt_num(estoque['SUB'])}** pacotes  ({fmt_num(estoque['SUB'] * 50)} munições)"
        ),
        inline=False
    )
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="📌 OPÇÕES DISPONÍVEIS",
        value="[📝 Registrar Venda]\n[🔄 Atualizar Estoque]",
        inline=False
    )
    embed.set_footer(text=f"🛡 Vida Rasa 442 • Atualizado em {agora().strftime('%d/%m/%Y %H:%M:%S')}", icon_url=bot.user.display_avatar.url if bot.user else None)
    view = CalculadoraView()
    await enviar_ou_atualizar_painel("painel_vendas", CANAL_VENDAS_ID, embed, view)

# =========================================================
# ==================== PARTE 13: SISTEMA DE PRODUÇÃO ======
# =========================================================

# =========================================================
# 13.1 FUNÇÕES DE BANCO DE DADOS - PRODUÇÃO
# =========================================================
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

async def atualizar_estoque_capsulas(quantidade, operacao="adicionar"):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            if operacao == "adicionar":
                await conn.execute("UPDATE estoque_capsulas SET quantidade = quantidade + $1, ultima_atualizacao = NOW() WHERE id = 1", quantidade)
            else:
                await conn.execute("UPDATE estoque_capsulas SET quantidade = quantidade - $1, ultima_atualizacao = NOW() WHERE id = 1 AND quantidade >= $1", quantidade)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar estoque de cápsulas: {e}")

async def atualizar_estoque_embalagens(quantidade, operacao="adicionar"):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            if operacao == "adicionar":
                await conn.execute("UPDATE estoque_embalagens SET quantidade = quantidade + $1, ultima_atualizacao = NOW() WHERE id = 1", quantidade)
            else:
                await conn.execute("UPDATE estoque_embalagens SET quantidade = quantidade - $1, ultima_atualizacao = NOW() WHERE id = 1 AND quantidade >= $1", quantidade)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar estoque de embalagens: {e}")

async def registrar_entrada_insumos(tipo, quantidade, registrado_por, obs=""):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO entrada_insumos (tipo, quantidade, registrado_por, obs) VALUES ($1, $2, $3, $4)", tipo, quantidade, str(registrado_por), obs)
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
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO producao_municao (tipo, pacotes, municoes, produzido_por, obs, capsulas_consumidas, embalagens_consumidas) VALUES ($1, $2, $3, $4, $5, $6, $7)", tipo, pacotes, municoes, str(produzido_por), obs, capsulas_consumidas, embalagens_consumidas)
            await atualizar_estoque(tipo, pacotes, "adicionar")
    except Exception as e:
        logger.error(f"❌ Erro ao registrar produção de munição: {e}")

async def salvar_polvora_db(user_id, qtd, valor):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            data_str = agora().isoformat()
            await conn.execute("INSERT INTO polvoras (user_id, quantidade, valor, data) VALUES ($1, $2, $3, $4)", str(user_id), qtd, valor, data_str)
    except Exception as e:
        logger.error(f"❌ Erro ao salvar pólvora: {e}")

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

async def limpar_polvoras_db():
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM polvoras")
    except Exception as e:
        logger.error(f"❌ Erro ao limpar pólvoras: {e}")

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
                "INSERT INTO producoes (pid, galpao, autor, inicio, fim, obs, msg_id, canal_id, segunda_task_user, segunda_task_time, polvora, qtd_galpoes, polvora_por_galpao) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13) ON CONFLICT (pid) DO UPDATE SET galpao=$2, autor=$3, inicio=$4, fim=$5, obs=$6, msg_id=$7, canal_id=$8, segunda_task_user=$9, segunda_task_time=$10, polvora=$11, qtd_galpoes=$12, polvora_por_galpao=$13",
                pid, dados["galpao"], str(dados["autor"]), inicio_str, fim_str, dados.get("obs", ""), str(dados["msg_id"]), str(dados["canal_id"]), segunda_user, segunda_time, dados.get("polvora", 400), qtd_galpoes, polvora_por_galpao
            )
    except Exception as e:
        logger.error(f"❌ Erro ao salvar produção {pid}: {e}")

async def deletar_producao(pid):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM producoes WHERE pid=$1", pid)
    except Exception as e:
        logger.error(f"❌ Erro ao deletar produção {pid}: {e}")

async def salvar_aluguel(galpao, dias):
    pool = await get_pool()
    if not pool:
        return False
    try:
        dias = safe_int(dias)
        async with pool.acquire() as conn:
            existe = await conn.fetchval("SELECT id FROM alugueis WHERE galpao = $1 AND ativo = true", galpao)
            if existe:
                await conn.execute("UPDATE alugueis SET dias_alugados = dias_alugados + $1::INTEGER, data_atualizacao = NOW() WHERE galpao = $2 AND ativo = true", dias, galpao)
            else:
                await conn.execute("INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo) VALUES ($1, $2::INTEGER, NOW(), true)", galpao, dias)
            return True
    except Exception as e:
        logger.error(f"❌ ERRO AO SALVAR ALUGUEL: {e}")
        return False

async def carregar_alugueis():
    pool = await get_pool()
    if not pool:
        return {"GALPÕES NORTE": {"dias": 0, "inicio": None}, "GALPÕES SUL": {"dias": 0, "inicio": None}}
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE alugueis SET ativo = false WHERE galpao NOT IN ('GALPÕES NORTE', 'GALPÕES SUL') AND ativo = true")
            existe_norte = await conn.fetchval("SELECT 1 FROM alugueis WHERE galpao = 'GALPÕES NORTE'")
            if not existe_norte:
                await conn.execute("INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo) VALUES ('GALPÕES NORTE', 0, NOW(), true)")
            existe_sul = await conn.fetchval("SELECT 1 FROM alugueis WHERE galpao = 'GALPÕES SUL'")
            if not existe_sul:
                await conn.execute("INSERT INTO alugueis (galpao, dias_alugados, data_inicio, ativo) VALUES ('GALPÕES SUL', 0, NOW(), true)")
            rows = await conn.fetch("SELECT galpao, dias_alugados, data_inicio FROM alugueis WHERE ativo = true AND galpao IN ('GALPÕES NORTE', 'GALPÕES SUL')")
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

# =========================================================
# 13.2 VIEWS E MODAIS DE PRODUÇÃO
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
            prod["segunda_task_confirmada"] = {"user": interaction.user.id, "time": agora().isoformat()}
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
        desc += f"**Pólvora por galpão:** {polvora_por_galpao}\n**Pólvora total:** {polvora_total}\nInício: <t:{int(inicio.timestamp())}:t>\nTérmino: <t:{int(fim.timestamp())}:t>\n\n⏳ **Restante:** {tempo_real} min\n{barra(0)}"
        msg = await safe_request(canal.send, embed=discord.Embed(title=f"🏭 Produção - {qtd} Galpão(ões)", description=desc, color=0x3498db), view=SegundaTaskView(pid))
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
            embed_bau.add_field(name="📊 ESTOQUE APÓS PRODUÇÃO", value=f"**Munições:**\n🔫 PT: {fmt_num(estoque_municoes['PT'])} pacotes\n🔫 SUB: {fmt_num(estoque_municoes['SUB'])} pacotes\n\n**Insumos restantes:**\n💊 Cápsulas: {fmt_num(estoque_insumos['capsulas'])}\n📦 Embalagens: {fmt_num(estoque_insumos['embalagens'])}", inline=False)
            await canal_bau.send(embed=embed_bau)
        await interaction.followup.send("✅ **Produção realizada com sucesso!**", ephemeral=True)
        await enviar_painel_fabricacao()

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

class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🏭 Galpões Norte", style=discord.ButtonStyle.primary, custom_id="fabricacao_norte", row=0)
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

    @discord.ui.button(label="🏭 Galpões Sul", style=discord.ButtonStyle.primary, custom_id="fabricacao_sul", row=0)
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

    @discord.ui.button(label="🔫 Produzir Munição", style=discord.ButtonStyle.success, custom_id="fabricacao_municao", emoji="🎯", row=0)
    async def produzir_municao(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(ProducaoMunicaoModal())

    @discord.ui.button(label="💊 Registrar Cápsulas", style=discord.ButtonStyle.primary, custom_id="registrar_capsulas", emoji="💊", row=1)
    async def registrar_capsulas(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarCapsulasModal())

    @discord.ui.button(label="📦 Registrar Embalagens", style=discord.ButtonStyle.primary, custom_id="registrar_embalagens", emoji="📦", row=1)
    async def registrar_embalagens(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistrarEmbalagensModal())

    @discord.ui.button(label="📊 Relatório Produção", style=discord.ButtonStyle.secondary, custom_id="fabricacao_relatorio", emoji="📊", row=1)
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RelatorioProducaoModal())

    @discord.ui.button(label="📅 Alugar Galpão", style=discord.ButtonStyle.primary, custom_id="alugar_galpao", emoji="📅", row=2)
    async def alugar_galpao(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(AlugarGalpaoModal())

    @discord.ui.button(label="📊 Alugueis", style=discord.ButtonStyle.secondary, custom_id="ver_alugueis", emoji="📊", row=2)
    async def ver_alugueis(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        alugueis = await carregar_alugueis()
        embed = discord.Embed(title="📅 ── STATUS DOS ALUGUEIS ── 📅", description="🏭 VDR 442 • Galpões", color=0x1a1a2e, timestamp=agora())
        embed.set_author(name="🛡 Vida Rasa 442 • Alugueis", icon_url=bot.user.display_avatar.url if bot.user else None)
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
            embed.add_field(name=f"🏭 {galpao}", value=f"```yaml\nDias alugados: {dias}\nStatus: {status}\n```", inline=True)
        embed.set_footer(text="🛡 Vida Rasa 442", icon_url=bot.user.display_avatar.url if bot.user else None)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="🔄 Atualizar Painel", style=discord.ButtonStyle.secondary, custom_id="atualizar_painel_btn", emoji="🔄", row=2)
    async def atualizar_painel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await enviar_painel_fabricacao()
        await interaction.followup.send("✅ Painel atualizado!", ephemeral=True)

    # =========================================================
    # BOTÃO EDITAR ESTOQUE - VOLTOU!
    # =========================================================
    @discord.ui.button(label="✏️ Editar Estoque", style=discord.ButtonStyle.primary, custom_id="editar_estoque_btn", emoji="✏️", row=2)
    async def editar_estoque(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)

        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ Apenas **Administradores** ou **Gerentes** podem editar o estoque!", ephemeral=True)
            return

        estoque = await carregar_estoque()
        insumos = await carregar_estoque_insumos()
        modal = EditarEstoqueCompletoModal()
        modal.pt.placeholder = f"Atual: {fmt_num(estoque['PT'])} pacotes"
        modal.sub.placeholder = f"Atual: {fmt_num(estoque['SUB'])} pacotes"
        modal.capsulas.placeholder = f"Atual: {fmt_num(insumos['capsulas'])} unidades"
        modal.embalagens.placeholder = f"Atual: {fmt_num(insumos['embalagens'])} unidades"
        await interaction.response.send_modal(modal)

class EditarEstoqueCompletoModal(discord.ui.Modal, title="📦 EDITAR ESTOQUE COMPLETO"):
    def __init__(self):
        super().__init__(timeout=300)

    pt = discord.ui.TextInput(
        label="🔫 Quantidade de PT (pacotes)",
        placeholder="Digite a quantidade atual de PT",
        required=False,
        max_length=10
    )

    sub = discord.ui.TextInput(
        label="🔫 Quantidade de SUB (pacotes)",
        placeholder="Digite a quantidade atual de SUB",
        required=False,
        max_length=10
    )

    capsulas = discord.ui.TextInput(
        label="💊 Quantidade de Cápsulas",
        placeholder="Digite a quantidade atual de cápsulas",
        required=False,
        max_length=10
    )

    embalagens = discord.ui.TextInput(
        label="📦 Quantidade de Embalagens",
        placeholder="Digite a quantidade atual de embalagens",
        required=False,
        max_length=10
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return

        try:
            async with pool.acquire() as conn:
                if self.pt.value and self.pt.value.strip():
                    nova_pt = int(self.pt.value.replace(".", "").replace(",", ""))
                    if nova_pt < 0:
                        raise ValueError("Valores não podem ser negativos")
                    await conn.execute(
                        "UPDATE estoque_municoes SET quantidade = $1, ultima_atualizacao = NOW() WHERE tipo = 'PT'",
                        nova_pt
                    )

                if self.sub.value and self.sub.value.strip():
                    nova_sub = int(self.sub.value.replace(".", "").replace(",", ""))
                    if nova_sub < 0:
                        raise ValueError("Valores não podem ser negativos")
                    await conn.execute(
                        "UPDATE estoque_municoes SET quantidade = $1, ultima_atualizacao = NOW() WHERE tipo = 'SUB'",
                        nova_sub
                    )

                if self.capsulas.value and self.capsulas.value.strip():
                    nova_capsulas = int(self.capsulas.value.replace(".", "").replace(",", ""))
                    if nova_capsulas < 0:
                        raise ValueError("Valores não podem ser negativos")
                    await conn.execute(
                        "UPDATE estoque_capsulas SET quantidade = $1, ultima_atualizacao = NOW() WHERE id = 1",
                        nova_capsulas
                    )

                if self.embalagens.value and self.embalagens.value.strip():
                    nova_embalagens = int(self.embalagens.value.replace(".", "").replace(",", ""))
                    if nova_embalagens < 0:
                        raise ValueError("Valores não podem ser negativos")
                    await conn.execute(
                        "UPDATE estoque_embalagens SET quantidade = $1, ultima_atualizacao = NOW() WHERE id = 1",
                        nova_embalagens
                    )

            await enviar_painel_fabricacao()

            estoque_atual = await carregar_estoque()
            insumos_atual = await carregar_estoque_insumos()

            embed = discord.Embed(
                title="✅ ── ESTOQUE ATUALIZADO ── ✅",
                description="📦 Sistema de Estoque • VDR 442",
                color=0x2ecc71,
                timestamp=agora()
            )

            embed.set_author(
                name="🛡 Vida Rasa 442 • Estoque",
                icon_url=bot.user.display_avatar.url if bot.user else None
            )

            embed.add_field(
                name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                value="",
                inline=False
            )

            embed.add_field(
                name="🔫 MUNIÇÕES",
                value=(
                    f"```yaml\n"
                    f"PT: {fmt_num(estoque_atual['PT'])} pacotes\n"
                    f"SUB: {fmt_num(estoque_atual['SUB'])} pacotes\n"
                    f"```"
                ),
                inline=True
            )

            embed.add_field(
                name="💊 INSUMOS",
                value=(
                    f"```yaml\n"
                    f"Cápsulas: {fmt_num(insumos_atual['capsulas'])} unidades\n"
                    f"Embalagens: {fmt_num(insumos_atual['embalagens'])} unidades\n"
                    f"```"
                ),
                inline=True
            )

            embed.set_footer(
                text=f"🛡 Vida Rasa 442 • Atualizado por {interaction.user.display_name}",
                icon_url=bot.user.display_avatar.url if bot.user else None
            )

            await interaction.followup.send(embed=embed, ephemeral=True)

        except ValueError as e:
            await interaction.followup.send(f"❌ {str(e)}", ephemeral=True)
        except Exception as e:
            logger.error(f"❌ Erro ao editar estoque: {e}")
            await interaction.followup.send(f"❌ Erro ao editar estoque: {e}", ephemeral=True)

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

class PolvoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📝 Registrar Compra de Pólvora", style=discord.ButtonStyle.primary, custom_id="polvora_btn", emoji="📝")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PolvoraModal())

    @discord.ui.button(label="💣 Relatório de Pólvora (Hoje)", style=discord.ButtonStyle.success, custom_id="polvora_relatorio_hoje", emoji="💣", row=1)
    async def relatorio_hoje(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        
        data_hoje = agora().strftime("%d/%m/%Y")
        data_inicio = agora().replace(hour=0, minute=0, second=0, microsecond=0)
        data_fim = agora().replace(hour=23, minute=59, second=59, microsecond=0)
        
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT user_id, SUM(quantidade) as total_quantidade, SUM(valor) as total_valor, COUNT(*) as quantidade
                FROM polvoras 
                WHERE data::date BETWEEN $1::date AND $2::date
                GROUP BY user_id 
                ORDER BY total_quantidade DESC""",
                data_inicio, data_fim
            )
            total_quantidade = await conn.fetchval(
                "SELECT COALESCE(SUM(quantidade), 0) FROM polvoras WHERE data::date BETWEEN $1::date AND $2::date",
                data_inicio, data_fim
            )
            total_valor = await conn.fetchval(
                "SELECT COALESCE(SUM(valor), 0) FROM polvoras WHERE data::date BETWEEN $1::date AND $2::date",
                data_inicio, data_fim
            )
        
        if not rows:
            await interaction.followup.send(f"📭 Nenhuma compra de pólvora registrada hoje.", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="💣 RELATÓRIO DE PÓLVORA - HOJE",
            description=f"📅 **Data:** {data_hoje}",
            color=Cores.PRODUCAO,
            timestamp=agora()
        )
        embed.set_author(name="🛡 Vida Rasa 442 • Relatório de Pólvora")
        
        texto = ""
        for i, row in enumerate(rows, 1):
            user = await pegar_usuario(int(row["user_id"]))
            nome = user.display_name if user else row["user_id"]
            texto += f"**{i}.** {nome}\n"
            texto += f"   💣 Quantidade: **{fmt_num(row['total_quantidade'])}** unidades\n"
            texto += f"   💰 Valor: **{formatar_dinheiro(row['total_valor'])}**\n"
            texto += f"   📦 Compras: **{row['quantidade']}**\n\n"
        
        embed.add_field(name="👥 COMPRADORES", value=texto, inline=False)
        embed.add_field(name="💣 TOTAL DE PÓLVORA", value=f"{fmt_num(total_quantidade)} unidades", inline=True)
        embed.add_field(name="💰 TOTAL GASTO", value=formatar_dinheiro(total_valor), inline=True)
        embed.add_field(name="📦 TOTAL DE COMPRAS", value=sum(r["quantidade"] for r in rows), inline=True)
        embed.set_footer(text="Relatório gerado pelo sistema VDR")
        
        await interaction.followup.send(embed=embed, ephemeral=False)

    @discord.ui.button(label="📅 Relatório por Data", style=discord.ButtonStyle.secondary, custom_id="polvora_relatorio_data", emoji="📅", row=1)
    async def relatorio_data(self, interaction: discord.Interaction, button: discord.ui.Button):
        modal = RelatorioPolvoraModal()
        await interaction.response.send_modal(modal)

class RelatorioPolvoraModal(discord.ui.Modal, title="📅 RELATÓRIO DE PÓLVORA"):
    data_inicio = discord.ui.TextInput(
        label="📅 Data INÍCIO (DD/MM/AAAA)",
        placeholder="Ex: 01/08/2026",
        required=True
    )
    data_fim = discord.ui.TextInput(
        label="📅 Data FIM (DD/MM/AAAA)",
        placeholder="Ex: 31/08/2026",
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        data_inicio_str = self.data_inicio.value.strip()
        data_fim_str = self.data_fim.value.strip()
        
        try:
            data_inicio = datetime.strptime(data_inicio_str, "%d/%m/%Y").replace(hour=0, minute=0, second=0)
            data_fim = datetime.strptime(data_fim_str, "%d/%m/%Y").replace(hour=23, minute=59, second=59)
        except:
            await interaction.followup.send("❌ Formato inválido! Use DD/MM/AAAA", ephemeral=True)
            return
        
        if data_fim < data_inicio:
            await interaction.followup.send("❌ Data FIM deve ser depois da data INÍCIO!", ephemeral=True)
            return
        
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """SELECT user_id, SUM(quantidade) as total_quantidade, SUM(valor) as total_valor, COUNT(*) as quantidade
                FROM polvoras 
                WHERE data::date BETWEEN $1::date AND $2::date
                GROUP BY user_id 
                ORDER BY total_quantidade DESC""",
                data_inicio, data_fim
            )
            total_quantidade = await conn.fetchval(
                "SELECT COALESCE(SUM(quantidade), 0) FROM polvoras WHERE data::date BETWEEN $1::date AND $2::date",
                data_inicio, data_fim
            )
            total_valor = await conn.fetchval(
                "SELECT COALESCE(SUM(valor), 0) FROM polvoras WHERE data::date BETWEEN $1::date AND $2::date",
                data_inicio, data_fim
            )
        
        if not rows:
            await interaction.followup.send(f"📭 Nenhuma compra de pólvora no período **{data_inicio_str}** a **{data_fim_str}**", ephemeral=True)
            return
        
        embed = discord.Embed(
            title="💣 RELATÓRIO DE PÓLVORA",
            description=f"📅 **Período:** {data_inicio_str} a {data_fim_str}",
            color=Cores.PRODUCAO,
            timestamp=agora()
        )
        embed.set_author(name="🛡 Vida Rasa 442 • Relatório de Pólvora")
        
        texto = ""
        for i, row in enumerate(rows, 1):
            user = await pegar_usuario(int(row["user_id"]))
            nome = user.display_name if user else row["user_id"]
            texto += f"**{i}.** {nome}\n"
            texto += f"   💣 Quantidade: **{fmt_num(row['total_quantidade'])}** unidades\n"
            texto += f"   💰 Valor: **{formatar_dinheiro(row['total_valor'])}**\n"
            texto += f"   📦 Compras: **{row['quantidade']}**\n\n"
        
        embed.add_field(name="👥 COMPRADORES", value=texto, inline=False)
        embed.add_field(name="💣 TOTAL DE PÓLVORA", value=f"{fmt_num(total_quantidade)} unidades", inline=True)
        embed.add_field(name="💰 TOTAL GASTO", value=formatar_dinheiro(total_valor), inline=True)
        embed.add_field(name="📦 TOTAL DE COMPRAS", value=sum(r["quantidade"] for r in rows), inline=True)
        embed.set_footer(text="Relatório gerado pelo sistema VDR")
        
        await interaction.followup.send(embed=embed, ephemeral=False)

# =========================================================
# 13.3 FUNÇÕES DE ACOMPANHAR PRODUÇÃO
# =========================================================
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
        desc = f"**Galpão:** {prod['galpao']}\n**Quantidade de galpões:** {qtd_galpoes}\n**Iniciado por:** <@{prod['autor']}>\n"
        if prod.get("obs"):
            desc += f"📝 **Obs:** {prod['obs']}\n"
        desc += f"**Pólvora por galpão:** {prod.get('polvora_por_galpao', 400)}\n**Pólvora total:** {polvora_total}\nInício: <t:{int(inicio.timestamp())}:t>\nTérmino: <t:{int(fim.timestamp())}:t>\n\n⏳ **Restante:** {mins}m {segundos}s\n{barra(pct)}"
        if prod.get("segunda_task_confirmada"):
            uid = prod["segunda_task_confirmada"]["user"]
            desc += f"\n\n✅ **Segunda task concluída por:** <@{uid}>"
        return desc
    except Exception as e:
        logger.error(f"❌ Erro ao gerar descrição: {e}")
        return f"**Galpão:** {prod.get('galpao', 'Desconhecido')}\n⏳ **Erro ao carregar dados...**"

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
                return
            canal = bot.get_channel(prod["canal_id"])
            if not canal:
                await asyncio.sleep(10)
                continue
            guild = bot.get_guild(GUILD_ID)
            autor_apelido = await pegar_apelido(prod["autor"], guild)
            if msg is None:
                try:
                    msg = await safe_fetch_message(canal, prod["msg_id"])
                except:
                    embed = discord.Embed(title="🏭 ── PRODUÇÃO EM ANDAMENTO ── 🏭", description="🔫 Sistema de Produção • VDR 442", color=0x3498db, timestamp=agora())
                    embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
                    embed.set_author(name="🛡 Vida Rasa 442 • Produção", icon_url=bot.user.display_avatar.url if bot.user else None)
                    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                    embed.add_field(name="🏭 GALPÃO", value=f"```yaml\n{prod['galpao']}\n```", inline=True)
                    embed.add_field(name="👤 INICIADO POR", value=f"```yaml\n{autor_apelido}\n```", inline=True)
                    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                    embed.add_field(name="💣 PÓLVORA", value=f"```yaml\nPor galpão: {prod.get('polvora_por_galpao', 400)}\nTotal: {prod.get('polvora', 400)}\n```", inline=True)
                    embed.add_field(name="📊 QUANTIDADE", value=f"```yaml\n{prod.get('qtd_galpoes', 1)} galpão(ões)\n```", inline=True)
                    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                    if prod.get("obs"):
                        embed.add_field(name="📝 OBSERVAÇÃO", value=f"```yaml\n{prod['obs']}\n```", inline=False)
                    embed.add_field(name="📅 HORÁRIOS", value=f"```yaml\nInício: {inicio.strftime('%H:%M')}\nTérmino: {fim.strftime('%H:%M')}\n```", inline=False)
                    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                    embed.add_field(name="⏳ AGUARDANDO INÍCIO", value="```yaml\nA produção vai começar em breve...\n```", inline=False)
                    embed.set_footer(text=f"🛡 Vida Rasa 442 • Atualizado em {agora().strftime('%d/%m/%Y %H:%M:%S')}", icon_url=bot.user.display_avatar.url if bot.user else None)
                    view = None if prod.get("segunda_task_confirmada") else SegundaTaskView(pid)
                    msg = await safe_request(canal.send, embed=embed, view=view)
                    if msg:
                        await BotaoPersistente.salvar_botao(msg.id, canal.id, "producao", {"pid": pid})
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
                    mins = int(restante // 60)
                    segundos = int(restante % 60)
                    embed = discord.Embed(title="🏭 ── PRODUÇÃO EM ANDAMENTO ── 🏭", description="🔫 Sistema de Produção • VDR 442", color=0x3498db, timestamp=agora())
                    embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
                    embed.set_author(name="🛡 Vida Rasa 442 • Produção", icon_url=bot.user.display_avatar.url if bot.user else None)
                    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                    embed.add_field(name="🏭 GALPÃO", value=f"```yaml\n{prod['galpao']}\n```", inline=True)
                    embed.add_field(name="👤 INICIADO POR", value=f"```yaml\n{autor_apelido}\n```", inline=True)
                    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                    embed.add_field(name="💣 PÓLVORA", value=f"```yaml\nPor galpão: {prod.get('polvora_por_galpao', 400)}\nTotal: {prod.get('polvora', 400)}\n```", inline=True)
                    embed.add_field(name="📊 QUANTIDADE", value=f"```yaml\n{prod.get('qtd_galpoes', 1)} galpão(ões)\n```", inline=True)
                    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                    embed.add_field(name="⏳ RESTANTE", value=f"```yaml\n{mins}m {segundos}s\n```", inline=True)
                    embed.add_field(name="📊 PROGRESSO", value=f"```yaml\n{int(pct * 100)}%\n```", inline=True)
                    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                    embed.add_field(name="📅 HORÁRIOS", value=f"```yaml\nInício: {inicio.strftime('%H:%M')}\nTérmino: {fim.strftime('%H:%M')}\n```", inline=False)
                    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                    barra_progresso = "▓" * int(pct * 20) + "░" * (20 - int(pct * 20))
                    embed.add_field(name=f"📊 PROGRESSO • {int(pct * 100)}%", value=f"```prolog\n{barra_progresso}\n```", inline=False)
                    if prod.get("obs"):
                        embed.add_field(name="📝 OBSERVAÇÃO", value=f"```yaml\n{prod['obs']}\n```", inline=False)
                    if prod.get("segunda_task_confirmada"):
                        segunda_apelido = await pegar_apelido(prod["segunda_task_confirmada"]["user"], guild)
                        embed.add_field(name="✅ SEGUNDA TASK", value=f"```yaml\nConcluída por: {segunda_apelido}\n```", inline=False)
                    embed.set_footer(text=f"🛡 Vida Rasa 442 • Atualizado em {agora().strftime('%d/%m/%Y %H:%M:%S')}", icon_url=bot.user.display_avatar.url if bot.user else None)
                    try:
                        await safe_request(msg.edit, embed=embed)
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
                await conn.execute("INSERT INTO producoes_finalizadas (user_id, capsulas, data, polvora, galpao) VALUES ($1, $2, $3, $4, $5)", str(prod["autor"]), capsulas_total, agora_db(), polvora_total, f"{galpao} ({qtd_galpoes} galpões)")
                await conn.execute("UPDATE estoque_capsulas SET quantidade = quantidade + $1, ultima_atualizacao = NOW() WHERE id = 1", capsulas_total)
                await conn.execute("INSERT INTO entrada_insumos (tipo, quantidade, registrado_por, obs) VALUES ($1, $2, $3, $4)", "capsulas", capsulas_total, str(prod["autor"]), f"Produção do {galpao} - {qtd_galpoes} galpões - {polvora_total} pólvora")
        guild = bot.get_guild(GUILD_ID)
        autor_apelido = await pegar_apelido(prod["autor"], guild)
        if msg:
            try:
                embed = discord.Embed(title="🏭 ── PRODUÇÃO FINALIZADA ── 🏭", description="🔫 Sistema de Produção • VDR 442", color=0x2ecc71, timestamp=agora())
                embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
                embed.set_author(name="🛡 Vida Rasa 442 • Produção Concluída", icon_url=bot.user.display_avatar.url if bot.user else None)
                embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                embed.add_field(name="🏭 GALPÃO", value=f"```yaml\n{galpao}\n```", inline=True)
                embed.add_field(name="👤 PRODUZIDO POR", value=f"```yaml\n{autor_apelido}\n```", inline=True)
                embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                embed.add_field(name="💊 CÁPSULAS PRODUZIDAS", value=f"```yaml\n{fmt_num(capsulas_total)} unidades\n```", inline=True)
                embed.add_field(name="📦 POR GALPÃO", value=f"```yaml\n{fmt_num(capsulas_por_galpao)} cápsulas\n```", inline=True)
                embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                embed.add_field(name="🏭 QUANTIDADE", value=f"```yaml\n{qtd_galpoes} galpão(ões)\n```", inline=True)
                embed.add_field(name="⚖️ PESO TOTAL", value=f"```yaml\n{peso_total:.2f} kg\n```", inline=True)
                embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                embed.add_field(name="💣 PÓLVORA UTILIZADA", value=f"```yaml\nPor galpão: {polvora_por_galpao}\nTotal: {polvora_total}\n```", inline=False)
                embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
                if segunda:
                    segunda_apelido = await pegar_apelido(segunda["user"], guild)
                    embed.add_field(name="✅ SEGUNDA TASK", value=f"```yaml\nConcluída por: {segunda_apelido}\n```", inline=False)
                embed.add_field(name="✅ STATUS", value="💊 **As cápsulas foram adicionadas ao estoque de insumos!**", inline=False)
                embed.set_footer(text=f"🛡 Vida Rasa 442 • Finalizado em {agora().strftime('%d/%m/%Y %H:%M:%S')}", icon_url=bot.user.display_avatar.url if bot.user else None)
                await safe_request(msg.edit, embed=embed, view=None)
            except Exception as e:
                logger.error(f"Erro ao editar mensagem final: {e}")
        await deletar_producao(pid)
        if pid in producoes_tasks:
            del producoes_tasks[pid]
        canal_bau = bot.get_channel(CANAL_BAU_GALPAO_ID)
        if canal_bau:
            embed_bau = discord.Embed(title="🏭 ── PRODUÇÃO FINALIZADA ── 🏭", description="🔫 Sistema de Produção • VDR 442", color=0x2ecc71, timestamp=agora())
            embed_bau.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
            embed_bau.set_author(name="🛡 Vida Rasa 442 • Produção Concluída", icon_url=bot.user.display_avatar.url if bot.user else None)
            embed_bau.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed_bau.add_field(name="🏭 GALPÃO", value=f"```yaml\n{galpao}\n```", inline=True)
            embed_bau.add_field(name="🏭 QUANTIDADE", value=f"```yaml\n{qtd_galpoes} galpão(ões)\n```", inline=True)
            embed_bau.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed_bau.add_field(name="💊 CÁPSULAS PRODUZIDAS", value=f"```yaml\n{fmt_num(capsulas_total)} unidades\n```", inline=True)
            embed_bau.add_field(name="📦 POR GALPÃO", value=f"```yaml\n{fmt_num(capsulas_por_galpao)} cápsulas\n```", inline=True)
            embed_bau.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed_bau.add_field(name="💣 PÓLVORA TOTAL", value=f"```yaml\n{polvora_total}\n```", inline=True)
            embed_bau.add_field(name="👤 PRODUZIDO POR", value=f"```yaml\n{autor_apelido}\n```", inline=True)
            embed_bau.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            if segunda:
                segunda_apelido = await pegar_apelido(segunda["user"], guild)
                embed_bau.add_field(name="✅ SEGUNDA TASK", value=f"```yaml\nConcluída por: {segunda_apelido}\n```", inline=False)
            embed_bau.add_field(name="✅ STATUS", value="💊 **Cápsulas adicionadas ao estoque de insumos!**", inline=False)
            embed_bau.set_footer(text=f"🛡 Vida Rasa 442 • Finalizado em {agora().strftime('%d/%m/%Y %H:%M:%S')}", icon_url=bot.user.display_avatar.url if bot.user else None)
            await canal_bau.send(embed=embed_bau)
        await enviar_painel_fabricacao()
    except Exception as e:
        logger.error(f"❌ ERRO ao finalizar produção {pid}: {e}")

# =========================================================
# 13.4 FUNÇÃO DE VERIFICAR HEARTBEAT DE PRODUÇÕES
# =========================================================
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

# =========================================================
# 13.5 FUNÇÃO DE RESTAURAR PRODUÇÕES
# =========================================================
async def restaurar_producoes():
    """Restaura produções ativas após reinicialização do bot"""
    try:
        pool = await get_pool()
        if not pool:
            logger.error("❌ Banco de dados indisponível para restaurar produções")
            return
        
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT pid FROM producoes WHERE CAST(fim AS timestamp) > NOW()")
        
        if not rows:
            logger.info("📭 Nenhuma produção ativa para restaurar")
            return
        
        logger.info(f"🔄 Restaurando {len(rows)} produções...")
        
        for row in rows:
            pid = row["pid"]
            if pid not in producoes_tasks or producoes_tasks[pid].done():
                if pid in producoes_tasks:
                    del producoes_tasks[pid]
                task = asyncio.create_task(acompanhar_producao(pid))
                producoes_tasks[pid] = task
                logger.info(f"✅ Produção {pid} restaurada")
        
        logger.info(f"✅ {len(rows)} produções restauradas com sucesso!")
        
    except Exception as e:
        logger.error(f"❌ Erro ao restaurar produções: {e}")

# =========================================================
# 13.6 FUNÇÃO DE ENVIAR PAINEL DE FABRICAÇÃO
# =========================================================
async def enviar_painel_fabricacao():
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    if not canal:
        logger.error("❌ Canal de fabricação não encontrado")
        return
    estoque_municoes = await carregar_estoque()
    estoque_insumos = await carregar_estoque_insumos()
    alugueis = await carregar_alugueis()
    embed = discord.Embed(title="🛢️ ── PAINEL DE FABRICAÇÃO ── 🛢️", description="🔫 Sistema de Produção • VDR 442", color=Cores.PRODUCAO, timestamp=agora())
    embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
    embed.set_author(name="🛡 Vida Rasa 442 • Sistema de Produção", icon_url=bot.user.display_avatar.url if bot.user else None)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    texto_alugueis = ""
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
        texto_alugueis += f"🏭 {galpao}  →  {dias} dias  ({status})\n"
    embed.add_field(name="📅 ALUGUEL DE GALPÕES", value=f"```yaml\n{texto_alugueis or 'Nenhum aluguel registrado'}\n```", inline=False)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="🔫 ESTOQUE DE MUNIÇÕES",
        value=f"🔫 PT   →  **{fmt_num(estoque_municoes['PT'])}** pacotes  ({fmt_num(estoque_municoes['PT'] * 50)} munições)\n🔫 SUB  →  **{fmt_num(estoque_municoes['SUB'])}** pacotes  ({fmt_num(estoque_municoes['SUB'] * 50)} munições)",
        inline=True
    )
    embed.add_field(
        name="💊 ESTOQUE DE INSUMOS",
        value=f"💊 Cápsulas     →  **{fmt_num(estoque_insumos['capsulas'])}** unidades\n📦 Embalagens   →  **{fmt_num(estoque_insumos['embalagens'])}** unidades",
        inline=True
    )
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="🏭 PRODUÇÃO DE CÁPSULAS",
        value="```yaml\n🟢 GALPÕES NORTE  →  65 min (3 galpões)\n🟢 GALPÕES SUL    →  130 min (3 galpões)\n\n💡 INFORME:\n   • Quantos galpões (1, 2 ou 3)\n   • Pólvora por galpão\n```",
        inline=False
    )
    embed.set_footer(text=f"🛡 Vida Rasa 442 • Atualizado em {agora().strftime('%d/%m/%Y %H:%M:%S')}", icon_url=bot.user.display_avatar.url if bot.user else None)
    view = FabricacaoView()
    try:
        async for msg in canal.history(limit=20):
            if msg.author == bot.user and msg.embeds and len(msg.embeds) > 0:
                if "PAINEL DE FABRICAÇÃO" in msg.embeds[0].title:
                    try:
                        await msg.delete()
                    except:
                        pass
        await canal.send(embed=embed, view=view)
    except Exception as e:
        logger.error(f"❌ Erro ao enviar painel de fabricação: {e}")

async def enviar_painel_polvoras():
    canal = bot.get_channel(CANAL_CALCULO_POLVORA_ID)
    if not canal:
        logger.error("❌ Canal de pólvora não encontrado")
        return
    embed = discord.Embed(
        title="💣 Registro de Pólvora",
        description=f"**Clique no botão abaixo para registrar a compra de pólvora.**\n\n📌 **Informe apenas a quantidade comprada.**\n💰 O valor será calculado automaticamente (R$ {PRECO_POLVORA:.2f} por unidade).",
        color=0xe67e22
    )
    await enviar_ou_atualizar_painel("painel_polvora", CANAL_CALCULO_POLVORA_ID, embed, PolvoraView())

# =========================================================
# ==================== PARTE 14: SISTEMA DE METAS =========
# =========================================================
# (SEM PÓLVORA - TABELA METAS SEM CAMPO POLVORA)

# =========================================================
# 14.1 FUNÇÕES DE BANCO DE DADOS - METAS
# =========================================================
async def carregar_metas_db():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT user_id, canal_id, dinheiro, acao, dinheiro_acoes, saldo_excedente FROM metas")
    except Exception as e:
        logger.error(f"❌ Erro ao carregar metas: {e}")
        return []

async def salvar_meta_db(user_id, canal_id, dinheiro, acao):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            if acao is not None:
                acao = str(acao)
            await conn.execute("INSERT INTO metas (user_id, canal_id, dinheiro, acao, dinheiro_acoes, saldo_excedente) VALUES ($1,$2,$3,$4,0,0) ON CONFLICT (user_id) DO UPDATE SET canal_id=$2, dinheiro=$3, acao=$4", str(user_id), str(canal_id), dinheiro, acao)
    except Exception as e:
        logger.error(f"❌ Erro ao salvar meta: {e}")

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
            falta_para_meta = max(0, META_LIMITE - dinheiro_atual)
            if valor <= falta_para_meta:
                novo_dinheiro = dinheiro_atual + valor
                await conn.execute("UPDATE metas SET dinheiro = $1 WHERE user_id = $2", novo_dinheiro, str(user_id))
            else:
                novo_dinheiro = META_LIMITE
                novo_excedente = saldo_excedente + (valor - falta_para_meta)
                await conn.execute("UPDATE metas SET dinheiro = $1, saldo_excedente = $2 WHERE user_id = $3", novo_dinheiro, novo_excedente, str(user_id))
            return True
    except Exception as e:
        logger.error(f"❌ Erro ao adicionar dinheiro: {e}")
        return False

async def depositar_na_meta(user_id, valor, motivo):
    pool = await get_pool()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT dinheiro, dinheiro_acoes, saldo_excedente FROM metas WHERE user_id = $1", str(user_id))
            if not meta:
                return False
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
                    await conn.execute("UPDATE metas SET dinheiro = $1, saldo_excedente = $2 WHERE user_id = $3", novo_dinheiro, novo_excedente, str(user_id))
                canal_id = await conn.fetchval("SELECT canal_id FROM metas WHERE user_id = $1", str(user_id))
                if canal_id:
                    canal = bot.get_channel(int(canal_id))
                    if canal:
                        await canal.send(f"💰 **Depósito recebido!**\n📝 Motivo: {motivo}\n💵 Valor: {formatar_dinheiro(valor)}\n✨ **Saldo atualizado na sua meta!**")
            return True
    except Exception as e:
        logger.error(f"❌ Erro ao depositar na meta: {e}")
        return False

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

async def criar_sala_meta(member: discord.Member):
    """Cria uma sala de meta para um membro"""
    guild = member.guild
    pool = await get_pool()
    if not pool:
        logger.error("❌ Banco de dados indisponível em criar_sala_meta")
        return None
    
    try:
        async with pool.acquire() as conn:
            # =========================================================
            # VERIFICAR SE JÁ TEM META NO BANCO
            # =========================================================
            meta_existente = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(member.id))
            
            if meta_existente:
                canal_id = int(meta_existente["canal_id"])
                canal_existe = guild.get_channel(canal_id)
                
                if canal_existe:
                    # Sala existe, apenas atualizar cache e retornar
                    metas_cache[str(member.id)] = {
                        "canal_id": canal_id,
                        "dinheiro": meta_existente["dinheiro"] or 0,
                        "acao": meta_existente["acao"],
                        "dinheiro_acoes": meta_existente.get("dinheiro_acoes") or 0,
                        "saldo_excedente": meta_existente.get("saldo_excedente") or 0
                    }
                    await atualizar_embed_meta(member.id)
                    
                    # Dar acesso aos responsáveis
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
                    # Canal não existe mais, deletar do banco
                    await conn.execute("DELETE FROM metas WHERE user_id = $1", str(member.id))
                    if str(member.id) in metas_cache:
                        del metas_cache[str(member.id)]
            
            # =========================================================
            # PROCURAR CANAL EXISTENTE PELO NOME
            # =========================================================
            nome_canal = f"📁・{member.display_name.lower().replace(' ', '-')}"
            for canal in guild.text_channels:
                if canal.name.lower() == nome_canal.lower():
                    # Encontrou um canal com o mesmo nome
                    await salvar_meta_db(member.id, canal.id, 0, 0)
                    metas_cache[str(member.id)] = {
                        "canal_id": canal.id,
                        "dinheiro": 0,
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
            
            # =========================================================
            # CRIAR NOVA SALA
            # =========================================================
            categoria_id = obter_categoria_meta(member)
            if not categoria_id:
                logger.error(f"❌ Categoria não encontrada para {member.display_name}")
                return None
            
            categoria = guild.get_channel(categoria_id)
            if not categoria:
                logger.error(f"❌ Categoria {categoria_id} não encontrada")
                return None
            
            # Criar overwrites
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
            
            # Criar o canal
            nome_canal = f"📁・{member.display_name.lower().replace(' ', '-')}"
            canal = await guild.create_text_channel(nome_canal, category=categoria, overwrites=overwrites)
            
            # Salvar no banco
            await salvar_meta_db(member.id, canal.id, 0, 0)
            metas_cache[str(member.id)] = {
                "canal_id": canal.id,
                "dinheiro": 0,
                "acao": None,
                "dinheiro_acoes": 0,
                "saldo_excedente": 0
            }
            
            await asyncio.sleep(1)
            await atualizar_embed_meta(member.id)
            
            # Dar acesso aos responsáveis
            cargo_resp = guild.get_role(CARGO_RESP_METAS_ID)
            if cargo_resp:
                for resp_member in guild.members:
                    if cargo_resp in resp_member.roles:
                        try:
                            await canal.set_permissions(resp_member, view_channel=True, send_messages=True)
                        except Exception as e:
                            logger.error(f"❌ Erro ao dar acesso a {resp_member.display_name}: {e}")
            
            logger.info(f"✅ Sala criada para {member.display_name}: {canal.name}")
            return canal
            
    except Exception as e:
        logger.error(f"❌ Erro ao criar sala meta para {member.display_name}: {e}")
        return None

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
            await salvar_meta_db(user_id, canal.id, 0, 0)
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(user_id))
            if not meta:
                return
            metas_cache[str(user_id)] = {"canal_id": canal.id, "dinheiro": 0, "acao": None, "dinheiro_acoes": 0, "saldo_excedente": 0}
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
        saldo_excedente = meta.get("saldo_excedente") or 0
        acao = meta.get("acao") or "Nenhuma"
        meta_total = await definir_valor_meta_por_cargo(member) if member else 300000
        embed = discord.Embed(title=f"💀 ── META SEMANAL ── 💀", description=f"👤 {nome.upper()} • VDR 442", color=Cores.META, timestamp=agora())
        if member:
            embed.set_thumbnail(url=member.display_avatar.url)
        embed.set_author(name="🛡 Vida Rasa 442 • Sistema de Metas", icon_url=bot.user.display_avatar.url if bot.user else None)
        embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
        embed.add_field(name="💰 DINHEIRO SUJO (META)", value=f"```yaml\n{formatar_dinheiro(dinheiro_meta)}\n```", inline=False)
        if is_soldado:
            embed.add_field(name="🎯 DINHEIRO DE AÇÕES", value=f"```yaml\n{formatar_dinheiro(dinheiro_acoes)}\n```", inline=False)
        if saldo_excedente > 0:
            embed.add_field(name="📦 SALDO EXCEDENTE", value=f"```yaml\n{formatar_dinheiro(saldo_excedente)}\n```", inline=False)
        embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
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
        elif progresso >= 1:
            status_meta = "✅ META CONCLUÍDA! 🎉"
        elif progresso >= 0.7:
            status_meta = "🟢 Quase lá!"
        elif progresso >= 0.4:
            status_meta = "🟡 Vamos acelerar!"
        elif progresso >= 0.1:
            status_meta = "🟠 Começando..."
        else:
            status_meta = "🔴 Comece já!"
        embed.add_field(name=f"📊 PROGRESSO • {porcentagem}%", value=f"```prolog\n{barra_progresso}\n{formatar_dinheiro(valor_progresso)} / {formatar_dinheiro(meta_total)}\n\n{status_meta}\n```", inline=False)
        embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
        if is_soldado:
            texto_acao = "**🎯 Participar de Ações** - Sua meta é paga com ações realizadas\n**💰 Adicionar Dinheiro Sujo** - Registre dinheiro extra"
        else:
            texto_acao = "**💰 Adicionar Dinheiro Sujo** - Registre dinheiro da meta"
        embed.add_field(name="⚙️ COMO USAR", value=texto_acao, inline=False)
        embed.set_footer(text=f"🛡 Vida Rasa 442 • Atualizado em {agora().strftime('%d/%m/%Y %H:%M')} • ID: {user_id}", icon_url=bot.user.display_avatar.url if bot.user else None)
        async for msg in canal.history(limit=30):
            if msg.author == bot.user:
                try:
                    await msg.delete()
                    await asyncio.sleep(0.3)
                except:
                    pass
        msg = await canal.send(embed=embed, view=MetaView(user_id))
        await BotaoPersistente.salvar_botao(msg.id, canal.id, "meta", {"user_id": user_id})
        await verificar_meta_concluida(user_id, valor_progresso)
    except Exception as e:
        logger.error(f"❌ Erro ao atualizar embed da meta: {e}")

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
            await asyncio.sleep(1.5)
            await atualizar_embed_meta(user_id)
        except Exception as e:
            logger.error(f"Erro ao recolocar painel: {e}")
    except Exception as e:
        logger.error(f"❌ Erro ao fixar painel: {e}")

# =========================================================
# 14.2 VIEWS DE METAS
# =========================================================
class MetaView(discord.ui.View):
    def __init__(self, user_id):
        super().__init__(timeout=None)
        self.user_id = user_id

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
            dados = {"dinheiro": meta["dinheiro"] or 0, "saldo_excedente": meta.get("saldo_excedente") or 0}
            await interaction.response.send_modal(EditarMetaModal(self.user_id, dados))
        except Exception as e:
            logger.error(f"❌ Erro no botão Editar Meta: {e}")
            try:
                await interaction.response.send_message(f"❌ Erro: {str(e)[:100]}", ephemeral=True)
            except:
                pass

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

        # Verificar se a meta existe
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

class EditarMetaModal(discord.ui.Modal, title="✏️ Editar Meta"):
    def __init__(self, user_id, dados_atuais):
        super().__init__(timeout=300)
        self.user_id = user_id
        self.dinheiro = discord.ui.TextInput(label="💰 Dinheiro Sujo (Meta)", placeholder="Digite o valor correto", default=str(dados_atuais.get("dinheiro", 0)), required=True, max_length=15)
        self.saldo_excedente = discord.ui.TextInput(label="📦 Saldo Excedente (Próxima semana)", placeholder="Digite o valor correto", default=str(dados_atuais.get("saldo_excedente", 0)), required=True, max_length=15)
        self.add_item(self.dinheiro)
        self.add_item(self.saldo_excedente)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            novo_dinheiro = safe_int(self.dinheiro.value)
            novo_saldo_excedente = safe_int(self.saldo_excedente.value)
            if novo_dinheiro < 0 or novo_saldo_excedente < 0:
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
                await conn.execute("UPDATE metas SET dinheiro = $1, saldo_excedente = $2 WHERE user_id = $3", novo_dinheiro, novo_saldo_excedente, str(self.user_id))
            if str(self.user_id) in metas_cache:
                metas_cache[str(self.user_id)]["dinheiro"] = novo_dinheiro
                metas_cache[str(self.user_id)]["saldo_excedente"] = novo_saldo_excedente
            await atualizar_embed_meta(self.user_id)
            embed = discord.Embed(title="✅ META ATUALIZADA COM SUCESSO!", description=f"**👤 <@{self.user_id}>**", color=0x2ecc71, timestamp=agora())
            embed.add_field(name="💰 Dinheiro Sujo", value=formatar_dinheiro(novo_dinheiro), inline=True)
            embed.add_field(name="📦 Saldo Excedente", value=formatar_dinheiro(novo_saldo_excedente), inline=True)
            await interaction.followup.send(embed=embed, ephemeral=True)
        except Exception as e:
            logger.error(f"❌ Erro ao editar meta: {e}")
            await interaction.followup.send(f"❌ Erro ao editar meta: {str(e)}", ephemeral=True)

class SolicitarSalaView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="➕ Criar Sala para Membro", style=discord.ButtonStyle.success, custom_id="criar_sala_gerencia")
    async def criar(self, interaction: discord.Interaction, button: discord.ui.Button):
        # =========================================================
        # VERIFICAR PERMISSÃO
        # =========================================================
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID, CARGO_01_ID, CARGO_02_ID] for r in interaction.user.roles)
        
        if not is_admin and not is_gerente:
            await interaction.response.send_message(
                "❌ **Apenas Gerentes, Cargo 01, Cargo 02 e ADM podem criar salas para outros membros!**",
                ephemeral=True
            )
            return
        
        # =========================================================
        # ABRIR MODAL PARA ESCOLHER O MEMBRO
        # =========================================================
        modal = CriarSalaParaMembroModal()
        await interaction.response.send_modal(modal)


class CriarSalaParaMembroModal(discord.ui.Modal, title="📂 Criar Sala para Membro"):
    membro_id = discord.ui.TextInput(
        label="🆔 ID do Membro",
        placeholder="Digite o ID do membro (ex: 123456789)",
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        
        try:
            user_id = int(self.membro_id.value.strip())
        except:
            await interaction.followup.send("❌ ID inválido! Digite apenas números.", ephemeral=True)
            return
        
        guild = interaction.guild
        member = guild.get_member(user_id)
        
        if not member:
            await interaction.followup.send(f"❌ Membro com ID `{user_id}` não encontrado no servidor!", ephemeral=True)
            return
        
        # Verificar se o membro já tem sala
        pool = await get_pool()
        if not pool:
            await interaction.followup.send("❌ Banco de dados indisponível!", ephemeral=True)
            return
        
        async with pool.acquire() as conn:
            meta = await conn.fetchrow("SELECT * FROM metas WHERE user_id = $1", str(user_id))
        
        if meta:
            canal = guild.get_channel(meta["canal_id"])
            if canal:
                await interaction.followup.send(f"✅ {member.mention} já possui uma sala! {canal.mention}", ephemeral=True)
                return
        
        # Criar a sala
        sala = await criar_sala_meta(member)
        
        if sala:
            await interaction.followup.send(
                f"✅ **Sala criada com sucesso para {member.mention}!**\n"
                f"📁 {sala.mention}",
                ephemeral=True
            )
        else:
            await interaction.followup.send(f"❌ Erro ao criar sala para {member.mention}!", ephemeral=True)

# =========================================================
# 14.3 FUNÇÕES DE ENVIAR PAINÉIS DE METAS
# =========================================================
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

# =========================================================
# 14.4 BOTÕES DE RELATÓRIO E FECHAMENTO
# =========================================================
class RelatorioMetasButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="📊 Gerar Relatório de Metas", style=discord.ButtonStyle.success, custom_id="relatorio_metas_btn", emoji="📊")

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(RelatorioMetasModal())

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
# 14.5 FUNÇÕES DE METAS
# =========================================================
async def buscar_historico_metas(data_inicio, data_fim):
    pool = await get_pool()
    if not pool:
        logger.error("❌ Pool do banco indisponível em buscar_historico_metas")
        return []
    try:
        async with pool.acquire() as conn:
            inicio_dt = data_inicio.replace(tzinfo=None) if hasattr(data_inicio, 'replace') else data_inicio
            fim_dt = data_fim.replace(tzinfo=None) if hasattr(data_fim, 'replace') else data_fim
            rows = await conn.fetch("SELECT * FROM metas_historico WHERE data_fechamento >= $1 AND data_fechamento <= $2 ORDER BY data_fechamento DESC", inicio_dt, fim_dt)
            return rows
    except Exception as e:
        logger.error(f"❌ Erro ao buscar histórico: {e}")
        return []

async def carregar_metas_cache():
    global metas_cache
    try:
        rows = await carregar_metas_db()
        metas_cache = {}
        for r in rows:
            metas_cache[str(r["user_id"])] = {"canal_id": int(r["canal_id"]), "dinheiro": r["dinheiro"], "acao": r["acao"], "dinheiro_acoes": r.get("dinheiro_acoes") or 0, "saldo_excedente": r.get("saldo_excedente") or 0}
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao recarregar cache de metas: {e}")
        return False

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
                acao = meta["acao"] or "N/A"
                dinheiro_acoes = meta.get("dinheiro_acoes") or 0
                try:
                    await conn.execute("INSERT INTO metas_historico (user_id, dinheiro, acao, dinheiro_acoes, data_inicio, data_fim, data_fechamento) VALUES ($1, $2, $3, $4, $5, $6, $7)", user_id, min(dinheiro, 300000), acao, dinheiro_acoes, data_inicio_naive, data_fim_naive, data_fechamento)
                    salvos += 1
                except Exception as e:
                    logger.error(f"❌ Erro ao salvar meta de {user_id} no histórico: {e}")
                relatorio.append({"user_id": user_id, "dinheiro": min(dinheiro, 300000), "acao": acao, "dinheiro_acoes": dinheiro_acoes, "total_meta": min(dinheiro, 300000), "status": status})
            membros_sem_meta = []
            if guild:
                cargos_meta = [CARGO_AGREGADO_ID, CARGO_MEMBRO_ID, CARGO_SOLDADO_ID, CARGO_01_ID, CARGO_02_ID, CARGO_RESP_METAS_ID, CARGO_RESP_ACAO_ID, CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID]
                for member in guild.members:
                    if member.bot:
                        continue
                    tem_cargo = any(r.id in cargos_meta for r in member.roles)
                    if tem_cargo:
                        tem_meta = any(m["user_id"] == str(member.id) for m in metas)
                        if not tem_meta:
                            membros_sem_meta.append({"user_id": str(member.id), "nome": member.display_name, "menção": member.mention})
            return relatorio, membros_sem_meta
    except Exception as e:
        logger.error(f"❌ Erro ao fechar todas as metas: {e}")
        return None, []

async def zerar_exibicao_metas():
    try:
        guild = bot.get_guild(GUILD_ID)
        if not guild:
            logger.error("❌ Guild não encontrada para zerar exibição")
            return 0
        pool = await get_pool()
        if not pool:
            logger.error("❌ Banco de dados indisponível para zerar exibição")
            return 0
        async with pool.acquire() as conn:
            await conn.execute("UPDATE metas SET dinheiro = 0, dinheiro_acoes = 0, saldo_excedente = 0, acao = NULL")
            logger.info("⚠️ METAS ZERADAS PARA A NOVA SEMANA!")
        await carregar_metas_cache()
        contador = 0
        for uid in list(metas_cache.keys()):
            try:
                await atualizar_embed_meta(int(uid))
                contador += 1
                await asyncio.sleep(0.5)
            except Exception as e:
                logger.error(f"❌ Erro ao atualizar meta {uid}: {e}")
        logger.info(f"✅ {contador} embeds de metas zerados e atualizados")
        return contador
    except Exception as e:
        logger.error(f"❌ Erro ao zerar exibição das metas: {e}")
        return 0

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
            "gerentes": {"cargos": [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID], "nome": "🟢 GERENTES (ISENTOS)", "cor": 0x2ecc71, "itens": [], "is_isento": True},
            "cargos_01_02": {"cargos": [CARGO_01_ID, CARGO_02_ID], "nome": "🟡 CARGOS 01/02 (ISENTOS)", "cor": 0xf1c40f, "itens": [], "is_isento": True},
            "responsaveis": {"cargos": [CARGO_RESP_METAS_ID, CARGO_RESP_ACAO_ID, CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID], "nome": "🔵 RESPONSÁVEIS", "cor": 0x3498db, "itens": [], "is_isento": False},
            "soldados": {"cargos": [CARGO_SOLDADO_ID], "nome": "🟠 SOLDADOS", "cor": 0xe67e22, "itens": [], "is_isento": False},
            "membros": {"cargos": [CARGO_MEMBRO_ID], "nome": "🔴 MEMBROS", "cor": 0xe74c3c, "itens": [], "is_isento": False},
            "agregados": {"cargos": [CARGO_AGREGADO_ID], "nome": "⚪ AGREGADOS", "cor": 0x95a5a6, "itens": [], "is_isento": False}
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
                    grupos["outros"] = {"nome": "📌 OUTROS", "cor": 0x808080, "itens": [], "is_isento": False}
                grupos["outros"]["itens"].append(item_dict)
        canal_resultados = interaction.guild.get_channel(RESULTADOS_METAS_ID)
        if not canal_resultados:
            canal_resultados = interaction.channel
        titulo = f"📊 RELATÓRIO DE METAS FECHADAS"
        if titulo_extra:
            titulo = f"📊 {titulo_extra}"
        embed_resumo = discord.Embed(title=titulo, description=f"📅 **Período:** {data_inicio_str} até {data_fim_str}", color=0x2ecc71, timestamp=agora())
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
        await asyncio.sleep(1.5)
        for grupo_key, grupo_data in grupos.items():
            if not grupo_data["itens"]:
                continue
            itens_ordenados = sorted(grupo_data["itens"], key=lambda x: x["total_geral"], reverse=True)
            if grupo_data.get("is_isento", False):
                for i in range(0, len(itens_ordenados), 10):
                    grupo = itens_ordenados[i:i+10]
                    embed = discord.Embed(title=f"🟡 {grupo_data['nome']} ({len(itens_ordenados)} membros) - Parte {i//10 + 1}", color=grupo_data["cor"])
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
                    embed = discord.Embed(title=f"✅ {grupo_data['nome']} - QUEM PAGOU ({len(pagaram)} membros) - Parte {i//5 + 1}", color=grupo_data["cor"])
                    texto = ""
                    for idx, item in enumerate(grupo, i + 1):
                        texto += f"**{idx}.** {item['nome']}\n   💰 Meta: {formatar_dinheiro(item['total_meta'])}\n   🎯 Ações: {formatar_dinheiro(item['total_acoes'])}\n   📦 Total: {formatar_dinheiro(item['total_geral'])}\n\n"
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
                    embed = discord.Embed(title=f"❌ {grupo_data['nome']} - QUEM NÃO PAGOU ({len(nao_pagaram)} membros) - Parte {i//10 + 1}", color=0xe74c3c)
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
                            
                            # =========================================================
                            # VDRZINHO - Meta concluída
                            # =========================================================
                            await canal.send(embed=vdrzinho.embed_resposta(
                                tipo="meta_concluida",
                                user_id=user_id,
                                nome=member.display_name if member else str(user_id),
                                dados={"valor": valor_total, "meta": meta_total}
                            ))
                            
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
                        await salvar_meta_db(member.id, canal_existente.id, 0, 0)
                    else:
                        await criar_sala_meta(member)
                    meta = await conn.fetchrow("SELECT dinheiro, dinheiro_acoes FROM metas WHERE user_id = $1", user_id)
                    if not meta:
                        continue
                dinheiro = meta["dinheiro"] or 0
                dinheiro_acoes = meta.get("dinheiro_acoes") or 0
                total = dinheiro + dinheiro_acoes
                if total == 0:
                    ja_avisado = await conn.fetchval("SELECT 1 FROM metas_avisos WHERE user_id = $1 AND tipo = 'quarta' AND data::date = $2", user_id, hoje.date())
                    if not ja_avisado:
                        await conn.execute("INSERT INTO metas_avisos (user_id, tipo, data) VALUES ($1, 'quarta', $2)", user_id, agora_db())
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

@tasks.loop(hours=1)
async def verificar_avisos_meta():
    try:
        await verificar_avisos_quarta()
    except Exception as e:
        logger.error(f"❌ Erro ao verificar avisos de meta: {e}")

@tasks.loop(hours=1)
async def fechar_metas_semanais():
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
# ==================== PARTE 15: SISTEMA DE GRUPOS ========
# =========================================================

# =========================================================
# 15.1 CONSTANTES DOS GRUPOS
# =========================================================
TIPOS_ORGANIZACAO = {
    "PISTA SEM PAINEL": {"nome": "📋 PISTA SEM PAINEL", "descricao": "APENAS PT", "pode_pt": True, "pode_sub": False, "emoji": "📋", "produtos": ["PT"]},
    "PISTA COM PAINEL": {"nome": "📱 PISTA COM PAINEL", "descricao": "PT E SUB", "pode_pt": True, "pode_sub": True, "emoji": "📱", "produtos": ["PT", "SUB"]},
    "MAFIAS": {"nome": "🤵 MAFIAS", "descricao": "PT E SUB", "pode_pt": True, "pode_sub": True, "emoji": "🤵", "produtos": ["MUNIÇÃO FUZIL", "MUNIÇÃO PISTOLA", "SUB", "ARMAS", "LAVAGEM", "CONTRABANDO", "KIT REPARO"]},
    "FAVELAS": {"nome": "🏚️ FAVELAS", "descricao": "PT E SUB", "pode_pt": True, "pode_sub": True, "emoji": "🏚️", "produtos": ["HAXIXE", "AQUABLITS", "LEAN", "MD", "COCA", "LANÇA", "BALÃO", "K9", "KETAMINA"]},
    "MECÂNICA ILEGAL": {"nome": "🔧 MECÂNICA ILEGAL", "descricao": "PT E SUB", "pode_pt": True, "pode_sub": True, "emoji": "🔧", "produtos": ["TUNNING DE VEÍCULOS", "PEÇAS ILEGAIS", "PLACA FALSA", "NITRO"]}
}

# =========================================================
# 15.2 FUNÇÕES DE BANCO DE DADOS - GRUPOS
# =========================================================
async def salvar_grupo_db(grupo_id, nome_org, lider_nome, lider_telefone, braco_nome, braco_telefone, produto, tipo_org="PISTA SEM PAINEL", observacoes=""):
    pool = await get_pool()
    if not pool:
        logger.error("❌ Banco de dados indisponível para salvar grupo!")
        return False
    try:
        async with pool.acquire() as conn:
            existente = await conn.fetchval("SELECT grupo_id FROM grupos WHERE grupo_id = $1", grupo_id)
            if existente:
                await conn.execute("UPDATE grupos SET nome_org = $2, lider_nome = $3, lider_telefone = $4, braco_nome = $5, braco_telefone = $6, produto = $7, tipo_org = $8, observacoes = $9, data_atualizacao = NOW(), ativo = true WHERE grupo_id = $1", grupo_id, nome_org.upper(), lider_nome.upper(), lider_telefone.upper(), braco_nome.upper() if braco_nome else None, braco_telefone.upper() if braco_telefone else None, produto.upper(), tipo_org, observacoes.upper() if observacoes else "")
            else:
                await conn.execute("INSERT INTO grupos (grupo_id, nome_org, lider_nome, lider_telefone, braco_nome, braco_telefone, produto, tipo_org, observacoes, data_criacao, ativo) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, true)", grupo_id, nome_org.upper(), lider_nome.upper(), lider_telefone.upper(), braco_nome.upper() if braco_nome else None, braco_telefone.upper() if braco_telefone else None, produto.upper(), tipo_org, observacoes.upper() if observacoes else "", agora_db())
            logger.info(f"✅ Grupo {nome_org} salvo com sucesso! ID: {grupo_id}")
            return True
    except Exception as e:
        logger.error(f"❌ ERRO AO SALVAR GRUPO: {e}")
        return False

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

async def atualizar_grupo_db(grupo_id, nome_org, lider_nome, lider_telefone, braco_nome, braco_telefone, produto, tipo_org=None, observacoes=None):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            query = "UPDATE grupos SET nome_org = $2, lider_nome = $3, lider_telefone = $4, braco_nome = $5, braco_telefone = $6, produto = $7, data_atualizacao = $8"
            params = [grupo_id, nome_org.upper(), lider_nome.upper(), lider_telefone.upper(), braco_nome.upper() if braco_nome else None, braco_telefone.upper() if braco_telefone else None, produto.upper(), agora_db()]
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
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("UPDATE grupos SET ativo = false, data_exclusao = $1 WHERE grupo_id = $2", agora_db(), grupo_id)
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")

async def registrar_compra_grupo_db(grupo_id, tipo, quantidade, valor):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute("INSERT INTO compras_grupo (grupo_id, tipo, quantidade, valor, data) VALUES ($1, $2, $3, $4, $5)", grupo_id, tipo.upper(), quantidade, valor, agora_db())
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")

async def carregar_compras_grupo_db(grupo_id):
    pool = await get_pool()
    if not pool:
        return {"PT": {"quantidade": 0, "valor": 0}, "SUB": {"quantidade": 0, "valor": 0}}
    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT tipo, SUM(quantidade) as total_quantidade, SUM(valor) as total_valor FROM compras_grupo WHERE grupo_id = $1 GROUP BY tipo", grupo_id)
            compras = {"PT": {"quantidade": 0, "valor": 0}, "SUB": {"quantidade": 0, "valor": 0}}
            for row in rows:
                tipo = row["tipo"]
                compras[tipo] = {"quantidade": row["total_quantidade"] or 0, "valor": row["total_valor"] or 0}
            return compras
    except Exception as e:
        logger.error(f"❌ ERRO: {e}")
        return {"PT": {"quantidade": 0, "valor": 0}, "SUB": {"quantidade": 0, "valor": 0}}

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

# =========================================================
# 15.3 FUNÇÕES DE PAINEL DE GRUPOS
# =========================================================
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
        logger.info(f"🗑️ {deletadas} mensagens antigas deletadas")
        await asyncio.sleep(2)
        await enviar_painel_grupos()
        logger.info("✅ Painel de grupos recriado com sucesso!")
        return True
    except Exception as e:
        logger.error(f"❌ ERRO AO RECRIAR PAINEL: {e}")
        return False

async def enviar_painel_grupos():
    canal = bot.get_channel(CANAL_GRUPOS_ID)
    if not canal:
        logger.error(f"❌ CANAL NÃO ENCONTRADO")
        return
    try:
        grupos = await carregar_grupos_db()
        embed = discord.Embed(
            title="👥 ── GERENCIAMENTO DE GRUPOS ── 👥",
            description="📋 VDR 442 • Organizações",
            color=0x1a1a2e,
            timestamp=agora()
        )
        embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
        embed.add_field(
            name="📌 TIPOS DE ORGANIZAÇÃO",
            value=(
                "📋 PISTA SEM PAINEL  →  APENAS PT\n"
                "📱 PISTA COM PAINEL  →  PT E SUB\n"
                "🤵 MAFIAS            →  PT E SUB\n"
                "🏚️ FAVELAS           →  PT E SUB\n"
                "🔧 MECÂNICA ILEGAL   →  PT E SUB"
            ),
            inline=False
        )
        embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
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
                value=f"👥 {len(grupos)} GRUPOS\n🔫 PT:  {fmt_num(total_pt)} pacotes\n🔫 SUB: {fmt_num(total_sub)} pacotes",
                inline=False
            )
        else:
            embed.add_field(name="📭 NENHUM GRUPO", value="CLIQUE EM **➕ NOVO GRUPO** PARA CADASTRAR.", inline=False)
        embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
        embed.add_field(name="📋 SELECIONE UM GRUPO", value="👇 ESCOLHA UMA OPÇÃO NO DROPDOWN", inline=False)
        embed.set_footer(text="🛡 Vida Rasa 442 • Sistema de Grupos", icon_url=bot.user.display_avatar.url if bot.user else None)
        view = PainelGruposView(grupos)
        await canal.send(embed=embed, view=view)
    except Exception as e:
        logger.error(f"❌ ERRO AO ENVIAR PAINEL: {e}")

# =========================================================
# 15.4 VIEWS DE GRUPOS
# =========================================================
class PainelGruposView(discord.ui.View):
    def __init__(self, grupos, pagina_atual=0):
        super().__init__(timeout=None)
        self.grupos = grupos
        self.pagina_atual = pagina_atual
        self.itens_por_pagina = 25
        self.total_paginas = (len(grupos) + self.itens_por_pagina - 1) // self.itens_por_pagina
        if not grupos:
            self.add_item(discord.ui.Button(label="➕ NOVO GRUPO", style=discord.ButtonStyle.success, custom_id="novo_padrao", emoji="➕"))
            self.add_item(discord.ui.Button(label="🔄 ATUALIZAR", style=discord.ButtonStyle.secondary, custom_id="atualizar_padrao", emoji="🔄"))
            self.add_item(discord.ui.Button(label="📋 RELATÓRIO", style=discord.ButtonStyle.success, custom_id="relatorio_grupos_btn", emoji="📋", row=2))
            return
        inicio = self.pagina_atual * self.itens_por_pagina
        fim = min(inicio + self.itens_por_pagina, len(grupos))
        grupos_pagina = grupos[inicio:fim]
        options = []
        for grupo in grupos_pagina:
            nome = grupo['nome_org'][:45]
            tipo = grupo.get('tipo_org', 'PISTA SEM PAINEL')
            emoji = TIPOS_ORGANIZACAO.get(tipo, {}).get('emoji', '🏷️')
            options.append(discord.SelectOption(label=nome, description=f"{emoji} {grupo['lider_nome'][:20]}", value=grupo['grupo_id'], emoji="🏷️"))
        if not options:
            options.append(discord.SelectOption(label="Nenhum grupo nesta página", value="none", emoji="📭"))
        import time
        self.uid = str(int(time.time()))[-6:]
        select = discord.ui.Select(placeholder=f"📋 PÁGINA {self.pagina_atual + 1}/{self.total_paginas} - {len(grupos)} GRUPOS", options=options, min_values=1, max_values=1, custom_id=f"select_{self.uid}")
        select.callback = self.select_callback
        self.add_item(select)
        if self.pagina_atual > 0:
            self.add_item(discord.ui.Button(label="◀️ Anterior", style=discord.ButtonStyle.secondary, custom_id=f"anterior_{self.uid}", row=1))
        else:
            self.add_item(discord.ui.Button(label="◀️ Anterior", style=discord.ButtonStyle.secondary, custom_id=f"anterior_{self.uid}", disabled=True, row=1))
        self.add_item(discord.ui.Button(label=f"📄 {self.pagina_atual + 1}/{self.total_paginas}", style=discord.ButtonStyle.secondary, custom_id=f"pagina_{self.uid}", disabled=True, row=1))
        if self.pagina_atual < self.total_paginas - 1:
            self.add_item(discord.ui.Button(label="▶️ Próxima", style=discord.ButtonStyle.secondary, custom_id=f"proxima_{self.uid}", row=1))
        else:
            self.add_item(discord.ui.Button(label="▶️ Próxima", style=discord.ButtonStyle.secondary, custom_id=f"proxima_{self.uid}", disabled=True, row=1))
        self.add_item(discord.ui.Button(label="➕ NOVO GRUPO", style=discord.ButtonStyle.success, custom_id="novo_padrao", emoji="➕", row=2))
        self.add_item(discord.ui.Button(label="🔄 ATUALIZAR", style=discord.ButtonStyle.secondary, custom_id="atualizar_padrao", emoji="🔄", row=2))
        self.add_item(discord.ui.Button(label="📋 RELATÓRIO", style=discord.ButtonStyle.success, custom_id="relatorio_grupos_btn", emoji="📋", row=2))

    async def select_callback(self, interaction: discord.Interaction):
        try:
            grupo_id = interaction.data["values"][0]
            if grupo_id == "none":
                await interaction.response.send_message("📭 Nenhum grupo selecionado.", ephemeral=True)
                return
            await interaction.response.defer(ephemeral=True)
            dados = await carregar_grupo_db(grupo_id)
            if not dados:
                await interaction.followup.send("❌ GRUPO NÃO ENCONTRADO!", ephemeral=True)
                return
            compras = await carregar_compras_grupo_db(grupo_id)
            tipo_org = dados.get('tipo_org', 'PISTA SEM PAINEL')
            info_tipo = TIPOS_ORGANIZACAO.get(tipo_org, TIPOS_ORGANIZACAO['PISTA SEM PAINEL'])
            embed = discord.Embed(title=f"{info_tipo['emoji']} {dados['nome_org']}", color=0x3498db, timestamp=agora())
            info = f"**👤 LÍDER:** {dados['lider_nome']}\n**📱 TELEFONE:** {dados['lider_telefone']}\n"
            if dados.get('braco_nome'):
                info += f"**👤 BRAÇO:** {dados['braco_nome']}\n"
            if dados.get('braco_telefone'):
                info += f"**📱 TELEFONE BRAÇO:** {dados['braco_telefone']}\n"
            info += f"\n**🔫 PRODUTO:** {dados['produto']}\n\n**📌 TIPO:** {info_tipo['nome']}\n**📝 {info_tipo['descricao']}**"
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

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        custom_id = interaction.data.get("custom_id", "")
        if custom_id == "novo_padrao":
            is_admin = interaction.user.guild_permissions.administrator
            is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
            if not is_admin and not is_gerente:
                await interaction.response.send_message("❌ APENAS ADM OU GERENTES!", ephemeral=True)
                return False
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
            return False
        elif custom_id == "atualizar_padrao":
            await interaction.response.defer(ephemeral=True)
            await recriar_painel_grupos()
            await interaction.followup.send("✅ PAINEL ATUALIZADO!", ephemeral=True)
            return False
        elif custom_id.startswith("anterior_"):
            nova_pagina = self.pagina_atual - 1
            await interaction.response.defer(ephemeral=True)
            await self.recarregar_painel(interaction, nova_pagina)
            return False
        elif custom_id.startswith("proxima_"):
            nova_pagina = self.pagina_atual + 1
            await interaction.response.defer(ephemeral=True)
            await self.recarregar_painel(interaction, nova_pagina)
            return False

        elif custom_id == "relatorio_grupos_btn":
            await interaction.response.defer(ephemeral=True)
            grupos = await carregar_grupos_db()
            if not grupos:
                await interaction.followup.send("📭 Nenhum grupo ativo cadastrado.", ephemeral=True)
                return

            # Definir larguras fixas para cada coluna
            LARGURA_NOME = 30
            LARGURA_LIDER = 25
            LARGURA_DATA = 20

            # Cabeçalho
            relatorio = "📋 **RELATÓRIO DE GRUPOS ATIVOS**\n"
            relatorio += "═" * 85 + "\n\n"
            relatorio += f"{'┌'}{'─' * LARGURA_NOME}{'┬'}{'─' * LARGURA_LIDER}{'┬'}{'─' * LARGURA_DATA}{'┐'}\n"
            relatorio += f"│ {'GRUPO':<{LARGURA_NOME-1}}│ {'LÍDER':<{LARGURA_LIDER-1}}│ {'DATA DA CRIAÇÃO':<{LARGURA_DATA-1}}│\n"
            relatorio += f"{'├'}{'─' * LARGURA_NOME}{'┼'}{'─' * LARGURA_LIDER}{'┼'}{'─' * LARGURA_DATA}{'┤'}\n"

            # Listar grupos
            for i, grupo in enumerate(grupos, 1):
                nome = grupo['nome_org'][:LARGURA_NOME-3]
                lider = grupo['lider_nome'][:LARGURA_LIDER-3]
                data_criacao = grupo['data_criacao'].strftime('%d/%m/%Y %H:%M') if grupo['data_criacao'] else 'N/A'
                data_criacao = data_criacao[:LARGURA_DATA-3]

                relatorio += f"│ {i:>2}. {nome:<{LARGURA_NOME-5}}│ {lider:<{LARGURA_LIDER-1}}│ {data_criacao:<{LARGURA_DATA-1}}│\n"

            # Rodapé
            relatorio += f"{'└'}{'─' * LARGURA_NOME}{'┴'}{'─' * LARGURA_LIDER}{'┴'}{'─' * LARGURA_DATA}{'┘'}\n\n"
            relatorio += f"📊 **TOTAL DE GRUPOS:** {len(grupos)}"

            # Enviar
            if len(relatorio) > 1900:
                await interaction.followup.send(
                    content="📋 Relatório de Grupos",
                    file=discord.File(io.BytesIO(relatorio.encode('utf-8')), filename="relatorio_grupos.txt"),
                    ephemeral=True
                )
            else:
                await interaction.followup.send(f"```prolog\n{relatorio}\n```", ephemeral=True)
            return False
        return True
   

    async def recarregar_painel(self, interaction, nova_pagina):
        try:
            grupos = await carregar_grupos_db()
            nova_view = PainelGruposView(grupos, nova_pagina)
            total_pt = 0
            total_sub = 0
            for grupo in grupos:
                try:
                    compras = await carregar_compras_grupo_db(grupo["grupo_id"])
                    total_pt += compras.get("PT", {}).get("quantidade", 0)
                    total_sub += compras.get("SUB", {}).get("quantidade", 0)
                except:
                    pass
            embed = discord.Embed(
                title="👥 ── GERENCIAMENTO DE GRUPOS ── 👥",
                description="📋 VDR 442 • Organizações",
                color=0x1a1a2e,
                timestamp=agora()
            )
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed.add_field(name="📌 TIPOS DE ORGANIZAÇÃO", value="📋 PISTA SEM PAINEL  →  APENAS PT\n📱 PISTA COM PAINEL  →  PT E SUB\n🤵 MAFIAS            →  PT E SUB\n🏚️ FAVELAS           →  PT E SUB\n🔧 MECÂNICA ILEGAL   →  PT E SUB", inline=False)
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            if grupos:
                embed.add_field(name="📊 RESUMO", value=f"👥 {len(grupos)} GRUPOS\n🔫 PT:  {fmt_num(total_pt)} pacotes\n🔫 SUB: {fmt_num(total_sub)} pacotes", inline=False)
            else:
                embed.add_field(name="📭 NENHUM GRUPO", value="CLIQUE EM **➕ NOVO GRUPO** PARA CADASTRAR.", inline=False)
            embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
            embed.add_field(name="📋 SELECIONE UM GRUPO", value=f"👇 PÁGINA {nova_pagina + 1}/{nova_view.total_paginas} - {len(grupos)} GRUPOS", inline=False)
            embed.set_footer(text="🛡 Vida Rasa 442 • Sistema de Grupos", icon_url=bot.user.display_avatar.url if bot.user else None)
            await interaction.message.edit(embed=embed, view=nova_view)
        except Exception as e:
            logger.error(f"❌ Erro ao recarregar painel: {e}")
            await interaction.followup.send(f"❌ Erro ao recarregar: {e}", ephemeral=True)

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
        self.add_item(discord.ui.Button(label="🚫 DESATIVAR", style=discord.ButtonStyle.danger, custom_id=f"desativar_{self.uid}", emoji="🚫", row=2))

    async def desativar(self, interaction: discord.Interaction, button: discord.ui.Button):
        is_admin = interaction.user.guild_permissions.administrator
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_admin and not is_gerente:
            await interaction.response.send_message("❌ APENAS ADM OU GERENTES!", ephemeral=True)
            return
        view = ConfirmarDesativarView(self.grupo_id, self.nome_org)
        await interaction.response.send_message(f"⚠️ **DESATIVAR GRUPO {self.nome_org}?**\nO grupo não aparecerá mais no painel.", view=view, ephemeral=True)

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        custom_id = interaction.data.get("custom_id", "")
        if custom_id.startswith(f"desativar_{self.uid}"):
            await self.desativar(interaction, None)
            return False
        elif custom_id.startswith(f"editar_{self.uid}"):
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

class ConfirmarDesativarView(discord.ui.View):
    def __init__(self, grupo_id, nome_org):
        super().__init__(timeout=60)
        self.grupo_id = grupo_id
        self.nome_org = nome_org
        import time
        self.uid = str(int(time.time()))[-6:]
        self.add_item(discord.ui.Button(label="✅ SIM, DESATIVAR", style=discord.ButtonStyle.danger, custom_id=f"des_confirm_{self.uid}", emoji="✅"))
        self.add_item(discord.ui.Button(label="❌ CANCELAR", style=discord.ButtonStyle.secondary, custom_id=f"des_cancel_{self.uid}", emoji="❌"))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        custom_id = interaction.data.get("custom_id", "")
        if custom_id.startswith("des_confirm_"):
            await self.confirmar(interaction)
            return False
        elif custom_id.startswith("des_cancel_"):
            await self.cancelar(interaction)
            return False
        return True

    async def confirmar(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        pool = await get_pool()
        if pool:
            async with pool.acquire() as conn:
                await conn.execute("UPDATE grupos SET ativo = false, data_exclusao = NOW() WHERE grupo_id = $1", self.grupo_id)
        await recriar_painel_grupos()
        await interaction.followup.send(f"✅ **GRUPO {self.nome_org} DESATIVADO!**", ephemeral=True)

    async def cancelar(self, interaction: discord.Interaction):
        await interaction.response.send_message("❌ CANCELADO.", ephemeral=True)

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
        tipos = {"tipo_pista_sem": "PISTA SEM PAINEL", "tipo_pista_com": "PISTA COM PAINEL", "tipo_mafias": "MAFIAS", "tipo_favelas": "FAVELAS", "tipo_mecanica": "MECÂNICA ILEGAL"}
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
        logger.info(f"📝 Salvando grupo: {self.nome_org.value.strip().upper()}")
        sucesso = await salvar_grupo_db(grupo_id, self.nome_org.value.strip().upper(), lider_nome.upper(), lider_telefone.upper(), braco_nome.upper() if braco_nome else None, braco_telefone.upper() if braco_telefone else None, self.produto.value.strip().upper(), tipo_org, "")
        if sucesso:
            await recriar_painel_grupos()
            await interaction.followup.send(f"✅ **GRUPO {self.nome_org.value.upper()} REGISTRADO!**", ephemeral=True)
        else:
            await interaction.followup.send(f"❌ **ERRO AO REGISTRAR GRUPO!** Verifique os logs.", ephemeral=True)
        await asyncio.sleep(5)
        try:
            await interaction.delete_original_response()
        except:
            pass

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
# ==================== PARTE 16: SISTEMA DE MENSAGENS =====
# =========================================================

# =========================================================
# 16.1 FUNÇÕES DE CONTROLE DE MENSAGENS
# =========================================================
async def limpar_mensagem_andamento(user_id):
    if user_id in mensagens_em_andamento:
        mensagens_em_andamento.remove(user_id)
    if user_id in mensagens_timers:
        del mensagens_timers[user_id]

async def limpar_timer_mensagem(user_id, tempo_segundos):
    await asyncio.sleep(tempo_segundos)
    await limpar_mensagem_andamento(user_id)

# =========================================================
# 16.2 VIEW DE COPIAR MENSAGEM
# =========================================================
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

# =========================================================
# 16.3 MODAIS DE MENSAGENS
# =========================================================
class MensagemPedidoProntoModal(discord.ui.Modal, title="📦 Pedido Pronto"):
    def __init__(self):
        super().__init__(timeout=300)
    nome = discord.ui.TextInput(label="👤 Nome do comprador", placeholder="Ex: Leon Winchester", required=True, max_length=100)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        mensagem = f"""📝 PEDIDO PRONTO!

🚚 Sua encomenda já está pronta para entrega!

📍 Assim que vocês confirmarem que estão disponíveis para receber, enviaremos a localização (LOC) para que a entrega seja realizada.

⚠️ Caso não haja confirmação de disponibilidade em até 24 horas, o pedido será cancelado automaticamente.

📞 Confirme o quanto antes que está disponível e aguarde o envio da LOC de entrega.

{self.nome.value} — {agora().strftime('%d/%m/%Y %H:%M')}"""
        embed = discord.Embed(title="📋 ── MENSAGEM GERADA ── 📋", description="📦 Pedido Pronto", color=0x2ecc71, timestamp=agora())
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n📅 {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="🛡 Vida Rasa 442 • Clique em 'Copiar' para copiar", icon_url=bot.user.display_avatar.url if bot.user else None)
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

    async def on_error(self, interaction: discord.Interaction, error: Exception):
        await limpar_mensagem_andamento(interaction.user.id)
        logger.error(f"Erro no modal: {error}")

class MensagemPedidoCanceladoModal(discord.ui.Modal, title="❌ Pedido Cancelado"):
    def __init__(self):
        super().__init__(timeout=300)
    nome = discord.ui.TextInput(label="👤 Nome do comprador", placeholder="Ex: Leon Winchester", required=True, max_length=100)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        mensagem = f"""❌ PEDIDO CANCELADO

Sua encomenda foi cancelada por não haver ninguém disponível para receber dentro do prazo de 24 horas.

Caso ainda tenha interesse, será necessário realizar um novo pedido.

{self.nome.value} — {agora().strftime('%d/%m/%Y %H:%M')}"""
        embed = discord.Embed(title="📋 ── MENSAGEM GERADA ── 📋", description="❌ Pedido Cancelado", color=0xe74c3c, timestamp=agora())
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n📅 {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="🛡 Vida Rasa 442 • Clique em 'Copiar' para copiar", icon_url=bot.user.display_avatar.url if bot.user else None)
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class MensagemPedidoFinalizadoModal(discord.ui.Modal, title="✅ Pedido Finalizado"):
    def __init__(self):
        super().__init__(timeout=300)
    nome = discord.ui.TextInput(label="👤 Nome do comprador", placeholder="Ex: Leon Winchester", required=True, max_length=100)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        mensagem = f"""✅ PEDIDO FINALIZADO

Sua encomenda foi entregue e o pagamento foi confirmado.

Agradecemos pela preferência!

{self.nome.value} — {agora().strftime('%d/%m/%Y %H:%M')}"""
        embed = discord.Embed(title="📋 ── MENSAGEM GERADA ── 📋", description="✅ Pedido Finalizado", color=0x2ecc71, timestamp=agora())
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n📅 {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="🛡 Vida Rasa 442 • Clique em 'Copiar' para copiar", icon_url=bot.user.display_avatar.url if bot.user else None)
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class MensagemPagamentoPendenteModal(discord.ui.Modal, title="🔔 Pagamento Pendente"):
    def __init__(self):
        super().__init__(timeout=300)
    nome = discord.ui.TextInput(label="👤 Nome do comprador", placeholder="Ex: Leon Winchester", required=True, max_length=100)
    valor = discord.ui.TextInput(label="💰 Valor pendente", placeholder="Ex: 50000", required=True, max_length=50)
    pix = discord.ui.TextInput(label="📱 Chave PIX", placeholder="Ex: 820 - Leon", required=True, max_length=100)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        try:
            valor_formatado = formatar_dinheiro(safe_int(self.valor.value))
        except:
            valor_formatado = self.valor.value
        mensagem = f"""🔔 ATENÇÃO!

✅ Sua encomenda foi entregue.

💰 Pagamento pendente: R$ {valor_formatado}
📱 Chave PIX: {self.pix.value}

{self.nome.value} — {agora().strftime('%d/%m/%Y %H:%M')}"""
        embed = discord.Embed(title="📋 ── MENSAGEM GERADA ── 📋", description="🔔 Pagamento Pendente", color=0xe67e22, timestamp=agora())
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n💰 Valor: R$ {valor_formatado}\n📱 PIX: {self.pix.value}\n📅 {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="🛡 Vida Rasa 442 • Clique em 'Copiar' para copiar", icon_url=bot.user.display_avatar.url if bot.user else None)
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class MensagemPendenciaPagamentoModal(discord.ui.Modal, title="💰 Pendência de Pagamento"):
    def __init__(self):
        super().__init__(timeout=300)
    valor = discord.ui.TextInput(label="💰 Valor pendente", placeholder="Ex: 50000", required=True, max_length=50)
    pix = discord.ui.TextInput(label="📱 Chave PIX", placeholder="Ex: 820 - Leon", required=True, max_length=100)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        try:
            valor_formatado = formatar_dinheiro(safe_int(self.valor.value))
        except:
            valor_formatado = self.valor.value
        mensagem = f"""🔔 ATENÇÃO – PENDÊNCIA DE PAGAMENTO

Consta uma pendência referente à sua última encomenda.

💰 Valor pendente: R$ {valor_formatado}
📱 Chave PIX: {self.pix.value}

Pedimos que o pagamento seja realizado o quanto antes.

Obrigado!"""
        embed = discord.Embed(title="📋 ── MENSAGEM GERADA ── 📋", description="💰 Pendência de Pagamento", color=0xf1c40f, timestamp=agora())
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n💰 Valor: R$ {valor_formatado}\n📱 PIX: {self.pix.value}\n📅 {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="🛡 Vida Rasa 442 • Clique em 'Copiar' para copiar", icon_url=bot.user.display_avatar.url if bot.user else None)
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class MensagemEntregaParcialModal(discord.ui.Modal, title="📦 Entrega Parcial"):
    def __init__(self):
        super().__init__(timeout=300)
    entrega_atual = discord.ui.TextInput(label="📦 Entrega atual", placeholder="Ex: 1", required=True, max_length=10)
    total_entregas = discord.ui.TextInput(label="📦 Total de entregas", placeholder="Ex: 3", required=True, max_length=10)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        try:
            atual = safe_int(self.entrega_atual.value)
            total = safe_int(self.total_entregas.value)
            restantes = total - atual
        except:
            atual = self.entrega_atual.value
            total = self.total_entregas.value
            restantes = "?"
        mensagem = f"""📦 ENTREGA PARCIAL REALIZADA!
✅ Pedido {atual}/{total} entregue com sucesso!
🚚 Uma etapa da sua encomenda foi concluída.
⏳ Restam {restantes} entrega(s) para a conclusão total do pedido.
⚠️ O pedido será considerado FINALIZADO somente após a conclusão de todas as entregas.
📍 Aguarde o contato para a próxima entrega.

{interaction.user.display_name} — {agora().strftime('%d/%m/%Y %H:%M')}"""
        embed = discord.Embed(title="📋 ── MENSAGEM GERADA ── 📋", description="📦 Entrega Parcial Realizada", color=0x3498db, timestamp=agora())
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n📦 {atual}/{total}\n📅 {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="🛡 Vida Rasa 442 • Clique em 'Copiar' para copiar", icon_url=bot.user.display_avatar.url if bot.user else None)
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class MensagemAvisoEntregaParcialModal(discord.ui.Modal, title="🔔 Aviso Entrega Parcial"):
    def __init__(self):
        super().__init__(timeout=300)
    entrega_atual = discord.ui.TextInput(label="📦 Entrega atual", placeholder="Ex: 1", required=True, max_length=10)
    total_entregas = discord.ui.TextInput(label="📦 Total de entregas", placeholder="Ex: 3", required=True, max_length=10)
    valor = discord.ui.TextInput(label="💰 Valor desta entrega", placeholder="Ex: 50000", required=True, max_length=50)
    pix = discord.ui.TextInput(label="📱 Chave PIX do entregador", placeholder="Ex: 820 - Leon", required=True, max_length=100)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        try:
            atual = safe_int(self.entrega_atual.value)
            total = safe_int(self.total_entregas.value)
            valor_formatado = formatar_dinheiro(safe_int(self.valor.value))
        except:
            atual = self.entrega_atual.value
            total = self.total_entregas.value
            valor_formatado = self.valor.value
        mensagem = f"""🔔 ATENÇÃO!

✅ Entrega {atual}/{total} realizada com sucesso!
📦 Esta entrega faz parte de um pedido dividido em {total} entregas.
💰 Valor referente a esta entrega: R$ {valor_formatado}
📱 Chave PIX: {self.pix.value}
📊 Progresso do pedido: {atual}/{total}"""
        embed = discord.Embed(title="📋 ── MENSAGEM GERADA ── 📋", description="🔔 Aviso de Entrega Parcial", color=0x3498db, timestamp=agora())
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n📦 {atual}/{total}\n💰 R$ {valor_formatado}\n📱 PIX: {self.pix.value}\n📅 {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="🛡 Vida Rasa 442 • Clique em 'Copiar' para copiar", icon_url=bot.user.display_avatar.url if bot.user else None)
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

class MensagemPendenciaProximaEntregaModal(discord.ui.Modal, title="🔔 Pendência Próxima Entrega"):
    def __init__(self):
        super().__init__(timeout=300)
    valor = discord.ui.TextInput(label="💰 Valor pendente", placeholder="Ex: 50000", required=True, max_length=50)
    pix = discord.ui.TextInput(label="📱 Chave PIX", placeholder="Ex: 820 - Leon", required=True, max_length=100)

    async def on_submit(self, interaction: discord.Interaction):
        await limpar_mensagem_andamento(interaction.user.id)
        try:
            valor_formatado = formatar_dinheiro(safe_int(self.valor.value))
        except:
            valor_formatado = self.valor.value
        mensagem = f"""🔔 ATENÇÃO – PENDÊNCIA DE PAGAMENTO

Consta uma pendência de pagamento referente à última entrega realizada.

💰 Valor pendente: R$ {valor_formatado}
📱 Chave PIX: {self.pix.value}

⚠️ A próxima entrega somente será liberada após a confirmação do pagamento da entrega anterior.

Assim que o pagamento for identificado, daremos continuidade às próximas entregas do seu pedido.

{interaction.user.display_name} — {agora().strftime('%d/%m/%Y %H:%M')}"""
        embed = discord.Embed(title="📋 ── MENSAGEM GERADA ── 📋", description="🔔 Pendência com Próxima Entrega", color=0xe74c3c, timestamp=agora())
        embed.add_field(name="📝 MENSAGEM", value=f"```\n{mensagem}\n```", inline=False)
        embed.add_field(name="📌 DETALHES", value=f"👤 Gerado por: {interaction.user.mention}\n💰 R$ {valor_formatado}\n📱 PIX: {self.pix.value}\n📅 {agora().strftime('%d/%m/%Y %H:%M:%S')}", inline=False)
        embed.set_footer(text="🛡 Vida Rasa 442 • Clique em 'Copiar' para copiar", icon_url=bot.user.display_avatar.url if bot.user else None)
        view = CopiarMensagemView(mensagem)
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# =========================================================
# 16.4 VIEW DE SELEÇÃO DE MENSAGENS
# =========================================================
class SelecionarMensagemView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=120)
        self.add_item(discord.ui.Button(label="📦 Pedido Pronto", style=discord.ButtonStyle.success, custom_id="msg_pedido_pronto", emoji="📦", row=0))
        self.add_item(discord.ui.Button(label="❌ Pedido Cancelado", style=discord.ButtonStyle.danger, custom_id="msg_pedido_cancelado", emoji="❌", row=0))
        self.add_item(discord.ui.Button(label="✅ Pedido Finalizado", style=discord.ButtonStyle.success, custom_id="msg_pedido_finalizado", emoji="✅", row=0))
        self.add_item(discord.ui.Button(label="🔔 Pagamento Pendente", style=discord.ButtonStyle.primary, custom_id="msg_pagamento_pendente", emoji="🔔", row=1))
        self.add_item(discord.ui.Button(label="💰 Pendência de Pagamento", style=discord.ButtonStyle.primary, custom_id="msg_pendencia_pagamento", emoji="💰", row=1))
        self.add_item(discord.ui.Button(label="📦 Entrega Parcial", style=discord.ButtonStyle.primary, custom_id="msg_entrega_parcial", emoji="📦", row=1))
        self.add_item(discord.ui.Button(label="🔔 Aviso Entrega Parcial", style=discord.ButtonStyle.primary, custom_id="msg_aviso_entrega_parcial", emoji="🔔", row=2))
        self.add_item(discord.ui.Button(label="🔔 Pendência Próxima Entrega", style=discord.ButtonStyle.danger, custom_id="msg_pendencia_proxima_entrega", emoji="🔔", row=2))
        self.add_item(discord.ui.Button(label="❌ Fechar", style=discord.ButtonStyle.secondary, custom_id="fechar_mensagens", emoji="❌", row=2))

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
            "msg_pagamento_pendente": self.handle_pagamento_pendente,
            "msg_pendencia_pagamento": self.handle_pendencia_pagamento,
            "msg_entrega_parcial": self.handle_entrega_parcial,
            "msg_aviso_entrega_parcial": self.handle_aviso_entrega_parcial,
            "msg_pendencia_proxima_entrega": self.handle_pendencia_proxima_entrega,
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
        modal = MensagemPedidoProntoModal()
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

    async def handle_pagamento_pendente(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(limpar_timer_mensagem(interaction.user.id, 300))
        modal = MensagemPagamentoPendenteModal()
        await interaction.response.send_modal(modal)

    async def handle_pendencia_pagamento(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(limpar_timer_mensagem(interaction.user.id, 300))
        modal = MensagemPendenciaPagamentoModal()
        await interaction.response.send_modal(modal)

    async def handle_entrega_parcial(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(limpar_timer_mensagem(interaction.user.id, 300))
        modal = MensagemEntregaParcialModal()
        await interaction.response.send_modal(modal)

    async def handle_aviso_entrega_parcial(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(limpar_timer_mensagem(interaction.user.id, 300))
        modal = MensagemAvisoEntregaParcialModal()
        await interaction.response.send_modal(modal)

    async def handle_pendencia_proxima_entrega(self, interaction: discord.Interaction):
        if interaction.user.id in mensagens_em_andamento:
            await limpar_mensagem_andamento(interaction.user.id)
        mensagens_em_andamento.add(interaction.user.id)
        mensagens_timers[interaction.user.id] = asyncio.create_task(limpar_timer_mensagem(interaction.user.id, 300))
        modal = MensagemPendenciaProximaEntregaModal()
        await interaction.response.send_modal(modal)

# =========================================================
# 16.5 VIEW DO MENU PRINCIPAL DE MENSAGENS
# =========================================================
class MenuMensagensView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📝 Gerar Mensagem", style=discord.ButtonStyle.primary, custom_id="gerar_mensagem_venda", emoji="📝")
    async def gerar_mensagem(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = discord.Embed(
            title="📝 ── SELECIONE O TIPO ── 📝",
            description="Escolha a mensagem que deseja gerar:",
            color=0x1a1a2e,
            timestamp=agora()
        )
        embed.add_field(
            name="📌 OPÇÕES",
            value=(
                "📦 **Pedido Pronto**\n"
                "❌ **Pedido Cancelado**\n"
                "✅ **Pedido Finalizado**\n"
                "🔔 **Pagamento Pendente**\n"
                "💰 **Pendência de Pagamento**\n"
                "📦 **Entrega Parcial Realizada**\n"
                "🔔 **Aviso de Entrega Parcial**\n"
                "🔔 **Pendência com Próxima Entrega**"
            ),
            inline=False
        )
        embed.set_footer(text="🛡 Vida Rasa 442 • Clique no botão correspondente", icon_url=bot.user.display_avatar.url if bot.user else None)
        view = SelecionarMensagemView()
        await interaction.response.send_message(embed=embed, view=view, ephemeral=True)

# =========================================================
# 16.6 FUNÇÃO DE ENVIAR PAINEL DE MENSAGENS
# =========================================================
async def enviar_painel_mensagens():
    canal = bot.get_channel(CANAL_TEXTOS_VENDAS_ID)
    if not canal:
        logger.error("❌ Canal de textos vendas não encontrado")
        return
    embed = discord.Embed(
        title="📝 ── GERADOR DE MENSAGENS ── 📝",
        description="🛒 Sistema de Mensagens • VDR 442",
        color=0x1a1a2e,
        timestamp=agora()
    )
    embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
    embed.set_author(name="🛡 Vida Rasa 442 • Gerador de Mensagens", icon_url=bot.user.display_avatar.url if bot.user else None)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="📌 TIPOS DE MENSAGENS DISPONÍVEIS",
        value=(
            "```yaml\n"
            "📦 Pedido Pronto\n"
            "❌ Pedido Cancelado\n"
            "✅ Pedido Finalizado\n"
            "🔔 Pagamento Pendente\n"
            "💰 Pendência de Pagamento\n"
            "📦 Entrega Parcial Realizada\n"
            "🔔 Aviso de Entrega Parcial\n"
            "🔔 Pendência com Próxima Entrega\n"
            "```"
        ),
        inline=False
    )
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(
        name="📋 COMO USAR",
        value=(
            "1️⃣ Clique em **'Gerar Mensagem'**\n"
            "2️⃣ Selecione o tipo de mensagem\n"
            "3️⃣ Preencha os campos solicitados\n"
            "4️⃣ Copie a mensagem gerada\n"
            "5️⃣ Cole no canal desejado"
        ),
        inline=False
    )
    embed.set_footer(text="🛡 Vida Rasa 442 • Sistema de Mensagens", icon_url=bot.user.display_avatar.url if bot.user else None)
    view = MenuMensagensView()
    try:
        async for msg in canal.history(limit=20):
            if msg.author == bot.user and msg.embeds and len(msg.embeds) > 0:
                if "GERADOR DE MENSAGENS" in msg.embeds[0].title:
                    try:
                        await msg.edit(embed=embed, view=view)
                        return
                    except:
                        pass
        await canal.send(embed=embed, view=view)
    except Exception as e:
        logger.error(f"❌ Erro ao enviar painel de mensagens: {e}")

# =========================================================
# ==================== PARTE 17: SISTEMA DE LOGS ==========
# =========================================================

# =========================================================
# 17.1 EVENTOS DE LOG
# =========================================================
@bot.event
async def on_message_delete(message):
    if message.author.bot:
        return
    canal_log = bot.get_channel(CANAL_LOGS_GERAIS_ID)
    if not canal_log:
        return
    embed = discord.Embed(title="🗑️ MENSAGEM DELETADA", description=f"👤 **{message.author.display_name}** deletou uma mensagem", color=0xe74c3c, timestamp=agora())
    embed.add_field(name="📌 Canal", value=f"#{message.channel.name}", inline=True)
    embed.add_field(name="📝 Conteúdo", value=message.content[:500] if message.content else "📎 (sem texto)", inline=False)
    if message.attachments:
        embed.add_field(name="📎 Anexos", value=f"{len(message.attachments)} arquivo(s)", inline=False)
    embed.set_footer(text=f"Vida Rasa 442 • ID: {message.id}")
    await canal_log.send(embed=embed)

@bot.event
async def on_message_edit(before, after):
    if before.author.bot:
        return
    if before.content == after.content:
        return
    canal_log = bot.get_channel(CANAL_LOGS_GERAIS_ID)
    if not canal_log:
        return
    embed = discord.Embed(title="✏️ MENSAGEM EDITADA", description=f"👤 **{before.author.display_name}** editou uma mensagem", color=0xf1c40f, timestamp=agora())
    embed.add_field(name="📌 Canal", value=f"#{before.channel.name}", inline=True)
    embed.add_field(name="📝 ANTES", value=before.content[:500] if before.content else "(vazio)", inline=False)
    embed.add_field(name="📝 DEPOIS", value=after.content[:500] if after.content else "(vazio)", inline=False)
    embed.set_footer(text=f"Vida Rasa 442 • ID: {before.id}")
    await canal_log.send(embed=embed)

@bot.event
async def on_member_join(member):
    if member.bot:
        return
    
    # =========================================================
    # CANAL DE ENTRADA
    # =========================================================
    canal_entrada = bot.get_channel(1229526645111656562)
    
    if canal_entrada:
        embed = discord.Embed(
            title="📥 MEMBRO ENTROU",
            description=f"👤 **{member.display_name}** ({member.name}) entrou no servidor!",
            color=0x2ecc71,
            timestamp=agora()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="🆔 ID", value=member.id, inline=True)
        embed.add_field(name="📅 Conta criada", value=member.created_at.strftime("%d/%m/%Y %H:%M"), inline=True)
        embed.set_footer(text="🛡 Vida Rasa 442 • Logs de Entrada")
        await canal_entrada.send(embed=embed)
    
    # =========================================================
    # SISTEMA XLSPY - VERIFICAÇÃO AUTOMÁTICA
    # =========================================================
    await verificar_seguranca_entrada(member)

@bot.event
async def on_member_remove(member):
    if member.bot:
        return
    
    # =========================================================
    # CANAL DE SAÍDA
    # =========================================================
    canal_saida = bot.get_channel(1229526645111656563)
    
    if canal_saida:
        embed = discord.Embed(
            title="📤 MEMBRO SAIU",
            description=f"👤 **{member.display_name}** ({member.name}) saiu do servidor!",
            color=0xe74c3c,
            timestamp=agora()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="🆔 ID", value=member.id, inline=True)
        embed.add_field(name="📅 Entrou em", value=member.joined_at.strftime("%d/%m/%Y %H:%M") if member.joined_at else "Desconhecido", inline=True)
        embed.set_footer(text="🛡 Vida Rasa 442 • Logs de Saída")
        await canal_saida.send(embed=embed)


    if member.bot:
        return
    try:
        await member.send(f"Olá {member.display_name}, você saiu do servidor Vida Rasa. Caso precise, entre em contato com a gerência.")
    except:
        pass

@bot.event
async def on_voice_state_update(member, before, after):
    canal_log = bot.get_channel(CANAL_LOGS_GERAIS_ID)
    if not canal_log:
        return
    if before.channel is None and after.channel is not None:
        embed = discord.Embed(title="🔊 ENTROU NA CALL", description=f"👤 **{member.display_name}** entrou no canal de voz {after.channel.mention}", color=0x2ecc71, timestamp=agora())
        embed.set_footer(text="Vida Rasa 442 • Logs")
        await canal_log.send(embed=embed)
    elif before.channel is not None and after.channel is None:
        embed = discord.Embed(title="🔇 SAIU DA CALL", description=f"👤 **{member.display_name}** saiu do canal de voz {before.channel.mention}", color=0xe74c3c, timestamp=agora())
        embed.set_footer(text="Vida Rasa 442 • Logs")
        await canal_log.send(embed=embed)
    elif before.channel is not None and after.channel is not None and before.channel != after.channel:
        embed = discord.Embed(title="🔁 MUDOU DE CALL", description=f"👤 **{member.display_name}** moveu de {before.channel.mention} para {after.channel.mention}", color=0xf1c40f, timestamp=agora())
        embed.set_footer(text="Vida Rasa 442 • Logs")
        await canal_log.send(embed=embed)

@bot.event
async def on_guild_channel_create(channel):
    canal_log = bot.get_channel(CANAL_LOGS_GERAIS_ID)
    if not canal_log:
        return
    embed = discord.Embed(title="📢 CANAL CRIADO", description=f"📌 {channel.mention}\n📂 **Categoria:** {channel.category.name if channel.category else 'Nenhuma'}", color=0x2ecc71, timestamp=agora())
    embed.add_field(name="🆔 ID", value=channel.id, inline=True)
    embed.add_field(name="📌 Tipo", value=str(channel.type).capitalize(), inline=True)
    embed.set_footer(text="Vida Rasa 442 • Logs")
    await canal_log.send(embed=embed)

@bot.event
async def on_guild_channel_delete(channel):
    canal_log = bot.get_channel(CANAL_LOGS_GERAIS_ID)
    if not canal_log:
        return
    embed = discord.Embed(title="🗑️ CANAL DELETADO", description=f"📌 #{channel.name}\n📂 **Categoria:** {channel.category.name if channel.category else 'Nenhuma'}", color=0xe74c3c, timestamp=agora())
    embed.add_field(name="🆔 ID", value=channel.id, inline=True)
    embed.add_field(name="📌 Tipo", value=str(channel.type).capitalize(), inline=True)
    embed.set_footer(text="Vida Rasa 442 • Logs")
    await canal_log.send(embed=embed)

@bot.event
async def on_member_update(before, after):
    if before.bot:
        return
    canal_log = bot.get_channel(CANAL_LOGS_GERAIS_ID)
    if not canal_log:
        return
    # Nickname alterado
    if before.display_name != after.display_name:
        embed = discord.Embed(title="✏️ NICKNAME ALTERADO", description=f"👤 **{before.display_name}** alterou o nickname", color=0xf1c40f, timestamp=agora())
        embed.add_field(name="📝 ANTES", value=before.display_name, inline=True)
        embed.add_field(name="📝 DEPOIS", value=after.display_name, inline=True)
        embed.set_footer(text="Vida Rasa 442 • Logs")
        await canal_log.send(embed=embed)
    # Cargos adicionados/removidos
    cargos_adicionados = [r for r in after.roles if r not in before.roles]
    cargos_removidos = [r for r in before.roles if r not in after.roles]
    if cargos_adicionados:
        for cargo in cargos_adicionados:
            embed = discord.Embed(title="➕ CARGO ADICIONADO", description=f"👤 **{after.display_name}**\n🏷️ **Cargo:** {cargo.mention}", color=0x2ecc71, timestamp=agora())
            embed.add_field(name="🆔 ID do cargo", value=cargo.id, inline=True)
            embed.set_footer(text="Vida Rasa 442 • Logs")
            await canal_log.send(embed=embed)
    if cargos_removidos:
        for cargo in cargos_removidos:
            embed = discord.Embed(title="➖ CARGO REMOVIDO", description=f"👤 **{after.display_name}**\n🏷️ **Cargo:** {cargo.name}", color=0xe74c3c, timestamp=agora())
            embed.add_field(name="🆔 ID do cargo", value=cargo.id, inline=True)
            embed.set_footer(text="Vida Rasa 442 • Logs")
            await canal_log.send(embed=embed)

# =========================================================
# ==================== PARTE 18: TASKS E EVENTOS ==========
# =========================================================

# =========================================================
# 18.1 TASKS BACKGROUND
# =========================================================
# Salvar memória a cada 5 minutos
@tasks.loop(minutes=5)
async def salvar_memoria_vdrzinho():
    await vdrzinho.salvar_memoria()

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
            content=f"🧨 **RELATÓRIO SEMANAL DE PÓLVORA**\n📅 Período: {inicio_semana.strftime('%d/%m')} até {fim_semana.strftime('%d/%m')}\n\n👤 Comprado por: {user.mention}\n💰 Valor a ressarcir: **{formatar_dinheiro(total)}**",
            view=ConfirmarPagamentoView()
        )

class ConfirmarPagamentoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Confirmar pagamento", style=discord.ButtonStyle.success, custom_id="confirmar_pagamento")
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.message.edit(content=interaction.message.content + "\n\n✅ **PAGO**", view=None)
        await interaction.response.defer()

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

@tasks.loop(minutes=15)
async def limpar_lavagens_pendentes():
    global lavagens_pendentes
    if lavagens_pendentes:
        lavagens_pendentes.clear()
        logger.info("🧹 Lavagens pendentes limpas")

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
# 18.2 FUNÇÕES DE INICIAR TAREFAS
# =========================================================
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
        if not salvar_memoria_vdrzinho.is_running():
            salvar_memoria_vdrzinho.start()

async def limpeza_cache_periodica():
    while True:
        try:
            await asyncio.sleep(3600)
            removidos = await cache.clean_expired()
            if removidos > 0:
                logger.info(f"🧹 Cache limpo: {removidos} entradas removidas")
        except Exception as e:
            logger.error(f"Erro na limpeza de cache: {e}")

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

async def health_check_avancado():
    while True:
        try:
            await asyncio.sleep(30)
            if bot.is_closed():
                logger.warning("⚠️ Bot desconectado! Reconectando...")
                await bot.close()
                await bot.start(TOKEN)
                continue
            pool = get_db()
            if not pool or pool._closed:
                logger.warning("⚠️ Pool do banco inativo! Reconectando...")
                await conectar_db()
                continue
            tasks_ativas = [
                ("verificar_lives", verificar_lives),
                ("verificar_ausencias", verificar_ausencias_expiradas),
                ("verificar_avisos_meta", verificar_avisos_meta),
                ("limpar_lavagens_pendentes", limpar_lavagens_pendentes),
                ("limpar_cache_lives", limpar_cache_lives),
                ("relatorio_semanal_polvoras", relatorio_semanal_polvoras)
            ]
            for nome, task in tasks_ativas:
                if hasattr(task, 'is_running') and not task.is_running():
                    logger.warning(f"⚠️ Task {nome} parada! Reiniciando...")
                    try:
                        task.start()
                    except Exception as e:
                        logger.error(f"❌ Erro ao reiniciar {nome}: {e}")
            try:
                import psutil
                memoria = psutil.Process().memory_info().rss / 1024 / 1024
                if memoria > 500:
                    logger.warning(f"⚠️ Memória alta: {memoria:.2f} MB. Limpando cache...")
                    await cache.clear()
                    gc.collect()
            except:
                pass
            await verificar_heartbeat_producoes()
            if int(time_module.time()) % 300 == 0:
                logger.info(f"📊 Stats - Metas: {len(metas_cache)}, Produções: {len(producoes_tasks)}, Cache: {cache.size()}")
        except Exception as e:
            logger.error(f"❌ Erro no health check: {e}")
            await asyncio.sleep(10)

async def setup_status():
    async def get_stats():
        return {
            "membros": len([m for m in bot.get_guild(GUILD_ID).members if not m.bot]) if bot.get_guild(GUILD_ID) else 0,
            "producoes": len([p for p in producoes_tasks.values() if hasattr(p, 'done') and not p.done()]) if producoes_tasks else 0,
            "metas": len(metas_cache),
            "estoque_pt": (await carregar_estoque()).get('PT', 0),
            "estoque_sub": (await carregar_estoque()).get('SUB', 0),
        }

    @tasks.loop(minutes=3)
    async def atualizar_status():
        try:
            stats = await get_stats()
            statuses = [
                f"🎮 {stats['membros']} membros",
                f"🏭 {stats['producoes']} produções",
                f"💰 {stats['metas']} metas",
                f"🔫 PT {stats['estoque_pt']} • SUB {stats['estoque_sub']}",
                f"🕒 {agora().strftime('%H:%M')} • VDR 442",
            ]
            status_text = random.choice(statuses)
            await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name=status_text))
        except Exception as e:
            logger.error(f"Erro ao atualizar status: {e}")

    if not atualizar_status.is_running():
        atualizar_status.start()

# =========================================================
# 18.3 FUNÇÕES DE ENVIAR/ATUALIZAR PAINEL
# =========================================================
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
                await conn.execute("INSERT INTO paineis (nome, canal_id, mensagem_id) VALUES ($1,$2,$3) ON CONFLICT (nome) DO UPDATE SET canal_id=$2, mensagem_id=$3", nome, str(canal_id), str(msg.id))
    except Exception as e:
        logger.error(f"❌ Erro crítico ao enviar painel {nome}: {e}")

# =========================================================
# 18.4 CLASSE DE BOTÃO PERSISTENTE
# =========================================================
class BotaoPersistente:
    @staticmethod
    async def salvar_botao(mensagem_id, canal_id, tipo, dados=None):
        pool = await get_pool()
        if not pool:
            return
        try:
            async with pool.acquire() as conn:
                existente = await conn.fetchval("SELECT 1 FROM botoes_persistentes WHERE mensagem_id = $1 AND canal_id = $2", str(mensagem_id), str(canal_id))
                if existente:
                    await conn.execute("UPDATE botoes_persistentes SET tipo = $1, dados = $2, criado_em = NOW() WHERE mensagem_id = $3 AND canal_id = $4", tipo, json.dumps(dados) if dados else None, str(mensagem_id), str(canal_id))
                else:
                    await conn.execute("INSERT INTO botoes_persistentes (mensagem_id, canal_id, tipo, dados) VALUES ($1, $2, $3, $4)", str(mensagem_id), str(canal_id), tipo, json.dumps(dados) if dados else None)
        except Exception as e:
            logger.error(f"❌ Erro ao salvar botão persistente: {e}")

    @staticmethod
    async def restaurar_botoes():
        pool = await get_pool()
        if not pool:
            return
        try:
            async with pool.acquire() as conn:
                rows = await conn.fetch("SELECT mensagem_id, canal_id, tipo, dados FROM botoes_persistentes ORDER BY criado_em DESC")
            for row in rows:
                canal = bot.get_channel(int(row["canal_id"]))
                if not canal:
                    continue
                try:
                    msg = await canal.fetch_message(int(row["mensagem_id"]))
                    if not msg:
                        continue
                    tipo = row["tipo"]
                    dados = json.loads(row["dados"]) if row["dados"] else {}
                    view = BotaoPersistente.criar_view(tipo, dados)
                    if view:
                        await msg.edit(view=view)
                        logger.info(f"🔄 Botão restaurado: {tipo} - {row['mensagem_id']}")
                except discord.NotFound:
                    try:
                        async with pool.acquire() as conn_delete:
                            await conn_delete.execute("DELETE FROM botoes_persistentes WHERE mensagem_id = $1 AND canal_id = $2", row["mensagem_id"], row["canal_id"])
                    except Exception as e:
                        logger.error(f"❌ Erro ao deletar botão {row['mensagem_id']}: {e}")
                except Exception as e:
                    logger.error(f"❌ Erro ao restaurar botão {row['mensagem_id']}: {e}")
        except Exception as e:
            logger.error(f"❌ Erro ao restaurar botões: {e}")

    @staticmethod
    def criar_view(tipo, dados):
        if tipo == "meta":
            return MetaView(dados.get("user_id"))
        elif tipo == "venda":
            return StatusView(
                entrega_id=dados.get("entrega_id"),
                total_entregas=dados.get("total_entregas", 1),
                entrega_atual=dados.get("entrega_atual", 1),
                disabled=dados.get("disabled", False),
                valor_total=dados.get("valor_total", 0),
                pt=dados.get("pt", 0),
                sub=dados.get("sub", 0),
                pedido_numero=dados.get("pedido_numero", 0)
            )
        elif tipo == "acao":
            return AcaoView(dados.get("acao_id"), dados.get("criador_id"))
        elif tipo == "producao":
            return SegundaTaskView(dados.get("pid"))
        return None

# =========================================================
# ==================== PARTE 19: COMANDOS =================
# =========================================================

# =========================================================
# 19.1 COMANDO DE STATUS
# =========================================================
@bot.command(name="status")
async def cmd_status(ctx):
    estoque = await carregar_estoque()
    estoque_insumos = await carregar_estoque_insumos()
    metas_ativas = len(metas_cache)
    producoes_ativas = len(producoes_tasks)
    pool = await get_pool()
    vendas_hoje = 0
    if pool:
        async with pool.acquire() as conn:
            hoje = agora().strftime("%d/%m/%Y")
            row = await conn.fetchval("SELECT COALESCE(SUM(valor), 0) FROM vendas WHERE data = $1", hoje)
            vendas_hoje = row or 0
    guild = ctx.guild
    membros_online = len([m for m in guild.members if m.status != discord.Status.offline])
    membros_total = len([m for m in guild.members if not m.bot])
    embed = discord.Embed(title="📊 STATUS - VIDA RASA 442", color=0x1e3a8a, timestamp=agora())
    embed.set_thumbnail(url=bot.user.display_avatar.url)
    embed.add_field(name="🟢 SISTEMA", value=f"✅ Online\n⏱️ {int(metricas.get_uptime() // 3600)}h {(int(metricas.get_uptime()) % 3600) // 60}m", inline=True)
    embed.add_field(name="👥 MEMBROS", value=f"🟢 {membros_online} online\n👤 {membros_total} total", inline=True)
    embed.add_field(name="📊 MÉTRICAS", value=f"📝 {metricas.comandos_executados} comandos\n📦 {cache.size()} cache", inline=True)
    embed.add_field(name="📦 ESTOQUE", value=f"🔫 PT: {fmt_num(estoque['PT'])} pacotes\n🔫 SUB: {fmt_num(estoque['SUB'])} pacotes", inline=True)
    embed.add_field(name="💊 INSUMOS", value=f"💊 {fmt_num(estoque_insumos['capsulas'])} cápsulas\n📦 {fmt_num(estoque_insumos['embalagens'])} embalagens", inline=True)
    embed.add_field(name="🏭 PRODUÇÃO", value=f"🏭 {producoes_ativas} ativas\n📊 {metas_ativas} metas", inline=True)
    embed.add_field(name="💰 VENDAS HOJE", value=formatar_dinheiro(vendas_hoje), inline=True)
    embed.set_footer(text=f"🔄 {agora().strftime('%d/%m/%Y %H:%M:%S')}")
    await ctx.send(embed=embed)

# =========================================================
# 19.2 COMANDO DE DIAGNÓSTICO
# =========================================================
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
    embed.set_footer(text=f"Versão 7.0 • {agora().strftime('%d/%m/%Y %H:%M:%S')}")
    await ctx.send(embed=embed)

# =========================================================
# 19.3 COMANDO DE HELP
# =========================================================
@bot.command(name="help_vdr")
async def cmd_help_vdr(ctx):
    embed = discord.Embed(title="📋 LISTA DE COMANDOS - VDR BOT", description="**Comandos disponíveis para todos os membros:**", color=0x3498db)
    embed.add_field(name="📊 ESTOQUE E PRODUÇÃO", value="`!estoque` - Ver estoque completo\n`!historico_producao` - Histórico de produção\n`!historico_vendas_estoque` - Histórico de vendas", inline=False)
    embed.add_field(name="🎥 LIVES", value="`!listar_lives` - Lista lives cadastradas\n`!testar_live twitch NOME` - Testa se está ao vivo", inline=False)
    embed.add_field(name="📊 ESTATÍSTICAS", value="`!status` - Status do bot\n`!dashboard` - Dashboard completo", inline=False)
    embed.add_field(name="👑 COMANDOS DE ADM", value="`!ausentes` - Lista ausentes\n`!remover_ausencia @membro` - Remove ausência\n`!limpar_sala` - Limpa o canal\n`!atualizar_metas` - Atualiza metas\n`!recriar_metas` - Recria todos os painéis\n`!recriar_meta @membro` - Recria painel de um membro\n`!diagnostico` - Diagnóstico do bot\n`!atualizar_paineis_metas` - Atualiza painéis de metas\n`!atualizar_acesso_resp` - Atualiza acesso dos responsáveis\n`!testar_aviso_quarta` - Testa aviso de quarta\n`!recriar_vendas` - Recria mensagens de vendas\n`!enviar_bau` - Envia painel do baú\n`!enviar_armas` - Envia painel de armas\n`!atualizar_avisos` - Atualiza painel de avisos", inline=False)
    embed.set_footer(text="Sistema VDR • v7.0 COMPLETO")
    await ctx.send(embed=embed)

# =========================================================
# 19.4 COMANDO DE ESTOQUE
# =========================================================
@bot.command(name="estoque")
async def cmd_ver_estoque(ctx):
    estoque_municoes = await carregar_estoque()
    estoque_insumos = await carregar_estoque_insumos()
    embed = discord.Embed(title="📦 ESTOQUE COMPLETO", color=0x3498db)
    embed.add_field(
        name="🔫 MUNIÇÕES",
        value=f"**PT:** {fmt_num(estoque_municoes['PT'])} pacotes ({fmt_num(estoque_municoes['PT'] * 50)} munições)\n**SUB:** {fmt_num(estoque_municoes['SUB'])} pacotes ({fmt_num(estoque_municoes['SUB'] * 50)} munições)",
        inline=False
    )
    embed.add_field(
        name="💊 INSUMOS",
        value=f"**Cápsulas:** {fmt_num(estoque_insumos['capsulas'])} unidades\n**Embalagens:** {fmt_num(estoque_insumos['embalagens'])} unidades",
        inline=False
    )
    await ctx.send(embed=embed)

# =========================================================
# 19.5 COMANDO DE HISTÓRICO DE PRODUÇÃO
# =========================================================
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
        embed.add_field(
            name=f"{data.strftime('%d/%m/%Y %H:%M')}",
            value=f"🔫 **{row['tipo']}** • {fmt_num(row['pacotes'])} pacotes ({fmt_num(row['municoes'])} munições)\n💊 Consumiu: {fmt_num(row['capsulas_consumidas'])} cápsulas + {fmt_num(row['embalagens_consumidas'])} embalagens\n👤 <@{row['produzido_por']}>",
            inline=False
        )
    await ctx.send(embed=embed)

# =========================================================
# 19.6 COMANDO DE HISTÓRICO DE VENDAS
# =========================================================
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
        embed.add_field(
            name=f"Pedido #{row['pedido_numero']} - {data.strftime('%d/%m/%Y %H:%M')}",
            value=f"🔫 **{row['tipo']}** • {fmt_num(row['pacotes'])} pacotes\n👤 Retirado por: <@{row['retirado_por']}>",
            inline=False
        )
    await ctx.send(embed=embed)

# =========================================================
# 19.7 COMANDO DE LISTAR AUSENTES
# =========================================================
@bot.command(name="ausentes")
@commands.has_permissions(administrator=True)
async def listar_ausentes(ctx):
    ausencias = await buscar_ausencias_ativas_db()
    if not ausencias:
        await ctx.send("📭 Nenhum membro ausente.")
        return
    embed = discord.Embed(title="📋 Membros Ausentes", color=0xe67e22)
    for ausencia in ausencias:
        embed.add_field(
            name=f"👤 {ausencia['nome']}",
            value=f"📅 {ausencia['data_inicio'].strftime('%d/%m/%Y')} a {ausencia['data_fim'].strftime('%d/%m/%Y')}\n📝 {ausencia['motivo'][:50]}",
            inline=False
        )
    await ctx.send(embed=embed)

# =========================================================
# 19.8 COMANDO DE REMOVER AUSÊNCIA
# =========================================================
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

# =========================================================
# 19.9 COMANDO DE TESTAR LIVE
# =========================================================
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

# =========================================================
# 19.10 COMANDO DE LISTAR LIVES
# =========================================================
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
# 19.11 COMANDO DE ATUALIZAR PAINÉIS DE METAS
# =========================================================
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

# =========================================================
# 19.12 COMANDO DE ATUALIZAR METAS
# =========================================================
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

# =========================================================
# 19.13 COMANDO DE RECRIAR METAS
# =========================================================
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
                await asyncio.sleep(1.5)
            except Exception as e:
                logger.error(f"❌ Erro ao recriar meta {uid}: {e}")
        await ctx.send(f"✅ **{contador} painéis de metas recriados com sucesso!**")
    except Exception as e:
        logger.error(f"❌ Erro ao recriar metas: {e}")
        await ctx.send(f"❌ Erro ao recriar metas: {e}")

# =========================================================
# 19.14 COMANDO DE RECRIAR META
# =========================================================
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

# =========================================================
# 19.15 COMANDO DE ATUALIZAR ACESSO RESP
# =========================================================
@bot.command(name="atualizar_acesso_resp")
@commands.has_permissions(administrator=True)
async def cmd_atualizar_acesso_resp(ctx):
    await ctx.send("🔄 Atualizando acesso dos responsáveis...")
    await atualizar_acesso_responsaveis()
    await ctx.send("✅ Acesso dos responsáveis atualizado!")

# =========================================================
# 19.16 COMANDO DE TESTAR AVISO QUARTA
# =========================================================
@bot.command(name="testar_aviso_quarta")
@commands.has_permissions(administrator=True)
async def cmd_testar_aviso_quarta(ctx):
    await ctx.send("🔄 Testando aviso de quarta-feira...")
    resultado = await verificar_avisos_quarta_forcado()
    if resultado:
        await ctx.send("✅ Avisos enviados com sucesso!")
    else:
        await ctx.send("❌ Erro ao enviar avisos. Verifique os logs.")

# =========================================================
# 19.17 COMANDO DE LIMPAR SALA
# =========================================================
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
                    await asyncio.sleep(1.5)
            except:
                pass
        embed = discord.Embed(title="🧹 SALA LIMPA!", description=f"✅ **{deletadas} mensagens deletadas!**\n📌 A última mensagem do bot foi mantida.", color=0x2ecc71, timestamp=agora())
        embed.set_footer(text=f"Comando executado por {ctx.author.display_name}")
        if ultima_msg_bot:
            if ultima_msg_bot.embeds:
                embed_original = ultima_msg_bot.embeds[0]
                novo_embed = discord.Embed(title=embed_original.title, description=embed_original.description, color=embed_original.color, timestamp=agora())
                for field in embed_original.fields:
                    novo_embed.add_field(name=field.name, value=field.value, inline=field.inline)
                novo_embed.add_field(name="🧹 LIMPEZA REALIZADA", value=f"✅ {deletadas} mensagens deletadas por {ctx.author.mention}", inline=False)
                novo_embed.set_footer(text=f"Última limpeza: {agora().strftime('%d/%m/%Y %H:%M:%S')}")
                await ultima_msg_bot.edit(embed=novo_embed)
            else:
                await canal.send(embed=embed)
        else:
            await canal.send(embed=embed)
    except Exception as e:
        logger.error(f"Erro ao limpar sala: {e}")
        await ctx.send(f"❌ **Erro ao limpar a sala:** {e}")

# =========================================================
# 19.18 COMANDO DE RECRIAR VENDAS
# =========================================================
@bot.command(name="recriar_vendas")
@commands.has_permissions(administrator=True)
async def cmd_recriar_vendas(ctx):
    await ctx.send("🔄 Recriando mensagens de vendas...")
    await recriar_mensagens_vendas()
    await ctx.send("✅ Mensagens de vendas recriadas!")

# =========================================================
# 19.19 COMANDO DE ENVIAR BAÚ
# =========================================================
@bot.command(name="enviar_bau")
@commands.has_permissions(administrator=True)
async def cmd_enviar_bau(ctx):
    await ctx.send("🔄 Enviando painel do baú...")
    await enviar_painel_bau()
    await ctx.send("✅ Painel do baú enviado!")

# =========================================================
# 19.20 COMANDO DE ENVIAR ARMAS
# =========================================================
@bot.command(name="enviar_armas")
@commands.has_permissions(administrator=True)
async def cmd_enviar_armas(ctx):
    await ctx.send("🔄 Enviando painel de armas...")
    await enviar_painel_armas()
    await ctx.send("✅ Painel de armas enviado!")

# =========================================================
# 19.21 COMANDO DE ATUALIZAR AVISOS
# =========================================================
@bot.command(name="atualizar_avisos")
@commands.has_permissions(administrator=True)
async def cmd_atualizar_avisos(ctx):
    await ctx.send("🔄 Atualizando painel de avisos...")
    await enviar_painel_avisos()
    await ctx.send("✅ Painel de avisos atualizado!")

# =========================================================
# 19.22 COMANDO DE DASHBOARD
# =========================================================
@bot.command(name="dashboard")
async def cmd_dashboard(ctx):
    estoque = await carregar_estoque()
    estoque_insumos = await carregar_estoque_insumos()
    metas_ativas = len(metas_cache)
    producoes_ativas = len(producoes_tasks)
    pool = await get_pool()
    vendas_hoje = 0
    vendas_mes = 0
    if pool:
        async with pool.acquire() as conn:
            hoje = agora().strftime("%d/%m/%Y")
            row = await conn.fetchval("SELECT COALESCE(SUM(valor), 0) FROM vendas WHERE data = $1", hoje)
            vendas_hoje = row or 0
            mes_atual = agora().strftime("%m/%Y")
            row = await conn.fetchval("SELECT COALESCE(SUM(valor), 0) FROM vendas WHERE data LIKE $1", f"%/{mes_atual}")
            vendas_mes = row or 0
    guild = ctx.guild
    membros_online = len([m for m in guild.members if m.status != discord.Status.offline])
    membros_total = len([m for m in guild.members if not m.bot])
    pool = get_db()
    status_geral = "🟢 ONLINE"
    cor_status = Cores.SUCESSO
    if not pool or pool._closed:
        status_geral = "🟡 BANCO OFFLINE"
        cor_status = Cores.AVISO
    embed = discord.Embed(title=f"{Emojis.ESTATISTICA} DASHBOARD • VIDA RASA 442", description="**Sistema de Gerenciamento da Facção**", color=cor_status, timestamp=agora())
    embed.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
    embed.set_author(name="🛡 Vida Rasa 442 • Dashboard", icon_url=bot.user.display_avatar.url if bot.user else None)
    embed.add_field(name="🟢 STATUS DO SISTEMA", value=f"```yaml\n{status_geral}\nUptime: {int(metricas.get_uptime() // 3600)}h {(int(metricas.get_uptime()) % 3600) // 60}m\n```", inline=True)
    embed.add_field(name="👥 MEMBROS", value=f"```yaml\nOnline: {membros_online}\nTotal: {membros_total}\n```", inline=True)
    embed.add_field(name="📊 MÉTRICAS", value=f"```yaml\nComandos: {metricas.comandos_executados}\nCache: {cache.size()} itens\n```", inline=True)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(name="📦 ESTOQUE", value=f"```yaml\nPT: {fmt_num(estoque['PT'])} pacotes\nSUB: {fmt_num(estoque['SUB'])} pacotes\n```", inline=True)
    embed.add_field(name="💊 INSUMOS", value=f"```yaml\nCápsulas: {fmt_num(estoque_insumos['capsulas'])}\nEmbalagens: {fmt_num(estoque_insumos['embalagens'])}\n```", inline=True)
    embed.add_field(name="🏭 PRODUÇÃO", value=f"```yaml\nAtivas: {producoes_ativas}\nMetas: {metas_ativas}\n```", inline=True)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(name=f"{Emojis.FINANCEIRO} VENDAS", value=f"```yaml\nHoje: {formatar_dinheiro(vendas_hoje)}\nMês: {formatar_dinheiro(vendas_mes)}\n```", inline=True)
    try:
        import psutil
        memoria = psutil.Process().memory_info().rss / 1024 / 1024
        cpu = psutil.Process().cpu_percent()
        embed.add_field(name="⚡ PERFORMANCE", value=f"```yaml\nMemória: {memoria:.1f} MB\nCPU: {cpu:.1f}%\n```", inline=True)
    except:
        pass
    embed.add_field(name="📡 DISCORD", value=f"```yaml\nPing: {round(bot.latency * 1000)}ms\nShards: {bot.shard_count or 1}\n```", inline=True)
    embed.add_field(name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━", value="", inline=False)
    embed.add_field(name="📌 INFORMAÇÕES DO SISTEMA", value=f"```yaml\nVersão: 7.0\nPython: 3.12.0\nDiscord.py: {discord.__version__}\n```", inline=False)
    embed.set_footer(text=f"🛡 Vida Rasa 442 • Dashboard • {agora().strftime('%d/%m/%Y %H:%M:%S')}", icon_url=bot.user.display_avatar.url if bot.user else None)
    await ctx.send(embed=embed)

# =========================================================
# 19.22 COMANDO DESATIVAR VENDAS CONCLUIDAS
# =========================================================
@bot.command(name="desativar_vendas_concluidas")
@commands.has_permissions(administrator=True)
async def cmd_desativar_vendas_concluidas(ctx):
    """Desativa todos os botões de vendas já concluídas"""
    await ctx.send("🔄 Desativando botões de vendas concluídas...")
    
    canal = bot.get_channel(CANAL_ENCOMENDAS_ID)
    if not canal:
        await ctx.send("❌ Canal de encomendas não encontrado!")
        return
    
    contador = 0
    async for msg in canal.history(limit=500):
        if msg.author == bot.user and msg.embeds:
            embed = msg.embeds[0]
            titulo = embed.title or ""
            
            # Verificar se é uma venda
            if "ENTREGA" not in titulo and "ENCOMENDA" not in titulo and "VENDA" not in titulo:
                continue
            
            # Verificar se está concluída
            concluida = False
            for field in embed.fields:
                if field.name == "📌 STATUS DO PEDIDO":
                    if "Pago e Entregue" in field.value or "TRANSFERÊNCIA CONFIRMADA" in field.value:
                        concluida = True
                    break
            
            if not concluida:
                # Verificar pelos campos de conclusão
                for field in embed.fields:
                    if "VENDA FINALIZADA COM SUCESSO" in field.name or "PAGO E ENTREGUE" in field.value.upper():
                        concluida = True
                        break
            
            if concluida:
                # Desabilitar os botões
                try:
                    # Extrair dados para criar view desabilitada
                    entrega_id = None
                    if embed.footer and "ID:" in embed.footer.text:
                        try:
                            entrega_id = safe_int(embed.footer.text.split("ID:")[1].strip().split(" ")[0])
                        except:
                            pass
                    
                    view = StatusView(
                        disabled=True,
                        entrega_id=entrega_id,
                        total_entregas=1,
                        entrega_atual=1,
                        pago_ja_clicado=True,
                        mensagem_original=msg,
                        transferencia_confirmada=True
                    )
                    await msg.edit(view=view)
                    contador += 1
                    await asyncio.sleep(0.5)
                except Exception as e:
                    logger.error(f"Erro ao desativar {msg.id}: {e}")
    
    await ctx.send(f"✅ **{contador} vendas concluídas desativadas!**")

# =========================================================
# ==================== SISTEMA XLSPY ======================
# =========================================================
# FUNCIONALIDADES:
# - Monitoramento automático de entrada
# - Comando !verificar @user
# - Comando !add_suspeito @user motivo
# - Comando !remove_suspeito @user
# - Comando !suspeitos
# - Comando !logs_verificacao
# - Botões de ação (Banir, Expulsar, Ignorar)
# =========================================================

# =========================================================
# 1. FUNÇÕES DE BANCO DE DADOS
# =========================================================

async def adicionar_suspeito_db(user_id, motivo, adicionado_por):
    pool = await get_pool()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO suspeitos (user_id, motivo, adicionado_por) VALUES ($1, $2, $3)",
                str(user_id), motivo, str(adicionado_por)
            )
            return True
    except Exception as e:
        logger.error(f"❌ Erro ao adicionar suspeito: {e}")
        return False

async def remover_suspeito_db(user_id):
    pool = await get_pool()
    if not pool:
        return False
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE suspeitos SET ativo = false WHERE user_id = $1 AND ativo = true",
                str(user_id)
            )
            return True
    except Exception as e:
        logger.error(f"❌ Erro ao remover suspeito: {e}")
        return False

async def verificar_suspeito_db(user_id):
    pool = await get_pool()
    if not pool:
        return None
    try:
        async with pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM suspeitos WHERE user_id = $1 AND ativo = true",
                str(user_id)
            )
    except Exception as e:
        logger.error(f"❌ Erro ao verificar suspeito: {e}")
        return None

async def listar_suspeitos_db():
    pool = await get_pool()
    if not pool:
        return []
    try:
        async with pool.acquire() as conn:
            return await conn.fetch(
                "SELECT * FROM suspeitos WHERE ativo = true ORDER BY data_adicao DESC"
            )
    except Exception as e:
        logger.error(f"❌ Erro ao listar suspeitos: {e}")
        return []

async def registrar_verificacao_db(user_id, verificador, resultado, servidor=None):
    pool = await get_pool()
    if not pool:
        return
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO verificacoes (user_id, verificador, resultado, servidor) VALUES ($1, $2, $3, $4)",
                str(user_id), str(verificador), resultado, servidor
            )
    except Exception as e:
        logger.error(f"❌ Erro ao registrar verificação: {e}")

# =========================================================
# 2. VIEW - AÇÕES PARA SUSPEITOS
# =========================================================

class AcaoSuspeitoView(discord.ui.View):
    def __init__(self, user_id, mensagem_original):
        super().__init__(timeout=60)
        self.user_id = user_id
        self.mensagem_original = mensagem_original

    @discord.ui.button(label="🔨 Banir", style=discord.ButtonStyle.danger, emoji="🔨")
    async def banir(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Apenas administradores podem banir!", ephemeral=True)
            return
        
        member = interaction.guild.get_member(int(self.user_id))
        if not member:
            await interaction.response.send_message("❌ Usuário não encontrado!", ephemeral=True)
            return
        
        try:
            await member.ban(reason="Usuário na lista de suspeitos")
            await interaction.response.send_message(f"✅ {member.mention} foi banido!", ephemeral=True)
            await remover_suspeito_db(self.user_id)
            if self.mensagem_original:
                embed = self.mensagem_original.embeds[0]
                embed.color = 0x2ecc71
                embed.add_field(
                    name="🔨 AÇÃO REALIZADA",
                    value=f"✅ **Usuário banido por {interaction.user.mention}**",
                    inline=False
                )
                await self.mensagem_original.edit(embed=embed, view=None)
        except Exception as e:
            await interaction.response.send_message(f"❌ Erro ao banir: {e}", ephemeral=True)

    @discord.ui.button(label="🚫 Expulsar", style=discord.ButtonStyle.danger, emoji="🚫")
    async def expulsar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Apenas administradores podem expulsar!", ephemeral=True)
            return
        
        member = interaction.guild.get_member(int(self.user_id))
        if not member:
            await interaction.response.send_message("❌ Usuário não encontrado!", ephemeral=True)
            return
        
        try:
            await member.kick(reason="Usuário na lista de suspeitos")
            await interaction.response.send_message(f"✅ {member.mention} foi expulso!", ephemeral=True)
            await remover_suspeito_db(self.user_id)
            if self.mensagem_original:
                embed = self.mensagem_original.embeds[0]
                embed.color = 0x2ecc71
                embed.add_field(
                    name="🚫 AÇÃO REALIZADA",
                    value=f"✅ **Usuário expulso por {interaction.user.mention}**",
                    inline=False
                )
                await self.mensagem_original.edit(embed=embed, view=None)
        except Exception as e:
            await interaction.response.send_message(f"❌ Erro ao expulsar: {e}", ephemeral=True)

    @discord.ui.button(label="⏭️ Ignorar", style=discord.ButtonStyle.secondary, emoji="⏭️")
    async def ignorar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.administrator:
            await interaction.response.send_message("❌ Apenas administradores podem ignorar!", ephemeral=True)
            return
        
        await interaction.response.send_message(f"✅ Usuário ignorado.", ephemeral=True)
        
        if self.mensagem_original:
            embed = self.mensagem_original.embeds[0]
            embed.add_field(
                name="⏭️ AÇÃO REALIZADA",
                value=f"✅ **Usuário ignorado por {interaction.user.mention}** (permanece na lista)",
                inline=False
            )
            await self.mensagem_original.edit(embed=embed, view=None)

# =========================================================
# FUNÇÃO DE VERIFICAÇÃO DE SEGURANÇA
# =========================================================
async def verificar_seguranca_entrada(member):
    """Verifica automaticamente um membro que entrou no servidor"""
    
    canal_verificacao = bot.get_channel(1544676962570608721)
    if not canal_verificacao:
        return
    
    # Verificar se está na lista de suspeitos
    suspeito = await verificar_suspeito_db(member.id)
    
    # Contar quantas vezes foi detectado
    pool = await get_pool()
    deteccoes = 0
    ultima_deteccao = None
    rows = []
    if pool:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM verificacoes WHERE user_id = $1 ORDER BY data_verificacao DESC",
                str(member.id)
            )
            deteccoes = len(rows)
            if rows:
                ultima_deteccao = rows[0]["data_verificacao"]
    
    # Análise de risco da conta
    score = 0
    
    # 1. Idade da conta
    idade = (agora() - member.created_at).days
    
    # 2. Avatar padrão
    if not member.avatar:
        score += 2
    
    # 3. Badges
    if not member.public_flags.value:
        score += 1
    
    # 4. Nome com números suspeitos
    import re
    if re.search(r'\d{4,}', member.name):
        score += 1
    
    # Definir nível de risco
    if suspeito:
        cor = 0xe74c3c
    elif score >= 7:
        cor = 0xe74c3c
    elif score >= 4:
        cor = 0xf39c12
    elif score >= 2:
        cor = 0xf1c40f
    else:
        cor = 0x2ecc71
    
    user_id = member.id
    
    # =========================================================
    # SERVIDORES SUSPEITOS
    # =========================================================
    servidores_suspeitos = []
    
    # Lista de servidores suspeitos conhecidos (IDs)
    SERVIDORES_SUSPEITOS_IDS = [
        # ADICIONE OS IDs DOS SERVIDORES QUE VOCÊ QUER MONITORAR
        # Exemplo: 123456789012345678,  # Randolas
    ]
    
    for servidor_id in SERVIDORES_SUSPEITOS_IDS:
        try:
            guild = bot.get_guild(servidor_id)
            if guild:
                member_check = guild.get_member(int(user_id))
                if member_check:
                    servidores_suspeitos.append(guild.name)
        except:
            pass
    
    for guild in bot.guilds:
        nome = guild.name.lower()
        if any(palavra in nome for palavra in ["randolas", "hack", "cheat", "xlspy", "xiter", "alt"]):
            member_check = guild.get_member(int(user_id))
            if member_check and guild.name not in servidores_suspeitos:
                servidores_suspeitos.append(guild.name)
    
    # =========================================================
    # EMBED ESTILO XISPY (SEM CUPOM)
    # =========================================================
    embed = discord.Embed(
        title="🕵️ Usuário Suspeito | XISpy" if suspeito else "🕵️ Verificação | XISpy",
        color=cor,
        timestamp=agora()
    )
    embed.set_thumbnail(url=member.display_avatar.url)
    
    # Descrição
    if servidores_suspeitos:
        descricao = f"O usuário **{member.display_name}** ({member.name}) • `{user_id}` está em **{len(servidores_suspeitos)}** servidor(es) suspeito(s)."
    else:
        descricao = f"O usuário **{member.display_name}** ({member.name}) • `{user_id}`"
    embed.description = descricao
    
    # Conta Criada
    anos = idade // 365
    if anos > 0:
        embed.add_field(
            name="📅 Conta Criada Em",
            value=f"{member.created_at.strftime('%d de %B de %Y %H:%M')} • **há {anos} anos**",
            inline=False
        )
    else:
        embed.add_field(
            name="📅 Conta Criada Em",
            value=f"{member.created_at.strftime('%d de %B de %Y %H:%M')} • **há {idade} dias**",
            inline=False
        )
    
    # Já foi detectado?
    if suspeito:
        embed.add_field(
            name="🚨 JÁ FOI DETECTADO PELO SISTEMA?",
            value=f"✅ **O usuário já foi detectado como suspeito {deteccoes} vez(es).**",
            inline=False
        )
        if ultima_deteccao:
            embed.add_field(
                name="📌 ÚLTIMA DETECÇÃO",
                value=f"{ultima_deteccao.strftime('%d de %B de %Y %H:%M')}",
                inline=False
            )
        embed.add_field(
            name="⚠️ MOTIVO",
            value=f"{suspeito['motivo']}",
            inline=False
        )
    else:
        if deteccoes > 0:
            embed.add_field(
                name="📌 JÁ FOI DETECTADO PELO SISTEMA?",
                value=f"⚠️ O usuário já foi detectado {deteccoes} vez(es), mas NÃO está na lista de suspeitos.",
                inline=False
            )
            if ultima_deteccao:
                embed.add_field(
                    name="📌 ÚLTIMA DETECÇÃO",
                    value=f"{ultima_deteccao.strftime('%d de %B de %Y %H:%M')}",
                    inline=False
                )
        else:
            embed.add_field(
                name="📌 JÁ FOI DETECTADO PELO SISTEMA?",
                value="❌ Nenhuma detecção registrada.",
                inline=False
            )
    
    # Servidores Suspeitos
    if servidores_suspeitos:
        servidores_texto = ""
        for nome in servidores_suspeitos:
            servidores_texto += f"### {nome}\n"
        embed.add_field(
            name="🔒 SERVIDORES SUSPEITOS",
            value=servidores_texto,
            inline=False
        )
    else:
        embed.add_field(
            name="🔒 SERVIDORES SUSPEITOS",
            value="✅ Nenhum servidor suspeito conhecido encontrado.",
            inline=False
        )
    
    # Detecções Anteriores
    if deteccoes > 0:
        embed.add_field(
            name="📋 DETECÇÕES ANTERIORES",
            value=f"### {servidores_suspeitos[0] if servidores_suspeitos else 'N/A'}",
            inline=False
        )
    
    # Rodapé (SEM CUPOM)
    embed.set_footer(
        text=f"⚠️ Essa é uma mensagem automática do sistema - {agora().strftime('%d/%m/%Y %H:%M')}",
        icon_url=bot.user.display_avatar.url if bot.user else None
    )
    
    await canal_verificacao.send(embed=embed)
    
    # Registrar verificação
    resultado = "suspeito" if suspeito else "limpo"
    await registrar_verificacao_db(member.id, bot.user.id, resultado)

# =========================================================
# 3. COMANDOS
# =========================================================
@bot.command(name="verificar")
async def cmd_verificar(ctx, *, alvo: str = None):
    """Verifica um usuário (por @menção, nome ou ID)"""
    
    member = None
    user = None
    user_id = None
    
    # =========================================================
    # 1. SE NÃO PASSOU NADA → VERIFICA A SI MESMO
    # =========================================================
    if not alvo:
        member = ctx.author
        user = ctx.author
        user_id = ctx.author.id
    
    # =========================================================
    # 2. SE PASSOU UM ID (APENAS NÚMEROS)
    # =========================================================
    elif alvo.isdigit():
        user_id = int(alvo)
        try:
            user = await bot.fetch_user(user_id)
        except:
            await ctx.send(f"❌ Usuário com ID `{alvo}` não encontrado!")
            return
        member = ctx.guild.get_member(user_id)
    
    # =========================================================
    # 3. SE PASSOU UMA MENÇÃO (@alguem)
    # =========================================================
    else:
        try:
            member = await commands.MemberConverter().convert(ctx, alvo)
            user = member
            user_id = member.id
        except:
            try:
                for m in ctx.guild.members:
                    if alvo.lower() in m.name.lower() or alvo.lower() in m.display_name.lower():
                        member = m
                        user = m
                        user_id = m.id
                        break
            except:
                pass
            if not user:
                await ctx.send(f"❌ Usuário `{alvo}` não encontrado! Use o ID ou @menção.")
                return
    
    if not user:
        await ctx.send(f"❌ Usuário não encontrado!")
        return
    
    # =========================================================
    # 4. VERIFICAR PERMISSÃO
    # =========================================================
    if str(user_id) != str(ctx.author.id):
        if not ctx.author.guild_permissions.administrator:
            is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID, CARGO_01_ID, CARGO_02_ID] for r in ctx.author.roles)
            if not is_gerente:
                await ctx.send("❌ Apenas administradores ou gerentes podem verificar outros usuários!")
                return
    
    await ctx.send(f"🔍 Verificando {user.mention if member else f'**{user.display_name}** (ID: {user_id})'}...")
    
    suspeito = await verificar_suspeito_db(user_id)
    
    pool = await get_pool()
    deteccoes = 0
    ultima_deteccao = None
    rows = []
    if pool:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM verificacoes WHERE user_id = $1 ORDER BY data_verificacao DESC",
                str(user_id)
            )
            deteccoes = len(rows)
            if rows:
                ultima_deteccao = rows[0]["data_verificacao"]
    
    nome_exibicao = member.display_name if member else user.display_name
    idade = (agora() - user.created_at).days
    
    # =========================================================
    # SERVIDORES SUSPEITOS
    # =========================================================
    servidores_suspeitos = []
    
    # Lista de servidores suspeitos conhecidos (IDs)
    SERVIDORES_SUSPEITOS_IDS = [
        # ADICIONE OS IDs DOS SERVIDORES QUE VOCÊ QUER MONITORAR
        # Exemplo: 123456789012345678,  # Randolas
    ]
    
    # Verificar se o bot está nesses servidores e se o usuário também está
    for servidor_id in SERVIDORES_SUSPEITOS_IDS:
        try:
            guild = bot.get_guild(servidor_id)
            if guild:
                member_check = guild.get_member(int(user_id))
                if member_check:
                    servidores_suspeitos.append(guild.name)
        except:
            pass
    
    # Também verificar servidores com nomes suspeitos
    for guild in bot.guilds:
        nome = guild.name.lower()
        if any(palavra in nome for palavra in ["randolas", "hack", "cheat", "xlspy", "xiter", "alt"]):
            member_check = guild.get_member(int(user_id))
            if member_check and guild.name not in servidores_suspeitos:
                servidores_suspeitos.append(guild.name)
    
    # =========================================================
    # EMBED ESTILO XISPY (SEM CUPOM)
    # =========================================================
    cor = 0xe74c3c if suspeito else 0x2ecc71
    
    embed = discord.Embed(
        title="🕵️ Usuário Suspeito | XISpy" if suspeito else "🕵️ Verificação | XISpy",
        color=cor,
        timestamp=agora()
    )
    embed.set_thumbnail(url=user.display_avatar.url)
    
    # Descrição: "O usuário @user#tag (ID) está em X servidor(es) suspeito(s)"
    if servidores_suspeitos:
        descricao = f"O usuário **{user.name}** ({user.display_name}) • `{user_id}` está em **{len(servidores_suspeitos)}** servidor(es) suspeito(s)."
    else:
        descricao = f"O usuário **{user.name}** ({user.display_name}) • `{user_id}`"
    embed.description = descricao
    
    # Conta Criada
    anos = idade // 365
    if anos > 0:
        embed.add_field(
            name="📅 Conta Criada Em",
            value=f"{user.created_at.strftime('%d de %B de %Y %H:%M')} • **há {anos} anos**",
            inline=False
        )
    else:
        embed.add_field(
            name="📅 Conta Criada Em",
            value=f"{user.created_at.strftime('%d de %B de %Y %H:%M')} • **há {idade} dias**",
            inline=False
        )
    
    # Já foi detectado?
    if suspeito:
        embed.add_field(
            name="🚨 JÁ FOI DETECTADO PELO SISTEMA?",
            value=f"✅ **O usuário já foi detectado como suspeito {deteccoes} vez(es).**",
            inline=False
        )
        if ultima_deteccao:
            embed.add_field(
                name="📌 ÚLTIMA DETECÇÃO",
                value=f"{ultima_deteccao.strftime('%d de %B de %Y %H:%M')}",
                inline=False
            )
        embed.add_field(
            name="⚠️ MOTIVO",
            value=f"{suspeito['motivo']}",
            inline=False
        )
    else:
        if deteccoes > 0:
            embed.add_field(
                name="📌 JÁ FOI DETECTADO PELO SISTEMA?",
                value=f"⚠️ O usuário já foi detectado {deteccoes} vez(es), mas NÃO está na lista de suspeitos.",
                inline=False
            )
            if ultima_deteccao:
                embed.add_field(
                    name="📌 ÚLTIMA DETECÇÃO",
                    value=f"{ultima_deteccao.strftime('%d de %B de %Y %H:%M')}",
                    inline=False
                )
        else:
            embed.add_field(
                name="📌 JÁ FOI DETECTADO PELO SISTEMA?",
                value="❌ Nenhuma detecção registrada.",
                inline=False
            )
    
    # Servidores Suspeitos (mostrar cada um)
    if servidores_suspeitos:
        servidores_texto = ""
        for nome in servidores_suspeitos:
            servidores_texto += f"### {nome}\n"
        embed.add_field(
            name="🔒 SERVIDORES SUSPEITOS",
            value=servidores_texto,
            inline=False
        )
    else:
        embed.add_field(
            name="🔒 SERVIDORES SUSPEITOS",
            value="✅ Nenhum servidor suspeito conhecido encontrado.",
            inline=False
        )
    
    # Detecções Anteriores (se tiver)
    if deteccoes > 0:
        embed.add_field(
            name="📋 DETECÇÕES ANTERIORES",
            value=f"### {servidores_suspeitos[0] if servidores_suspeitos else 'N/A'}",
            inline=False
        )
    
    # Rodapé estilo XISpy (SEM CUPOM)
    embed.set_footer(
        text=f"⚠️ Essa é uma mensagem automática do sistema - {agora().strftime('%d/%m/%Y %H:%M')}",
        icon_url=bot.user.display_avatar.url if bot.user else None
    )
    
    await ctx.send(embed=embed)
    
    # Registrar verificação
    resultado = "suspeito" if suspeito else "limpo"
    await registrar_verificacao_db(user_id, ctx.author.id, resultado)
    
# =========================================================
# 4. EVENTO ON_MEMBER_JOIN MODIFICADO
# =========================================================

# NOTA: Se você já tem um on_member_join, substitua ou mescle com este
# Se não tiver, apenas cole este bloco

@bot.event
async def on_member_join(member):
    if member.bot:
        return
    
    # =========================================================
    # SISTEMA XLSPY - VERIFICAÇÃO AUTOMÁTICA
    # =========================================================
    suspeito = await verificar_suspeito_db(member.id)
    
    canal_log = bot.get_channel(CANAL_LOGS_GERAIS_ID)
    
    if suspeito:
        embed = discord.Embed(
            title="🚨 ALERTA: SUSPEITO ENTROU!",
            description=f"👤 {member.mention}",
            color=0xe74c3c,
            timestamp=agora()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(
            name="⚠️ STATUS",
            value="🔴 **Este usuário está na lista de suspeitos!**",
            inline=False
        )
        embed.add_field(
            name="📋 Motivo",
            value=suspeito['motivo'],
            inline=False
        )
        embed.add_field(
            name="👤 Adicionado por",
            value=f"<@{suspeito['adicionado_por']}>",
            inline=True
        )
        embed.add_field(
            name="📅 Data",
            value=suspeito['data_adicao'].strftime('%d/%m/%Y %H:%M'),
            inline=True
        )
        embed.set_footer(text="🛡 Sistema de Segurança VDR")
        
        if canal_log:
            await canal_log.send(embed=embed, view=AcaoSuspeitoView(member.id, None))
    else:
        embed = discord.Embed(
            title="🔍 NOVO MEMBRO - VERIFICADO",
            description=f"👤 {member.mention} entrou no servidor",
            color=0x2ecc71,
            timestamp=agora()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(
            name="✅ STATUS",
            value="✅ **Usuário verificado - Sem restrições**",
            inline=False
        )
        embed.set_footer(text="🛡 Sistema de Segurança VDR")
        
        if canal_log:
            await canal_log.send(embed=embed)
    
    # =========================================================
    # SISTEMA DE REGISTRO ORIGINAL
    # =========================================================
    try:
        cargo_em_registro = member.guild.get_role(EM_REGISTRO_ROLE_ID)
        if cargo_em_registro:
            await member.add_roles(cargo_em_registro)
    except Exception as e:
        logger.error(f"❌ Erro ao adicionar cargo de registro: {e}")

# =========================================================
# FIM DO SISTEMA XLSPY
# =========================================================

# =========================================================
# ==================== PARTE 20: MAIN =====================
# =========================================================

# =========================================================
# 20.1 VARIÁVEIS DE MÉTRICAS
# =========================================================
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
# 20.2 FUNÇÃO ON_READY
# =========================================================
@bot.event
async def on_ready():
    global http_session
    if hasattr(bot, "ja_iniciado"):
        return
    bot.ja_iniciado = True
    logger.info("🔄 Iniciando configuração do bot...")
    logger.info(f"✅ Logado como {bot.user}")
    if not http_session:
        http_session = aiohttp.ClientSession()
    db_pool = await conectar_db()
    if not db_pool:
        logger.critical("❌ Não foi possível conectar ao banco de dados!")
        return
    guild = bot.get_guild(GUILD_ID)
    if guild:
        try:
            await guild.chunk()
        except Exception as e:
            logger.error(f"Erro ao carregar membros: {e}")
    logger.info(f"🕒 Horário Brasília: {agora().strftime('%d/%m/%Y %H:%M:%S')}")

    # Carregar cache
    await carregar_metas_cache()

    # Iniciar tasks
    await iniciar_tarefas_background()
    bot.loop.create_task(limpeza_cache_periodica())
    bot.loop.create_task(health_check_avancado())
    if not hasattr(bot, "edit_worker_started"):
        bot.loop.create_task(edit_worker())
        bot.edit_worker_started = True

    # Carregar dados iniciais
    await carregar_dados_iniciais()

    # Enviar painéis
    await enviar_paineis_iniciais(guild)

    # Restaurar botões
    await BotaoPersistente.restaurar_botoes()
    await restaurar_botoes_vendas()
    await restaurar_acoes()
    await restaurar_botoes_metas()
    
    # Bot Animado IA
    await vdrzinho.carregar_memoria()

    # Setup status
    await setup_status()

    gc.collect()
    logger.info("=" * 50)
    logger.info("✅ BOT ONLINE 100% COMPLETO - v7.0")
    logger.info("=" * 50)

# =========================================================
# 20.3 FUNÇÃO CARREGAR_DADOS_INICIAIS
# =========================================================
async def carregar_dados_iniciais():
    try:
        rows = await carregar_metas_db()
        for r in rows:
            metas_cache[str(r["user_id"])] = {
                "canal_id": int(r["canal_id"]),
                "dinheiro": r["dinheiro"],
                "acao": r["acao"],
                "dinheiro_acoes": r.get("dinheiro_acoes") or 0,
                "saldo_excedente": r.get("saldo_excedente") or 0
            }
    except Exception as e:
        logger.error(f"Erro ao carregar metas: {e}")
    await restaurar_producoes()

# =========================================================
# 20.4 FUNÇÃO RESTAURAR_BOTOES_METAS
# =========================================================
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
                                await asyncio.sleep(1.5)
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
                                    await asyncio.sleep(1.5)
                            break
                if not mensagem_encontrada:
                    await atualizar_embed_meta(int(uid))
                    contador += 1
                    await asyncio.sleep(1.5)
            except Exception as e:
                logger.error(f"❌ Erro ao restaurar meta {uid}: {e}")
        logger.info(f"✅ {contador} painéis de metas restaurados com botões!")
        return contador
    except Exception as e:
        logger.error(f"❌ Erro ao restaurar botões das metas: {e}")
        return 0

# =========================================================
# 20.5 FUNÇÃO ENVIAR_PAINEIS_INICIAIS
# =========================================================
async def enviar_paineis_iniciais(guild):
    try:
        paineis = [
            ("Registro", enviar_painel_registro),
            ("Fabricação", enviar_painel_fabricacao),
            ("Lives", enviar_painel_lives),
            ("Pólvora", enviar_painel_polvoras),
            ("Lavagem", enviar_painel_lavagem),
            ("Vendas", enviar_painel_vendas),
            ("Relatório Financeiro", enviar_painel_relatorio_financeiro),
            ("Registrar Compra", enviar_painel_registrar_compra),
            ("Solicitar Sala", enviar_painel_solicitar_sala),
            ("Botão Ausência", enviar_painel_ausencia),
            ("Relatório Metas", enviar_painel_relatorio_metas),
            ("Baú", enviar_painel_bau),
            ("Armas", enviar_painel_armas),
            ("Avisos", enviar_painel_avisos),
            ("Grupos", enviar_painel_grupos),
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

# =========================================================
# 20.6 FUNÇÃO ON_MESSAGE
# =========================================================
@bot.event
async def on_message(message: discord.Message):
    # =========================================================
    # CAPTURAR E REFORMATAR MENSAGENS DO XISPY
    # =========================================================
    if message.author.id == 1100419913971150868:  # ID do XISpy
        if message.embeds:
            try:
                # Copiar os dados do embed do XISpy
                embed_original = message.embeds[0]

                # Criar novo embed no formato que você quer (SEM CUPOM)
                novo_embed = discord.Embed(
                    title=embed_original.title,
                    description=embed_original.description,
                    color=embed_original.color,
                    timestamp=agora()
                )

                # Copiar os campos do embed original
                for field in embed_original.fields:
                    novo_embed.add_field(
                        name=field.name,
                        value=field.value,
                        inline=field.inline
                    )

                # Definir thumbnail se existir
                if embed_original.thumbnail:
                    novo_embed.set_thumbnail(url=embed_original.thumbnail.url)

                # Rodapé personalizado (SEM CUPOM)
                novo_embed.set_footer(
                    text=f"⚠️ Essa é uma mensagem automática do sistema - {agora().strftime('%d/%m/%Y %H:%M')}",
                    icon_url=bot.user.display_avatar.url if bot.user else None
                )

                # Apagar a mensagem original do XISpy
                await message.delete()

                # Enviar a mensagem reformatada no mesmo canal
                await message.channel.send(embed=novo_embed)

            except Exception as e:
                logger.error(f"❌ Erro ao processar mensagem do XISpy: {e}")
                # Se der erro, não apaga a mensagem original
                return

        # Se for mensagem do XISpy, não processar comandos
        return

    # =========================================================
    # SISTEMA DE METAS (seu código existente)
    # =========================================================
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

    # =========================================================
    # SISTEMA DE LAVAGEM (seu código existente)
    # =========================================================
    await on_message_lavagem(message)

    # =========================================================
    # PROCESSAR COMANDOS
    # =========================================================
    await bot.process_commands(message)

    # =========================================================
    # SISTEMA DE METAS (seu código existente)
    # =========================================================
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

    # =========================================================
    # SISTEMA DE LAVAGEM (seu código existente)
    # =========================================================
    await on_message_lavagem(message)

    # =========================================================
    # PROCESSAR COMANDOS
    # =========================================================
    await bot.process_commands(message)

    # =========================================================
    # SISTEMA DE METAS (seu código existente)
    # =========================================================
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

    # =========================================================
    # SISTEMA DE LAVAGEM (seu código existente)
    # =========================================================
    await on_message_lavagem(message)

    # =========================================================
    # PROCESSAR COMANDOS
    # =========================================================
    await bot.process_commands(message)

# =========================================================
# 20.7 EVENTOS DE MEMBRO
# =========================================================
@bot.event
async def on_member_update(before, after):
    if after.bot:
        return
    tinha_resp = any(r.id == CARGO_RESP_METAS_ID for r in before.roles)
    tem_resp = any(r.id == CARGO_RESP_METAS_ID for r in after.roles)
    if not tinha_resp and tem_resp:
        await atualizar_acesso_responsaveis()
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
    if str(after.id) in metas_cache:
        await atualizar_categoria_meta(after)

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

# =========================================================
# 20.8 FUNÇÃO DE SHUTDOWN
# =========================================================
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

# =========================================================
# 20.9 MAIN
# =========================================================
if __name__ == "__main__":
    logger.info("🚀 Iniciando bot v7.0 COMPLETO...")
    try:
        import signal
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))
                break
            except RuntimeError:
                pass
    except Exception:
        pass
    try:
        bot.run(TOKEN, reconnect=True)
    except discord.LoginFailure:
        logger.critical("❌ Falha no login! TOKEN inválido?")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"❌ Erro fatal: {e}")
        sys.exit(1)
