# =========================================================
# ======================== IMPORTS =========================
# =========================================================

# ================= SISTEMA =================
import os
import json
import gc
import re
import tweepy

# ================= ASYNC =================
import asyncio
import aiohttp

# ================= BANCO =================
import asyncpg

# ================= DISCORD =================
import discord
from discord.ext import commands, tasks
from discord.utils import escape_markdown

# ================= TEMPO =================
import time as time_module
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo

# =========================================================
# ================= FILA GLOBAL DE EDIÇÃO =================
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


# =========================================================
# ================ ANTI ERRO DE INTERAÇÃO =================
# =========================================================

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

# ================= TWITTER =================

auth = tweepy.OAuth1UserHandler(
    os.environ.get("API_KEY"),
    os.environ.get("API_SECRET"),
    os.environ.get("ACCESS_TOKEN"),
    os.environ.get("ACCESS_SECRET")
)

api = tweepy.API(auth)

client = tweepy.Client(
    consumer_key=os.environ.get("API_KEY"),
    consumer_secret=os.environ.get("API_SECRET"),
    access_token=os.environ.get("ACCESS_TOKEN"),
    access_token_secret=os.environ.get("ACCESS_SECRET")
)
# =========================================================
# ==================== HTTP SESSÃO =========================
# =========================================================

http_session = None

_cache = {}

# =========================================================
# ================= CACHE DE CANAIS =======================
# =========================================================

channel_cache = {}

def pegar_canal(cid):

    if cid in channel_cache:
        return channel_cache[cid]

    canal = bot.get_channel(cid)

    if canal:
        channel_cache[cid] = canal

    return canal


# =========================================================
# ================= CACHE DE USUÁRIOS =====================
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
# ===================== RELÓGIO GLOBAL ====================
# =========================================================

BRASIL = ZoneInfo("America/Sao_Paulo")

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

# =========================================================
# ================= FUNÇÃO PEGAR APELIDO ==================
# =========================================================

async def pegar_apelido(guild, user_id):
    """Pega o apelido/nickname do usuário no servidor"""
    try:
        member = guild.get_member(int(user_id))
        if member:
            if member.nick:
                return member.nick
            return member.display_name
        return str(user_id)
    except:
        return str(user_id)
# =========================================================
# ======================== CONFIG ==========================
# =========================================================

TOKEN = os.environ.get("TOKEN")

tentativas = 0

while not TOKEN:
    tentativas += 1
    print(f"⚠️ TOKEN não carregado... tentativa {tentativas}")
    time_module.sleep(2)
    TOKEN = os.environ.get("TOKEN")

print("🔐 TOKEN carregado com sucesso.")

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")


# =========================================================
# ================= BANCO POSTGRES ========================
# =========================================================

DATABASE_URL = os.environ.get("DATABASE_URL")
db = None


async def conectar_db():
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

async def enviar_ou_atualizar_painel(nome, canal_id, embed, view):

    canal = bot.get_channel(canal_id)

    if not canal:
        return

    async for msg in canal.history(limit=20):

        if msg.author == bot.user and msg.embeds:

            if msg.embeds[0].title == embed.title:
                try:
                    await msg.edit(embed=embed, view=view)
                    return
                except:
                    pass

    await canal.send(embed=embed, view=view)

# =========================================================
# ======================== ARQUIVOS ========================
# =========================================================

BASE_PATH = "/mnt/data"
os.makedirs(BASE_PATH, exist_ok=True)

ARQUIVO_PRODUCOES = f"{BASE_PATH}/producoes.json"
ARQUIVO_PEDIDOS = f"{BASE_PATH}/pedidos.json"
ARQUIVO_POLVORAS = f"{BASE_PATH}/polvoras.json"
ARQUIVO_LIVES = f"{BASE_PATH}/lives.json"
ARQUIVO_LAVAGENS = f"{BASE_PATH}/lavagens.json"
ARQUIVO_METAS = f"{BASE_PATH}/metas.json"


# =========================================================
# ======================== GUILD ===========================
# =========================================================

GUILD_ID = 1229526644193099880
GUILD = discord.Object(id=GUILD_ID)

# =========================================================
# ======================== CANAIS ==========================
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

# POLVORA
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

# 
CANAL_ESCALACOES_ID = 1241406819545514064
CANAL_RELATORIO_ACOES_ID = 1477308788531921019

CANAL_POSTAGEM_X = 1486353689680547900
CANAL_BOTAO_AUSENCIA_ID = 1491427870277374162  # Canal onde fica o botão
CANAL_REGISTRO_AUSENCIA_ID = 1313854772545196032  # Mesmo canal (para registrar)
CANAL_GERENCIA_ID = 1237393478414241854
# ================= CLIPES =================

CANAL_CLIPES_ID = 1229526645837271134
EMOJI_APROVACAO = "✅"

clips_postados = set()
fila_clipes = None


# =========================================================
# ======================== CARGOS ==========================
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
    CARGO_GERENTE_ID,
    CARGO_RESP_ACAO_ID,
    CARGO_RESP_VENDAS_ID,
    CARGO_RESP_PRODUCAO_ID,
    CARGO_MEMBRO_ID,
    CARGO_SOLDADO_ID,
     CARGO_AGREGADO_ID
]

CARGOS_PERMITIDOS_ESCALACAO = [
    CARGO_AGREGADO_ID,
    CARGO_MEMBRO_ID,
    CARGO_SOLDADO_ID,
    CARGO_01_ID,
    CARGO_02_ID,
    CARGO_GERENTE_ID,
    CARGO_GERENTE_GERAL_ID
]


# =========================================================
# ===================== CATEGORIAS =========================
# =========================================================

CATEGORIA_META_GERENTE_ID = 1337374002422743122
CATEGORIA_META_RESPONSAVEIS_ID = 1462810826992783422
CATEGORIA_META_SOLDADO_ID = 1461335635519475894
CATEGORIA_META_MEMBRO_ID = 1461335697209163900
CATEGORIA_META_AGREGADO_ID = 1461335748870541323


# =========================================================
# ======================== BOT SETUP =======================
# =========================================================

intents = discord.Intents.default()
intents.members = True
intents.message_content = True
intents.guilds = True
intents.presences = True
intents.reactions = True

bot = commands.Bot(command_prefix="!", intents=intents)

registro_msg_id = None


# =========================================================
# ================= TWITCH TOKEN CACHE ====================
# =========================================================

twitch_token = None
twitch_token_expira = 0


async def obter_token_twitch():

    global twitch_token
    global twitch_token_expira

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

# ================= DOWNLOAD CLIP =================

async def baixar_video(url, caminho):

    async with aiohttp.ClientSession() as session:
        async with session.get(url) as resp:

            if resp.status != 200:
                return None

            data = await resp.read()

            with open(caminho, "wb") as f:
                f.write(data)

    return caminho

# ================= ENVIO PARA POSTAGEM =================

async def postar_clipe_x(message):

    try:

        canal = bot.get_channel(CANAL_POSTAGEM_X)

        if not canal:
            await message.reply("❌ Canal de postagem não encontrado.")
            return

        link = message.content if message.content else "Sem link"

        texto = (
            f"🚀 **CLIPE APROVADO**\n\n"
            f"👤 Autor: {message.author.mention}\n"
            f"🔗 Link: {link}\n\n"
            f"━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📝 **COPIAR E POSTAR NO X:**\n\n"
            f"🔥 Olha esse clipe!\n\n"
            f"{link}\n\n"
            f"#fivem #clips #gaming\n"
            f"━━━━━━━━━━━━━━━━━━━━━━"
        )

        await canal.send(texto)

        await message.reply("📤 Enviado para canal de postagem!")

    except Exception as e:

        print("ERRO CLIP:", e)
        await message.reply("❌ Erro ao enviar.")
# =========================================================
# ======================= REGISTRO =========================
# =========================================================

class RegistroModal(discord.ui.Modal, title="Registro de Entrada"):
    nome = discord.ui.TextInput(label="Nome Completo")
    passaporte = discord.ui.TextInput(label="Passaporte")
    indicado = discord.ui.TextInput(label="Indicado por", required=False)
    telefone = discord.ui.TextInput(label="Telefone In Game")

    async def on_submit(self, interaction: discord.Interaction):

        membro = interaction.user
        guild = interaction.guild

        # Atualiza nick
        await membro.edit(
            nick=f"{self.passaporte.value} - {self.nome.value}"
        )

        view = TipoRegistroView(
            self.nome.value,
            self.passaporte.value,
            self.indicado.value,
            self.telefone.value
        )

        await interaction.response.send_message(
            "Selecione o tipo de entrada:",
            view=view,
            ephemeral=True
        )


# =========================================================
# =================== SELETOR DE TIPO ======================
# =========================================================

class TipoRegistroSelect(discord.ui.Select):

    def __init__(self, nome, passaporte, indicado, telefone):

        self.nome = nome
        self.passaporte = passaporte
        self.indicado = indicado
        self.telefone = telefone

        options = [
            discord.SelectOption(
                label="Agregado",
                description="Se for se tornar membro da fac",
                emoji="🕴️"
            ),
            discord.SelectOption(
                label="Amigos",
                description="Se está apenas para resenha ou reunião",
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

        agregado = guild.get_role(AGREGADO_ROLE_ID)
        amigos = guild.get_role(1309121290241704046)
        convidado = guild.get_role(CONVIDADO_ROLE_ID)

        escolha = self.values[0]

        if escolha == "Agregado":
            if agregado:
                await membro.add_roles(agregado)

        if escolha == "Amigos":
            if amigos:
                await membro.add_roles(amigos)

        if convidado:
            await membro.remove_roles(convidado)

        canal_log = guild.get_channel(CANAL_LOG_REGISTRO_ID)

        if canal_log:

            embed = discord.Embed(
                title="📋 Novo Registro",
                color=0x2ecc71
            )

            embed.add_field(name="Nome", value=self.nome)
            embed.add_field(name="Passaporte", value=self.passaporte)
            embed.add_field(name="Indicado por", value=self.indicado)
            embed.add_field(name="Telefone", value=self.telefone)
            embed.add_field(name="Tipo", value=escolha)
            embed.add_field(name="Usuário", value=membro.mention)

            await canal_log.send(embed=embed)

        await interaction.response.send_message(
            f"✅ Registro concluído como **{escolha}**",
            ephemeral=True
        )


# =========================================================
# ======================== VIEWS ===========================
# =========================================================

class TipoRegistroView(discord.ui.View):

    def __init__(self, nome, passaporte, indicado, telefone):
        super().__init__(timeout=300)

        self.add_item(
            TipoRegistroSelect(
                nome,
                passaporte,
                indicado,
                telefone
            )
        )


class RegistroView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="📋 Fazer Registro",
        style=discord.ButtonStyle.success,
        custom_id="registro_fazer"
    )
    async def registro(
        self,
        interaction: discord.Interaction,
        button: discord.ui.Button
    ):
        await interaction.response.send_modal(
            RegistroModal()
        )

# =========================================================
# ================= PAINEL REGISTRO =======================
# =========================================================

async def enviar_painel_registro():

    canal = bot.get_channel(CANAL_REGISTRO_ID)

    if not canal:
        try:
            canal = await bot.fetch_channel(CANAL_REGISTRO_ID)
        except Exception as e:
            print("❌ Canal registro não encontrado:", e)
            return

    embed = discord.Embed(
        title="📋 Registro",
        description="Clique no botão abaixo para realizar seu registro.",
        color=0x2ecc71
    )

    # 🔍 PROCURA MENSAGEM EXISTENTE
    async for msg in canal.history(limit=20):

        if msg.author == bot.user and msg.embeds:

            if msg.embeds[0].title == "📋 Registro":
                try:
                    await msg.edit(embed=embed, view=RegistroView())
                    print("🔄 Painel de registro atualizado")
                    return
                except Exception as e:
                    print("Erro ao editar painel:", e)

    # 🆕 SE NÃO EXISTIR, CRIA
    await canal.send(embed=embed, view=RegistroView())
    print("✅ Painel de registro criado")

# =========================================================
# ================= SISTEMA DE SAÍDA (VERSÃO COMPLETA) ====
# =========================================================

# Canal de log da gerência
CANAL_GERENCIA_ID = 1237393478414241854

# Cargos que o usuário tinha (precisamos salvar antes de sair)
# Infelizmente o Discord não permite pegar os cargos após a saída
# Mas podemos logar outras informações

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
    
    # Ignorar bots
    if member.bot:
        return
    
    # Aguardar um pouco
    await asyncio.sleep(2)
    
    # Coletar informações detalhadas
    nome_servidor = member.display_name
    nome_usuario = member.name
    nome_global = member.global_name or nome_usuario
    
    # Verificar diferença entre apelido e nome
    if nome_servidor != nome_usuario and nome_servidor != nome_global:
        status_apelido = "✅ **Diferente do nome de usuário**"
        apelido_detalhe = f"**Apelido no servidor:** {nome_servidor}\n**Nome de usuário:** {nome_usuario}"
    else:
        status_apelido = "ℹ️ **Mesmo nome de usuário**"
        apelido_detalhe = f"**Nome usado:** {nome_servidor}"
    
    # Tentar enviar DM
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
    
    # =============================================
    # LOG COMPLETO NO CANAL DA GERÊNCIA
    # =============================================
    canal_gerencia = bot.get_channel(CANAL_GERENCIA_ID)
    
    if canal_gerencia:
        
        # Calcular tempo de permanência no servidor
        tempo_permanencia = "Desconhecido"
        if member.joined_at:
            dias = (agora() - member.joined_at.replace(tzinfo=BRASIL)).days
            if dias > 0:
                tempo_permanencia = f"{dias} dia(s)"
            else:
                horas = (agora() - member.joined_at.replace(tzinfo=BRASIL)).seconds // 3600
                tempo_permanencia = f"{horas} hora(s)"
        
        embed_log = discord.Embed(
            title="📤 USUÁRIO SAIU DO SERVIDOR",
            color=cor_log,
            timestamp=agora()
        )
        
        # Seção 1: Informações do usuário
        embed_log.add_field(
            name="👤 INFORMAÇÕES DO USUÁRIO",
            value=(
                f"```\n"
                f"Mencão: {member.mention}\n"
                f"ID: {member.id}\n"
                f"Nome de usuário: {member.name}\n"
                f"Alias global: {member.global_name or 'Nenhum'}\n"
                f"```"
            ),
            inline=False
        )
        
        # Seção 2: Informações do apelido
        embed_log.add_field(
            name="🏷️ APELIDO NO SERVIDOR",
            value=(
                f"```\n"
                f"Apelido: {nome_servidor}\n"
                f"Status: {status_apelido}\n"
                f"```"
            ),
            inline=False
        )
        
        # Seção 3: Tempo de permanência
        embed_log.add_field(
            name="⏱️ TEMPO NO SERVIDOR",
            value=(
                f"```\n"
                f"Entrou em: {member.joined_at.strftime('%d/%m/%Y %H:%M') if member.joined_at else 'Desconhecido'}\n"
                f"Permanência: {tempo_permanencia}\n"
                f"Conta criada: {member.created_at.strftime('%d/%m/%Y')}\n"
                f"```"
            ),
            inline=False
        )
        
        # Seção 4: Status da DM
        if dm_sucesso:
            icon_dm = "✅"
        else:
            icon_dm = "❌"
        
        embed_log.add_field(
            name=f"{icon_dm} STATUS DA MENSAGEM",
            value=status_dm,
            inline=False
        )
        
        # Adicionar avatar se disponível
        if member.display_avatar:
            embed_log.set_thumbnail(url=member.display_avatar.url)
        
        # Adicionar rodapé
        data_saida = agora().strftime('%d/%m/%Y às %H:%M:%S')
        embed_log.set_footer(text=f"Sistema Automático • Saída em {data_saida}")
        
        # Enviar no canal da gerência
        await canal_gerencia.send(embed=embed_log)
        
        print(f"📋 [SAÍDA] Log enviado para gerência: {member.name} (DM: {'OK' if dm_sucesso else 'FALHA'})")
        
    else:
        print(f"❌ [SAÍDA] Canal da gerência NÃO ENCONTRADO! ID: {CANAL_GERENCIA_ID}")

# =========================================================
# ======================== VENDAS ==========================
# =========================================================

ORGANIZACOES_CONFIG = {
    "VDR": {"emoji": "🔥", "cor": 0xe74c3c},
    "POLICIA": {"emoji": "🚓", "cor": 0x3498db},
    "EXERCITO": {"emoji": "🪖", "cor": 0x2ecc71},
    "MAFIA": {"emoji": "💀", "cor": 0x8e44ad},
    "CIVIL": {"emoji": "👤", "cor": 0x95a5a6},
}


# =========================================================
# ================= BANCO DE DADOS =====================
# =========================================================

async def proximo_pedido():

    async with db.acquire() as conn:

        row = await conn.fetchrow(
            "SELECT ultimo FROM pedidos WHERE id=1"
        )

        if not row:
            await conn.execute(
                "INSERT INTO pedidos (id, ultimo) VALUES (1, 1)"
            )
            return 1

        novo = row["ultimo"] + 1

        await conn.execute(
            "UPDATE pedidos SET ultimo=$1 WHERE id=1",
            novo
        )

        return novo


async def salvar_venda_db(vendedor_id, valor, pedido_numero):

    async with db.acquire() as conn:

        await conn.execute(
            """
            INSERT INTO vendas (user_id, valor, data, pedido_numero)
            VALUES ($1, $2, $3, $4)
            """,
            vendedor_id,
            valor,
            agora().strftime("%d/%m/%Y"),
            pedido_numero
        )


async def atualizar_valor_venda_db(pedido_numero, valor):

    async with db.acquire() as conn:

        await conn.execute(
            """
            UPDATE vendas
            SET valor = $1
            WHERE pedido_numero = $2
            """,
            valor,
            pedido_numero
        )


async def carregar_vendas_db():

    async with db.acquire() as conn:

        rows = await conn.fetch(
            "SELECT * FROM vendas"
        )

        return rows


# =========================================================
# ================= STATUS DOS BOTÕES =====================
# =========================================================

class StatusView(discord.ui.View):

    def __init__(self, disabled: bool = False):
        super().__init__(timeout=None)

        if disabled:
            for item in self.children:
                item.disabled = True

    # =====================================================
    # ================= UTILIDADES ========================
    # =====================================================

    def get_status(self, embed):

        for i, field in enumerate(embed.fields):
            if field.name == "📌 Status":
                return i, field.value.split("\n")

        return None, []


    def set_status(self, embed, idx, linhas):

        if not linhas:
            linhas = ["📦 A entregar"]

        embed.set_field_at(
            idx,
            name="📌 Status",
            value="\n".join(linhas),
            inline=False
        )

        return embed


    def pedido_pago(self, linhas):
        return any(l.startswith("💰") for l in linhas)


    def pedido_cancelado(self, linhas):
        return any(l.startswith("❌") for l in linhas)


    # =====================================================
    # ================= BOTÃO PAGO ========================
    # =====================================================

    @discord.ui.button(
        label="💰 Pago",
        style=discord.ButtonStyle.primary,
        custom_id="status_pago"
    )
    async def pago(self, interaction: discord.Interaction, button: discord.ui.Button):

        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        if self.pedido_cancelado(linhas):
            await interaction.response.send_message(
                "⚠️ Este pedido foi cancelado e não pode ser pago.",
                ephemeral=True
            )
            return

        if self.pedido_pago(linhas):
            await interaction.response.send_message(
                "⚠️ Este pedido já foi pago.",
                ephemeral=True
            )
            return

        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        user = interaction.user.mention

        linhas = [l for l in linhas if not l.startswith("⏳")]
        linhas = [l for l in linhas if not l.startswith("💰")]

        linhas.append(
            f"💰 Pago • Recebido por {user} • {agora_str}"
        )

        embed = self.set_status(embed, idx, linhas)

        # ================= FINALIZA VENDA =================

        finalizado = (
            any(l.startswith("💰") for l in linhas)
            and any(l.startswith("✅") for l in linhas)
        )

        if finalizado:

            embed.color = 0x2ecc71
            embed.title = "🎉 VENDA CONCLUÍDA"

            embed.add_field(
                name="━━━━━━━━━━━━━━━━━━━━━━━━━━",
                value="",
                inline=False
            )

            embed.add_field(
                name="✅ VENDA FINALIZADA COM SUCESSO",
                value="💰 **Pagamento recebido**\n📦 **Pedido entregue ao cliente**",
                inline=False
            )

            embed.add_field(
                name="━━━━━━━━━━━━━━━━━━━━━━━━━━",
                value="🔥 **Pedido encerrado no sistema**",
                inline=False
            )

            await interaction.message.edit(
                embed=embed,
                view=StatusView(disabled=True)
            )

        else:

            await interaction.message.edit(
                embed=embed,
                view=self
            )

        await responder_interacao(interaction, defer=True)


    # =====================================================
    # ================= BOTÃO ENTREGUE ====================
    # =====================================================

    @discord.ui.button(
        label="✅ Entregue",
        style=discord.ButtonStyle.success,
        custom_id="status_entregue"
    )
    async def entregue(self, interaction: discord.Interaction, button: discord.ui.Button):

        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        if self.pedido_cancelado(linhas):
            await interaction.response.send_message(
                "⚠️ Este pedido foi cancelado.",
                ephemeral=True
            )
            return

        # ===============================
        # VERIFICAR ESTOQUE ANTES DE ENTREGAR
        # ===============================
        
        pacotes_pt = 0
        pacotes_sub = 0
        
        for field in embed.fields:
            if field.name == "🔫 PT":
                try:
                    linhas_field = field.value.split("\n")
                    for l in linhas_field:
                        if "📦" in l:
                            pacotes_pt = int(
                                l.replace("📦", "")
                                .replace("pacotes", "")
                                .strip()
                            )
                except:
                    pass
            
            if field.name == "🔫 SUB":
                try:
                    linhas_field = field.value.split("\n")
                    for l in linhas_field:
                        if "📦" in l:
                            pacotes_sub = int(
                                l.replace("📦", "")
                                .replace("pacotes", "")
                                .strip()
                            )
                except:
                    pass
        
        # Verificar estoque PT
        if pacotes_pt > 0:
            estoque_suficiente_pt = await verificar_estoque_suficiente("PT", pacotes_pt)
            if not estoque_suficiente_pt:
                estoque_atual = await carregar_estoque()
                await interaction.response.send_message(
                    f"❌ **ESTOQUE INSUFICIENTE!**\n\n"
                    f"🔫 PT: {pacotes_pt} pacotes necessários\n"
                    f"📦 Estoque atual: {estoque_atual['PT']} pacotes\n\n"
                    f"⚠️ **ATENÇÃO!** Antes de entregar, você PRECISA produzir mais munição PT!\n"
                    f"Use o botão **🎯 PRODUZIR MUNIÇÃO** no painel de Fabricação.",
                    ephemeral=True
                )
                return
        
        # Verificar estoque SUB
        if pacotes_sub > 0:
            estoque_suficiente_sub = await verificar_estoque_suficiente("SUB", pacotes_sub)
            if not estoque_suficiente_sub:
                estoque_atual = await carregar_estoque()
                await interaction.response.send_message(
                    f"❌ **ESTOQUE INSUFICIENTE!**\n\n"
                    f"🔫 SUB: {pacotes_sub} pacotes necessários\n"
                    f"📦 Estoque atual: {estoque_atual['SUB']} pacotes\n\n"
                    f"⚠️ **ATENÇÃO!** Antes de entregar, você PRECISA produzir mais munição SUB!\n"
                    f"Use o botão **🎯 PRODUZIR MUNIÇÃO** no painel de Fabricação.",
                    ephemeral=True
                )
                return
        
        # Registrar saída do estoque
        titulo = embed.title
        pedido_numero = int(titulo.split("#")[1]) if "#" in titulo else 0
        
        if pacotes_pt > 0:
            await registrar_saida_estoque(pedido_numero, "PT", pacotes_pt, interaction.user.id)
            print(f"📦 Saída de estoque PT: {pacotes_pt} pacotes (Pedido #{pedido_numero})")
        
        if pacotes_sub > 0:
            await registrar_saida_estoque(pedido_numero, "SUB", pacotes_sub, interaction.user.id)
            print(f"📦 Saída de estoque SUB: {pacotes_sub} pacotes (Pedido #{pedido_numero})")
        
        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        user = interaction.user

        linhas = [l for l in linhas if not l.startswith("📦")]
        linhas = [l for l in linhas if not l.startswith("✅")]

        linhas.append(
            f"✅ Entregue por {user.mention} • {agora_str}"
        )

        embed = self.set_status(embed, idx, linhas)

        finalizado = (
            any(l.startswith("💰") for l in linhas)
            and any(l.startswith("✅") for l in linhas)
        )

        if finalizado:

            embed.color = 0x2ecc71
            embed.title = "🎉 VENDA CONCLUÍDA"

            embed.add_field(
                name="━━━━━━━━━━━━━━━━━━━━━━━━━━",
                value="",
                inline=False
            )

            embed.add_field(
                name="✅ VENDA FINALIZADA COM SUCESSO",
                value="💰 **Pagamento recebido**\n📦 **Pedido entregue ao cliente**\n📊 **Estoque atualizado**",
                inline=False
            )

            embed.add_field(
                name="━━━━━━━━━━━━━━━━━━━━━━━━━━",
                value="🔥 **Pedido encerrado no sistema**",
                inline=False
            )

            await interaction.message.edit(
                embed=embed,
                view=StatusView(disabled=True)
            )

        else:

            await interaction.message.edit(
                embed=embed,
                view=self
            )

        await responder_interacao(interaction, defer=True)

        # ===============================
        # ENVIAR NO BAÚ (SE TIVER PACOTES)
        # ===============================

        if pacotes_pt > 0 or pacotes_sub > 0:
            canal_bau = interaction.guild.get_channel(
                CANAL_BAU_GALPAO_SUL_ID
            )

            if canal_bau:
                try:
                    texto = f"📦 **Retirada do Baú**\n\n"
                    texto += f"👤 Retirado por: {interaction.user.mention}\n"
                    if pacotes_pt > 0:
                        texto += f"🔫 PT: {pacotes_pt} pacotes\n"
                    if pacotes_sub > 0:
                        texto += f"🔫 SUB: {pacotes_sub} pacotes"

                    await canal_bau.send(texto)
                except Exception as e:
                    print("Erro envio baú:", e)


    # =====================================================
    # ================= BOTÃO CANCELADO ===================
    # =====================================================

    @discord.ui.button(
        label="❌ Pedido cancelado",
        style=discord.ButtonStyle.danger,
        custom_id="status_cancelado"
    )
    async def cancelado(self, interaction: discord.Interaction, button: discord.ui.Button):

        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        if self.pedido_pago(linhas):
            await interaction.response.send_message(
                "⚠️ Este pedido já foi pago e não pode ser cancelado.",
                ephemeral=True
            )
            return

        if self.pedido_cancelado(linhas):
            await interaction.response.send_message(
                "⚠️ Este pedido já foi cancelado.",
                ephemeral=True
            )
            return

        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        user = interaction.user.mention

        linhas = [
            f"❌ Pedido cancelado por {user} • {agora_str}"
        ]

        embed = self.set_status(embed, idx, linhas)

        # Finaliza pedido → trava botões
        await interaction.message.edit(
            embed=embed,
            view=StatusView(disabled=True)
        )

        await responder_interacao(interaction, defer=True)


    # =====================================================
    # ================= BOTÃO EDITAR ======================
    # =====================================================

    @discord.ui.button(
        label="✏️ Editar Venda",
        style=discord.ButtonStyle.secondary,
        custom_id="status_editar_venda"
    )
    async def editar(self, interaction: discord.Interaction, button: discord.ui.Button):

        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        if self.pedido_cancelado(linhas):

            await interaction.response.send_message(
                "⚠️ Pedido cancelado não pode ser editado.",
                ephemeral=True
            )
            return

        await interaction.response.send_modal(
            EditarVendaModal(interaction.message)
        )


# =========================================================
# ================= MODAL DE VENDA ========================
# =========================================================

class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):

    organizacao = discord.ui.TextInput(label="Organização")
    qtd_pt = discord.ui.TextInput(label="Quantidade PT")
    qtd_sub = discord.ui.TextInput(label="Quantidade SUB")

    observacoes = discord.ui.TextInput(
        label="Observ",
        style=discord.TextStyle.paragraph,
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):

        try:
            pt = int(self.qtd_pt.value.strip())
            sub = int(self.qtd_sub.value.strip())
        except:
            await interaction.response.send_message(
                "Valores inválidos.",
                ephemeral=True
            )
            return

        numero_pedido = await proximo_pedido()

        pacotes_pt = pt // 50
        pacotes_sub = sub // 50

        total = (pt * 50) + (sub * 90)
        valor_novo = total

        # ================= SALVA NO POSTGRES =================

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
                name="📝 Observ",
                value=self.observacoes.value,
                inline=False
            )

        embed.set_footer(
            text="🛡 Sistema de Encomendas • VDR 442"
        )

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)

        await canal.send(
            embed=embed,
            view=StatusView()
        )

        await responder_interacao(interaction, defer=True)


# =========================================================
# ================= MODAL RELATÓRIO =======================
# =========================================================

class RelatorioModal(discord.ui.Modal, title="📊 Relatório de Vendas"):

    data_inicio = discord.ui.TextInput(
        label="Data inicial",
        placeholder="Ex: 01/03/2026"
    )

    data_fim = discord.ui.TextInput(
        label="Data final",
        placeholder="Ex: 17/03/2026"
    )

    async def on_submit(self, interaction: discord.Interaction):

        await interaction.response.defer(ephemeral=True)

        try:

            inicio = datetime.strptime(
                self.data_inicio.value,
                "%d/%m/%Y"
            )

            fim = datetime.strptime(
                self.data_fim.value,
                "%d/%m/%Y"
            )

            fim = fim + timedelta(days=1)

        except Exception:

            await interaction.followup.send(
                "Formato inválido. Use **DD/MM/AAAA**",
                ephemeral=True
            )
            return

        async with db.acquire() as conn:

            rows = await conn.fetch(
                """
                SELECT user_id, SUM(valor) as total
                FROM vendas
                WHERE TO_DATE(data, 'DD/MM/YYYY') BETWEEN $1 AND $2
                GROUP BY user_id
                """,
                inicio,
                fim
            )

        if not rows:

            await interaction.followup.send(
                "Nenhuma venda no período.",
                ephemeral=True
            )
            return

        total = 0
        linhas = []

        for r in rows:

            valor = r["total"]
            total += valor

            linhas.append(
                f"👤 <@{r['user_id']}> • R$ {valor:,.2f}"
                .replace(",", "X")
                .replace(".", ",")
                .replace("X", ".")
            )

        total_fmt = (
            f"{total:,.2f}"
            .replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )

        embed = discord.Embed(
            title="📊 Relatório de Vendas",
            color=0x2ecc71
        )

        embed.add_field(
            name="💰 Total Vendido",
            value=f"R$ {total_fmt}",
            inline=False
        )

        embed.add_field(
            name="👥 Por vendedor",
            value="\n".join(linhas),
            inline=False
        )

        canal = interaction.guild.get_channel(1365372467723501723)

        if canal:
            await canal.send(embed=embed)

        await interaction.followup.send(
            "Relatório enviado no canal de vendas.",
            ephemeral=True
        )


# =========================================================
# ================= EDITAR VENDA ===========================
# =========================================================

class EditarVendaModal(discord.ui.Modal, title="Editar Venda"):

    qtd_pt = discord.ui.TextInput(label="Nova Quantidade PT")
    qtd_sub = discord.ui.TextInput(label="Nova Quantidade SUB")

    organizacao = discord.ui.TextInput(
        label="Nova Organização (opcional)",
        required=False
    )

    observacao = discord.ui.TextInput(
        label="Nova Observação (opcional)",
        style=discord.TextStyle.paragraph,
        required=False
    )

    def __init__(self, message):
        super().__init__()
        self.message = message

    async def on_submit(self, interaction: discord.Interaction):

        try:
            pt = int(self.qtd_pt.value.strip())
            sub = int(self.qtd_sub.value.strip())
        except:
            await interaction.response.send_message(
                "Valores inválidos.",
                ephemeral=True
            )
            return

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

        # ===============================
        # PEGAR VALORES ANTIGOS
        # ===============================

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

        # ===============================
        # ATUALIZAR EMBED
        # ===============================

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

            if field.name == "📝 Observ" and self.observacao.value:
                embed.set_field_at(
                    i,
                    name="📝 Observ",
                    value=self.observacao.value.strip(),
                    inline=False
                )

        titulo = embed.title
        pedido_numero = int(titulo.split("#")[1])

        await atualizar_valor_venda_db(pedido_numero, total)

        await self.message.edit(embed=embed)

        # ===============================
        # DETECTAR ALTER
        # ===============================

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

        # ===============================
        # LOG
        # ===============================

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
                name="🔧 Alter",
                value=alteracao_texto,
                inline=False
            )

            await canal_log.send(embed=embed_log)

        await interaction.response.send_message(
            "Venda editada com sucesso.",
            ephemeral=True
        )


# =========================================================
# ================= VIEW CALCULADORA ======================
# =========================================================

class CalculadoraView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Registrar Venda",
        style=discord.ButtonStyle.primary,
        custom_id="calc_registrar_venda"
    )
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(
            VendaModal()
        )

    @discord.ui.button(
        label="Relatório",
        style=discord.ButtonStyle.success,
        custom_id="calc_relatorio_vendas"
    )
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(
            RelatorioModal()
        )


# =========================================================
# ================= PAINEL ================================
# =========================================================

async def enviar_painel_vendas():

    canal = pegar_canal(CANAL_VENDAS_ID)

    if not canal:
        print("❌ Canal de vendas não encontrado")
        return

    embed = discord.Embed(
        title="🛒 Painel de Vendas",
        description="Escolha uma opção abaixo.\n\n⚠️ **ATENÇÃO:** Antes de entregar um pedido, verifique se há ESTOQUE disponível!",
        color=0x2ecc71
    )
    
    # Mostrar estoque atual no painel
    estoque = await carregar_estoque()
    
    def fmt_num(valor):
        return f"{valor:,.0f}".replace(",", ".")
    
    embed.add_field(
        name="📦 ESTOQUE DISPONÍVEL",
        value=f"🔫 PT: **{fmt_num(estoque['PT'])}** pacotes\n🔫 SUB: **{fmt_num(estoque['SUB'])}** pacotes",
        inline=False
    )

    await enviar_ou_atualizar_painel(
        "painel_vendas",
        CANAL_VENDAS_ID,
        embed,
        CalculadoraView()
    )

    print("🛒 Painel de vendas verificado/atualizado")

# =========================================================
# ======================== PRODUÇÃO ========================
# =========================================================

producoes_tasks = {}
galpoes_ativos = set()
producoes_ativas = set()

# =========================================================
# ================= FUNÇÕES DE ESTOQUE ====================
# =========================================================

async def carregar_estoque():
    """Carrega o estoque atual de munições do banco"""
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT tipo, quantidade FROM estoque_municoes")
    
    estoque = {"PT": 0, "SUB": 0}
    for row in rows:
        estoque[row["tipo"]] = row["quantidade"]
    return estoque


async def atualizar_estoque(tipo, quantidade, operacao="adicionar"):
    """Atualiza o estoque de munições"""
    async with db.acquire() as conn:
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


# =========================================================
# ================= FUNÇÕES DE ESTOQUE DE INSUMOS =========
# =========================================================

async def carregar_estoque_insumos():
    """Carrega o estoque de cápsulas e embalagens"""
    async with db.acquire() as conn:
        capsulas_row = await conn.fetchrow("SELECT quantidade FROM estoque_capsulas WHERE id = 1")
        capsulas = capsulas_row["quantidade"] if capsulas_row else 0
        
        embalagens_row = await conn.fetchrow("SELECT quantidade FROM estoque_embalagens WHERE id = 1")
        embalagens = embalagens_row["quantidade"] if embalagens_row else 0
    
    return {"capsulas": capsulas, "embalagens": embalagens}


async def atualizar_estoque_capsulas(quantidade, operacao="adicionar"):
    """Atualiza o estoque de cápsulas"""
    async with db.acquire() as conn:
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
    """Atualiza o estoque de embalagens"""
    async with db.acquire() as conn:
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
    """Registra entrada de cápsulas ou embalagens"""
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO entrada_insumos (tipo, quantidade, registrado_por, obs)
            VALUES ($1, $2, $3, $4)
            """,
            tipo, quantidade, str(registrado_por), obs
        )
        
        if tipo == "capsulas":
            await atualizar_estoque_capsulas(quantidade, "adicionar")
        elif tipo == "embalagens":
            await atualizar_estoque_embalagens(quantidade, "adicionar")


async def verificar_insumos_producao(tipo, pacotes):
    """Verifica se há insumos suficientes para produzir a munição"""
    estoque = await carregar_estoque_insumos()
    
    if tipo == "PT":
        capsulas_necessarias = pacotes * 25
        embalagens_necessarias = pacotes * 5
    else:
        capsulas_necessarias = pacotes * 65
        embalagens_necessarias = pacotes * 25
    
    capsulas_suficiente = estoque["capsulas"] >= capsulas_necessarias
    embalagens_suficiente = estoque["embalagens"] >= embalagens_necessarias
    
    return {
        "suficiente": capsulas_suficiente and embalagens_suficiente,
        "capsulas_necessarias": capsulas_necessarias,
        "embalagens_necessarias": embalagens_necessarias,
        "capsulas_disponiveis": estoque["capsulas"],
        "embalagens_disponiveis": estoque["embalagens"]
    }


async def consumir_insumos_producao(tipo, pacotes):
    """Consome os insumos necessários para produção"""
    if tipo == "PT":
        capsulas_consumir = pacotes * 25
        embalagens_consumir = pacotes * 5
    else:
        capsulas_consumir = pacotes * 65
        embalagens_consumir = pacotes * 25
    
    await atualizar_estoque_capsulas(capsulas_consumir, "remover")
    await atualizar_estoque_embalagens(embalagens_consumir, "remover")
    
    return capsulas_consumir, embalagens_consumir


async def registrar_producao_municao(tipo, pacotes, produzido_por, obs=""):
    """Registra a produção de munição no histórico e consome insumos"""
    municoes = pacotes * 50
    capsulas_consumidas, embalagens_consumidas = await consumir_insumos_producao(tipo, pacotes)
    
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO producao_municao (tipo, pacotes, municoes, produzido_por, obs, capsulas_consumidas, embalagens_consumidas)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
            tipo, pacotes, municoes, str(produzido_por), obs, capsulas_consumidas, embalagens_consumidas
        )
        await atualizar_estoque(tipo, pacotes, "adicionar")


async def registrar_saida_estoque(pedido_numero, tipo, pacotes, retirado_por):
    """Registra a saída de estoque por venda"""
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO saida_estoque (pedido_numero, tipo, pacotes, retirado_por, data)
            VALUES ($1, $2, $3, $4, NOW())
            """,
            pedido_numero, tipo, pacotes, str(retirado_por)
        )
        await atualizar_estoque(tipo, pacotes, "remover")


async def verificar_estoque_suficiente(tipo, pacotes_necessarios):
    """Verifica se há estoque suficiente para a venda"""
    estoque = await carregar_estoque()
    return estoque.get(tipo, 0) >= pacotes_necessarios


# =========================================================
# ================= FUNÇÕES DE PRODUÇÃO ===================
# =========================================================

async def carregar_producao(pid):
    """Carrega produção do banco com tratamento correto de datas"""
    async with db.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM producoes WHERE pid=$1", pid)

    if not r:
        return None

    dados = {
        "galpao": r["galpao"],
        "autor": int(r["autor"]),
        "inicio": r["inicio"],
        "fim": r["fim"],
        "obs": r["obs"],
        "msg_id": int(r["msg_id"]),
        "canal_id": int(r["canal_id"]),
        "polvora": r["polvora"] or 400,
        "qtd_galpoes": r.get("qtd_galpoes", 1) if r.get("qtd_galpoes") else 1,
        "polvora_por_galpao": r.get("polvora_por_galpao", 400) if r.get("polvora_por_galpao") else 400
    }

    if r["segunda_task_user"]:
        dados["segunda_task_confirmada"] = {
            "user": int(r["segunda_task_user"]),
            "time": r["segunda_task_time"]
        }

    return dados


async def salvar_producao(pid, dados):
    """Salva produção no banco com datas no formato correto"""
    segunda_user = None
    segunda_time = None
    qtd_galpoes = dados.get("qtd_galpoes", 1)
    polvora_por_galpao = dados.get("polvora_por_galpao", dados.get("polvora", 400) // qtd_galpoes if qtd_galpoes > 0 else 400)

    if "segunda_task_confirmada" in dados:
        segunda_user = str(dados["segunda_task_confirmada"]["user"])
        segunda_time = dados["segunda_task_confirmada"]["time"]

    # 🔥 CORREÇÃO: Converte datetime para string ISO
    if isinstance(dados["inicio"], datetime):
        inicio = dados["inicio"].isoformat()
    else:
        inicio = dados["inicio"]
    
    if isinstance(dados["fim"], datetime):
        fim = dados["fim"].isoformat()
    else:
        fim = dados["fim"]

    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO producoes 
            (pid, galpao, autor, inicio, fim, obs, msg_id, canal_id, segunda_task_user, segunda_task_time, polvora, qtd_galpoes, polvora_por_galpao)
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
            inicio,
            fim,
            dados["obs"],
            str(dados["msg_id"]),
            str(dados["canal_id"]),
            segunda_user,
            segunda_time,
            dados.get("polvora", 400),
            qtd_galpoes,
            polvora_por_galpao
        )


async def deletar_producao(pid):
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM producoes WHERE pid=$1", pid)


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
# ================= 2ª TASK ===============================
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
                await interaction.followup.send("⚠️ Esta produção já terminou!", ephemeral=True)
                return
            
            prod["segunda_task_confirmada"] = {
                "user": interaction.user.id,
                "time": agora().isoformat()
            }
            await salvar_producao(self.pid, prod)
            await interaction.message.edit(view=None)
            await interaction.followup.send("✅ Segunda task confirmada com sucesso!", ephemeral=True)
            
        except Exception as e:
            print("Erro segunda task:", e)
            await interaction.followup.send(f"❌ Erro: {str(e)}", ephemeral=True)


# =========================================================
# ================= MODAL ÚNICO DE PRODUÇÃO ===============
# =========================================================

class ProducaoCompletaModal(discord.ui.Modal, title="🏭 Iniciar Produção"):
    
    qtd_galpoes = discord.ui.TextInput(
        label="📊 Quantos galpões?",
        placeholder="Digite 1, 2 ou 3",
        required=True,
        max_length=1
    )
    
    polvora_por_galpao = discord.ui.TextInput(
        label="💣 Pólvora por galpão",
        placeholder="Ex: 400",
        required=True
    )
    
    obs = discord.ui.TextInput(
        label="📝 Observação (opcional)",
        style=discord.TextStyle.paragraph,
        required=False
    )
    
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
        tempo_real = max(1, int(self.tempo_base * (polvora_por_galpao / 400)))
        
        pid = f"{self.galpao}_{qtd}g_{interaction.id}_{int(time_module.time())}"
        inicio = agora()
        fim = inicio + timedelta(minutes=tempo_real)
        canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)

        if not canal:
            await interaction.followup.send("❌ Canal de produção não encontrado.", ephemeral=True)
            return

        desc = (
            f"**Galpão:** {self.galpao}\n"
            f"**Quantidade de galpões:** {qtd}\n"
            f"**Iniciado por:** {interaction.user.mention}\n"
        )

        if self.obs.value:
            desc += f"📝 **Obs:** {self.obs.value}\n"

        desc += (
            f"**Pólvora por galpão:** {polvora_por_galpao}\n"
            f"**Pólvora total:** {polvora_total}\n"
            f"Início: <t:{int(inicio.timestamp())}:t>\n"
            f"Término: <t:{int(fim.timestamp())}:t>\n\n"
            f"⏳ **Restante:** {tempo_real} min\n"
            f"{barra(0)}"
        )

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
        
        def fmt_num(valor):
            return f"{valor:,.0f}".replace(",", ".")
        
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
# ================= LOOP DE ACOMPANHAMENTO =================
# =========================================================

async def gerar_desc_producao(prod, pct=None, restante=None):
    """Gera a descrição da produção para o embed"""
    if pct is None:
        inicio = prod["inicio"]
        fim = prod["fim"]
        if isinstance(inicio, str):
            inicio = str_para_datetime(inicio)
        if isinstance(fim, str):
            fim = str_para_datetime(fim)
        
        agora_dt = agora()
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
    
    desc = f"**Galpão:** {prod['galpao']}\n**Iniciado por:** <@{prod['autor']}>\n"
    if prod.get("obs"):
        desc += f"📝 **Obs:** {prod['obs']}\n"
    
    inicio = prod["inicio"]
    fim = prod["fim"]
    if isinstance(inicio, str):
        inicio = str_para_datetime(inicio)
    if isinstance(fim, str):
        fim = str_para_datetime(fim)
    
    desc += f"Início: <t:{int(inicio.timestamp())}:t>\nTérmino: <t:{int(fim.timestamp())}:t>\n\n"
    desc += f"⏳ **Restante:** {mins}m {segundos}s\n{barra(pct)}"
    
    if prod.get("segunda_task_confirmada"):
        uid = prod["segunda_task_confirmada"]["user"]
        desc += f"\n\n✅ **Segunda task concluída por:** <@{uid}>"
    
    return desc


async def acompanhar_producao(pid):
    """Acompanha a produção com verificação robusta de estado"""
    print(f"▶ Produção iniciada/restaurada: {pid}")
    msg = None
    ultimo_pct = -1
    
    while True:
        try:
            prod = await carregar_producao(pid)
            if not prod:
                print(f"❌ Produção {pid} não encontrada no banco")
                return
            
            inicio = prod["inicio"]
            fim = prod["fim"]
            if isinstance(inicio, str):
                inicio = str_para_datetime(inicio)
            if isinstance(fim, str):
                fim = str_para_datetime(fim)
            
            agora_dt = agora()
            
            if agora_dt >= fim:
                print(f"⏰ Produção {pid} já expirou, finalizando...")
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
            
            if msg is None:
                canal = bot.get_channel(prod["canal_id"])
                if canal:
                    try:
                        msg = await canal.fetch_message(prod["msg_id"])
                    except discord.NotFound:
                        print(f"⚠️ Mensagem da produção {pid} não encontrada, recriando...")
                        canal = bot.get_channel(CANAL_REGISTRO_GALPAO_ID)
                        if canal:
                            desc = await gerar_desc_producao(prod)
                            embed = discord.Embed(title="🏭 Produção", description=desc, color=0x34495e)
                            view = None if prod.get("segunda_task_confirmada") else SegundaTaskView(pid)
                            msg = await canal.send(embed=embed, view=view)
                            prod["msg_id"] = msg.id
                            await salvar_producao(pid, prod)
                    except Exception as e:
                        print(f"Erro ao buscar mensagem {pid}: {e}")
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
                if pct_int != ultimo_pct:
                    ultimo_pct = pct_int
                    desc = await gerar_desc_producao(prod, pct, restante)
                    try:
                        await msg.edit(embed=discord.Embed(title="🏭 Produção", description=desc, color=0x34495e))
                    except discord.HTTPException as e:
                        if e.status == 429:
                            await asyncio.sleep(3)
                        else:
                            print(f"Erro ao editar mensagem {pid}: {e}")
        
        except Exception as e:
            print(f"Erro no acompanhar_producao {pid}: {e}")
            import traceback
            traceback.print_exc()
        
        await asyncio.sleep(10)


async def finalizar_producao(pid, msg, prod):
    """Finaliza a produção e registra no banco"""
    
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
        
        async with db.acquire() as conn:
            await conn.execute(
                """INSERT INTO producoes_finalizadas (user_id, capsulas, data, polvora, galpao) 
                   VALUES ($1, $2, $3, $4, $5)""",
                str(prod["autor"]), capsulas_total, agora_db(), polvora_total, f"{galpao} ({qtd_galpoes} galpões)"
            )
            await conn.execute(
                """UPDATE estoque_capsulas SET quantidade = quantidade + $1, ultima_atualizacao = NOW() WHERE id = 1""",
                capsulas_total
            )
            await conn.execute(
                """INSERT INTO entrada_insumos (tipo, quantidade, registrado_por, obs) 
                   VALUES ($1, $2, $3, $4)""",
                "capsulas", capsulas_total, str(prod["autor"]), 
                f"Produção do {galpao} - {qtd_galpoes} galpões - {polvora_total} pólvora"
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
                def fmt_num(valor):
                    return f"{valor:,.0f}".replace(",", ".")
                
                desc += (f"\n\n🔵 **Produção Finalizada**\n\n"
                        f"🧪 Produziu **{fmt_num(capsulas_total)} cápsulas**\n"
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
        
        canal_bau = bot.get_channel(1448561598384963747)
        if canal_bau:
            def fmt_num(valor):
                return f"{valor:,.0f}".replace(",", ".")
            embed_bau = discord.Embed(title="🏭 PRODUÇÃO DE CÁPSULAS FINALIZADA", color=0x2ecc71, timestamp=agora())
            embed_bau.add_field(name="🏭 Galpão", value=galpao, inline=True)
            embed_bau.add_field(name="🏭 Quantidade", value=f"{qtd_galpoes} galpão(ões)", inline=True)
            embed_bau.add_field(name="💊 Cápsulas produzidas", value=f"**{fmt_num(capsulas_total)}** unidades", inline=True)
            embed_bau.add_field(name="📦 Por galpão", value=f"{fmt_num(capsulas_por_galpao)} cápsulas", inline=True)
            embed_bau.add_field(name="💣 Pólvora total", value=f"**{polvora_total}**", inline=True)
            embed_bau.add_field(name="👤 Produzido por", value=f"<@{prod['autor']}>", inline=True)
            await canal_bau.send(embed=embed_bau)
        
        await atualizar_painel_fabricacao()
        print(f"✅ Produção {pid} finalizada com {capsulas_total} cápsulas ({qtd_galpoes} galpões)")
    
    except Exception as e:
        print(f"❌ ERRO ao finalizar produção {pid}: {e}")
        import traceback
        traceback.print_exc()


# =========================================================
# ================= MODAL REGISTRAR CÁPSULAS ==============
# =========================================================

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
        
        def fmt_num(valor):
            return f"{valor:,.0f}".replace(",", ".")
        
        canal_bau = interaction.guild.get_channel(1448561598384963747)
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
        await atualizar_painel_fabricacao()


# =========================================================
# ================= MODAL REGISTRAR EMBALAGENS ============
# =========================================================

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
        
        def fmt_num(valor):
            return f"{valor:,.0f}".replace(",", ".")
        
        canal_bau = interaction.guild.get_channel(1448561598384963747)
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
        await atualizar_painel_fabricacao()


# =========================================================
# ================= MODAL PRODUÇÃO MUNIÇÃO =================
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
            pacotes = int(self.quantidade_pacotes.value.replace(".", "").replace(",", ""))
            if pacotes <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ **Quantidade inválida!**", ephemeral=True)
            return
        
        verificacao = await verificar_insumos_producao(tipo, pacotes)
        
        def fmt_num(valor):
            return f"{valor:,.0f}".replace(",", ".")
        
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
        
        canal_bau = interaction.guild.get_channel(1448561598384963747)
        
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
        await atualizar_painel_fabricacao()


# =========================================================
# ================= VIEW FABRICAÇÃO ========================
# =========================================================

class FabricacaoView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🏭 Galpões Norte", style=discord.ButtonStyle.primary, custom_id="fabricacao_norte")
    async def norte(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with db.acquire() as conn:
            ativo = await conn.fetchval("SELECT 1 FROM producoes WHERE galpao LIKE 'GALPÕES NORTE%' AND CAST(fim AS timestamp) > NOW()")
        if ativo:
            await interaction.response.send_message("⚠️ Galpões Norte já está em produção.", ephemeral=True)
            return
        await interaction.response.send_modal(ProducaoCompletaModal("GALPÕES NORTE", 65))

    @discord.ui.button(label="🏭 Galpões Sul", style=discord.ButtonStyle.secondary, custom_id="fabricacao_sul")
    async def sul(self, interaction: discord.Interaction, button: discord.ui.Button):
        async with db.acquire() as conn:
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
        def fmt_num(valor):
            return f"{valor:,.0f}".replace(",", ".")
        embed = discord.Embed(title="📊 ESTOQUE COMPLETO", color=0x3498db)
        embed.add_field(name="🔫 MUNIÇÕES", value=f"**PT:** {fmt_num(estoque_municoes['PT'])} pacotes ({fmt_num(estoque_municoes['PT'] * 50)} munições)\n**SUB:** {fmt_num(estoque_municoes['SUB'])} pacotes ({fmt_num(estoque_municoes['SUB'] * 50)} munições)", inline=False)
        embed.add_field(name="💊 INSUMOS", value=f"**Cápsulas:** {fmt_num(estoque_insumos['capsulas'])} unidades\n**Embalagens:** {fmt_num(estoque_insumos['embalagens'])} unidades", inline=False)
        embed.add_field(name="📋 TABELA DE CONSUMO", value=("**Por pacote PT:**\n• 25 cápsulas\n• 5 embalagens\n\n**Por pacote SUB:**\n• 65 cápsulas\n• 25 embalagens"), inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

    @discord.ui.button(label="📊 Relatório Produção", style=discord.ButtonStyle.secondary, custom_id="fabricacao_relatorio")
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RelatorioProducaoModal())

    @discord.ui.button(label="🔄 Atualizar Painel", style=discord.ButtonStyle.secondary, custom_id="atualizar_painel_btn", emoji="🔄")
    async def atualizar_painel(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.defer(ephemeral=True)
        await atualizar_painel_fabricacao()
        await interaction.followup.send("✅ Painel atualizado!", ephemeral=True)


# =========================================================
# ================= RELATÓRIO PRODUÇÃO ====================
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

            async with db.acquire() as conn:
                rows = await conn.fetch("SELECT user_id, SUM(capsulas) as total_capsulas, SUM(polvora) as total_polvora FROM producoes_finalizadas WHERE data >= $1 AND data <= $2 GROUP BY user_id ORDER BY total_capsulas DESC", inicio_dt, fim_dt)

            if not rows:
                await interaction.followup.send(f"📭 Nenhuma produção no período **{self.data_inicio.value}** a **{self.data_fim.value}**.", ephemeral=True)
                return

            total_capsulas = sum(r["total_capsulas"] or 0 for r in rows)
            total_polvora = sum(r["total_polvora"] or 0 for r in rows)
            total_capsulas_fmt = f"{total_capsulas:,.0f}".replace(",", ".")
            total_polvora_fmt = f"{total_polvora:,.0f}".replace(",", ".")

            linhas = []
            for r in rows:
                uid = r["user_id"]
                capsulas = int(r["total_capsulas"] or 0)
                polvora = int(r["total_polvora"] or 0)
                capsulas_fmt = f"{capsulas:,.0f}".replace(",", ".")
                polvora_fmt = f"{polvora:,.0f}".replace(",", ".")
                try:
                    user = await bot.fetch_user(int(uid))
                    nome = user.display_name if user else str(uid)
                except:
                    nome = str(uid)
                linhas.append(f"**{nome}** — {capsulas_fmt} cápsulas | 💣 {polvora_fmt} pólvora")

            embed = discord.Embed(title="📊 RELATÓRIO DE PRODUÇÃO DE CÁPSULAS", description=f"📅 **Período:** {self.data_inicio.value} até {self.data_fim.value}\n💰 **Total produzido:** `{total_capsulas_fmt}` cápsulas\n💣 **Total pólvora gasto:** `{total_polvora_fmt}`", color=0x2ecc71)
            embed.add_field(name="🏆 RANKING", value="\n".join(linhas) if linhas else "Nenhum", inline=False)

            canal = interaction.guild.get_channel(1422853066541109338)
            if canal:
                await canal.send(embed=embed)
                await interaction.followup.send(f"✅ Relatório enviado!\n📅 {self.data_inicio.value} a {self.data_fim.value}\n💰 Total: {total_capsulas_fmt} cápsulas\n💣 Pólvora: {total_polvora_fmt}", ephemeral=True)
            else:
                await interaction.followup.send(embed=embed, ephemeral=True)

        except ValueError:
            await interaction.followup.send("❌ **Formato de data inválido!**\nUse o formato: `DD/MM/AAAA`", ephemeral=True)
        except Exception as e:
            print("ERRO RELATORIO:", e)
            await interaction.followup.send("❌ Erro ao gerar relatório.", ephemeral=True)


# =========================================================
# ================= PAINEL FABRICAÇÃO =====================
# =========================================================

async def enviar_painel_fabricacao():
    """Envia o painel de fabricação - DELETA a mensagem antiga e cria nova"""
    
    canal = pegar_canal(CANAL_FABRICACAO_ID)
    
    if not canal:
        print("❌ Canal de fabricação não encontrado")
        return
    
    estoque_municoes = await carregar_estoque()
    estoque_insumos = await carregar_estoque_insumos()
    
    def fmt_num(valor):
        return f"{valor:,.0f}".replace(",", ".")
    
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
        value="• **Galpões Norte:** 65 minutos (3 galpões)\n• **Galpões Sul:** 130 minutos (3 galpões)\n\n💡 Ao clicar, informe:\n   - Quantos galpões (1, 2 ou 3)\n   - Pólvora por galpão",
        inline=False
    )
    
    embed.set_footer(text="Utilize os botões abaixo para gerenciar o estoque")
    
    async for msg in canal.history(limit=10):
        if msg.author == bot.user and msg.embeds:
            if msg.embeds[0].title == "🏭 PAINEL DE FABRICAÇÃO":
                try:
                    await msg.delete()
                    print(f"🗑️ Mensagem antiga deletada")
                except:
                    pass
    
    await canal.send(embed=embed, view=FabricacaoView())
    print("🏭 Novo painel de fabricação enviado")


async def atualizar_painel_fabricacao():
    """Atualiza o painel de fabricação"""
    await enviar_painel_fabricacao()


# =========================================================
# ================= LOOP DE VERIFICAÇÃO ===================
# =========================================================

@tasks.loop(minutes=1)
async def verificar_producoes_orfas():
    """Verifica produções que deveriam estar ativas mas não estão na memória"""
    try:
        async with db.acquire() as conn:
            rows = await conn.fetch("SELECT pid FROM producoes WHERE CAST(fim AS timestamp) > NOW()")
        
        for r in rows:
            pid = r["pid"]
            if pid not in producoes_tasks:
                print(f"🔧 Produção órfã encontrada: {pid}, restaurando...")
                task = bot.loop.create_task(acompanhar_producao(pid))
                producoes_tasks[pid] = task
    except Exception as e:
        print(f"Erro no verificar_producoes_orfas: {e}")


# =========================================================
# ================= COMANDOS DE ESTOQUE ====================
# =========================================================

@bot.command(name="estoque")
async def cmd_ver_estoque(ctx):
    """Ver o estoque completo (munições + insumos)"""
    estoque_municoes = await carregar_estoque()
    estoque_insumos = await carregar_estoque_insumos()
    
    def fmt_num(valor):
        return f"{valor:,.0f}".replace(",", ".")
    
    embed = discord.Embed(title="📦 ESTOQUE COMPLETO", color=0x3498db)
    embed.add_field(name="🔫 MUNIÇÕES", value=f"**PT:** {fmt_num(estoque_municoes['PT'])} pacotes ({fmt_num(estoque_municoes['PT'] * 50)} munições)\n**SUB:** {fmt_num(estoque_municoes['SUB'])} pacotes ({fmt_num(estoque_municoes['SUB'] * 50)} munições)", inline=False)
    embed.add_field(name="💊 INSUMOS", value=f"**Cápsulas:** {fmt_num(estoque_insumos['capsulas'])} unidades\n**Embalagens:** {fmt_num(estoque_insumos['embalagens'])} unidades", inline=False)
    await ctx.send(embed=embed)


@bot.command(name="historico_producao")
async def cmd_historico_producao(ctx, limite: int = 10):
    """Ver histórico de produção de munição"""
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM producao_municao ORDER BY data DESC LIMIT $1", limite)
    
    if not rows:
        await ctx.send("📭 Nenhuma produção registrada ainda.")
        return
    
    def fmt_num(valor):
        return f"{valor:,.0f}".replace(",", ".")
    
    embed = discord.Embed(title="📋 HISTÓRICO DE PRODUÇÃO DE MUNIÇÃO", color=0x2ecc71)
    for row in rows:
        data = row["data"]
        if data.tzinfo is None:
            data = data.replace(tzinfo=BRASIL)
        embed.add_field(name=f"{data.strftime('%d/%m/%Y %H:%M')}", value=f"🔫 **{row['tipo']}** • {fmt_num(row['pacotes'])} pacotes ({fmt_num(row['municoes'])} munições)\n💊 Consumiu: {fmt_num(row['capsulas_consumidas'])} cápsulas + {fmt_num(row['embalagens_consumidas'])} embalagens\n👤 <@{row['produzido_por']}>", inline=False)
    
    await ctx.send(embed=embed)


@bot.command(name="historico_vendas_estoque")
async def cmd_historico_vendas_estoque(ctx, limite: int = 10):
    """Ver histórico de saída de estoque por vendas"""
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM saida_estoque ORDER BY data DESC LIMIT $1", limite)
    
    if not rows:
        await ctx.send("📭 Nenhuma venda registrada ainda.")
        return
    
    def fmt_num(valor):
        return f"{valor:,.0f}".replace(",", ".")
    
    embed = discord.Embed(title="📋 HISTÓRICO DE VENDAS (ESTOQUE)", color=0xe74c3c)
    for row in rows:
        data = row["data"]
        if data.tzinfo is None:
            data = data.replace(tzinfo=BRASIL)
        embed.add_field(name=f"Pedido #{row['pedido_numero']} - {data.strftime('%d/%m/%Y %H:%M')}", value=f"🔫 **{row['tipo']}** • {fmt_num(row['pacotes'])} pacotes\n👤 Retirado por: <@{row['retirado_por']}>", inline=False)
    
    await ctx.send(embed=embed)
    
# =========================================================
# ============= FUNÇÃO RESTAURAR PRODUÇÕES ================
# =========================================================

async def restaurar_producoes():
    """Restaura produções ativas após reinício - VERSÃO COMPLETA E CORRIGIDA"""
    try:
        print("🔄 Iniciando restauração de produções...")
        
        async with db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT pid, galpao, inicio, fim, msg_id, canal_id, autor, obs, 
                       polvora, qtd_galpoes, polvora_por_galpao, 
                       segunda_task_user, segunda_task_time
                FROM producoes 
                WHERE CAST(fim AS timestamp) > NOW()
                """
            )
        
        if not rows:
            print("📭 Nenhuma produção ativa para restaurar")
            return
        
        print(f"🏭 Encontradas {len(rows)} produções ativas para restaurar")
        
        for r in rows:
            pid = r["pid"]
            
            # Verifica se já está sendo acompanhada
            if pid in producoes_tasks:
                print(f"⏩ Produção {pid} já está sendo acompanhada")
                continue
            
            # Reconstrói os dados da produção
            dados = {
                "galpao": r["galpao"],
                "autor": int(r["autor"]),
                "inicio": r["inicio"],
                "fim": r["fim"],
                "obs": r["obs"],
                "msg_id": int(r["msg_id"]),
                "canal_id": int(r["canal_id"]),
                "polvora": r["polvora"] or 400,
                "qtd_galpoes": r.get("qtd_galpoes", 1) if r.get("qtd_galpoes") else 1,
                "polvora_por_galpao": r.get("polvora_por_galpao", 400) if r.get("polvora_por_galpao") else 400
            }
            
            if r["segunda_task_user"]:
                dados["segunda_task_confirmada"] = {
                    "user": int(r["segunda_task_user"]),
                    "time": r["segunda_task_time"]
                }
            
            # Salva na memória (cache)
            await salvar_producao(pid, dados)
            
            # VERIFICA SE A MENSAGEM EXISTE NO DISCORD
            canal = bot.get_channel(int(r["canal_id"]))
            msg_existe = False
            
            if canal:
                try:
                    msg = await canal.fetch_message(int(r["msg_id"]))
                    msg_existe = True
                    print(f"✅ Mensagem encontrada para produção {pid}")
                    
                    # Se a segunda task já foi confirmada, remove o botão
                    if r["segunda_task_user"]:
                        try:
                            await msg.edit(view=None)
                            print(f"✅ Botão removido para produção {pid} (segunda task já confirmada)")
                        except:
                            pass
                except discord.NotFound:
                    print(f"⚠️ Mensagem da produção {pid} não encontrada no Discord!")
                except Exception as e:
                    print(f"❌ Erro ao buscar mensagem {pid}: {e}")
            
            # SE A MENSAGEM NÃO EXISTE, RECRIA
            if not msg_existe:
                print(f"🔄 Recriando mensagem para produção {pid}...")
                canal = bot.get_channel(CANAL_REGISTRO_GALPAO_ID)
                if canal:
                    try:
                        # Gera descrição inicial
                        desc = f"**Galpão:** {r['galpao']}\n"
                        desc += f"**Iniciado por:** <@{r['autor']}>\n"
                        desc += f"Início: <t:{int(datetime.fromisoformat(r['inicio']).timestamp())}:t>\n"
                        desc += f"Término: <t:{int(datetime.fromisoformat(r['fim']).timestamp())}:t>\n\n"
                        desc += f"⏳ **Restante:** Aguardando atualização...\n{barra(0)}"
                        
                        embed = discord.Embed(title=f"🏭 Produção", description=desc, color=0x3498db)
                        view = SegundaTaskView(pid) if not r["segunda_task_user"] else None
                        
                        msg = await canal.send(embed=embed, view=view)
                        
                        # Atualiza o msg_id no banco
                        dados["msg_id"] = msg.id
                        await salvar_producao(pid, dados)
                        print(f"✅ Mensagem recriada para produção {pid}")
                    except Exception as e:
                        print(f"❌ Erro ao recriar mensagem {pid}: {e}")
            
            # CRIA A TASK DE ACOMPANHAMENTO
            task = asyncio.create_task(acompanhar_producao(pid))
            producoes_tasks[pid] = task
            print(f"✅ Task criada para produção {pid} - {r['galpao']}")
            
    except Exception as e:
        print(f"❌ ERRO CRÍTICO ao restaurar produções: {e}")
        import traceback
        traceback.print_exc()

# =========================================================
# ======================== POLVORAS ========================
# =========================================================

# ================= BANCO =================

async def salvar_polvora_db(user_id, vendedor, qtd, valor):

    async with db.acquire() as conn:

        await conn.execute(
            """
            INSERT INTO polvoras (user_id, vendedor, quantidade, valor, data)
            VALUES ($1,$2,$3,$4,$5)
            """,
            str(user_id),
            vendedor,
            qtd,
            valor,
            agora().isoformat()
        )


async def carregar_polvoras_db():

    async with db.acquire() as conn:

        rows = await conn.fetch(
            "SELECT * FROM polvoras"
        )

        return rows


async def limpar_polvoras_db():

    async with db.acquire() as conn:

        await conn.execute(
            "DELETE FROM polvoras"
        )


# =========================================================
# ================= MODAL PÓLVORA ==========================
# =========================================================

class PolvoraModal(discord.ui.Modal, title="Registro de Compra de Pólvora"):

    vendedor = discord.ui.TextInput(
        label="Vendedor (preencha o nome)",
        placeholder="Digite o nome do vendedor"
    )

    quantidade = discord.ui.TextInput(
        label="Quantidade",
        placeholder="Ex: 100"
    )

    async def on_submit(self, interaction: discord.Interaction):

        try:
            qtd = int(self.quantidade.value)
        except:
            await interaction.response.send_message(
                "Quantidade inválida.",
                ephemeral=True
            )
            return

        valor = qtd * 80

        valor_formatado = (
            f"{valor:,.2f}"
            .replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )

        await salvar_polvora_db(
            interaction.user.id,
            self.vendedor.value,
            qtd,
            valor
        )

        canal = interaction.guild.get_channel(CANAL_REGISTRO_POLVORA_ID)

        embed = discord.Embed(
            title="🧨 Registro de Pólvora",
            color=0xe67e22
        )

        embed.add_field(
            name="Vendedor",
            value=self.vendedor.value,
            inline=False
        )

        embed.add_field(
            name="Comprado por",
            value=interaction.user.mention,
            inline=False
        )

        embed.add_field(
            name="Quantidade",
            value=str(qtd),
            inline=True
        )

        embed.add_field(
            name="Valor",
            value=f"**R$ {valor_formatado}**",
            inline=True
        )

        await canal.send(embed=embed)

        await interaction.response.send_message(
            "Registro feito com sucesso!",
            ephemeral=True
        )


# =========================================================
# ================= CONFIRMAR PAGAMENTO ====================
# =========================================================

class ConfirmarPagamentoView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Confirmar pagamento",
        style=discord.ButtonStyle.success,
        custom_id="confirmar_pagamento"
    )
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.message.edit(
            content=interaction.message.content + "\n\n✅ **PAGO**",
            view=None
        )

        await responder_interacao(interaction, defer=True)


# =========================================================
# ================= VIEW PÓLVORA ===========================
# =========================================================

class PolvoraView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Registrar Compra de Pólvora",
        style=discord.ButtonStyle.primary,
        custom_id="polvora_btn"
    )
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(
            PolvoraModal()
        )


# =========================================================
# ================= RELATÓRIO SEMANAL =====================
# =========================================================

@tasks.loop(minutes=1)
async def relatorio_semanal_polvoras():

    agora_br = agora()

    if (
        agora_br.weekday() != 6
        or agora_br.hour != 23
        or agora_br.minute != 59
    ):
        return

    dados = await carregar_polvoras_db()

    inicio_semana = agora_br - timedelta(days=6)
    inicio_semana = inicio_semana.replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0
    )

    fim_semana = agora_br.replace(
        hour=23,
        minute=59,
        second=59
    )

    resumo = {}

    for item in dados:

        data_item = datetime.fromisoformat(item["data"])

        if inicio_semana <= data_item <= fim_semana:

            resumo.setdefault(item["user_id"], 0)
            resumo[item["user_id"]] += item["valor"]

    if not resumo:
        return

    canal = pegar_canal(CANAL_REGISTRO_POLVORA_ID)

    for user_id, total in resumo.items():

        user = await pegar_usuario(int(user_id))

        valor_formatado = (
            f"{total:,.2f}"
            .replace(",", "X")
            .replace(".", ",")
            .replace("X", ".")
        )

        await canal.send(
            content=(
                f"🧨 **RELATÓRIO SEMANAL DE PÓLVORA**\n"
                f"📅 Período: {inicio_semana.strftime('%d/%m')} até {fim_semana.strftime('%d/%m')}\n\n"
                f"👤 Comprado por: {user.mention}\n"
                f"💰 Valor a ressarcir: **R$ {valor_formatado}**"
            ),
            view=ConfirmarPagamentoView()
        )

# =========================================================
# ================= PAINEL PÓLVORA ========================
# =========================================================

async def enviar_painel_polvoras():

    canal = bot.get_channel(CANAL_CALCULO_POLVORA_ID)

    if not canal:
        print("❌ Canal de pólvora não encontrado")
        return

    embed = discord.Embed(
        title="💣 Registro de Pólvora",
        description="Clique no botão para registrar.",
        color=0xe67e22
    )

    await enviar_ou_atualizar_painel(
        "painel_polvora",
        CANAL_CALCULO_POLVORA_ID,
        embed,
        PolvoraView()
    )

    print("💣 Painel de pólvora verificado/atualizado")



# =========================================================
# ======================== LAVAGEM =========================
# =========================================================

lavagens_pendentes = {}


@tasks.loop(minutes=15)
async def limpar_lavagens_pendentes():
    lavagens_pendentes.clear()


def formatar_real(valor: int) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# =========================================================
# ================= BANCO (POSTGRES) ======================
# =========================================================

async def salvar_lavagem_db(user_id, valor_sujo, taxa, valor_retorno):

    async with db.acquire() as conn:

        await conn.execute(
            """
            INSERT INTO lavagens (user_id, valor, taxa, liquido, data)
            VALUES ($1,$2,$3,$4,$5)
            """,
            str(user_id),
            valor_sujo,
            taxa,
            valor_retorno,
            agora().replace(tzinfo=None)
        )


async def carregar_lavagens_db():

    async with db.acquire() as conn:

        rows = await conn.fetch(
            "SELECT * FROM lavagens"
        )

        return rows


async def limpar_lavagens_db():

    async with db.acquire() as conn:

        await conn.execute(
            "DELETE FROM lavagens"
        )


# =========================================================
# ================= MODAL LAVAGEM ==========================
# =========================================================

class LavagemModal(discord.ui.Modal, title="Iniciar Lavagem"):

    valor = discord.ui.TextInput(
        label="Valor do dinheiro sujo"
    )

    async def on_submit(self, interaction: discord.Interaction):

        await responder_interacao(interaction, defer=True)

        try:
            valor_sujo = int(
                self.valor.value
                .replace(".", "")
                .replace(",", "")
            )
        except:
            await interaction.followup.send(
                "Valor inválido.",
                ephemeral=True
            )
            return

        taxa = 20
        valor_retorno = int(valor_sujo * 0.8)

        msg_info = await interaction.channel.send(
            f"{interaction.user.mention} envie agora o PRINT da tela."
        )

        lavagens_pendentes[interaction.user.id] = {
            "sujo": valor_sujo,
            "retorno": valor_retorno,
            "taxa": taxa,
            "msg_info": msg_info
        }


# =========================================================
# =========== CAPTURA AUTOMÁTICA DO PRINT =================
# =========================================================

@bot.event
async def on_message(message: discord.Message):

    if message.author.bot:
        return

    # ================= LAVAGEM =================

    if message.channel.id == CANAL_INICIAR_LAVAGEM_ID:

        if message.attachments and message.author.id in lavagens_pendentes:

            dados_temp = lavagens_pendentes.pop(message.author.id)

            valor_sujo = dados_temp["sujo"]
            valor_retorno = dados_temp["retorno"]
            taxa = dados_temp["taxa"]

            canal_destino = pegar_canal(CANAL_LAVAGEM_MEMBROS_ID)

            arquivo = await message.attachments[0].to_file()

            try:
                await message.delete()
            except:
                pass

            try:
                await dados_temp["msg_info"].delete()
            except:
                pass

            await salvar_lavagem_db(
                message.author.id,
                valor_sujo,
                taxa,
                valor_retorno
            )

            embed = discord.Embed(
                title="🧼 Nova Lavagem",
                color=0x1abc9c
            )

            embed.add_field(
                name="Membro",
                value=message.author.mention,
                inline=False
            )

            embed.add_field(
                name="Valor sujo",
                value=formatar_real(valor_sujo),
                inline=True
            )

            embed.add_field(
                name="Valor a repassar (80%)",
                value=formatar_real(valor_retorno),
                inline=True
            )

            embed.set_image(
                url=f"attachment://{arquivo.filename}"
            )

            await canal_destino.send(
                embed=embed,
                file=arquivo
            )

            return

    await bot.process_commands(message)

# =========================================================
# ================= PERMISSÕES ============================
# =========================================================

def pode_gerenciar_lavagem(member: discord.Member):

    cargos_permitidos = [
        CARGO_GERENTE_ID,
        CARGO_01_ID,
        CARGO_02_ID,
        CARGO_GERENTE_GERAL_ID
    ]

    return any(role.id in cargos_permitidos for role in member.roles)


# =========================================================
# ================= VIEW LAVAGEM ==========================
# =========================================================

class LavagemView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Iniciar Lavagem",
        style=discord.ButtonStyle.primary,
        custom_id="lavagem_iniciar"
    )
    async def iniciar(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(
            LavagemModal()
        )


    @discord.ui.button(
        label="🧹 Limpar Sala",
        style=discord.ButtonStyle.danger,
        custom_id="lavagem_limpar"
    )
    async def limpar(self, interaction: discord.Interaction, button: discord.ui.Button):

        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message(
                "Você não tem permissão.",
                ephemeral=True
            )
            return

        canal = interaction.guild.get_channel(CANAL_LAVAGEM_MEMBROS_ID)

        async for msg in canal.history(limit=200):
            try:
                await msg.delete()
            except:
                pass

        await limpar_lavagens_db()

        await interaction.response.send_message(
            "Sala limpa!",
            ephemeral=True
        )


    @discord.ui.button(
        label="📊 Gerar Relatório",
        style=discord.ButtonStyle.success,
        custom_id="lavagem_relatorio"
    )
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):

        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message(
                "Você não tem permissão.",
                ephemeral=True
            )
            return

        dados = await carregar_lavagens_db()

        canal = interaction.guild.get_channel(CANAL_RELATORIO_LAVAGEM_ID)

        for item in dados:

            user = await bot.fetch_user(int(item["user_id"]))

            await canal.send(
                f"{user.mention} - Valor a repassar: {formatar_real(item['liquido'])} "
                f"- Valor sujo: {formatar_real(item['valor'])}"
            )

        await interaction.response.send_message(
            "Relatório enviado!",
            ephemeral=True
        )


    @discord.ui.button(
        label="📩 Avisar TODOS no DM",
        style=discord.ButtonStyle.primary,
        custom_id="lavagem_dm"
    )
    async def avisar_todos(self, interaction: discord.Interaction, button: discord.ui.Button):

        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message(
                "Você não tem permissão.",
                ephemeral=True
            )
            return

        dados = await carregar_lavagens_db()

        enviados = 0
        falhas = 0

        for item in dados:

            try:
                user = await bot.fetch_user(int(item["user_id"]))

                await user.send(
                    f"🧼 **Seu dinheiro foi lavado com sucesso!**\n\n"
                    f"💵 Dinheiro informado: {formatar_real(item['valor'])}\n"
                    f"💰 Valor repassado: {formatar_real(item['liquido'])}"
                )

                enviados += 1

            except:
                falhas += 1

        await interaction.response.send_message(
            f"DM enviada para {enviados} membros.\nFalhas: {falhas}",
            ephemeral=True
        )

# =========================================================
# ================= PAINEL LAVAGEM ========================
# =========================================================

async def enviar_painel_lavagem():

    canal = bot.get_channel(CANAL_INICIAR_LAVAGEM_ID)

    if not canal:
        print("❌ Canal de lavagem não encontrado")
        return

    embed = discord.Embed(
        title="🧼 Lavagem de Dinheiro",
        description="Clique para iniciar lavagem.",
        color=0x27ae60
    )

    await enviar_ou_atualizar_painel(
        "painel_lavagem",
        CANAL_INICIAR_LAVAGEM_ID,
        embed,
        LavagemView()
    )

    print("🧼 Painel de lavagem verificado/atualizado")

# =========================================================
# ========================= LIVES ==========================
# =========================================================

ADM_ID = 467673818375389194


# =========================================================
# ================= BANCO =================
# =========================================================

async def carregar_lives():
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM lives")
    
    lives = {}
    for r in rows:
        uid = r["user_id"]
        if uid not in lives:
            lives[uid] = []
        lives[uid].append({
            "link": r["link"],
            "divulgado": r["divulgado"]
        })
    return lives


async def salvar_live(user_id, link):
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO lives (user_id, link, divulgado) VALUES ($1, $2, false)",
            str(user_id), link
        )


async def atualizar_divulgado(link, valor):
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE lives SET divulgado=$1 WHERE link=$2",
            valor, link
        )


async def remover_live_db(user_id):
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM lives WHERE user_id=$1", str(user_id))


# =========================================================
# ================= DETECTAR PLATAFORMA ===================
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
    link = link.replace("https://", "")
    link = link.replace("http://", "")
    link = link.replace("www.", "")
    link = link.split("?")[0]
    link = link.rstrip("/")
    
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
        user = partes[1].replace("@", "") if len(partes) > 1 else None
        return user
    
    return None


# =========================================================
# ================= CHECAR KICK ===========================
# =========================================================

async def checar_kick(canal):
    """Verifica se um canal da Kick está ao vivo"""
    try:
        url_api = f"https://kick.com/api/v2/channels/{canal}"
        
        async with http_session.get(url_api, timeout=15) as r:
            if r.status == 200:
                data = await r.json()
                
                if data.get("livestream"):
                    livestream = data["livestream"]
                    titulo = livestream.get("session_title", f"Live na Kick - {canal}")
                    
                    thumbnail = livestream.get("thumbnail", {})
                    thumb_url = thumbnail.get("url") if thumbnail else None
                    
                    categoria = data.get("category", {})
                    jogo = categoria.get("name") if categoria else None
                    
                    print(f"✅ Kick API: {canal} está AO VIVO! Título: {titulo[:50] if titulo else 'None'}")
                    return True, titulo, jogo, thumb_url
                else:
                    print(f"📴 Kick API: {canal} está OFFLINE")
                    return False, None, None, None
        
        url_page = f"https://kick.com/{canal}"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
        }
        
        async with http_session.get(url_page, headers=headers, timeout=15) as r:
            if r.status == 200:
                html = await r.text()
                
                if "isLive:true" in html or '"livestream":{' in html:
                    import re
                    titulo_match = re.search(r'"sessionTitle":"([^"]+)"', html)
                    titulo = titulo_match.group(1) if titulo_match else f"Live na Kick - {canal}"
                    
                    print(f"✅ Kick HTML: {canal} está AO VIVO! Título: {titulo[:50]}")
                    return True, titulo, None, None
        
        return False, None, None, None
        
    except asyncio.TimeoutError:
        print(f"⏰ Timeout ao verificar Kick: {canal}")
        return False, None, None, None
    except Exception as e:
        print(f"❌ Erro ao verificar Kick para {canal}: {e}")
        return False, None, None, None


# =========================================================
# ================= CHECAR TWITCH =========================
# =========================================================

async def checar_twitch(canal):
    try:
        token = await obter_token_twitch()
        headers = {
            "Client-ID": TWITCH_CLIENT_ID,
            "Authorization": f"Bearer {token}"
        }
        url = f"https://api.twitch.tv/helix/streams?user_login={canal}"
        
        async with http_session.get(url, headers=headers, timeout=10) as r:
            data = await r.json()
            if data.get("data"):
                info = data["data"][0]
                thumbnail = info["thumbnail_url"].replace("{width}", "1280").replace("{height}", "720")
                return True, info.get("title"), info.get("game_name"), thumbnail
        return False, None, None, None
    except Exception as e:
        print(f"Erro Twitch API: {e}")
        return False, None, None, None


# =========================================================
# ================= CHECAR TIKTOK =========================
# =========================================================

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
            html = await response.text()
        
        import re
        if re.search(r'"isLive":\s*true', html, re.IGNORECASE):
            titulo_match = re.search(r'"title":"([^"]+)"', html)
            titulo = titulo_match.group(1) if titulo_match else f"Live no TikTok - @{username}"
            return True, titulo, "TikTok", None
        
        return False, None, None, None
    except Exception as e:
        print(f"Erro TikTok: {e}")
        return False, None, None, None


# =========================================================
# ================= DIVULGAR LIVE =========================
# =========================================================

async def divulgar_live(user_id, link, titulo, jogo, thumbnail):
    """Divulga a live no canal designado"""
    try:
        canal = bot.get_channel(CANAL_DIVULGACAO_LIVE_ID)
        
        if not canal:
            print(f"❌ Canal de divulgação NÃO ENCONTRADO! ID: {CANAL_DIVULGACAO_LIVE_ID}")
            return False

        user = await pegar_usuario(int(user_id))
        if not user:
            print(f"❌ Usuário {user_id} não encontrado")
            return False

        plataforma = detectar_plataforma(link)
        
        print(f"📢 DIVULGANDO LIVE - Plataforma: {plataforma}, Link: {link}")

        cores = {
            "twitch": 0x9146FF,
            "kick": 0x53FC18,
            "tiktok": 0x000000
        }

        nomes = {
            "twitch": "Twitch",
            "kick": "Kick",
            "tiktok": "TikTok"
        }

        icones = {
            "twitch": "🟣",
            "kick": "🟢",
            "tiktok": "📱"
        }

        embed = discord.Embed(
            title=f"{icones.get(plataforma, '🔴')} LIVE AO VIVO!",
            color=cores.get(plataforma, 0xff0000)
        )

        embed.description = (
            f"👤 **Streamer:** {user.mention}\n"
            f"📺 **Plataforma:** {nomes.get(plataforma, plataforma.upper())}\n"
        )
        
        if jogo and jogo != "TikTok" and jogo != "None":
            embed.description += f"🎮 **Jogo:** {jogo}\n"
            
        embed.description += f"📝 **Título:** {titulo or 'Sem título'}\n\n"
        embed.description += f"🔗 **Assistir:** {link}"

        if thumbnail and thumbnail != "None":
            embed.set_image(url=thumbnail)
        elif plataforma == "kick":
            embed.set_thumbnail(url="https://kick.com/favicon.ico")

        embed.set_footer(text=f"Live detectada • {agora().strftime('%d/%m/%Y %H:%M:%S')}")

        if plataforma == "tiktok":
            await canal.send(
                content=f"🔴 {user.mention} está ao vivo no TikTok!",
                embed=embed,
                allowed_mentions=discord.AllowedMentions(users=True)
            )
        else:
            await canal.send(
                content="@everyone 🔴 **LIVE INICIADA!**",
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=True)
            )

        print(f"✅ Live divulgada com sucesso: {plataforma} - {user.name}")
        return True

    except Exception as e:
        print(f"❌ ERRO ao divulgar live: {e}")
        import traceback
        traceback.print_exc()
        return False


# =========================================================
# ================= LOOP DE VERIFICAÇÃO ===================
# =========================================================

@tasks.loop(minutes=2)
async def verificar_lives():
    """Verifica todas as lives cadastradas (Twitch, Kick e TikTok)"""
    
    print("🔄 Verificando lives...")
    
    try:
        lives = await carregar_lives()
        
        if not lives:
            print("📭 Nenhuma live cadastrada")
            return
        
        for user_id, lista_lives in lives.items():
            for data in lista_lives:
                link = data.get("link", "")
                divulgado = data.get("divulgado", False)
                
                if not link:
                    continue
                
                plataforma = detectar_plataforma(link)
                canal_name = extrair_canal(link)
                
                if not plataforma or not canal_name:
                    print(f"⚠️ Link inválido: {link}")
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
                        ao_vivo, titulo, jogo, thumbnail = await checar_kick(canal_name)
                        print(f"🟢 Kick {canal_name}: ao_vivo={ao_vivo}, titulo={titulo[:50] if titulo else 'None'}")
                    
                    elif plataforma == "tiktok":
                        ao_vivo, titulo, jogo, thumbnail = await checar_tiktok(canal_name)
                        print(f"📱 TikTok {canal_name}: ao_vivo={ao_vivo}")
                    
                except Exception as e:
                    print(f"Erro ao verificar {plataforma}/{canal_name}: {e}")
                    continue
                
                if not ao_vivo and divulgado:
                    await atualizar_divulgado(link, False)
                    print(f"📴 Live encerrada: {plataforma}/{canal_name}")
                
                if ao_vivo and not divulgado:
                    print(f"🔴🔴🔴 LIVE DETECTADA ({plataforma.upper()}): {canal_name}")
                    print(f"   → User ID: {user_id}")
                    print(f"   → Link: {link}")
                    print(f"   → Título: {titulo}")
                    
                    resultado = await divulgar_live(user_id, link, titulo, jogo, thumbnail)
                    
                    if resultado:
                        await atualizar_divulgado(link, True)
                        print(f"✅ Live divulgada: {plataforma}/{canal_name}")
                    else:
                        print(f"❌ FALHA ao divulgar live: {plataforma}/{canal_name}")
                    
    except Exception as e:
        print(f"❌ Erro no loop de lives: {e}")
        import traceback
        traceback.print_exc()


# =========================================================
# ================= CADASTRO DE LIVE ======================
# =========================================================

class CadastrarLiveModal(discord.ui.Modal, title="🎥 Cadastrar Live"):

    link = discord.ui.TextInput(
        label="Cole o link da sua live",
        placeholder="https://kick.com/seucanal ou https://twitch.tv/seucanal"
    )

    async def on_submit(self, interaction: discord.Interaction):
        lives = await carregar_lives()
        novo_link = self.link.value.strip().lower()
        novo_link = novo_link.split("?")[0].rstrip("/")

        plataforma = detectar_plataforma(novo_link)
        novo_canal = extrair_canal(novo_link)

        if not plataforma or not novo_canal:
            await interaction.response.send_message(
                "❌ Link inválido. Use links da Twitch, Kick ou TikTok.",
                ephemeral=True
            )
            return

        for uid, lista_lives in lives.items():
            if str(uid) != str(interaction.user.id):
                continue
            for live in lista_lives:
                link_existente = live.get("link", "")
                if extrair_canal(link_existente) == novo_canal and detectar_plataforma(link_existente) == plataforma:
                    await interaction.response.send_message(
                        f"❌ Você já cadastrou o canal **{novo_canal}** na plataforma **{plataforma}**!",
                        ephemeral=True
                    )
                    return

        await salvar_live(interaction.user.id, novo_link)

        embed = discord.Embed(
            title="✅ Live cadastrada!",
            description=f"{interaction.user.mention}\n📺 **{plataforma.upper()}** - {novo_link}",
            color=0x2ecc71
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)


# =========================================================
# ================= VIEW CADASTRO =========================
# =========================================================

class CadastrarLiveView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🎥 Cadastrar minha Live",
        style=discord.ButtonStyle.primary,
        custom_id="cadastrar_live_btn"
    )
    async def cadastrar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(CadastrarLiveModal())


# =========================================================
# ================= REMOVER LIVE ==========================
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
        
        # Remover do banco
        await remover_live_db(self.user_id)
        
        # Enviar confirmação
        await interaction.followup.send(
            f"✅ **Lives removidas com sucesso!**\nUsuário: {self.nome}",
            ephemeral=True
        )
        
        # Tentar deletar a mensagem original
        try:
            await self.message.delete()
        except:
            pass
        
        # Atualizar painéis
        await enviar_painel_lives()
        await enviar_painel_admin_lives()
    
    @discord.ui.button(label="❌ Cancelar", style=discord.ButtonStyle.secondary, emoji="❌")
    async def cancelar(self, interaction: discord.Interaction, button):
        if interaction.user.id != ADM_ID:
            await interaction.response.send_message("❌ Apenas ADM.", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)
        
        await interaction.followup.send("❌ Operação cancelada.", ephemeral=True)
        
        # Tentar deletar a mensagem original
        try:
            await self.message.delete()
        except:
            pass


class RemoverLiveSelect(discord.ui.Select):
    def __init__(self, lives):
        options = []
        for uid in lives.keys():
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
        
        # Guardar a mensagem original
        original_message = interaction.message
        
        view = ConfirmarRemoverView(user_id, nome, original_message)
        await interaction.response.edit_message(
            content=f"⚠️ **Remover todas as lives de {nome}?**\nEsta ação é irreversível!",
            view=view
        )


class GerenciarLivesView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)

    @discord.ui.button(label="📋 Ver Lives", style=discord.ButtonStyle.secondary, emoji="📋")
    async def ver(self, interaction: discord.Interaction, button):
        if interaction.user.id != ADM_ID:
            await interaction.response.send_message("❌ Apenas ADM.", ephemeral=True)
            return
        
        await interaction.response.defer()
        
        lives = await carregar_lives()
        
        if not lives:
            await interaction.followup.send("📭 Nenhuma live cadastrada.", ephemeral=True)
            return
        
        texto = "**📡 LIVES CADASTRADAS:**\n\n"
        for uid, lista_lives in lives.items():
            user = bot.get_user(int(uid))
            nome = user.display_name if user else uid
            texto += f"👤 **{nome}** (ID: {uid})\n"
            for live in lista_lives:
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
        
        lives = await carregar_lives()
        
        if not lives:
            await interaction.response.send_message("📭 Nenhuma live cadastrada para remover.", ephemeral=True)
            return
        
        view = discord.ui.View(timeout=60)
        view.add_item(RemoverLiveSelect(lives))
        view.add_item(FecharButtonRemover())
        
        await interaction.response.send_message(
            "📋 **Selecione o usuário para remover as lives:**",
            view=view,
            ephemeral=True
        )


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
            view=view,
            ephemeral=True
        )

# =========================================================
# ================= PAINÉIS ===============================
# =========================================================

async def enviar_painel_admin_lives():
    embed = discord.Embed(
        title="⚙️ Painel ADM - Lives",
        description="Gerencie todas as lives cadastradas.",
        color=0xe74c3c
    )
    await enviar_ou_atualizar_painel("painel_lives_admin", CANAL_CADASTRO_LIVE_ID, embed, PainelLivesAdmin())


async def enviar_painel_lives():
    canal = bot.get_channel(CANAL_CADASTRO_LIVE_ID)
    if not canal:
        print("❌ Canal cadastro live não encontrado")
        return
    
    embed = discord.Embed(
        title="🎥 CADASTRO DE LIVE",
        description="**Clique no botão abaixo para cadastrar sua live!**\n\n"
                   "📌 **Plataformas suportadas:**\n"
                   "• Twitch\n"
                   "• Kick\n"
                   "• TikTok\n\n"
                   "⚠️ **Importante:**\n"
                   "• Cadastre apenas suas próprias lives\n"
                   "• O link será verificado automaticamente\n"
                   "• Quando você iniciar uma live, será divulgada automaticamente!",
        color=0x9146FF
    )
    
    await enviar_ou_atualizar_painel("painel_lives", CANAL_CADASTRO_LIVE_ID, embed, CadastrarLiveView())
    print("🎥 Painel de cadastro de live verificado/atualizado")


# =========================================================
# ================= COMANDOS ==============================
# =========================================================

@bot.command(name="testar_kick")
async def testar_kick(ctx, canal: str = None):
    """Testa a verificação de live da Kick"""
    if not canal:
        await ctx.send("❌ Use: !testar_kick <nome_do_canal>\nEx: !testar_kick alanzoka")
        return
    
    await ctx.send(f"🔍 Verificando canal Kick: **{canal}**...")
    
    ao_vivo, titulo, jogo, thumbnail = await checar_kick(canal)
    
    if ao_vivo:
        embed = discord.Embed(
            title="✅ ESTÁ AO VIVO!",
            description=f"**Canal:** {canal}\n**Título:** {titulo}\n**Jogo:** {jogo or 'N/A'}",
            color=0x2ecc71
        )
        if thumbnail:
            embed.set_image(url=thumbnail)
        await ctx.send(embed=embed)
    else:
        await ctx.send(f"❌ O canal **{canal}** NÃO está ao vivo no momento.")


@bot.command(name="testar_live")
async def testar_live_cmd(ctx, plataforma: str = None, canal: str = None):
    """Testa se uma live está sendo detectada corretamente"""
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
    """Lista todas as lives cadastradas"""
    lives = await carregar_lives()
    if not lives:
        await ctx.send("📭 Nenhuma live cadastrada.")
        return
    
    embed = discord.Embed(title="📡 LIVES CADASTRADAS", color=0x9146FF)
    for user_id, lista_lives in lives.items():
        user = await pegar_usuario(int(user_id))
        nome = user.display_name if user else f"ID: {user_id}"
        for live in lista_lives:
            link = live.get("link", "Sem link")
            divulgado = "✅ Divulgado" if live.get("divulgado") else "⏳ Aguardando"
            plataforma = detectar_plataforma(link)
            embed.add_field(
                name=f"👤 {nome}",
                value=f"📺 {plataforma.upper()}\n🔗 {link}\n📌 {divulgado}",
                inline=False
            )
    await ctx.send(embed=embed)


@bot.command(name="forcar_verificacao")
async def forcar_verificacao_lives(ctx):
    """Força a verificação imediata de todas as lives (ADM apenas)"""
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    await ctx.send("🔄 **Forçando verificação de todas as lives...**")
    await verificar_lives()
    await ctx.send("✅ **Verificação concluída!** Confira o canal de divulgação.")


@bot.command(name="remover_live")
async def cmd_remover_live(ctx, user_id: int = None):
    """Remove todas as lives de um usuário (ADM apenas)"""
    
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    if not user_id:
        await ctx.send("❌ Use: `!remover_live ID_DO_USUARIO`")
        return
    
    lives = await carregar_lives()
    
    if str(user_id) not in lives:
        await ctx.send(f"❌ Usuário ID `{user_id}` não possui lives cadastradas.")
        return
    
    user = await pegar_usuario(user_id)
    nome = user.display_name if user else str(user_id)
    
    await remover_live_db(str(user_id))
    
    await ctx.send(f"✅ **Todas as lives de {nome} foram removidas com sucesso!**")
    
    await enviar_painel_lives()
    await enviar_painel_admin_lives()
    
# =========================================================
# ========== FUNÇÃO GLOBAL DE PAINEL (OBRIGATÓRIA) =========
# =========================================================

async def enviar_ou_atualizar_painel(nome, canal_id, embed, view):

    canal = bot.get_channel(canal_id)

    if not canal:
        print(f"❌ Canal não encontrado para painel: {nome}")
        return

    async with db.acquire() as conn:

        row = await conn.fetchrow(
            "SELECT mensagem_id, canal_id FROM paineis WHERE nome=$1",
            nome
        )

        if row:

            try:

                canal_salvo = bot.get_channel(int(row["canal_id"])) or canal

                msg = await canal_salvo.fetch_message(
                    int(row["mensagem_id"])
                )

                await msg.edit(
                    embed=embed,
                    view=view
                )

                return

            except Exception as e:

                print(f"⚠️ Erro ao atualizar painel {nome}:", e)

        # se não existir ou falhar → cria novo

        msg = await canal.send(
            embed=embed,
            view=view
        )

        await conn.execute(
            """
            INSERT INTO paineis (nome, canal_id, mensagem_id)
            VALUES ($1,$2,$3)
            ON CONFLICT (nome)
            DO UPDATE SET canal_id=$2, mensagem_id=$3
            """,
            nome,
            str(canal_id),
            str(msg.id)
        )

# =========================================================
# ======================== AÇÕES ==========================
# =========================================================

ACOES_SEMANA = {
    # Ações com limite
    "Joalheria": 5,
    "Banco Fleeca": 4,
    "Banco de Paleto": 1,
    "Banco Central": 1,
    "Nióbio": 1,
    
    # Ações sem limite (None)
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
    
    # 🚁 HELICRASH (Ilimitado - horários específicos)
    "🚁 Helicrash (13h)": None,
    "🚁 Helicrash (15h)": None,
    "🚁 Helicrash (22h)": None,
    "🚁 Helicrash (02h)": None,
}

CARGOS_PERMITIDOS_ESCALACAO = [
    CARGO_AGREGADO_ID,
    CARGO_MEMBRO_ID,
    CARGO_SOLDADO_ID,
    CARGO_01_ID,
    CARGO_02_ID,
    CARGO_GERENTE_ID,
    CARGO_GERENTE_GERAL_ID
]


def formatar_dinheiro(valor):
    try:
        valor = float(valor)
    except:
        valor = 0
    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


async def gerar_embed_acoes():
    """Gera o embed com contagem de ações realizadas na semana"""
    async with db.acquire() as conn:
        # Conta ações CONCLUÍDAS (que já tiveram resultado finalizado)
        rows = await conn.fetch("""
            SELECT tipo, COUNT(*) as qtd
            FROM acoes_semana
            WHERE status = 'concluida' AND (resultado = 'ganhou' OR resultado = 'perdeu' OR resultado = 'concluida')
            GROUP BY tipo
        """)

    feitas = {r["tipo"]: r["qtd"] for r in rows}

    linhas = []
    total_feitas = 0
    total_meta = 0

    for nome, limite in ACOES_SEMANA.items():
        qtd = feitas.get(nome, 0)
        total_feitas += qtd

        if limite is None:
            # Ações sem limite
            linhas.append(f"• {nome}: {qtd}")
        else:
            restante = max(limite - qtd, 0)
            if qtd >= limite:
                linhas.append(f"• {nome}: ✅ {qtd}/{limite} (COMPLETO)")
            else:
                linhas.append(f"• {nome}: {qtd}/{limite} (restam {restante})")
            total_meta += limite

    # Calcula porcentagem geral
    if total_meta > 0:
        porcentagem = int((total_feitas / total_meta) * 100)
        barra_progresso = "▓" * (porcentagem // 5) + "░" * (20 - (porcentagem // 5))
        status_texto = f"📊 Progresso Semanal: {porcentagem}% {barra_progresso}\n\n"
    else:
        status_texto = ""

    embed = discord.Embed(
        title="📊 AÇÕES DA SEMANA", 
        description="**Controle de ações realizadas no período**",
        color=0x2ecc71
    )
    
    embed.add_field(
        name="📌 AÇÕES REALIZADAS", 
        value=status_texto + "\n".join(linhas), 
        inline=False
    )
    
    embed.add_field(
        name="📊 TOTAL", 
        value=f"{total_feitas}/{total_meta} ações realizadas" if total_meta > 0 else f"{total_feitas} ações realizadas (sem limite)",
        inline=False
    )
    
    embed.set_footer(text=f"Atualizado em {agora().strftime('%d/%m/%Y %H:%M')}")
    
    return embed


# =========================================================
# ================= VIEW PRINCIPAL ========================
# =========================================================

class PainelAcoesView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎯 Criar Nova Ação", style=discord.ButtonStyle.success, custom_id="criar_acao", emoji="🎯")
    async def criar_acao(self, interaction: discord.Interaction, button):
        """Abre o seletor de ações para criar uma nova"""
        await interaction.response.defer(ephemeral=True)
        
        view = SelecionarAcaoView()
        await interaction.followup.send(
            "**Selecione o tipo de ação:**",
            view=view,
            ephemeral=True
        )

    @discord.ui.button(label="📊 Ver Relatório", style=discord.ButtonStyle.primary, custom_id="acoes_relatorio", emoji="📊")
    async def relatorio(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(RelatorioPeriodoModal())

    @discord.ui.button(label="♻️ Resetar Ações", style=discord.ButtonStyle.danger, custom_id="acoes_reset", emoji="♻️")
    async def reset(self, interaction: discord.Interaction, button):
        await interaction.response.defer(ephemeral=True)
        
        # Verificar permissão (apenas gerentes/ADM)
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        if not is_gerente and not interaction.user.guild_permissions.administrator:
            await interaction.followup.send("❌ Apenas gerentes podem resetar as ações!", ephemeral=True)
            return
        
        async with db.acquire() as conn:
            await conn.execute("DELETE FROM acoes_semana")
            await conn.execute("DELETE FROM participantes_acoes")
        
        await atualizar_painel_acoes(interaction.guild)
        await interaction.followup.send("✅ Todas as ações foram resetadas!", ephemeral=True)


class SelecionarAcaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)
        
        # Criar select com as ações (organizado)
        options = []
        
        # Adiciona ações com limite primeiro
        for nome, limite in ACOES_SEMANA.items():
            if limite is not None:
                descricao = f"Limite: {limite}/semana"
                options.append(discord.SelectOption(label=nome, description=descricao, emoji="🎯"))
        
        # Adiciona ações sem limite
        for nome, limite in ACOES_SEMANA.items():
            if limite is None:
                if "Helicrash" in nome:
                    options.append(discord.SelectOption(label=nome, description="Ilimitado • Horário fixo", emoji="🚁"))
                else:
                    options.append(discord.SelectOption(label=nome, description="Ilimitado", emoji="🏪"))
        
        select = discord.ui.Select(
            placeholder="📋 Escolha a ação",
            options=options,
            max_values=1
        )
        select.callback = self.select_callback
        self.add_item(select)
        self.add_item(FecharButton())
    
    async def select_callback(self, interaction: discord.Interaction):
        acao_tipo = interaction.data["values"][0]
        
        await interaction.response.defer(ephemeral=True)
        
        # Verificar se já atingiu o limite semanal (apenas para ações COM limite)
        limite = ACOES_SEMANA.get(acao_tipo)
        
        if limite and limite is not None:
            async with db.acquire() as conn:
                qtd = await conn.fetchval(
                    "SELECT COUNT(*) FROM acoes_semana WHERE tipo=$1 AND status='concluida' AND (resultado='ganhou' OR resultado='perdeu')",
                    acao_tipo
                )
                
                if qtd >= limite:
                    await interaction.followup.send(
                        f"❌ Ação **{acao_tipo}** já atingiu o limite semanal de **{limite}** vez(es)!",
                        ephemeral=True
                    )
                    return
        
        # Criar nova ação
        async with db.acquire() as conn:
            acao_id = await conn.fetchval(
                "INSERT INTO acoes_semana (tipo, data, autor, status) VALUES ($1, $2, $3, 'aberta') RETURNING id",
                acao_tipo,
                agora_db(),
                str(interaction.user.id)
            )
        
        # Escolher cor e emoji baseado no tipo de ação
        if "Helicrash" in acao_tipo:
            cor = 0xe67e22  # Laranja
            emoji = "🚁"
        else:
            cor = 0x3498db  # Azul
            emoji = "🎯"
        
        # Criar embed de check-in
        embed = discord.Embed(
            title=f"{emoji} {acao_tipo}",
            description="**Clique no botão ✅ PARTICIPAR para se inscrever nesta ação!**\n\n"
                       "📌 Quem participar será registrado automaticamente.\n"
                       "👤 Quando terminar a escalação, o criador clica em 📤 Concluir.",
            color=cor
        )
        
        # Informação de horário para Helicrash
        if "Helicrash" in acao_tipo:
            horario = acao_tipo.split("(")[1].replace(")", "")
            embed.description += f"\n\n⏰ **Horário:** {horario} (horário de Brasília)"
        
        # Mostrar limite se houver
        if limite and limite is not None:
            async with db.acquire() as conn:
                qtd_feita = await conn.fetchval(
                    "SELECT COUNT(*) FROM acoes_semana WHERE tipo=$1 AND status='concluida' AND (resultado='ganhou' OR resultado='perdeu')",
                    acao_tipo
                )
                embed.description += f"\n\n📊 **Limite semanal:** {qtd_feita}/{limite} ações realizadas"
        
        embed.add_field(
            name="👥 Participantes (0)", 
            value="Nenhum participante ainda.\nClique no botão abaixo para participar!", 
            inline=False
        )
        embed.add_field(
            name="👤 Criado por", 
            value=interaction.user.mention, 
            inline=True
        )
        embed.add_field(
            name="📅 Data", 
            value=agora().strftime('%d/%m/%Y %H:%M'), 
            inline=True
        )
        embed.set_footer(text=f"ID: {acao_id}")
        
        # Enviar no canal de escalações
        canal = interaction.guild.get_channel(CANAL_ESCALACOES_ID)
        
        if canal:
            await canal.send(embed=embed, view=AcaoCheckinView(acao_id, interaction.user.id))
            await interaction.followup.send(f"✅ Ação **{acao_tipo}** criada! Os membros podem clicar em **✅ Participar** para se inscrever.", ephemeral=True)
        else:
            await interaction.followup.send("❌ Canal de escalações não encontrado!", ephemeral=True)


class FecharButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="❌ Fechar", style=discord.ButtonStyle.danger)
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.message.delete()


# =========================================================
# ================= VIEW DE CHECK-IN DA AÇÃO ==============
# =========================================================

class AcaoCheckinView(discord.ui.View):
    def __init__(self, acao_id, criador_id):
        super().__init__(timeout=None)
        self.acao_id = acao_id
        self.criador_id = criador_id

    @discord.ui.button(label="✅ Participar", style=discord.ButtonStyle.success, custom_id="acao_participar", emoji="✅")
    async def participar(self, interaction: discord.Interaction, button):
        # Verificar se o membro tem cargo permitido
        if not any(role.id in CARGOS_PERMITIDOS_ESCALACAO for role in interaction.user.roles):
            await interaction.response.send_message("❌ Você não tem permissão para participar de ações!", ephemeral=True)
            return
        
        # Verificar se a ação ainda está aberta
        async with db.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            
            if status != "aberta":
                await interaction.response.send_message("❌ Esta ação já foi concluída e não aceita mais participantes!", ephemeral=True)
                return
            
            # Verificar se já está participando
            ja_participa = await conn.fetchval(
                "SELECT 1 FROM participantes_acoes WHERE acao_id=$1 AND user_id=$2",
                self.acao_id, str(interaction.user.id)
            )
            
            if ja_participa:
                await interaction.response.send_message("⚠️ Você já está participando desta ação!", ephemeral=True)
                return
            
            # Adicionar participante
            await conn.execute(
                "INSERT INTO participantes_acoes (acao_id, user_id) VALUES ($1, $2)",
                self.acao_id, str(interaction.user.id)
            )
            
            # Buscar todos os participantes atuais
            participantes = await conn.fetch(
                "SELECT user_id FROM participantes_acoes WHERE acao_id=$1",
                self.acao_id
            )
            
            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)
        
        # Atualizar o embed
        embed = interaction.message.embeds[0]
        lista_participantes = "\n".join([f"<@{p['user_id']}>" for p in participantes]) if participantes else "Nenhum participante ainda."
        
        # Atualizar campo de participantes
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
        await interaction.response.send_message(f"✅ Você se inscreveu na ação **{acao['tipo']}**!", ephemeral=True)

    @discord.ui.button(label="📤 Concluir Escalação", style=discord.ButtonStyle.primary, custom_id="acao_concluir", emoji="📤")
    async def concluir(self, interaction: discord.Interaction, button):
        # Verificar se é o criador ou gerente
        is_criador = interaction.user.id == self.criador_id
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_criador and not is_gerente:
            await interaction.response.send_message("❌ Apenas o criador da ação ou gerentes podem concluir a escalação!", ephemeral=True)
            return
        
        # Verificar se já foi concluída
        async with db.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            
            if status != "aberta":
                await interaction.response.send_message("❌ Esta ação já foi concluída!", ephemeral=True)
                return
            
            # Buscar dados da ação
            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)
            participantes = await conn.fetch(
                "SELECT user_id FROM participantes_acoes WHERE acao_id=$1",
                self.acao_id
            )
            
            # Verificar se é HELICRASH
            is_helicrash = "Helicrash" in acao["tipo"]
            
            if not participantes:
                await interaction.response.send_message("⚠️ Nenhum participante se inscreveu! Ação cancelada.", ephemeral=True)
                await interaction.message.delete()
                return
            
            # Marcar como concluída
            await conn.execute("UPDATE acoes_semana SET status='concluida' WHERE id=$1", self.acao_id)
            
            # Se for HELICRASH, já finaliza automaticamente
            if is_helicrash:
                await conn.execute("UPDATE acoes_semana SET resultado='concluida', valor=0 WHERE id=$1", self.acao_id)
        
        # Criar relatório
        lista_participantes = "\n".join([f"<@{p['user_id']}>" for p in participantes])
        
        # =============================================
        # PARA HELICRASH - Envia direto sem botões
        # =============================================
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
                await interaction.response.send_message(
                    f"✅ Helicrash **{acao['tipo']}** registrado! Participantes: {len(participantes)}", 
                    ephemeral=True
                )
            else:
                await interaction.response.send_message("❌ Canal de relatório não encontrado!", ephemeral=True)
            
            await atualizar_painel_acoes(interaction.guild)
            return
        
        # =============================================
        # PARA AÇÕES NORMAIS - Com botões Ganhou/Perdeu
        # =============================================
        embed_relatorio = discord.Embed(
            title="🚨 RELATÓRIO DE AÇÃO", 
            color=0xe74c3c
        )
        embed_relatorio.add_field(name="🏦 Ação", value=acao["tipo"], inline=False)
        embed_relatorio.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
        embed_relatorio.add_field(name="🎯 Resultado", value="⏳ Aguardando finalização...", inline=False)
        embed_relatorio.set_footer(text=f"ID: {self.acao_id} • Criada por: <@{acao['autor']}>")
        
        canal_relatorio = interaction.guild.get_channel(CANAL_RELATORIO_ACOES_ID)
        
        if canal_relatorio:
            msg = await canal_relatorio.send(embed=embed_relatorio, view=None)
            await msg.edit(view=ResultadoAcaoView(self.acao_id, msg))
            await interaction.message.delete()
            await interaction.response.send_message(
                f"✅ Escalação concluída! Relatório enviado para <#{CANAL_RELATORIO_ACOES_ID}>.", 
                ephemeral=True
            )
            await atualizar_painel_acoes(interaction.guild)
        else:
            await interaction.response.send_message("❌ Canal de relatório não encontrado!", ephemeral=True)


# =========================================================
# ================= MODAL DE RESULTADO (GANHOU) ===========
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
            valor_total = int(self.dinheiro.value.replace(".", "").replace(",", ""))
            if valor_total <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ Valor inválido!", ephemeral=True)
            return

        async with db.acquire() as conn:
            # Verificar limite semanal (apenas para ações COM limite)
            acao = await conn.fetchrow("SELECT tipo FROM acoes_semana WHERE id=$1", self.acao_id)
            limite = ACOES_SEMANA.get(acao["tipo"])
            
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
            
            # Atualizar ação como ganhou
            await conn.execute(
                "UPDATE acoes_semana SET valor=$1, resultado='ganhou' WHERE id=$2",
                valor_total, self.acao_id
            )
            
            participantes = await conn.fetch(
                "SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id
            )

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
        embed.add_field(name="💰 Total Ganho", value=f"R$ {formatar_dinheiro(valor_total)}", inline=False)
        embed.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
        embed.add_field(name="💸 Valor por pessoa", value=f"R$ {formatar_dinheiro(valor_por_pessoa)}", inline=True)
        embed.add_field(name="✅ Depósitos", value=f"{depositos_ok}/{qtd} realizados", inline=True)

        if resto > 0:
            embed.add_field(name="📦 Restante", value=f"R$ {formatar_dinheiro(resto)}", inline=True)

        await self.mensagem_original.edit(embed=embed, view=None)
        await atualizar_painel_acoes(interaction.guild)
        await interaction.followup.send(f"✅ {depositos_ok} depósitos realizados!", ephemeral=True)


# =========================================================
# ================= MODAL DE RESULTADO (PERDEU) ===========
# =========================================================

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

        async with db.acquire() as conn:
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
        await atualizar_painel_acoes(interaction.guild)
        await interaction.followup.send(f"✅ Ação registrada como PERDIDA!", ephemeral=True)


# =========================================================
# ================= VIEW DO RESULTADO =====================
# =========================================================

class ResultadoAcaoView(discord.ui.View):
    def __init__(self, acao_id, mensagem_original):
        super().__init__(timeout=None)
        self.acao_id = acao_id
        self.mensagem_original = mensagem_original

    @discord.ui.button(label="🏆 Ganhou", style=discord.ButtonStyle.success, custom_id="resultado_ganhou")
    async def ganhou(self, interaction: discord.Interaction, button):
        async with db.acquire() as conn:
            acao = await conn.fetchrow("SELECT autor FROM acoes_semana WHERE id=$1", self.acao_id)
        
        is_autor = str(interaction.user.id) == acao["autor"]
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_autor and not is_gerente:
            await interaction.response.send_message("❌ Sem permissão!", ephemeral=True)
            return
        
        await interaction.response.send_modal(ResultadoGanhouModal(self.acao_id, self.mensagem_original))

    @discord.ui.button(label="💀 Perdeu", style=discord.ButtonStyle.danger, custom_id="resultado_perdeu")
    async def perdeu(self, interaction: discord.Interaction, button):
        async with db.acquire() as conn:
            acao = await conn.fetchrow("SELECT autor FROM acoes_semana WHERE id=$1", self.acao_id)
        
        is_autor = str(interaction.user.id) == acao["autor"]
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_autor and not is_gerente:
            await interaction.response.send_message("❌ Sem permissão!", ephemeral=True)
            return
        
        await interaction.response.send_modal(ResultadoPerdeuModal(self.acao_id, self.mensagem_original))


# =========================================================
# ================= PAINEL ================================
# =========================================================

async def enviar_painel_acoes(guild):
    """Envia o painel principal com contagem de ações"""
    canal = guild.get_channel(CANAL_ESCALACOES_ID)
    if not canal:
        print("❌ Canal ações não encontrado")
        return

    embed = await gerar_embed_acoes()
    
    await enviar_ou_atualizar_painel("painel_acoes", CANAL_ESCALACOES_ID, embed, PainelAcoesView())


async def atualizar_painel_acoes(guild):
    """Atualiza o painel principal com a nova contagem"""
    await enviar_painel_acoes(guild)


# =========================================================
# ================= MODAL RELATÓRIO PERÍODO ===============
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

        async with db.acquire() as conn:
            total = await conn.fetchval(
                "SELECT COALESCE(SUM(valor), 0) FROM acoes_semana WHERE DATE(data) BETWEEN DATE($1) AND DATE($2) AND resultado = 'ganhou'",
                inicio, fim
            )
            rows = await conn.fetch(
                "SELECT p.user_id, COUNT(*) as qtd FROM participantes_acoes p JOIN acoes_semana a ON a.id = p.acao_id WHERE DATE(a.data) BETWEEN DATE($1) AND DATE($2) GROUP BY p.user_id ORDER BY qtd DESC",
                inicio, fim
            )

        linhas = [f"<@{r['user_id']}> • {r['qtd']} participações" for r in rows]

        embed = discord.Embed(title="📊 Relatório de Ações", color=0x3498db)
        embed.add_field(name="📅 Período", value=f"{self.data_inicio.value} até {self.data_fim.value}", inline=False)
        embed.add_field(name="💰 Total Movimentado (vitórias)", value=f"R$ {formatar_dinheiro(total)}", inline=False)
        embed.add_field(name="👥 Participações", value="\n".join(linhas) if linhas else "Nenhuma", inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)
        
# =========================================================
# =========================== METAS ========================
# =========================================================

metas_cache = {}
criando_meta = set()


# =========================================================
# CARREGAR METAS DO BANCO
# =========================================================

async def carregar_metas_cache():

    global metas_cache

    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM metas")

    metas_cache = {
        str(r["user_id"]): {
            "canal_id": int(r["canal_id"]),
            "dinheiro": r["dinheiro"],
            "polvora": r["polvora"],
            "acao": r["acao"]
        }
        for r in rows
    }


async def salvar_meta(user_id, canal_id, dinheiro, polvora, acao):

    metas_cache[str(user_id)] = {
        "canal_id": canal_id,
        "dinheiro": dinheiro,
        "polvora": polvora,
        "acao": acao
    }

    async with db.acquire() as conn:

        await conn.execute(
            """
            INSERT INTO metas (user_id, canal_id, dinheiro, polvora, acao)
            VALUES ($1,$2,$3,$4,$5)
            ON CONFLICT (user_id)
            DO UPDATE SET
            canal_id=$2,
            dinheiro=$3,
            polvora=$4,
            acao=$5
            """,
            str(user_id),
            str(canal_id),
            dinheiro,
            polvora,
            acao
        )

async def depositar_na_meta(user_id, valor, motivo):
    """Deposita dinheiro na meta do usuário"""
    
    async with db.acquire() as conn:
        # Verifica se o usuário tem uma meta
        meta = await conn.fetchrow(
            "SELECT dinheiro FROM metas WHERE user_id = $1",
            str(user_id)
        )
        
        if meta:
            # Atualiza o dinheiro existente
            novo_valor = meta["dinheiro"] + valor
            await conn.execute(
                "UPDATE metas SET dinheiro = $1 WHERE user_id = $2",
                novo_valor,
                str(user_id)
            )
            
            # Tenta enviar mensagem no canal da meta
            canal_id = await conn.fetchval(
                "SELECT canal_id FROM metas WHERE user_id = $1",
                str(user_id)
            )
            
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
            # Usuário não tem meta, cria uma só para depósito
            print(f"⚠️ Usuário {user_id} não tem meta, criando...")
            
            # Tenta criar a sala de meta
            guild = bot.get_guild(GUILD_ID)
            member = guild.get_member(int(user_id))
            
            if member:
                canal = await criar_sala_meta(member)
                if canal:
                    await conn.execute(
                        "UPDATE metas SET dinheiro = $1 WHERE user_id = $2",
                        valor,
                        str(user_id)
                    )
                    return True
            
            return False


# =========================================================
# CATEGORIA POR CARGO
# =========================================================

def obter_categoria_meta(member: discord.Member):

    roles = [r.id for r in member.roles]

    if CARGO_GERENTE_ID in roles:
        return CATEGORIA_META_GERENTE_ID

    if any(r in roles for r in [
        CARGO_RESP_METAS_ID,
        CARGO_RESP_ACAO_ID,
        CARGO_RESP_VENDAS_ID,
        CARGO_RESP_PRODUCAO_ID
    ]):
        return CATEGORIA_META_RESPONSAVEIS_ID

    if CARGO_SOLDADO_ID in roles:
        return CATEGORIA_META_SOLDADO_ID

    if CARGO_MEMBRO_ID in roles:
        return CATEGORIA_META_MEMBRO_ID

    if AGREGADO_ROLE_ID in roles:
        return CATEGORIA_META_AGREGADO_ID

    return None


# =========================================================
# CRIAR SALA DE META
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

        await salvar_meta(
            member.id,
            canal_encontrado.id,
            0,
            0,
            0
        )

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

    canal = await guild.create_text_channel(
        nome_canal,
        category=categoria,
        overwrites=overwrites
    )

    await salvar_meta(member.id, canal.id, 0, 0, 0)

    print(f"📊 Sala criada para {member.display_name}")

    return canal


# =========================================================
# ATUALIZAR CATEGORIA SE MUDAR CARGO
# =========================================================

async def atualizar_categoria_meta(member):

    if str(member.id) not in metas_cache:
        return

    canal = member.guild.get_channel(
        metas_cache[str(member.id)]["canal_id"]
    )

    if not canal:
        return

    nova = obter_categoria_meta(member)

    if nova and canal.category_id != nova:

        await canal.edit(
            category=member.guild.get_channel(nova)
        )


# =========================================================
# EVENTOS
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
                async with db.acquire() as conn:

                    await conn.execute(
                        "DELETE FROM metas WHERE user_id=$1",
                        uid
                    )

            except Exception as e:
                print("Erro removendo meta:", e)

            break


# =========================================================
# BOTÃO SOLICITAR SALA
# =========================================================

class SolicitarSalaView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="➕ Criar Minha Sala",
        style=discord.ButtonStyle.success,
        custom_id="criar_sala_manual"
    )
    async def criar(self, interaction: discord.Interaction, button: discord.ui.Button):

        if str(interaction.user.id) in metas_cache:

            await interaction.response.send_message(
                "Você já possui uma sala.",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        await criar_sala_meta(interaction.user)

        await interaction.followup.send(
            "Sua sala foi criada com sucesso!",
            ephemeral=True
        )


# =========================================================
# PAINEL SOLICITAR SALA
# =========================================================

async def enviar_painel_solicitar_sala():

    canal = bot.get_channel(CANAL_SOLICITAR_SALA_ID)

    if not canal:
        print("❌ Canal solicitar sala não encontrado")
        return

    embed = discord.Embed(
        title="📂 Solicitar Sala",
        description="Clique no botão para criar sua sala.",
        color=0x2ecc71
    )

    await enviar_ou_atualizar_painel(
        "painel_solicitar_sala",
        CANAL_SOLICITAR_SALA_ID,
        embed,
        SolicitarSalaView()
    )
# ================= WORKER CLIP =================

async def worker_clipes():

    print("🎬 Worker clips iniciado")

    while True:

        message = await fila_clipes.get()

        try:
            await postar_clipe_x(message)

        except Exception as e:
            print("Erro worker:", e)

        await asyncio.sleep(5)

        fila_clipes.task_done()
# ================= REAÇÃO CLIP =================

@bot.event
async def on_reaction_add(reaction, user):

    try:

        print("REAÇÃO DETECTADA")

        if user.bot:
            return

        message = reaction.message

        if message.channel.id != CANAL_CLIPES_ID:
            return

        if str(reaction.emoji) != EMOJI_APROVACAO:
            return

        if message.id in clips_postados:
            return

        # ================= NOVO =================

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

        # ================= ENVIA =================

        clips_postados.add(message.id)

        await fila_clipes.put(message)

        await message.reply("🚀 Vai pro X!")

    except Exception as e:
        print("Erro reação clip:", e)


# =========================================================
# ================= SISTEMA DE AUSÊNCIA ===================
# =========================================================

# ================= CONFIGURAÇÕES =================
# Cargos permitidos para remover ausência
CARGOS_PERMITIDOS_REMOVER = [
    CARGO_GERENTE_ID,        # Gerente
    CARGO_01_ID,             # Cargo 01
    CARGO_02_ID,             # Cargo 02
    CARGO_GERENTE_GERAL_ID   # Gerente Geral
]

# ================= FUNÇÕES DE BANCO =================

async def salvar_ausencia_db(user_id, nome, motivo, data_inicio, data_fim):
    """Salva uma ausência no banco"""
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO ausencias (user_id, nome, motivo, data_inicio, data_fim, ativo)
            VALUES ($1, $2, $3, $4, $5, true)
            """,
            str(user_id),
            nome,
            motivo,
            data_inicio,
            data_fim
        )

async def buscar_ausencias_ativas():
    """Busca todas as ausências ativas"""
    async with db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT * FROM ausencias 
            WHERE ativo = true AND data_fim > NOW()
            ORDER BY data_fim ASC
            """
        )
        return rows

async def buscar_ausencia_por_user(user_id):
    """Busca ausência ativa de um usuário específico"""
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT * FROM ausencias 
            WHERE user_id = $1 AND ativo = true AND data_fim > NOW()
            """,
            str(user_id)
        )
        return row

async def desativar_ausencia(user_id):
    """Desativa uma ausência (remove o cargo)"""
    async with db.acquire() as conn:
        await conn.execute(
            """
            UPDATE ausencias 
            SET ativo = false 
            WHERE user_id = $1 AND ativo = true
            """,
            str(user_id)
        )

async def remover_ausencias_expiradas():
    """Remove cargos de ausências que expiraram"""
    async with db.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT user_id FROM ausencias 
            WHERE ativo = true AND data_fim <= NOW()
            """
        )
        
        for row in rows:
            await conn.execute(
                "UPDATE ausencias SET ativo = false WHERE user_id = $1",
                row["user_id"]
            )
        
        return [row["user_id"] for row in rows]

# ================= VERIFICAR PERMISSÃO =================

def pode_remover_ausencia(member: discord.Member):
    """Verifica se o membro tem permissão para remover ausência"""
    if not member:
        return False
    return any(role.id in CARGOS_PERMITIDOS_REMOVER for role in member.roles)

# ================= MODAL DE AUSÊNCIA =================

class AusenciaModal(discord.ui.Modal, title="📝 Solicitar Ausência"):
    
    nome = discord.ui.TextInput(
        label="Seu nome completo",
        placeholder="Digite seu nome",
        required=True
    )
    
    data_inicio = discord.ui.TextInput(
        label="Data de INÍCIO da ausência",
        placeholder="Ex: 10/04/2026",
        required=True
    )
    
    data_fim = discord.ui.TextInput(
        label="Data de RETORNO",
        placeholder="Ex: 15/04/2026",
        required=True
    )
    
    motivo = discord.ui.TextInput(
        label="Motivo da ausência",
        placeholder="Ex: Viagem, Problemas de saúde, etc",
        style=discord.TextStyle.paragraph,
        required=True
    )

    async def on_submit(self, interaction: discord.Interaction):
        
        await interaction.response.defer(ephemeral=True)
        
        # Converter as datas
        try:
            data_inicio_dt = datetime.strptime(self.data_inicio.value.strip(), "%d/%m/%Y")
            data_fim_dt = datetime.strptime(self.data_fim.value.strip(), "%d/%m/%Y")
            
            # Ajustar hora para início e fim do dia
            data_inicio_naive = data_inicio_dt.replace(hour=0, minute=0, second=0)
            data_fim_naive = data_fim_dt.replace(hour=23, minute=59, second=59)
            
        except ValueError:
            await interaction.followup.send(
                "❌ Formato de data inválido!\nUse o formato: `10/04/2026` (DIA/MÊS/ANO)",
                ephemeral=True
            )
            return
        
        # Verificar se data de fim é depois da data de início
        if data_fim_naive <= data_inicio_naive:
            await interaction.followup.send(
                f"❌ A data de RETORNO deve ser **depois** da data de INÍCIO!\n"
                f"Início: {self.data_inicio.value}\n"
                f"Retorno: {self.data_fim.value}",
                ephemeral=True
            )
            return
        
        # Verificar se já tem ausência ativa
        ausencia_existente = await buscar_ausencia_por_user(interaction.user.id)
        
        if ausencia_existente:
            await interaction.followup.send(
                "❌ Você já possui uma ausência ativa!\n"
                "Espere ela terminar ou peça para um admin cancelar.",
                ephemeral=True
            )
            return
        
        # Calcular dias de ausência
        dias_ausencia = (data_fim_naive - data_inicio_naive).days + 1
        
        # =============================================
        # VERIFICAR SE É 15 DIAS OU MAIS
        # =============================================
        
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
                print(f"✅ Alerta de ausência prolongada enviado para gerência")
        
        # Salvar no banco
        await salvar_ausencia_db(
            interaction.user.id,
            self.nome.value,
            self.motivo.value,
            data_inicio_naive,
            data_fim_naive
        )
        
        # Adicionar cargo de ausente
        cargo = interaction.guild.get_role(CARGO_AUSENTE_ID)
        if cargo:
            await interaction.user.add_roles(cargo)
            print(f"✅ Cargo ausente adicionado para {interaction.user.display_name}")
        
        # Formatar período
        periodo_formatado = f"{self.data_inicio.value} a {self.data_fim.value}"
        
        # =============================================
        # ENVIAR EMBED NO CANAL DE REGISTRO
        # =============================================
        
        canal_registro = interaction.guild.get_channel(CANAL_REGISTRO_AUSENCIA_ID)
        
        if canal_registro:
            embed_ausencia = discord.Embed(
                title="📋 AUSÊNCIA REGISTRADA",
                description=f"{interaction.user.mention} está ausente!",
                color=0xe67e22
            )
            
            embed_ausencia.add_field(name="👤 Nome", value=self.nome.value, inline=True)
            embed_ausencia.add_field(name="📅 Período", value=periodo_formatado, inline=True)
            embed_ausencia.add_field(name="⏳ Total de dias", value=f"{dias_ausencia} dia(s)", inline=True)
            embed_ausencia.add_field(name="📝 Motivo", value=self.motivo.value, inline=False)
            
            if dias_ausencia >= 15:
                embed_ausencia.add_field(name="⚠️ Atenção", value="Ausência prolongada! Gerência notificada.", inline=False)
            
            embed_ausencia.set_footer(text=f"Solicitado em {agora().strftime('%d/%m/%Y às %H:%M')}")
            
            await canal_registro.send(embed=embed_ausencia)
            print(f"✅ Embed enviado para {CANAL_REGISTRO_AUSENCIA_ID}")
        else:
            print(f"❌ Canal de registro não encontrado! ID: {CANAL_REGISTRO_AUSENCIA_ID}")
        
        # =============================================
        # CONFIRMAÇÃO PRIVADA
        # =============================================
        
        embed_privado = discord.Embed(
            title="✅ Ausência Registrada!",
            color=0x2ecc71
        )
        
        embed_privado.add_field(name="👤 Nome", value=self.nome.value, inline=True)
        embed_privado.add_field(name="📅 Período", value=periodo_formatado, inline=True)
        embed_privado.add_field(name="📝 Motivo", value=self.motivo.value[:100], inline=False)
        
        if dias_ausencia >= 15:
            embed_privado.add_field(name="⚠️ Observação", value="Por ser uma ausência prolongada (+15 dias), a gerência foi notificada.", inline=False)
        
        embed_privado.set_footer(text="Quando retornar, seu cargo será removido automaticamente!")
        
        await interaction.followup.send(embed=embed_privado, ephemeral=True)

# ================= VIEW DO BOTÃO (SOLICITAR) =================

class AusenciaBotaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="📝 Solicitar Ausência",
        style=discord.ButtonStyle.primary,
        custom_id="ausencia_solicitar_botao"
    )
    async def solicitar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(AusenciaModal())

# ================= SELECT PARA REMOVER AUSÊNCIA =================

class RemoverAusenciaSelect(discord.ui.Select):
    def __init__(self, ausencias):
        options = []
        for ausencia in ausencias:
            nome = ausencia['nome'][:50]
            periodo = f"{ausencia['data_inicio'].strftime('%d/%m')} a {ausencia['data_fim'].strftime('%d/%m')}"
            options.append(
                discord.SelectOption(
                    label=nome,
                    description=f"Período: {periodo}",
                    value=str(ausencia['user_id'])
                )
            )
        
        super().__init__(
            placeholder="Selecione a ausência para remover (volta antecipada)",
            min_values=1,
            max_values=1,
            options=options
        )
    
    async def callback(self, interaction: discord.Interaction):
        user_id = int(self.values[0])
        member = interaction.guild.get_member(user_id)
        
        # Buscar dados da ausência antes de remover
        ausencia = await buscar_ausencia_por_user(user_id)
        
        # Desativar ausência
        await desativar_ausencia(user_id)
        
        # Remover cargo
        cargo = interaction.guild.get_role(CARGO_AUSENTE_ID)
        if cargo and member and cargo in member.roles:
            await member.remove_roles(cargo)
        
        # Calcular dias restantes
        if ausencia:
            data_fim = ausencia["data_fim"]
            if data_fim.tzinfo is None:
                data_fim = data_fim.replace(tzinfo=BRASIL)
            dias_antecipados = (data_fim - agora()).days + 1
            if dias_antecipados < 0:
                dias_antecipados = 0
        else:
            dias_antecipados = 0
        
        # Confirmar remoção
        embed = discord.Embed(
            title="✅ AUSÊNCIA REMOVIDA (RETORNO ANTECIPADO)",
            description=f"A ausência de {member.mention if member else f'<@{user_id}>'} foi encerrada!",
            color=0x2ecc71
        )
        embed.add_field(name="👤 Usuário", value=member.mention if member else f"ID: {user_id}", inline=True)
        
        if dias_antecipados > 0:
            embed.add_field(name="📅 Dias antecipados", value=f"{dias_antecipados} dia(s) antes do previsto", inline=True)
        
        embed.add_field(name="📝 Status", value="Cargo ausente removido. Usuário pode solicitar nova ausência.", inline=False)
        
        await interaction.response.edit_message(content=None, embed=embed, view=None)
        
        # Também enviar no canal de registro
        canal_registro = interaction.guild.get_channel(CANAL_REGISTRO_AUSENCIA_ID)
        if canal_registro:
            await canal_registro.send(embed=embed)

# ================= VIEW DE REMOVER AUSÊNCIA =================

class RemoverAusenciaView(discord.ui.View):
    def __init__(self, ausencias):
        super().__init__(timeout=60)
        self.add_item(RemoverAusenciaSelect(ausencias))

# ================= BOTÃO DE REMOVER AUSÊNCIA (APENAS PARA CARGOS PERMITIDOS) =================

class BotaoRemoverAusenciaView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🔄 Remover Ausência (Retorno Antecipado)",
        style=discord.ButtonStyle.primary,
        custom_id="remover_ausencia_botao",
        emoji="🔄"
    )
    async def remover(self, interaction: discord.Interaction, button):
        # Verificar permissão
        if not pode_remover_ausencia(interaction.user):
            await interaction.response.send_message(
                "❌ Você não tem permissão para remover ausências!\n"
                "Apenas **Gerente, Cargo 01, Cargo 02 e Gerente Geral** podem usar este recurso.",
                ephemeral=True
            )
            return
        
        # Buscar ausências ativas
        ausencias = await buscar_ausencias_ativas()
        
        if not ausencias:
            await interaction.response.send_message("📭 Nenhuma ausência ativa no momento.", ephemeral=True)
            return
        
        # Enviar select com as ausências
        view = RemoverAusenciaView(ausencias)
        await interaction.response.send_message(
            "📋 Selecione o membro que **retornou antes do previsto**:\n"
            "(O cargo ausente será removido imediatamente)",
            view=view,
            ephemeral=True
        )

# ================= PAINEL DO BOTÃO (SOLICITAR) =================

async def enviar_painel_botao_ausencia():
    """Envia o painel com o botão para solicitar ausência"""
    
    canal = bot.get_channel(CANAL_BOTAO_AUSENCIA_ID)
    
    if not canal:
        print(f"❌ Canal do botão NÃO ENCONTRADO! ID: {CANAL_BOTAO_AUSENCIA_ID}")
        return
    
    embed = discord.Embed(
        title="📋 Solicitar Ausência",
        description="Clique no botão abaixo para solicitar sua ausência.\n\n"
                    "📌 **Como usar:**\n"
                    "• Digite seu nome completo\n"
                    "• Informe a **data de INÍCIO** (ex: `10/04/2026`)\n"
                    "• Informe a **data de RETORNO** (ex: `15/04/2026`)\n"
                    "• Digite o motivo\n\n"
                    "✅ Você receberá o cargo **Ausente**\n"
                    "✅ Quando o período acabar, o cargo será removido\n\n"
                    "⚠️ **Ausências de 15 dias ou mais** serão notificadas à gerência",
        color=0xe67e22
    )
    
    embed.add_field(
        name="📅 Exemplo",
        value="• Data INÍCIO: `10/04/2026`\n• Data RETORNO: `15/04/2026`\n(contando todos os dias entre 10 e 15)",
        inline=False
    )
    
    await enviar_ou_atualizar_painel(
        "painel_botao_ausencia",
        CANAL_BOTAO_AUSENCIA_ID,
        embed,
        AusenciaBotaoView()
    )
    
    print(f"✅ Painel do botão enviado para {CANAL_BOTAO_AUSENCIA_ID}")

# ================= PAINEL DE REMOVER AUSÊNCIA =================

async def enviar_painel_remover_ausencia():
    """Envia o painel com o botão para remover ausência (retorno antecipado)"""
    
    canal = bot.get_channel(CANAL_BOTAO_AUSENCIA_ID)
    
    if not canal:
        print(f"❌ Canal do botão NÃO ENCONTRADO! ID: {CANAL_BOTAO_AUSENCIA_ID}")
        return
    
    # Verificar se já existe um painel de remover
    async for msg in canal.history(limit=30):
        if msg.author == bot.user and msg.embeds and msg.embeds[0].title == "🔄 Remover Ausência (Retorno Antecipado)":
            return
    
    embed = discord.Embed(
        title="🔄 Remover Ausência (Retorno Antecipado)",
        description="Clique no botão abaixo caso um membro tenha **retornado antes do previsto**.\n\n"
                    "⚠️ **Apenas para:** Gerente, Cargo 01, Cargo 02 e Gerente Geral",
        color=0x3498db
    )
    
    embed.add_field(
        name="📌 Como usar",
        value="1. Clique no botão\n2. Selecione o membro na lista\n3. Confirme a remoção\n\nO cargo **Ausente** será removido imediatamente.",
        inline=False
    )
    
    await enviar_ou_atualizar_painel(
        "painel_remover_ausencia",
        CANAL_BOTAO_AUSENCIA_ID,
        embed,
        BotaoRemoverAusenciaView()
    )
    
    print(f"✅ Painel de remover ausência enviado para {CANAL_BOTAO_AUSENCIA_ID}")

# ================= LOOP DE VERIFICAÇÃO =================

@tasks.loop(minutes=60)
async def verificar_ausencias_expiradas():
    """Verifica se alguma ausência já passou da data de retorno"""
    
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return
    
    cargo_ausente = guild.get_role(CARGO_AUSENTE_ID)
    if not cargo_ausente:
        print(f"❌ Cargo ausente não encontrado! ID: {CARGO_AUSENTE_ID}")
        return
    
    users_para_remover = await remover_ausencias_expiradas()
    
    for user_id in users_para_remover:
        member = guild.get_member(int(user_id))
        if member and cargo_ausente in member.roles:
            await member.remove_roles(cargo_ausente)
            print(f"✅ Cargo ausente removido de {member.display_name}")
            
            # Enviar mensagem no canal de registro
            canal_registro = guild.get_channel(CANAL_REGISTRO_AUSENCIA_ID)
            if canal_registro:
                embed_retorno = discord.Embed(
                    title="🎉 RETORNO REGISTRADO",
                    description=f"{member.mention} retornou! O cargo ausente foi removido automaticamente.",
                    color=0x2ecc71
                )
                await canal_registro.send(embed=embed_retorno)

# ================= COMANDOS ADMIN =================

@bot.command(name="ausentes")
@commands.has_permissions(administrator=True)
async def listar_ausentes(ctx):
    """Lista todos os membros ausentes"""
    
    ausencias = await buscar_ausencias_ativas()
    
    if not ausencias:
        await ctx.send("📭 Nenhum membro ausente.")
        return
    
    embed = discord.Embed(title="📋 Membros Ausentes", color=0xe67e22)
    
    for ausencia in ausencias:
        data_fim = ausencia["data_fim"]
        data_inicio = ausencia["data_inicio"]
        
        embed.add_field(
            name=f"👤 {ausencia['nome']}",
            value=f"📅 {data_inicio.strftime('%d/%m/%Y')} a {data_fim.strftime('%d/%m/%Y')}\n📝 {ausencia['motivo'][:50]}",
            inline=False
        )
    
    await ctx.send(embed=embed)

@bot.command(name="remover_ausencia")
async def remover_ausencia_cmd(ctx, member: discord.Member):
    """Remove a ausência de um membro (retorno antecipado) - Apenas cargos autorizados"""
    
    # Verificar permissão
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
    
    embed = discord.Embed(
        title="✅ Ausência Removida (Retorno Antecipado)",
        description=f"A ausência de {member.mention} foi encerrada!",
        color=0x2ecc71
    )
    await ctx.send(embed=embed)

# ================== TESTES DE SISTEMA ====================

@bot.command(name="testar_saida")
async def testar_saida(ctx, usuario: discord.User = None):
    """Testa o envio da mensagem de saída (APENAS ADM)"""
    
    # Verificar permissão
    if not ctx.author.guild_permissions.administrator:
        await ctx.send("❌ Apenas ADM podem usar este comando!")
        return
    
    if not usuario:
        usuario = ctx.author
    
    await ctx.send(f"📤 Simulando saída para {usuario.mention}...")
    
    # Simular a mensagem que seria enviada
    try:
        embed = discord.Embed(
            title="📤 NOTIFICAÇÃO DE SAÍDA (TESTE)",
            description=(
                "Olá, tudo bom?\n\n"
                "**⚠️ ISSO É UM TESTE**\n\n"
                "Devido à sua saída do servidor **Vida Rasa** (MENSAGEM DE TESTE), "
                "pedimos que procure algum **gerente in game** "
                "para tomar seu **PD da fac**.\n\n"
                "⚠️ **Caso já tenha tomado seu PD, ignore este aviso.**"
            ),
            color=0xe74c3c
        )
        
        embed.set_footer(text="TESTE • Sistema Automático")
        
        await usuario.send(embed=embed)
        await ctx.send(f"✅ Mensagem de teste enviada para {usuario.name}!")
        
        # Também enviar log de teste no canal da gerência
        canal_gerencia = ctx.guild.get_channel(CANAL_GERENCIA_ID)
        if canal_gerencia:
            embed_log = discord.Embed(
                title="🧪 TESTE - SIMULAÇÃO DE SAÍDA",
                description=f"**Usuário:** {usuario.mention}\n**Nome:** {usuario.name}\n**ID:** {usuario.id}",
                color=0x3498db
            )
            embed_log.add_field(
                name="📨 Resultado",
                value="✅ Mensagem de teste enviada com sucesso!",
                inline=False
            )
            embed_log.set_footer(text="TESTE • Sistema de Saída")
            await canal_gerencia.send(embed=embed_log)
        
    except discord.Forbidden:
        await ctx.send(f"❌ Não foi possível enviar DM para {usuario.name} (DM fechada)")
    except Exception as e:
        await ctx.send(f"❌ Erro: {e}")

# =========================================================
# ================= SISTEMA DE ALUGUEL ====================
# =========================================================

alugueis_ativos = {}


def formatar_dinheiro(valor):
    """Formata valor para reais"""
    try:
        valor = float(valor)
    except:
        valor = 0
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


async def salvar_aluguel_db(galpao, user_id, data_fim, dias):
    """Salva aluguel no banco de dados"""
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO alugueis (galpao, user_id, data_inicio, data_fim, dias, ativo)
            VALUES ($1, $2, $3, $4, $5, true)
            ON CONFLICT (galpao) 
            DO UPDATE SET 
                user_id = $2, 
                data_fim = $4, 
                dias = $5,
                ativo = true
            """,
            galpao,
            str(user_id),
            agora_db(),
            data_fim,
            dias
        )


async def carregar_alugueis_db():
    """Carrega alugueis ativos do banco"""
    global alugueis_ativos
    async with db.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM alugueis WHERE ativo = true AND data_fim > NOW()"
        )
    
    alugueis_ativos.clear()
    for row in rows:
        data_fim = row["data_fim"]
        if data_fim.tzinfo is None:
            data_fim = data_fim.replace(tzinfo=BRASIL)
        
        alugueis_ativos[row["galpao"]] = {
            "user_id": int(row["user_id"]),
            "fim": data_fim,
            "dias": row["dias"]
        }


async def desativar_aluguel_db(galpao):
    """Desativa um aluguel"""
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE alugueis SET ativo = false WHERE galpao = $1",
            galpao
        )
    if galpao in alugueis_ativos:
        del alugueis_ativos[galpao]


async def renovar_aluguel_db(galpao, user_id, data_fim, dias):
    """Renova/edita um aluguel existente"""
    async with db.acquire() as conn:
        await conn.execute(
            """
            UPDATE alugueis 
            SET user_id = $2, data_fim = $3, dias = $4, ativo = true
            WHERE galpao = $1
            """,
            galpao,
            str(user_id),
            data_fim,
            dias
        )


def formatar_tempo_detalhado(data_fim):
    """Formata o tempo restante de forma detalhada"""
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


def calcular_barra_progresso(data_fim, dias_totais):
    """Calcula a barra de progresso baseada no tempo restante"""
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
    
    barra = "█" * preenchidos + "░" * (tamanho_barra - preenchidos)
    
    return f"{cor} `{barra}` {porcentagem_restante:.0f}%"


# =========================================================
# ================= MODAL DE ALUGUEL ======================
# =========================================================

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
        
        # Validar dias
        try:
            dias_aluguel = int(self.dias.value)
            if dias_aluguel <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ Número de dias inválido! Use números positivos (ex: 15, 30, 7)", ephemeral=True)
            return
        
        # Calcular data de fim
        data_fim = agora() + timedelta(days=dias_aluguel)
        
        if self.editando:
            # Editar aluguel existente
            await renovar_aluguel_db(self.galpao, self.user_id, data_fim.replace(tzinfo=None), dias_aluguel)
            
            # Atualizar cache
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
            # Verificar se já está alugado
            if self.galpao in alugueis_ativos:
                await interaction.followup.send(
                    f"❌ **{self.galpao}** já está alugado!\n"
                    f"👤 Alugado por: <@{alugueis_ativos[self.galpao]['user_id']}>\n"
                    f"⏰ Vence em: {alugueis_ativos[self.galpao]['fim'].strftime('%d/%m/%Y às %H:%M')}",
                    ephemeral=True
                )
                return
            
            # Salvar aluguel
            await salvar_aluguel_db(self.galpao, interaction.user.id, data_fim.replace(tzinfo=None), dias_aluguel)
            
            # Atualizar cache
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


# =========================================================
# ================= MODAL DE EDIÇÃO =======================
# =========================================================

class EditarAluguelModal(discord.ui.Modal, title="✏️ Editar Aluguel"):
    
    dias = discord.ui.TextInput(
        label="Novo número de dias",
        placeholder="Digite o novo número de dias (ex: 15, 30, 7)",
        required=True
    )
    
    def __init__(self, galpao, user_id, dias_atuais):
        super().__init__()
        self.galpao = galpao
        self.user_id = user_id
        self.dias_atuais = dias_atuais
        self.dias.placeholder = f"Dias atuais: {dias_atuais}"
    
    async def on_submit(self, interaction: discord.Interaction):
        
        await interaction.response.defer(ephemeral=True)
        
        # Verificar se é o dono do aluguel ou gerente
        is_dono = interaction.user.id == self.user_id
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_dono and not is_gerente:
            await interaction.followup.send("❌ Apenas o dono do aluguel ou gerentes podem editar!", ephemeral=True)
            return
        
        # Validar dias
        try:
            dias_aluguel = int(self.dias.value)
            if dias_aluguel <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ Número de dias inválido! Use números positivos (ex: 15, 30, 7)", ephemeral=True)
            return
        
        # Calcular nova data de fim
        data_fim = agora() + timedelta(days=dias_aluguel)
        
        # Atualizar aluguel
        await renovar_aluguel_db(self.galpao, self.user_id, data_fim.replace(tzinfo=None), dias_aluguel)
        
        # Atualizar cache
        alugueis_ativos[self.galpao] = {
            "user_id": self.user_id,
            "fim": data_fim,
            "dias": dias_aluguel
        }
        
        await interaction.followup.send(
            f"✅ **{self.galpao}** atualizado com sucesso!\n"
            f"📅 **Dias:** {self.dias_atuais} → {dias_aluguel} dia(s)\n"
            f"⏰ **Nova data de vencimento:** {data_fim.strftime('%d/%m/%Y às %H:%M')}",
            ephemeral=True
        )
        
        # Atualizar o painel
        await atualizar_painel_alugueis()


# =========================================================
# ================= BOTÕES DE ALUGUEL =====================
# =========================================================

class AlugarButton(discord.ui.Button):
    def __init__(self, galpao, label, custom_id):
        super().__init__(
            label=label,
            style=discord.ButtonStyle.success,
            custom_id=custom_id,
            emoji="💰"
        )
        self.galpao = galpao
    
    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(AluguelModal(self.galpao, editando=False))


class EditarAluguelButton(discord.ui.Button):
    def __init__(self, galpao, user_id, dias_atuais):
        super().__init__(
            label="✏️ Editar Aluguel",
            style=discord.ButtonStyle.secondary,
            custom_id=f"editar_aluguel_{galpao}",
            emoji="✏️",
            row=1
        )
        self.galpao = galpao
        self.user_id = user_id
        self.dias_atuais = dias_atuais
    
    async def callback(self, interaction: discord.Interaction):
        # Verificar se é o dono ou gerente
        is_dono = interaction.user.id == self.user_id
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_dono and not is_gerente:
            await interaction.response.send_message("❌ Apenas o dono do aluguel ou gerentes podem editar!", ephemeral=True)
            return
        
        await interaction.response.send_modal(EditarAluguelModal(self.galpao, self.user_id, self.dias_atuais))


# =========================================================
# ================= VIEW DE ALUGUEL =======================
# =========================================================

class AluguelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        
        # Botões de alugar (apenas Galpões Norte e Sul)
        self.add_item(AlugarButton(
            galpao="GALPÕES NORTE",
            label="💰 Alugar Galpões Norte",
            custom_id="aluguel_norte_btn"
        ))
        
        self.add_item(AlugarButton(
            galpao="GALPÕES SUL",
            label="💰 Alugar Galpões Sul",
            custom_id="aluguel_sul_btn"
        ))
    
    def adicionar_botoes_edicao(self, galpao, user_id, dias_atuais):
        """Adiciona botão de edição para um galpão específico"""
        self.add_item(EditarAluguelButton(galpao, user_id, dias_atuais))


# =========================================================
# ================= PAINEL DE ALUGUEL =====================
# =========================================================

async def enviar_painel_alugueis():
    """Envia o painel com contagem regressiva dos aluguéis (sem duplicar)"""
    
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    
    if not canal:
        print("❌ Canal de fabricação não encontrado")
        return
    
    # Carregar dados atualizados
    await carregar_alugueis_db()
    
    # Criar embed principal
    embed = discord.Embed(
        title="🏭💰 CONTROLE DE ALUGUEL - GALPÕES",
        description="**Contagem regressiva atualizada automaticamente**",
        color=0x3498db
    )
    
    # Criar view
    view = AluguelView()
    
    # ===== GALPÃO NORTE =====
    if "GALPÕES NORTE" in alugueis_ativos:
        dados = alugueis_ativos["GALPÕES NORTE"]
        data_fim = dados["fim"]
        barra = calcular_barra_progresso(data_fim, dados["dias"])
        
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
                f"{barra}"
            ),
            inline=False
        )
        # Adicionar botão de edição
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
        barra = calcular_barra_progresso(data_fim, dados["dias"])
        
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
                f"{barra}"
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
    
    # Usar a função que evita duplicação
    await enviar_ou_atualizar_painel(
        "painel_alugueis",
        CANAL_FABRICACAO_ID,
        embed,
        view
    )


async def atualizar_painel_alugueis():
    """Atualiza o painel de aluguéis"""
    await enviar_painel_alugueis()


# =========================================================
# ================= LOOPS DE ATUALIZAÇÃO ==================
# =========================================================

@tasks.loop(minutes=1)
async def atualizar_contagem_alugueis():
    """Atualiza a contagem regressiva a cada minuto"""
    if alugueis_ativos:
        await atualizar_painel_alugueis()
        print("🔄 Contagem de aluguéis atualizada")


@tasks.loop(minutes=5)
async def verificar_alugueis_expirados():
    """Verifica e remove aluguéis expirados a cada 5 minutos"""
    agora_br = agora()
    expirou = False
    
    for galpao, dados in list(alugueis_ativos.items()):
        if agora_br >= dados["fim"]:
            await desativar_aluguel_db(galpao)
            expirou = True
            print(f"⏰ Aluguel expirado: {galpao}")
            
            canal = bot.get_channel(CANAL_FABRICACAO_ID)
            if canal:
                await canal.send(
                    f"⚠️ **{galpao}** - O aluguel EXPIRou!\n"
                    f"👤 Dono: <@{dados['user_id']}>\n"
                    f"📅 Venceu em: {dados['fim'].strftime('%d/%m/%Y às %H:%M')}\n\n"
                    f"💰 O galpão está disponível para um novo aluguel!"
                )
    
    if expirou:
        await atualizar_painel_alugueis()




# =========================================================
# ================= COMANDOS ==============================
# =========================================================

@bot.command(name="alugueis")
async def cmd_ver_alugueis(ctx):
    """Ver status dos aluguéis com contagem regressiva"""
    await enviar_painel_alugueis()
    await ctx.send("📊 Painel de aluguéis atualizado!", ephemeral=True)


@bot.command(name="editar_aluguel")
async def cmd_editar_aluguel(ctx, galpao: str = None, dias: int = None):
    """Editar aluguel manualmente (ADM apenas)
    Uso: !editar_aluguel NORTE 30"""
    
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
    
    alugueis_ativos[nome_galpao] = {
        "user_id": user_id,
        "fim": data_fim,
        "dias": dias
    }
    
    await atualizar_painel_alugueis()
    await ctx.send(f"✅ **{nome_galpao}** editado para {dias} dias!")


@bot.command(name="renovar")
async def cmd_renovar_aluguel(ctx, galpao: str, dias: int):
    """Renovar aluguel manualmente (ADM apenas)
    Uso: !renovar NORTE 30"""
    
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
    
    alugueis_ativos[nome_galpao] = {
        "user_id": ctx.author.id,
        "fim": data_fim,
        "dias": dias
    }
    
    await atualizar_painel_alugueis()
    await ctx.send(f"✅ **{nome_galpao}** alugado por {dias} dias!")

# =========================================================
# ================= RELATÓRIO FINANCEIRO ==================
# =========================================================

# Preço da pólvora por unidade
PRECO_POLVORA = 80  # R$ 80,00 por pólvora

# Preço das embalagens (25.000 unidades = R$ 2.000.000)
PRECO_EMBALAGEM_POR_UNIDADE = 2000000 / 25000  # R$ 80 por embalagem


class RelatorioFinanceiroModal(discord.ui.Modal, title="📊 RELATÓRIO FINANCEIRO"):
    
    data_inicio = discord.ui.TextInput(
        label="📅 Data INÍCIO",
        placeholder="Ex: 01/04/2026",
        required=True
    )
    
    data_fim = discord.ui.TextInput(
        label="📅 Data FIM",
        placeholder="Ex: 30/04/2026",
        required=True
    )
    
    incluir_compras = discord.ui.TextInput(
        label="📦 Incluir compras registradas?",
        placeholder="Digite SIM ou NAO (padrão é SIM)",
        required=False
    )
    
    embalagens = discord.ui.TextInput(
        label="📦 Embalagens compradas (opcional)",
        placeholder="Ex: 25000 (deixe em branco se não quiser)",
        required=False
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        
        await interaction.response.defer(ephemeral=True)
        
        try:
            # Converte as datas
            inicio = datetime.strptime(self.data_inicio.value.strip(), "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value.strip(), "%d/%m/%Y")
            
            inicio_dt = inicio.replace(hour=0, minute=0, second=0)
            fim_dt = fim.replace(hour=23, minute=59, second=59)
            
            # Verifica se deve incluir compras
            incluir_compras = self.incluir_compras.value.strip().upper() != "NAO"
            
            # Processa embalagens (opcional)
            total_embalagens = 0
            total_gasto_embalagens = 0
            
            if self.embalagens.value and self.embalagens.value.strip():
                try:
                    total_embalagens = int(self.embalagens.value.replace(".", "").replace(",", ""))
                    total_gasto_embalagens = int(total_embalagens * PRECO_EMBALAGEM_POR_UNIDADE)
                except:
                    pass
            
            # =====================================================
            # ================= BUSCA DADOS =======================
            # =====================================================
            
            async with db.acquire() as conn:
                # 1. Total de pólvora usada na produção
                polvora_row = await conn.fetchrow(
                    """
                    SELECT COALESCE(SUM(polvora), 0) as total_polvora
                    FROM producoes_finalizadas
                    WHERE data >= $1 AND data <= $2
                    """,
                    inicio_dt, fim_dt
                )
                
                # 2. Total de vendas
                vendas_row = await conn.fetchrow(
                    """
                    SELECT COALESCE(SUM(valor), 0) as total_vendas
                    FROM vendas
                    WHERE TO_DATE(data, 'DD/MM/YYYY') BETWEEN $1 AND $2
                    """,
                    inicio.date(), fim.date()
                )
                
                # 3. Total de pólvora comprada (da tabela polvoras)
                polvora_comprada_row = await conn.fetchrow(
                    """
                    SELECT COALESCE(SUM(quantidade), 0) as total_quantidade,
                           COALESCE(SUM(valor), 0) as total_valor
                    FROM polvoras
                    WHERE data::date BETWEEN $1::date AND $2::date
                    """,
                    inicio, fim
                )
                
                # 4. 🔥 TOTAL DE COMPRAS REGISTRADAS (nova tabela)
                compras_row = None
                total_gasto_compras = 0
                lista_compras = []
                
                if incluir_compras:
                    compras_row = await conn.fetch(
                        """
                        SELECT produto, valor, comprado_por, data
                        FROM compras
                        WHERE data >= $1 AND data <= $2
                        ORDER BY data DESC
                        """,
                        inicio_dt, fim_dt
                    )
                    
                    for compra in compras_row:
                        total_gasto_compras += compra["valor"] or 0
                        lista_compras.append(compra)
            
            # =====================================================
            # ================= CÁLCULOS ==========================
            # =====================================================
            
            total_polvora_gasta = polvora_row["total_polvora"] or 0
            total_gasto_polvora = total_polvora_gasta * PRECO_POLVORA
            
            total_vendas = vendas_row["total_vendas"] or 0
            
            total_polvora_comprada = polvora_comprada_row["total_quantidade"] or 0
            total_gasto_polvora_comprada = polvora_comprada_row["total_valor"] or 0
            
            # Total de gastos (pólvora + embalagens + compras registradas)
            total_gastos = total_gasto_polvora + total_gasto_embalagens + total_gasto_compras
            saldo = total_vendas - total_gastos
            
            # =====================================================
            # ================= FORMATAR VALORES ==================
            # =====================================================
            
            def fmt(valor):
                return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            
            def fmt_num(valor):
                return f"{valor:,.0f}".replace(",", ".")
            
            # =====================================================
            # ================= CRIAR EMBED =======================
            # =====================================================
            
            embed = discord.Embed(
                title="📊 RELATÓRIO FINANCEIRO",
                description=f"📅 **Período:** {self.data_inicio.value} até {self.data_fim.value}",
                color=0x1abc9c
            )
            
            # 💣 PÓLVORA
            embed.add_field(
                name="💣 PÓLVORA",
                value=(
                    f"**Utilizada na produção:** {fmt_num(total_polvora_gasta)} unidades\n"
                    f"**💰 Gasto com pólvora:** {fmt(total_gasto_polvora)}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"**Comprada no período:** {fmt_num(total_polvora_comprada)} unidades\n"
                    f"**💰 Gasto na compra:** {fmt(total_gasto_polvora_comprada)}"
                ),
                inline=False
            )
            
            # 📦 EMBALAGENS (se tiver)
            if total_embalagens > 0:
                embed.add_field(
                    name="📦 EMBALAGENS",
                    value=(
                        f"**Quantidade comprada:** {fmt_num(total_embalagens)} unidades\n"
                        f"**💰 Gasto com embalagens:** {fmt(total_gasto_embalagens)}"
                    ),
                    inline=False
                )
            
            # 🛒 COMPRAS REGISTRADAS (se tiver)
            if incluir_compras and lista_compras:
                total_gasto_compras_fmt = fmt(total_gasto_compras)
                
                # Lista as compras (máximo 10 para não ficar muito grande)
                compras_texto = ""
                for compra in lista_compras[:10]:
                    data = compra["data"]
                    if data.tzinfo is None:
                        data = data.replace(tzinfo=BRASIL)
                    compras_texto += f"• {compra['produto']} - {fmt(compra['valor'])} - {data.strftime('%d/%m')}\n"
                
                if len(lista_compras) > 10:
                    compras_texto += f"\n*... e mais {len(lista_compras) - 10} compras*"
                
                embed.add_field(
                    name="📦 OUTRAS COMPRAS",
                    value=(
                        f"**Total gasto em outras compras:** {total_gasto_compras_fmt}\n"
                        f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                        f"{compras_texto}"
                    ),
                    inline=False
                )
            elif incluir_compras:
                embed.add_field(
                    name="📦 OUTRAS COMPRAS",
                    value="Nenhuma compra registrada no período.",
                    inline=False
                )
            
            # 🛒 VENDAS
            embed.add_field(
                name="🛒 VENDAS",
                value=f"**💰 Total de vendas:** {fmt(total_vendas)}",
                inline=False
            )
            
            # 📊 RESUMO
            cor_saldo = 0x2ecc71 if saldo >= 0 else 0xe74c3c
            emoji_saldo = "🟢" if saldo >= 0 else "🔴"
            
            embed.add_field(
                name="━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
                value="",
                inline=False
            )
            
            # Monta detalhamento dos gastos
            detalhe_gastos = f"• Pólvora: {fmt(total_gasto_polvora)}"
            if total_gasto_embalagens > 0:
                detalhe_gastos += f"\n• Embalagens: {fmt(total_gasto_embalagens)}"
            if incluir_compras and total_gasto_compras > 0:
                detalhe_gastos += f"\n• Outras compras: {fmt(total_gasto_compras)}"
            detalhe_gastos += f"\n• **TOTAL:** {fmt(total_gastos)}"
            
            embed.add_field(
                name="📊 RESUMO FINANCEIRO",
                value=(
                    f"**💰 Total de Vendas:** {fmt(total_vendas)}\n"
                    f"**💸 Total de Gastos:** {fmt(total_gastos)}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"{emoji_saldo} **SALDO:** {fmt(saldo)}\n"
                    f"━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\n"
                    f"**📋 DETALHAMENTO DOS GASTOS:**\n{detalhe_gastos}"
                ),
                inline=False
            )
            
            embed.set_footer(text=f"Relatório gerado em {agora().strftime('%d/%m/%Y às %H:%M')}")
            
            # =====================================================
            # ================= ENVIAR RELATÓRIO ===================
            # =====================================================
            
            canal = interaction.guild.get_channel(1498664038559776768)
            if canal:
                await canal.send(embed=embed)
                await interaction.followup.send(
                    f"✅ Relatório financeiro enviado no canal <#{1498664038559776768}>!",
                    ephemeral=True
                )
            else:
                await interaction.followup.send(embed=embed, ephemeral=True)
                
        except ValueError:
            await interaction.followup.send(
                "❌ **Formato de data inválido!**\nUse o formato: `DD/MM/AAAA`\nExemplo: `01/04/2026`",
                ephemeral=True
            )
        except Exception as e:
            print("ERRO RELATORIO FINANCEIRO:", e)
            await interaction.followup.send(f"❌ Erro ao gerar relatório: {str(e)}", ephemeral=True)
# =========================================================
# ================= VIEW DO PAINEL ========================
# =========================================================

class RelatorioFinanceiroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(
        label="📊 Gerar Relatório Financeiro",
        style=discord.ButtonStyle.success,
        custom_id="relatorio_financeiro_btn",
        emoji="💰"
    )
    async def gerar_relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RelatorioFinanceiroModal())

# =========================================================
# ================= PAINEL RELATÓRIO FINANCEIRO ===========
# =========================================================

async def enviar_painel_relatorio_financeiro():
    """Envia o painel para gerar relatório financeiro"""
    
    canal = bot.get_channel(1498664038559776768)
    
    if not canal:
        print("❌ Canal de relatório financeiro não encontrado")
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
            "• Incluir ou não outras compras (SIM/NAO)\n"
            
        ),
        color=0x1abc9c
    )
    
    embed.add_field(
        name="📌 EXEMPLO DE PREENCHIMENTO",
        value=(
            "**Data inicial:** `01/04/2026`\n"
            "**Data final:** `30/04/2026`\n"
            "**Incluir compras:** `SIM` (ou `NAO`)\n"
            
        ),
        inline=False
    )
    
    embed.set_footer(text="Os valores são calculados automaticamente com base no banco de dados")
    
    await enviar_ou_atualizar_painel(
        "painel_relatorio_financeiro",
        1498664038559776768,
        embed,
        RelatorioFinanceiroView()
    )
    
    print("💰 Painel de relatório financeiro enviado/atualizado")
# =========================================================
# ================= SISTEMA DE REGISTRO DE COMPRAS ========
# =========================================================

# IDs dos canais
CANAL_REGISTRAR_COMPRA_ID = 1498668853465448560  # Canal para registrar compra
CANAL_COMPRAS_REGISTRADAS_ID = 1270467793363669053  # Canal onde vai aparecer a compra registrada


async def salvar_compra_db(produto, valor, comprado_por):
    """Salva uma compra no banco de dados"""
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO compras (produto, valor, comprado_por, data)
            VALUES ($1, $2, $3, $4)
            """,
            produto,
            valor,
            str(comprado_por),
            agora_db()
        )


async def carregar_compras_db(data_inicio=None, data_fim=None):
    """Carrega compras do banco de dados"""
    async with db.acquire() as conn:
        if data_inicio and data_fim:
            rows = await conn.fetch(
                """
                SELECT * FROM compras
                WHERE data >= $1 AND data <= $2
                ORDER BY data DESC
                """,
                data_inicio, data_fim
            )
        else:
            rows = await conn.fetch(
                """
                SELECT * FROM compras
                ORDER BY data DESC
                LIMIT 50
                """
            )
        return rows


# =========================================================
# ================= MODAL DE REGISTRO DE COMPRA ===========
# =========================================================

class RegistrarCompraModal(discord.ui.Modal, title="📝 Registrar Compra"):
    
    produto = discord.ui.TextInput(
        label="📦 Nome do produto",
        placeholder="Ex: Pólvora, Embalagens, Munição, etc",
        required=True,
        max_length=100
    )
    
    valor = discord.ui.TextInput(
        label="💰 Valor da compra",
        placeholder="Ex: 50000",
        required=True
    )
    
    async def on_submit(self, interaction: discord.Interaction):
        
        # Responder imediatamente para evitar timeout
        await interaction.response.defer(ephemeral=True)
        
        # Validar produto
        produto = self.produto.value.strip()
        if not produto:
            await interaction.followup.send("❌ **Produto inválido!**", ephemeral=True)
            return
        
        # Validar valor
        try:
            valor_compra = int(self.valor.value.replace(".", "").replace(",", ""))
            if valor_compra <= 0:
                raise ValueError
        except:
            await interaction.followup.send(
                "❌ **Valor inválido!**\nDigite apenas números (ex: 50000, 1500, 2000)",
                ephemeral=True
            )
            return
        
        # Salvar no banco
        await salvar_compra_db(produto, valor_compra, interaction.user.id)
        
        # Formatar valores
        def fmt(valor):
            return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        
        data_atual = agora()
        
        # Criar embed para enviar no canal de compras registradas
        embed = discord.Embed(
            title="📦 NOVA COMPRA REGISTRADA",
            color=0x3498db,
            timestamp=data_atual
        )
        
        embed.add_field(name="📦 Produto", value=produto, inline=True)
        embed.add_field(name="💰 Valor", value=fmt(valor_compra), inline=True)
        embed.add_field(name="👤 Comprado por", value=interaction.user.mention, inline=True)
        embed.add_field(name="📅 Data da compra", value=f"<t:{int(data_atual.timestamp())}:F>", inline=False)
        
        embed.set_footer(text=f"Compra registrada no sistema")
        
        # Enviar no canal de compras registradas
        canal_destino = interaction.guild.get_channel(CANAL_COMPRAS_REGISTRADAS_ID)
        
        if canal_destino:
            await canal_destino.send(embed=embed)
            await interaction.followup.send(
                f"✅ **Compra registrada com sucesso!**\n"
                f"📦 Produto: {produto}\n"
                f"💰 Valor: {fmt(valor_compra)}\n"
                f"📅 Data: {data_atual.strftime('%d/%m/%Y às %H:%M')}\n\n"
                f"📨 A compra foi registrada no canal <#{CANAL_COMPRAS_REGISTRADAS_ID}>",
                ephemeral=True
            )
        else:
            await interaction.followup.send(
                f"✅ **Compra registrada com sucesso!**\n"
                f"📦 Produto: {produto}\n"
                f"💰 Valor: {fmt(valor_compra)}",
                ephemeral=True
            )
            print(f"❌ Canal {CANAL_COMPRAS_REGISTRADAS_ID} não encontrado!")


# =========================================================
# ================= VIEW DO BOTÃO =========================
# =========================================================

class RegistrarCompraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
    
    @discord.ui.button(
        label="📝 Registrar Nova Compra",
        style=discord.ButtonStyle.success,
        custom_id="registrar_compra_btn",
        emoji="💰"
    )
    async def registrar_compra(self, interaction: discord.Interaction, button: discord.ui.Button):
        # Abrir o modal imediatamente
        await interaction.response.send_modal(RegistrarCompraModal())


# =========================================================
# ================= PAINEL DE REGISTRO DE COMPRA ==========
# =========================================================

async def enviar_painel_registrar_compra():
    """Envia o painel para registrar compras"""
    
    canal = bot.get_channel(CANAL_REGISTRAR_COMPRA_ID)
    
    if not canal:
        print(f"❌ Canal de registrar compra não encontrado: {CANAL_REGISTRAR_COMPRA_ID}")
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
    
    embed.add_field(
        name="📌 EXEMPLO",
        value="**Produto:** Pólvora\n**Valor:** 50000",
        inline=False
    )
    
    embed.set_footer(text="Todas as compras ficam salvas no banco de dados para relatórios futuros")
    
    # 🔥 DELETAR MENSAGENS ANTIGAS
    async for msg in canal.history(limit=10):
        if msg.author == bot.user and msg.embeds:
            if msg.embeds[0].title == "💰 REGISTRAR COMPRA":
                try:
                    await msg.delete()
                    print(f"🗑️ Mensagem antiga do registrar compra deletada")
                except:
                    pass
    
    # Criar nova mensagem
    await canal.send(embed=embed, view=RegistrarCompraView())
    print(f"💰 Painel de registrar compra enviado para o canal {CANAL_REGISTRAR_COMPRA_ID}")
    
# =========================================================
# ========================= ON_READY ======================
# =========================================================

@bot.event
async def on_ready():

    # 🔥 PROTEÇÃO CONTRA DUPLO on_ready
    if hasattr(bot, "ja_iniciado"):
        return

    bot.ja_iniciado = True

    global db
    global http_session

    print("🔄 Iniciando configuração do bot...")


    global fila_clipes
    fila_clipes = asyncio.Queue()

    bot.loop.create_task(worker_clipes())

    print(f"Logado como {bot.user}")
    print("🎬 Sistema de clips ON")
    # =====================================================
    # ================= HTTP SESSION ======================
    # =====================================================

    if not http_session:
        http_session = aiohttp.ClientSession()

    # =====================================================
    # ================= CONECTA NO POSTGRES ===============
    # =====================================================

    if not db:
        await conectar_db()

    # =====================================================
    # ================= CARREGAR GUILD ====================
    # =====================================================

    guild = bot.get_guild(GUILD_ID)

    if guild:
        try:
            await guild.chunk()
            print("👥 Membros carregados no cache.")
        except Exception as e:
            print("Erro ao carregar membros:", e)

    print(f"🕒 Horário Brasília: {agora().strftime('%d/%m/%Y %H:%M:%S')}")

    if guild:
        await enviar_painel_acoes(guild)
    

    # =====================================================
    # ================= CACHE DE METAS ====================
    # =====================================================

    try:
        await carregar_metas_cache()
        print(f"📊 Metas carregadas: {len(metas_cache)}")
    except Exception as e:
        print("Erro ao carregar metas:", e)

    # =====================================================
    # ========== REGISTRAR VIEWS PERSISTENTES =============
    # =====================================================

    try:
        bot.add_view(SolicitarSalaView())
    except Exception as e:
        print("Erro registrando SolicitarSalaView:", e)

    outras_views = [
        RegistroView,
        CadastrarLiveView,
        PainelLivesAdmin,
        GerenciarLivesView,
        PolvoraView,
        ConfirmarPagamentoView,
        LavagemView,
        FabricacaoView,
        CalculadoraView,
        StatusView,
        PainelAcoesView        
    ]

    for view_class in outras_views:
        try:
            bot.add_view(view_class())
        except Exception as e:
            print(f"Erro ao registrar view {view_class.__name__}:", e)

    # =====================================================
    # ================= INICIAR LOOPS =====================
    # =====================================================

    try:
        if not verificar_lives.is_running():
            verificar_lives.start()
    except Exception as e:
        print("Erro loop lives:", e)

    # Carregar aluguéis
    try:
        await carregar_alugueis_db()
        print(f"💰 Aluguéis carregados: {len(alugueis_ativos)}")
    except Exception as e:
        print("Erro carregar aluguéis:", e)

    # Iniciar loops
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

    # Enviar painel
    try:
        await enviar_painel_alugueis()
    except Exception as e:
        print("Erro painel aluguéis:", e)
        
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
        if 'reset_acoes_segunda' in globals():
            if not reset_acoes_segunda.is_running():
                reset_acoes_segunda.start()
    except Exception as e:
        print("Erro reset ações semana:", e)

    
        agora_br = agora()

        if agora_br.weekday() == 0 and agora_br.hour == 0 and agora_br.minute == 0:

           async with db.acquire() as conn:
                await conn.execute("DELETE FROM acoes_semana")
                await conn.execute("DELETE FROM participantes_acoes")

           print("♻️ Ações resetadas (segunda)")

    # ========== INICIA LOOP DE VERIFICAÇÃO ==========
    if not verificar_producoes_orfas.is_running():
        verificar_producoes_orfas.start()
        print("🔄 Loop de verificação de produções iniciado")
    
    # ========== RESTAURA PRODUÇÕES ATIVAS ==========
    await restaurar_producoes()
    
    # =====================================================
    # ================= RESTAURAR GALPÕES ATIVOS ==========
    # =====================================================

    try:
        async with db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT galpao FROM producoes
                WHERE CAST(fim AS timestamp) > NOW()
                """
            )

        for r in rows:
            galpoes_ativos.add(r["galpao"])

        print(f"🏭 Galpões ativos restaurados: {len(rows)}")

    except Exception as e:
        print("Erro restaurar galpões:", e)

    # =====================================================
    # ================= ENVIAR PAINÉIS ====================
    # =====================================================

    await asyncio.sleep(2)

    try:

        painel_tasks = []

        try:
            painel_tasks.append(enviar_painel_registro())
        except:
            pass

        try:
            painel_tasks.append(enviar_painel_fabricacao())
        except:
            pass

        try:
            painel_tasks.append(enviar_painel_lives())
        except:
            pass

        try:
            painel_tasks.append(enviar_painel_admin_lives())
        except:
            pass

        try:
            painel_tasks.append(enviar_painel_polvoras())
        except:
            pass

        try:
            painel_tasks.append(enviar_painel_lavagem())
        except:
            pass

        try:
            painel_tasks.append(enviar_painel_vendas())
        except:
            pass

        try:
            painel_tasks.append(enviar_painel_remover_ausencia())
        except Exception as e:
            print("Erro painel remover ausência:", e)

        # =====================================================
        # ================= PAINEL RELATÓRIO FINANCEIRO =======
        # =====================================================

        try:
            await enviar_painel_relatorio_financeiro()
        except Exception as e:
            print("Erro painel relatório financeiro:", e)

        # =====================================================
        # ================= PAINEL REGISTRAR COMPRA ===========
        # =====================================================

        try:
            await enviar_painel_registrar_compra()
        except Exception as e:
            print("Erro painel registrar compra:", e)

       # 🔥 PAINEL AÇÕES (CORRETO)
        try:
            if guild:
                painel_tasks.append(enviar_painel_acoes(guild))
        except Exception as e:
            print("Erro painel ações:", e)

        try:
            painel_tasks.append(enviar_painel_solicitar_sala())
        except:
            pass
        try:
            painel_tasks.append(enviar_painel_botao_ausencia())
        except Exception as e:
            print("Erro painel botão ausência:", e)
        
        results = await asyncio.gather(*painel_tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                print("Erro em painel específico:", r)

        print("🖥️ Painéis verificados/enviados.")

    except Exception as e:
        print("Erro geral ao enviar painéis:", e)

    # 🔥 BOTÃO RESET AÇÕES
        try:
            canal = guild.get_channel(CANAL_RELATORIO_ACOES_ID)

            if canal:

        # evita duplicar botão
                async for msg in canal.history(limit=10):
                    if msg.author == bot.user and msg.components:
                        break
                else:
                    await canal.send(
                        "🔄 Controle de Ações:",
                        view=ResetAcoesView()
                    )

        except Exception as e:
            print("Erro botão reset ações:", e)

       
    # =====================================================
    # ================= INICIAR EDIT WORKER ===============
    # =====================================================

    try:
        if not hasattr(bot, "edit_worker_started"):

            bot.loop.create_task(edit_worker())

            bot.edit_worker_started = True

            print("🛠️ Edit worker iniciado.")

    except Exception as e:
        print("Erro iniciando edit worker:", e)
    

    bot.loop.create_task(worker_clipes())
    print("🎬 Sistema de clips ON")

    # =====================================================
    # ================= LIMPEZA FINAL =====================
    # =====================================================

    gc.collect()

    print("🧹 Limpeza de memória executada")
    print("=========================================")
    print("✅ BOT ONLINE 100% ESTÁVEL")
    print("=========================================")
    

# =========================================================
# ========================= START BOT =====================
# =========================================================

if __name__ == "__main__":
    print("🚀 Iniciando bot...")
    bot.run(TOKEN)
