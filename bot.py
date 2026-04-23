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
    """Retorna data/hora atual no horário do Brasil"""
    return datetime.now(BRASIL)

def agora_db():
    """Retorna data/hora sem fuso horário para salvar no banco"""
    return datetime.now(BRASIL).replace(tzinfo=None)

def str_para_datetime(data_str):
    """Converte string do banco para datetime com timezone"""
    if not data_str:
        return None
    dt = datetime.fromisoformat(data_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=BRASIL)
    return dt
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
# ===================== BANCO DE DADOS =====================
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

        # ===============================
        # PEGAR PACOTES DO EMBED
        # ===============================

        pacotes_pt = 0
        pacotes_sub = 0

        for field in embed.fields:

            if field.name == "🔫 PT":
                try:
                    linhas = field.value.split("\n")
                    for l in linhas:
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
                    linhas = field.value.split("\n")
                    for l in linhas:
                        if "📦" in l:
                            pacotes_sub = int(
                                l.replace("📦", "")
                                .replace("pacotes", "")
                                .strip()
                            )
                except:
                    pass


        # ===============================
        # ENVIAR NO BAÚ
        # ===============================

        canal_bau = interaction.guild.get_channel(
            CANAL_BAU_GALPAO_SUL_ID
        )

        if canal_bau:

            try:

                await canal_bau.send(
                    f"📦 **Retirada do Baú**\n\n"
                    f"👤 Retirado por: {interaction.user.mention}\n"
                    f"🔫 PT: {pacotes_pt} pacotes\n"
                    f"🔫 SUB: {pacotes_sub} pacotes"
                )

            except Exception as e:
                print("Erro envio baú:", e)

        else:
            print("Canal do baú não encontrado.")


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
        description="Escolha uma opção abaixo.",
        color=0x2ecc71
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
# ================= FUNÇÕES DE PRODUÇÃO ===================
# =========================================================

async def carregar_producao(pid):

    async with db.acquire() as conn:

        r = await conn.fetchrow(
            "SELECT * FROM producoes WHERE pid=$1",
            pid
        )

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
        "polvora": r["polvora"] or 400
    }

    if r["segunda_task_user"]:
        dados["segunda_task_confirmada"] = {
            "user": int(r["segunda_task_user"]),
            "time": r["segunda_task_time"]
        }

    return dados


async def salvar_producao(pid, dados):

    segunda_user = None
    segunda_time = None

    if "segunda_task_confirmada" in dados:

        segunda_user = str(dados["segunda_task_confirmada"]["user"])
        segunda_time = dados["segunda_task_confirmada"]["time"]

    async with db.acquire() as conn:

        await conn.execute(
            """
            INSERT INTO producoes 
            (pid, galpao, autor, inicio, fim, obs, msg_id, canal_id, segunda_task_user, segunda_task_time, polvora)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
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
            polvora=$11
            """,
            pid,
            dados["galpao"],
            str(dados["autor"]),
            dados["inicio"],
            dados["fim"],
            dados["obs"],
            str(dados["msg_id"]),
            str(dados["canal_id"]),
            segunda_user,
            segunda_time,
            dados.get("polvora", 400)
        )


async def deletar_producao(pid):

    async with db.acquire() as conn:

        await conn.execute(
            "DELETE FROM producoes WHERE pid=$1",
            pid
        )

# =========================================================

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
# ================= MODAL OBSERVAÇÃO ======================
# =========================================================

class ObservacaoProducaoModal(discord.ui.Modal, title="Iniciar Produção"):

    obs = discord.ui.TextInput(
        label="Observação inicial",
        style=discord.TextStyle.paragraph,
        required=False
    )

    def __init__(self, galpao, tempo):
        super().__init__()
        self.galpao = galpao
        self.tempo = tempo

    async def on_submit(self, interaction: discord.Interaction):

        await interaction.response.defer(ephemeral=True)

        pid = f"{self.galpao}_{interaction.id}_{int(time_module.time())}"

        inicio = agora()
        fim = inicio + timedelta(minutes=self.tempo)

        canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)

        desc = (
            f"**Galpão:** {self.galpao}\n"
            f"**Iniciado por:** {interaction.user.mention}\n"
        )

        if self.obs.value:
            desc += f"📝 **Obs:** {self.obs.value}\n"

        desc += (
            f"Início: <t:{int(inicio.timestamp())}:t>\n"
            f"Término: <t:{int(fim.timestamp())}:t>\n\n"
            f"⏳ **Restante:** {self.tempo} min\n"
            f"{barra(0)}"
        )

        msg = await canal.send(
            embed=discord.Embed(
                title="🏭 Produção",
                description=desc,
                color=0x3498db
            ),
            view=SegundaTaskView(pid)
        )

        dados = {
            "galpao": self.galpao,
            "autor": interaction.user.id,
            "inicio": inicio.isoformat(),
            "fim": fim.isoformat(),
            "obs": self.obs.value,
            "polvora": 400,
            "msg_id": msg.id,
            "canal_id": CANAL_REGISTRO_GALPAO_ID
        }

        await salvar_producao(pid, dados)

        await asyncio.sleep(1)

        if pid not in producoes_tasks:

            task = bot.loop.create_task(
                acompanhar_producao(pid)
            )

            producoes_tasks[pid] = task

# =========================================================
# ================= 2ª TASK ===============================
# =========================================================

class SegundaTaskView(discord.ui.View):

    def __init__(self, pid):
        super().__init__(timeout=None)
        self.pid = pid

    @discord.ui.button(
        label="✅ Confirmar 2ª Task",
        style=discord.ButtonStyle.success,
        custom_id="segunda_task_btn"
    )
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):

        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True)

        try:

            prod = await carregar_producao(self.pid)

            if not prod:
                return

            prod["segunda_task_confirmada"] = {
                "user": interaction.user.id,
                "time": agora().isoformat()
            }

            await salvar_producao(self.pid, prod)

            await interaction.message.edit(view=None)

        except Exception as e:
            print("Erro segunda task:", e)

# =========================================================
# ================= MODAL PÓLVORA =========================
# =========================================================

class PolvoraProducaoModal(discord.ui.Modal, title="Iniciar Produção"):

    polvora = discord.ui.TextInput(
        label="Quantas pólvoras foram colocadas?",
        placeholder="Ex: 400"
    )

    obs = discord.ui.TextInput(
        label="Observação (opcional)",
        style=discord.TextStyle.paragraph,
        required=False
    )

    def __init__(self, galpao, tempo):
        super().__init__()
        self.galpao = galpao
        self.tempo = tempo

    async def on_submit(self, interaction: discord.Interaction):

        await interaction.response.defer(ephemeral=True)

        try:

            try:
                polvora = int(self.polvora.value)
            except:
                await interaction.followup.send(
                    "Quantidade inválida.",
                    ephemeral=True
                )
                return

            pid = f"{self.galpao}_{interaction.id}_{int(time_module.time())}"

            inicio = agora()
            tempo_real = max(1, int(self.tempo * (polvora / 400)))
            fim = inicio + timedelta(minutes=tempo_real)

            canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)

            if not canal:
                await interaction.followup.send(
                    "Canal de produção não encontrado.",
                    ephemeral=True
                )
                return

            desc = (
                f"**Galpão:** {self.galpao}\n"
                f"**Iniciado por:** {interaction.user.mention}\n"
            )

            if self.obs.value:
                desc += f"📝 **Obs:** {self.obs.value}\n"

            desc += (
                f"Início: <t:{int(inicio.timestamp())}:t>\n"
                f"Término: <t:{int(fim.timestamp())}:t>\n\n"
                f"⏳ **Restante:** {tempo_real} min\n"
                f"{barra(0)}"
            )

            msg = await canal.send(
                embed=discord.Embed(
                    title="🏭 Produção",
                    description=desc,
                    color=0x3498db
                ),
                view=SegundaTaskView(pid)
            )

            dados = {
                "galpao": self.galpao,
                "autor": interaction.user.id,
                "inicio": inicio.isoformat(),
                "fim": fim.isoformat(),
                "obs": self.obs.value,
                "polvora": polvora,
                "msg_id": msg.id,
                "canal_id": CANAL_REGISTRO_GALPAO_ID
            }

            await salvar_producao(pid, dados)

            if pid not in producoes_tasks:
                task = asyncio.create_task(
                    acompanhar_producao(pid)
                )
                producoes_tasks[pid] = task

        except Exception as e:
            print("ERRO PRODUÇÃO:", e)

# =========================================================
# ================= VIEW FABRICAÇÃO ========================
# =========================================================

class FabricacaoView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🏭 Galpões Norte", style=discord.ButtonStyle.primary, custom_id="fabricacao_norte")
    async def norte(self, interaction: discord.Interaction, button: discord.ui.Button):

        async with db.acquire() as conn:
            ativo = await conn.fetchval(
                """
                SELECT 1 FROM producoes
                WHERE galpao=$1 AND CAST(fim AS timestamp) > NOW()
                """,
                "GALPÕES NORTE"
            )

        if ativo:
            await interaction.response.send_message("Já em produção.", ephemeral=True)
            return

        await interaction.response.send_modal(
            PolvoraProducaoModal("GALPÕES NORTE", 65)
        )

    @discord.ui.button(label="🏭 Galpões Sul", style=discord.ButtonStyle.secondary, custom_id="fabricacao_sul")
    async def sul(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(
            PolvoraProducaoModal("GALPÕES SUL", 130)
        )

    @discord.ui.button(label="🏭 ", style=discord.ButtonStyle.primary, custom_id="fabricacao_")
    async def (self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(
            PolvoraProducaoModal("", 65)
        )

    @discord.ui.button(label="📊 Relatório Produção", style=discord.ButtonStyle.success, custom_id="fabricacao_relatorio")
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(
            RelatorioProducaoModal()
        )

# =========================================================
# ================= LOOP DE ACOMPANHAMENTO =================
# =========================================================

async def acompanhar_producao(pid):
    print(f"▶ Produção iniciada: {pid}")
    msg = None

    while not bot.is_closed():
        prod = await carregar_producao(pid)
        
        if not prod:
            print(f"❌ Produção {pid} não encontrada")
            return

        canal = pegar_canal(prod["canal_id"])
        
        if not canal:
            await asyncio.sleep(10)
            continue

        if msg is None:
            try:
                msg = await canal.fetch_message(int(prod["msg_id"]))
            except Exception as e:
                print(f"Erro buscar mensagem {pid}:", e)
                await asyncio.sleep(5)
                continue

        # USANDO A FUNÇÃO CORRIGIDA
        inicio = str_para_datetime(prod["inicio"])
        fim = str_para_datetime(prod["fim"])
        agora_dt = agora()

        # Se já passou do fim, finaliza
        if agora_dt >= fim:
            await finalizar_producao(pid, msg, prod)
            return

        # Calcula tempo restante
        total = (fim - inicio).total_seconds()
        restante = (fim - agora_dt).total_seconds()
        restante = max(0, restante)
        
        if total <= 0:
            total = 1

        pct = 1 - (restante / total)
        pct = max(0, min(1, pct))

        mins = int(restante // 60)
        segundos = int(restante % 60)

        # Monta a mensagem
        desc = (
            f"**Galpão:** {prod['galpao']}\n"
            f"**Iniciado por:** <@{prod['autor']}>\n"
        )

        if prod.get("obs"):
            desc += f"📝 **Obs:** {prod['obs']}\n"

        desc += (
            f"Início: <t:{int(inicio.timestamp())}:t>\n"
            f"Término: <t:{int(fim.timestamp())}:t>\n\n"
            f"⏳ **Restante:** {mins}m {segundos}s\n"
            f"{barra(pct)}"
        )

        if prod.get("segunda_task_confirmada"):
            uid = prod["segunda_task_confirmada"]["user"]
            desc += f"\n\n✅ **Segunda task concluída por:** <@{uid}>"

        # Atualiza a mensagem
        try:
            await msg.edit(
                embed=discord.Embed(
                    title="🏭 Produção",
                    description=desc,
                    color=0x34495e
                )
            )
        except Exception as e:
            print(f"Erro ao editar mensagem {pid}:", e)

        await asyncio.sleep(5)

async def finalizar_producao(pid, msg, prod):
    """Finaliza a produção e registra no banco"""
    
    print(f"🔵 FINALIZANDO produção {pid}")
    
    polvora = prod.get("polvora", 400)
    segunda = prod.get("segunda_task_confirmada")
    
    # Calcula produção baseada no galpão
    base = 0
    if prod["galpao"] == "GALPÕES NORTE":
        base = 1777 if segunda else 1688
    elif prod["galpao"] == "GALPÕES SUL":
        base = 1618 if segunda else 1608
    elif prod["galpao"] == "":
        base = 1777 if segunda else 1688
    
    capsulas = (base * polvora) // 400
    peso = capsulas * 0.05
    
    print(f"📊 {prod['galpao']}: {capsulas} cápsulas, {peso}kg")
    
    # Salva no banco
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO producoes_finalizadas
            (user_id, capsulas, data)
            VALUES ($1,$2,$3)
            """,
            str(prod["autor"]),
            capsulas,
            agora_db()
        )
    
    # Pega a descrição atual
    desc = msg.embeds[0].description if msg.embeds else ""
    
    # 🔥 REMOVE a linha do timer e da barra
    linhas = desc.split("\n")
    novas_linhas = []
    
    for linha in linhas:
        # Pula linhas que contém timer ou barra de progresso
        if not linha.startswith("⏳ **Restante:**") and not "▓" in linha and not "░" in linha:
            # Também pula linhas vazias que sobraram
            if linha.strip():
                novas_linhas.append(linha)
    
    desc = "\n".join(novas_linhas)
    
    # Adiciona as informações finais
    desc += (
        f"\n\n🔵 **Produção Finalizada**"
        f"\n\n🧪 Produziu **{capsulas} cápsulas**"
        f"\n⚖️ Peso total: **{peso:.2f} kg**"
        f"\n💣 Pólvora utilizada: **{polvora}**"
    )
    
    # Atualiza a mensagem final
    await msg.edit(
        embed=discord.Embed(
            title="🏭 Produção",
            description=desc,
            color=0x34495e
        ),
        view=None
    )
    
    # Remove do banco
    await deletar_producao(pid)
    
    # Remove da lista de tarefas ativas
    if pid in producoes_tasks:
        del producoes_tasks[pid]
    
    print(f"✅ Produção {pid} finalizada com {capsulas} cápsulas")
# =========================================================
# ================= RELATÓRIO ==============================
# =========================================================

class RelatorioProducaoModal(discord.ui.Modal, title="📊 Relatório de Produção"):

    data_inicio = discord.ui.TextInput(label="Data inicial")
    data_fim = discord.ui.TextInput(label="Data final")

    async def on_submit(self, interaction: discord.Interaction):

        try:
            await interaction.response.defer(ephemeral=True)

            inicio = datetime.strptime(self.data_inicio.value.strip(), "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value.strip(), "%d/%m/%Y") + timedelta(days=1)

            async with db.acquire() as conn:
                rows = await conn.fetch(
                    """
                    SELECT user_id, SUM(capsulas) as total
                    FROM producoes_finalizadas
                    WHERE data >= $1 AND data < $2
                    GROUP BY user_id
                    """,
                    inicio, fim
                )

            if not rows:
                await interaction.followup.send("Sem dados.", ephemeral=True)
                return

            total_capsulas = sum(int(r["total"] or 0) for r in rows)

            ranking = sorted(
                [(str(r["user_id"]), int(r["total"])) for r in rows],
                key=lambda x: x[1],
                reverse=True
            )

            linhas = [
                f"<@{uid}> — {total:,}".replace(",", ".")
                for uid, total in ranking[:20]
            ]

            embed = discord.Embed(
                title="📊 RELATÓRIO",
                description=f"Total: {total_capsulas:,}".replace(",", ".")
            )

            embed.add_field(name="Ranking", value="\n".join(linhas))

            canal = interaction.guild.get_channel(1422853066541109338)
            if canal:
                await canal.send(embed=embed)

            await interaction.followup.send("Enviado.", ephemeral=True)

        except Exception as e:
            print("ERRO RELATORIO:", e)
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

        lives.setdefault(uid, [])

        lives[uid].append({
            "link": r["link"],
            "divulgado": r["divulgado"]
        })

    return lives


async def salvar_live(user_id, link):

    async with db.acquire() as conn:

        await conn.execute(
            """
            INSERT INTO lives (user_id, link, divulgado)
            VALUES ($1, $2, false)
            """,
            str(user_id),
            link
        )


async def atualizar_divulgado(link, valor):

    async with db.acquire() as conn:

        await conn.execute(
            """
            UPDATE lives
            SET divulgado=$1
            WHERE link=$2
            """,
            valor,
            link
        )


async def remover_live_db(user_id):

    async with db.acquire() as conn:

        await conn.execute(
            "DELETE FROM lives WHERE user_id=$1",
            str(user_id)
        )

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

        lives.setdefault(uid, [])

        lives[uid].append({
            "link": r["link"],
            "divulgado": r["divulgado"]
        })

    return lives


async def salvar_live(user_id, link):

    async with db.acquire() as conn:

        await conn.execute(
            """
            INSERT INTO lives (user_id, link, divulgado)
            VALUES ($1, $2, false)
            """,
            str(user_id),
            link
        )


async def atualizar_divulgado(link, valor):

    async with db.acquire() as conn:

        await conn.execute(
            """
            UPDATE lives
            SET divulgado=$1
            WHERE link=$2
            """,
            valor,
            link
        )


async def remover_live_db(user_id):

    async with db.acquire() as conn:

        await conn.execute(
            "DELETE FROM lives WHERE user_id=$1",
            str(user_id)
        )

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

        async with http_session.get(url, headers=headers) as r:

            data = await r.json()

            if data.get("data"):

                info = data["data"][0]

                thumbnail = (
                    info["thumbnail_url"]
                    .replace("{width}", "1280")
                    .replace("{height}", "720")
                )

                return True, info.get("title"), info.get("game_name"), thumbnail

        return False, None, None, None

    except Exception as e:

        print("Erro Twitch API:", e)
        return False, None, None, None


# =========================================================
# ================= CHECAR KICK ===========================
# =========================================================

async def checar_kick(canal):

    try:

        url = f"https://kick.com/api/v2/channels/{canal}"

        async with http_session.get(url) as r:

            data = await r.json()

        if data.get("livestream"):

            titulo = data["livestream"]["session_title"]
            thumb = data["livestream"]["thumbnail"]["url"]

            return True, titulo, None, thumb

        return False, None, None, None

    except:
        return False, None, None, None


# =========================================================
# ================= CHECAR TIKTOK =========================
# =========================================================

async def checar_tiktok(username):
    """Verifica se um usuário do TikTok está ao vivo"""
    try:
        # Limpa o username
        username = username.lower().replace("@", "").strip()
        
        # URL do perfil
        url = f"https://www.tiktok.com/@{username}"
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "pt-BR,pt;q=0.9,en;q=0.8",
            "Referer": "https://www.tiktok.com/",
        }
        
        async with http_session.get(url, headers=headers, timeout=10) as response:
            html = await response.text()
        
        # Procura pelo status da live no HTML
        # Método 1: Verificar no JSON embutido
        import re
        
        # Procura pelo padrão "isLive":true no HTML
        live_patterns = [
            r'"isLive":\s*true',
            r'"liveStatus":\s*1',
            r'"liveRoom":\s*\{',
            r'"status":\s*"live"',
            r'livePageProps.*?liveStatus.*?1',
        ]
        
        is_live = False
        for pattern in live_patterns:
            if re.search(pattern, html, re.IGNORECASE):
                is_live = True
                break
        
        # Método 2: Verificar se tem URL de live
        if not is_live:
            live_url_pattern = r'https://www\.tiktok\.com/@' + re.escape(username) + r'/live'
            if re.search(live_url_pattern, html):
                is_live = True
        
        if not is_live:
            return False, None, None, None
        
        # Tenta extrair título da live
        titulo = f"Live no TikTok - @{username}"
        
        title_patterns = [
            r'"title":"([^"]+)"',
            r'"roomId":"[^"]+","title":"([^"]+)"',
            r'"liveTitle":"([^"]+)"',
        ]
        
        for pattern in title_patterns:
            match = re.search(pattern, html)
            if match:
                titulo = match.group(1)
                break
        
        # Tenta extrair thumbnail
        thumbnail = None
        thumb_patterns = [
            r'"coverUrl":"([^"]+)"',
            r'"thumbnailUrl":"([^"]+)"',
            r'"liveCover":"([^"]+)"',
        ]
        
        for pattern in thumb_patterns:
            match = re.search(pattern, html)
            if match:
                thumbnail = match.group(1).replace("\\/", "/")
                break
        
        return True, titulo, "TikTok", thumbnail
        
    except Exception as e:
        print(f"Erro ao verificar TikTok para {username}: {e}")
        return False, None, None, None

# =========================================================
# ================= DIVULGAÇÃO ============================
# =========================================================

async def divulgar_live(user_id, link, titulo, jogo, thumbnail):
    """Divulga a live no canal designado"""
    try:
        canal = bot.get_channel(CANAL_DIVULGACAO_LIVE_ID)
        if not canal:
            print(f"❌ Canal de divulgação não encontrado: {CANAL_DIVULGACAO_LIVE_ID}")
            return

        user = await pegar_usuario(int(user_id))
        if not user:
            print(f"❌ Usuário {user_id} não encontrado")
            return

        plataforma = detectar_plataforma(link)

        # Cores e nomes das plataformas
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

        # Ícones para cada plataforma
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
            f"📺 **Plataforma:** {nomes.get(plataforma, plataforma)}\n"
        )
        
        if jogo and jogo != "TikTok":
            embed.description += f"🎮 **Jogo:** {jogo}\n"
            
        embed.description += f"📝 **Título:** {titulo or 'Sem título'}\n\n"
        embed.description += f"🔗 **Assistir:** {link}"

        if thumbnail:
            embed.set_image(url=thumbnail)
        elif plataforma == "tiktok":
            # Thumbnail padrão para TikTok se não tiver
            embed.set_thumbnail(url="https://cdn-icons-png.flaticon.com/512/3046/3046126.png")

        embed.set_footer(text=f"Live iniciada • {agora().strftime('%H:%M')}")

        # Para TikTok, não marcar @everyone para evitar spam
        if plataforma == "tiktok":
            await canal.send(
                content=f"🔴 {user.mention} está ao vivo no TikTok!",
                embed=embed,
                allowed_mentions=discord.AllowedMentions(users=True)
            )
        else:
            await canal.send(
                content="@everyone 🔴 Live iniciada!",
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=True)
            )

        print(f"✅ Live divulgada: {plataforma} - {user.name}")

    except Exception as e:
        print(f"❌ Erro ao divulgar live: {e}")
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


# =========================================================
# ================= EXTRAIR CANAL =========================
# =========================================================

def extrair_canal(link):

    link = link.lower().strip()

    link = link.replace("https://", "")
    link = link.replace("http://", "")
    link = link.replace("www.", "")

    partes = link.split("/")

    if "twitch.tv" in link:
        return partes[1]

    if "kick.com" in link:
        return partes[1]

    if "tiktok.com" in link:
        user = partes[1].replace("@", "")
        user = user.split("?")[0]
        return user

    return None
# =========================================================
# ================= LOOP =========================
# =========================================================

@tasks.loop(minutes=2)
async def verificar_lives():
    """Verifica todas as lives cadastradas"""
    
    print("🔄 Verificando lives...")
    
    try:
        lives = await carregar_lives()
        
        if not lives:
            return
        
        for user_id, lista_lives in lives.items():
            for data in lista_lives:
                link = data.get("link", "")
                divulgado = data.get("divulgado", False)
                
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
                    elif plataforma == "kick":
                        ao_vivo, titulo, jogo, thumbnail = await checar_kick(canal_name)
                    elif plataforma == "tiktok":
                        ao_vivo, titulo, jogo, thumbnail = await checar_tiktok(canal_name)
                    else:
                        continue
                        
                except Exception as e:
                    print(f"Erro ao verificar {plataforma}/{canal_name}: {e}")
                    continue
                
                # Se não está ao vivo mas estava marcado como divulgado, reseta
                if not ao_vivo and divulgado:
                    await atualizar_divulgado(link, False)
                    print(f"📴 Live encerrada: {plataforma}/{canal_name}")
                
                # Se está ao vivo e não foi divulgado ainda
                if ao_vivo and not divulgado:
                    print(f"🔴 LIVE detectada ({plataforma}): {canal_name}")
                    
                    await divulgar_live(
                        user_id,
                        link,
                        titulo,
                        jogo,
                        thumbnail
                    )
                    
                    await atualizar_divulgado(link, True)
                    print(f"✅ Live divulgada: {plataforma}/{canal_name}")
                    
    except Exception as e:
        print(f"❌ Erro no loop de lives: {e}")

# =========================================================
# ================= CADASTRO DE LIVE ======================
# =========================================================

class CadastrarLiveModal(discord.ui.Modal, title="🎥 Cadastrar Live"):

    link = discord.ui.TextInput(
        label="Cole o link da sua live",
        placeholder="https://twitch.tv/seucanal"
    )

    async def on_submit(self, interaction: discord.Interaction):

        lives = await carregar_lives()

        novo_link = self.link.value.strip().lower()
        novo_link = novo_link.split("?")[0].rstrip("/")

        plataforma = detectar_plataforma(novo_link)
        novo_canal = extrair_canal(novo_link)

        if not plataforma or not novo_canal:
            await interaction.response.send_message(
                "❌ Link inválido.",
                ephemeral=True
            )
            return

        plataforma_nova = detectar_plataforma(novo_link)

        for uid, lista_lives in lives.items():

            if str(uid) != str(interaction.user.id):
                continue

            for data in lista_lives:

                link_existente = data.get("link", "")

                plataforma_existente = detectar_plataforma(link_existente)
                canal_existente = extrair_canal(link_existente)

                if (
                    plataforma_existente == plataforma_nova
                    and canal_existente == novo_canal
                ):
                    await interaction.response.send_message(
                        "❌ Você já cadastrou esse canal nessa plataforma.",
                        ephemeral=True
                    )
                    return

        await salvar_live(interaction.user.id, novo_link)

        embed = discord.Embed(
            title="✅ Live cadastrada!",
            description=f"{interaction.user.mention}\n{novo_link}",
            color=0x2ecc71
        )

        await interaction.response.send_message(
            embed=embed,
            ephemeral=True
        )

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
    async def cadastrar(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(
            CadastrarLiveModal()
        )

# =========================================================
# ================= SELECT REMOVER LIVE ===================
# =========================================================

class SelectRemoverLive(discord.ui.Select):

    def __init__(self):

        self.options_list = []

        super().__init__(
            placeholder="Carregando...",
            options=[]
        )

    async def refresh_options(self):

        lives = await carregar_lives()

        self.options = [
            discord.SelectOption(
                label=data["link"][:80],
                description=f"Usuário: {uid}",
                value=uid
            )
            for uid, data in lives.items()
        ] or [
            discord.SelectOption(label="Nenhuma live", value="none")
        ]

    async def callback(self, interaction: discord.Interaction):

        if interaction.user.id != ADM_ID:
            await interaction.response.send_message(
                "❌ Apenas ADM.",
                ephemeral=True
            )
            return

        uid = self.values[0]

        await remover_live_db(uid)

        await interaction.response.send_message(
            "✅ Live removida.",
            ephemeral=True
        )

class RemoverLiveView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=60)

    async def setup(self):

        select = SelectRemoverLive()
        await select.refresh_options()

        self.clear_items()
        self.add_item(select)

# =========================================================
# ================= ADMIN LIVES ===========================
# =========================================================

class GerenciarLivesView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="📋 Ver Lives",
        style=discord.ButtonStyle.secondary,
        custom_id="ver_lives_admin_btn"
    )
    async def ver(self, interaction: discord.Interaction, button: discord.ui.Button):

        if interaction.user.id != ADM_ID:
            await interaction.response.send_message(
                "❌ Apenas ADM.",
                ephemeral=True
            )
            return

        lives = await carregar_lives()

        texto = ""

        for uid, lista_lives in lives.items():

            for data in lista_lives:

                texto += f"👤 <@{uid}>\n🔗 {data['link']}\n\n"

        embed = discord.Embed(
            title="📡 Lives cadastradas",
            description=texto or "Nenhuma.",
            color=0x3498db
        )

        await interaction.response.send_message(
            embed=embed,
            ephemeral=True
        )


    @discord.ui.button(
        label="🗑️ Remover Live",
        style=discord.ButtonStyle.danger,
        custom_id="remover_live_admin_btn"
    )
    async def remover(self, interaction: discord.Interaction, button: discord.ui.Button):

        if interaction.user.id != ADM_ID:
            await interaction.response.send_message(
                "❌ Apenas ADM.",
                ephemeral=True
            )
            return

        view = RemoverLiveView()
        await view.setup()

        await interaction.response.send_message(
            "Escolha a live para remover:",
            view=view,
            ephemeral=True
        )

class PainelLivesAdmin(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="⚙️ Gerenciar Lives (ADM)",
        style=discord.ButtonStyle.danger,
        custom_id="abrir_painel_admin_lives_btn"
    )
    async def abrir(self, interaction: discord.Interaction, button: discord.ui.Button):

        if interaction.user.id != ADM_ID:
            await interaction.response.send_message(
                "❌ Apenas ADM.",
                ephemeral=True
            )
            return

        await interaction.response.send_message(
            "Painel ADM:",
            view=GerenciarLivesView(),
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

    await enviar_ou_atualizar_painel(
        "painel_lives_admin",
        CANAL_CADASTRO_LIVE_ID,
        embed,
        PainelLivesAdmin()
    )


async def enviar_painel_lives():

    canal = bot.get_channel(CANAL_CADASTRO_LIVE_ID)

    if not canal:
        print("❌ Canal cadastro live não encontrado")
        return

    embed = discord.Embed(
        title="🎥 Cadastro de Live",
        description="Clique no botão abaixo para cadastrar sua live.",
        color=0x9146FF
    )

    view = CadastrarLiveView()

    await enviar_ou_atualizar_painel(
        "painel_lives",
        CANAL_CADASTRO_LIVE_ID,
        embed,
        view
    )

    print("🎥 Painel de cadastro de live verificado/atualizado")

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

}

def formatar_dinheiro(valor):
    try:
        valor = float(valor)
    except:
        valor = 0

    return (
        f"{valor:,.2f}"
        .replace(",", "X")
        .replace(".", ",")
        .replace("X", ".")
    )

# =========================================================
# ================= EMBED AÇÕES ===========================
# =========================================================

async def gerar_embed_acoes():

    async with db.acquire() as conn:
        rows = await conn.fetch("""
            SELECT tipo, COUNT(*) as qtd
            FROM acoes_semana
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
            linhas.append(f"• {nome}: {qtd}")
        else:
            restante = max(limite - qtd, 0)
            # Mostra o progresso
            if qtd >= limite:
                linhas.append(f"• {nome}: ✅ {qtd}/{limite} (COMPLETO)")
            else:
                linhas.append(f"• {nome}: {qtd}/{limite} (restam {restante})")
            total_meta += limite

    # Calcula porcentagem
    if total_meta > 0:
        porcentagem = int((total_feitas / total_meta) * 100)
        barra_progresso = "▓" * (porcentagem // 5) + "░" * (20 - (porcentagem // 5))
        status_texto = f"📊 Progresso: {porcentagem}% {barra_progresso}\n"
    else:
        status_texto = ""

    embed = discord.Embed(title="📊 Ações da Semana", color=0x2ecc71)

    embed.add_field(
        name="📌 Progresso", 
        value=f"{status_texto}\n" + "\n".join(linhas), 
        inline=False
    )
    
    embed.add_field(
        name="📊 Total", 
        value=f"{total_feitas}/{total_meta} ações realizadas", 
        inline=False
    )

    return embed

# =========================================================
# ================= RELATÓRIO PERÍODO =====================
# =========================================================

async def gerar_relatorio_periodo(data_inicio, data_fim):

    async with db.acquire() as conn:

        total = await conn.fetchval(
            """
            SELECT COALESCE(SUM(valor), 0)
            FROM acoes_semana
            WHERE DATE(data) BETWEEN DATE($1) AND DATE($2)
            """,
            data_inicio,
            data_fim
        )

        rows = await conn.fetch(
            """
            SELECT p.user_id, COUNT(*) as qtd
            FROM participantes_acoes p
            JOIN acoes_semana a ON a.id = p.acao_id
            WHERE DATE(a.data) BETWEEN DATE($1) AND DATE($2)
            GROUP BY p.user_id
            ORDER BY qtd DESC
            """,
            data_inicio,
            data_fim
        )

    linhas = [f"<@{r['user_id']}> • {r['qtd']} participações" for r in rows]

    embed = discord.Embed(title="📊 Relatório de Ações", color=0x3498db)

    embed.add_field(
        name="📅 Período",
        value=f"{data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}",
        inline=False
    )

    embed.add_field(
        name="💰 Total Movimentado",
        value=f"R$ {formatar_dinheiro(total)}",
        inline=False
    )

    embed.add_field(
        name="👥 Participações",
        value="\n".join(linhas) if linhas else "Nenhuma",
        inline=False
    )

    return embed

# =========================================================
# ================= MODAL RELATÓRIO =======================
# =========================================================

class RelatorioPeriodoModal(discord.ui.Modal, title="📊 Gerar Relatório"):

    data_inicio = discord.ui.TextInput(label="Data início (DD/MM/AAAA)")
    data_fim = discord.ui.TextInput(label="Data fim (DD/MM/AAAA)")

    async def on_submit(self, interaction: discord.Interaction):

        from datetime import datetime, timedelta

        try:
            inicio = datetime.strptime(self.data_inicio.value, "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value, "%d/%m/%Y") + timedelta(days=1)
        except:
            await interaction.response.send_message("❌ Data inválida.", ephemeral=True)
            return

        embed = await gerar_relatorio_periodo(inicio, fim)
        await interaction.response.send_message(embed=embed, ephemeral=True)

# =========================================================
# ================= VIEW PRINCIPAL ========================
# =========================================================

class PainelAcoesView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(AcaoSelect())

    @discord.ui.button(label="📊 Ver Relatório", style=discord.ButtonStyle.primary, custom_id="acoes_relatorio")
    async def relatorio(self, interaction, button):
        await interaction.response.send_modal(RelatorioPeriodoModal())

    @discord.ui.button(label="♻️ Resetar Ações", style=discord.ButtonStyle.danger, custom_id="acoes_reset")
    async def reset(self, interaction, button):

        async with db.acquire() as conn:
            await conn.execute("DELETE FROM acoes_semana")
            await conn.execute("DELETE FROM participantes_acoes")

        await atualizar_painel_acoes(interaction.guild)

        await interaction.response.send_message("✅ Ações resetadas.", ephemeral=True)

# =========================================================
# ================= SELECT ================================
# =========================================================

class AcaoSelect(discord.ui.Select):

    def __init__(self):
        options = [discord.SelectOption(label=a) for a in ACOES_SEMANA.keys()]

        super().__init__(
            placeholder="Escolha a ação",
            options=options,
            custom_id="acao_select_menu"
        )

    async def callback(self, interaction):
        await interaction.response.send_message(
            "Selecione os participantes:",
            view=SelecionarMembrosView(self.values[0]),
            ephemeral=True
        )

# =========================================================
# ================= SELECIONAR MEMBROS ====================
# =========================================================

class SelecionarMembros(discord.ui.UserSelect):

    def __init__(self, view_ref):
        super().__init__(min_values=1, max_values=10)
        self.view_ref = view_ref

    async def callback(self, interaction):

        membros_validos = []
        membros_invalidos = []

        for m in self.values:

            if any(role.id in CARGOS_PERMITIDOS_ESCALACAO for role in m.roles):
                membros_validos.append(m)
            else:
                membros_invalidos.append(m)

        self.view_ref.membros = membros_validos

        msg = f"✅ {len(membros_validos)} participantes válidos selecionados"

        if membros_invalidos:
            nomes = ", ".join(m.display_name for m in membros_invalidos)
            msg += f"\n⚠️ Ignorados: {nomes}"

        await interaction.response.send_message(msg, ephemeral=True)

class SelecionarMembrosView(discord.ui.View):

    def __init__(self, acao):
        super().__init__(timeout=300)
        self.acao = acao
        self.membros = []

        self.add_item(SelecionarMembros(self))
        self.add_item(EnviarEscalacaoButton(self))

class EnviarEscalacaoButton(discord.ui.Button):

    def __init__(self, view):
        super().__init__(label="📤 Enviar", style=discord.ButtonStyle.success)
        self.view_ref = view

    async def callback(self, interaction):

        await interaction.response.defer(ephemeral=True)

        membros = self.view_ref.membros

        if not membros:
            await interaction.followup.send("⚠️ Selecione participantes.", ephemeral=True)
            return

        async with db.acquire() as conn:

            acao_id = await conn.fetchval(
                "INSERT INTO acoes_semana (tipo,data,autor) VALUES ($1,$2,$3) RETURNING id",
                self.view_ref.acao,
                agora_db(),
                str(interaction.user.id)
            )

            for m in membros:
                await conn.execute(
                    "INSERT INTO participantes_acoes (acao_id,user_id) VALUES ($1,$2)",
                    acao_id,
                    str(m.id)
                )

        # ✅ REGISTRAR RELATÓRIO DA AÇÃO
        await registrar_relatorio_acao(interaction.guild, acao_id)
        
        # ✅ ATUALIZAR O PAINEL PRINCIPAL (AQUI ESTAVA FALTANDO!)
        await atualizar_painel_acoes(interaction.guild)

        await interaction.followup.send("✅ Escalação registrada!", ephemeral=True)
        
# =========================================================
# ================= MODAL DE RESULTADO (GANHOU) =================
# =========================================================

class ResultadoGanhouModal(discord.ui.Modal, title="🎉 Resultado - GANHOU"):
    """Modal para inserir o valor ganho na ação"""
    
    dinheiro = discord.ui.TextInput(
        label="Valor total ganho (em reais)",
        placeholder="Ex: 50000",
        required=True
    )

    def __init__(self, acao_id):
        super().__init__()
        self.acao_id = acao_id

    async def on_submit(self, interaction: discord.Interaction):
        
        await interaction.response.defer(ephemeral=True)

        try:
            valor_total = int(self.dinheiro.value.replace(".", "").replace(",", ""))
        except:
            await interaction.followup.send("❌ Valor inválido!", ephemeral=True)
            return

        if valor_total <= 0:
            await interaction.followup.send("❌ Valor deve ser maior que zero!", ephemeral=True)
            return

        async with db.acquire() as conn:
            # Atualiza a ação com o valor
            await conn.execute(
                "UPDATE acoes_semana SET valor=$1, resultado='ganhou' WHERE id=$2",
                valor_total,
                self.acao_id
            )

            # Busca todos os participantes da ação
            participantes = await conn.fetch(
                "SELECT user_id FROM participantes_acoes WHERE acao_id=$1",
                self.acao_id
            )

            acao = await conn.fetchrow(
                "SELECT tipo FROM acoes_semana WHERE id=$1",
                self.acao_id
            )

        # =============================================
        # DISTRIBUIR PARA OS PARTICIPANTES
        # =============================================
        
        ids_participantes = [str(p["user_id"]) for p in participantes]
        qtd_participantes = len(ids_participantes)
        
        if qtd_participantes == 0:
            await interaction.followup.send("⚠️ Nenhum participante registrado!", ephemeral=True)
            return
        
        # Calcula valor por pessoa
        valor_por_pessoa = valor_total // qtd_participantes
        resto = valor_total % qtd_participantes
        
        lista_depositos = []
        depositos_ok = 0
        depositos_falha = 0
        
        # Distribui para cada participante
        for uid in ids_participantes:
            user_id_int = int(uid)
            
            sucesso = await depositar_na_meta(
                user_id_int, 
                valor_por_pessoa,
                f"Ação: {acao['tipo']} (Ganhou)"
            )
            
            if sucesso:
                depositos_ok += 1
                lista_depositos.append(f"✅ <@{uid}> - R$ {formatar_dinheiro(valor_por_pessoa)}")
            else:
                depositos_falha += 1
                lista_depositos.append(f"❌ <@{uid}> - FALHA AO DEPOSITAR")
        
        # Restante vai para o primeiro
        if resto > 0 and ids_participantes:
            primeiro_uid = ids_participantes[0]
            await depositar_na_meta(
                int(primeiro_uid),
                resto,
                f"Ação: {acao['tipo']} (Restante)"
            )
            lista_depositos.append(f"➕ <@{primeiro_uid}> - Restante: R$ {formatar_dinheiro(resto)}")
        
        # Lista de participantes
        lista_participantes = "\n".join([f"<@{uid}>" for uid in ids_participantes])
        
        # Cria embed do resultado
        embed = discord.Embed(
            title="🎉 RESULTADO DA AÇÃO - GANHOU!",
            color=0x2ecc71
        )
        
        embed.add_field(name="🎯 Ação", value=acao["tipo"], inline=False)
        embed.add_field(name="💰 Total Ganho", value=f"R$ {formatar_dinheiro(valor_total)}", inline=False)
        embed.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
        embed.add_field(name="💸 Valor por pessoa", value=f"R$ {formatar_dinheiro(valor_por_pessoa)}", inline=True)
        embed.add_field(name="✅ Depósitos", value=f"{depositos_ok}/{qtd_participantes} realizados", inline=True)
        
        if resto > 0:
            embed.add_field(name="📦 Restante", value=f"R$ {formatar_dinheiro(resto)}", inline=True)
        
        await interaction.message.edit(embed=embed, view=None)
        await atualizar_painel_acoes(interaction.guild)
        
        await interaction.followup.send(
            f"✅ **{depositos_ok} depósitos realizados!**\n💰 Total distribuído: R$ {formatar_dinheiro(valor_por_pessoa * qtd_participantes)}",
            ephemeral=True
        )


# =========================================================
# ================= MODAL DE RESULTADO (PERDEU) =================
# =========================================================

class ResultadoPerdeuModal(discord.ui.Modal, title="💀 Resultado - PERDEU"):
    """Modal simples para confirmar que perdeu (sem pedir valor)"""
    
    confirmacao = discord.ui.TextInput(
        label="Digite CONFIRMAR para registrar a perda",
        placeholder="Digite: CONFIRMAR",
        required=True
    )

    def __init__(self, acao_id):
        super().__init__()
        self.acao_id = acao_id

    async def on_submit(self, interaction: discord.Interaction):
        
        if self.confirmacao.value.strip().upper() != "CONFIRMAR":
            await interaction.response.send_message("❌ Confirmação incorreta! Digite 'CONFIRMAR' para prosseguir.", ephemeral=True)
            return
        
        await interaction.response.defer(ephemeral=True)

        async with db.acquire() as conn:
            # Atualiza a ação como perdeu (valor 0)
            await conn.execute(
                "UPDATE acoes_semana SET valor=0, resultado='perdeu' WHERE id=$1",
                self.acao_id
            )

            # Busca todos os participantes da ação
            participantes = await conn.fetch(
                "SELECT user_id FROM participantes_acoes WHERE acao_id=$1",
                self.acao_id
            )

            acao = await conn.fetchrow(
                "SELECT tipo FROM acoes_semana WHERE id=$1",
                self.acao_id
            )

        # Lista de participantes
        ids_participantes = [str(p["user_id"]) for p in participantes]
        lista_participantes = "\n".join([f"<@{uid}>" for uid in ids_participantes]) if ids_participantes else "Ninguém"
        
        # Cria embed do resultado - PERDEU
        embed = discord.Embed(
            title="💀 RESULTADO DA AÇÃO - PERDEU!",
            description="A ação foi perdida, nenhum valor foi distribuído.",
            color=0xe74c3c
        )
        
        embed.add_field(name="🎯 Ação", value=acao["tipo"], inline=False)
        embed.add_field(name="👥 Participantes", value=lista_participantes, inline=False)
        embed.add_field(name="💰 Total", value="R$ 0,00", inline=True)
        embed.add_field(name="📝 Status", value="❌ AÇÃO PERDIDA", inline=True)
        
        await interaction.message.edit(embed=embed, view=None)
        await atualizar_painel_acoes(interaction.guild)
        
        await interaction.followup.send(
            f"✅ Ação **{acao['tipo']}** registrada como PERDIDA!\n👥 Participantes: {len(ids_participantes)} membros.",
            ephemeral=True
        )


# =========================================================
# ================= VIEW DO RESULTADO =====================
# =========================================================

class ResultadoAcaoView(discord.ui.View):

    def __init__(self, acao_id):
        super().__init__(timeout=None)
        self.acao_id = acao_id

    @discord.ui.button(
        label="🏆 Ganhou", 
        style=discord.ButtonStyle.success,
        custom_id="resultado_ganhou"
    )
    async def ganhou(self, interaction: discord.Interaction, button):
        # Verifica permissão
        async with db.acquire() as conn:
            acao = await conn.fetchrow(
                "SELECT autor FROM acoes_semana WHERE id=$1",
                self.acao_id
            )
        
        is_autor = str(interaction.user.id) == acao["autor"]
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_autor and not is_gerente:
            await interaction.response.send_message(
                "❌ Apenas o criador da ação ou gerentes podem finalizar o resultado!",
                ephemeral=True
            )
            return
        
        await interaction.response.send_modal(ResultadoGanhouModal(self.acao_id))

    @discord.ui.button(
        label="💀 Perdeu", 
        style=discord.ButtonStyle.danger,
        custom_id="resultado_perdeu"
    )
    async def perdeu(self, interaction: discord.Interaction, button):
        # Verifica permissão
        async with db.acquire() as conn:
            acao = await conn.fetchrow(
                "SELECT autor FROM acoes_semana WHERE id=$1",
                self.acao_id
            )
        
        is_autor = str(interaction.user.id) == acao["autor"]
        is_gerente = any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles)
        
        if not is_autor and not is_gerente:
            await interaction.response.send_message(
                "❌ Apenas o criador da ação ou gerentes podem finalizar o resultado!",
                ephemeral=True
            )
            return
        
        await interaction.response.send_modal(ResultadoPerdeuModal(self.acao_id))

        # =============================================
        # ✅ GANHOU - DISTRIBUIR PARA OS PARTICIPANTES
        # =============================================
        
        ids_participantes = [str(p["user_id"]) for p in participantes]
        qtd_participantes = len(ids_participantes)
        
        if qtd_participantes == 0:
            await interaction.followup.send("⚠️ Nenhum participante registrado!", ephemeral=True)
            return
        
        # Calcula valor por pessoa (divisão igual)
        valor_por_pessoa = valor_total // qtd_participantes
        resto = valor_total % qtd_participantes  # O resto fica para o grupo/cofre
        
        lista_depositos = []
        depositos_ok = 0
        depositos_falha = 0
        
        # Distribui para cada participante
        for uid in ids_participantes:
            user_id_int = int(uid)
            
            # Deposita na meta do participante
            sucesso = await depositar_na_meta(
                user_id_int, 
                valor_por_pessoa,
                f"Ação: {acao['tipo']} (Ganhou)"
            )
            
            if sucesso:
                depositos_ok += 1
                lista_depositos.append(f"✅ <@{uid}> - R$ {formatar_dinheiro(valor_por_pessoa)}")
            else:
                depositos_falha += 1
                lista_depositos.append(f"❌ <@{uid}> - FALHA AO DEPOSITAR")
        
        # Se tiver resto, adiciona no primeiro participante ou guarda
        if resto > 0:
            primeiro_uid = ids_participantes[0]
            await depositar_na_meta(
                int(primeiro_uid),
                resto,
                f"Ação: {acao['tipo']} (Restante da divisão)"
            )
            lista_depositos.append(f"➕ <@{primeiro_uid}> - Restante: R$ {formatar_dinheiro(resto)}")
        
        # =============================================
        # CRIAR EMBED DO RESULTADO
        # =============================================
        
        embed = discord.Embed(
            title="🎉 RESULTADO DA AÇÃO - GANHOU!",
            color=0x2ecc71
        )
        
        embed.add_field(name="🎯 Ação", value=acao["tipo"], inline=False)
        embed.add_field(name="💰 Total Ganho", value=f"R$ {formatar_dinheiro(valor_total)}", inline=False)
        embed.add_field(name="👥 Participantes", value=str(qtd_participantes), inline=True)
        embed.add_field(name="💸 Valor por pessoa", value=f"R$ {formatar_dinheiro(valor_por_pessoa)}", inline=True)
        
        if resto > 0:
            embed.add_field(name="📦 Restante", value=f"R$ {formatar_dinheiro(resto)} (adicionado ao primeiro)", inline=True)
        
        # Lista de depósitos
        lista_texto = "\n".join(lista_depositos[:15])  # Limita a 15 linhas
        if len(lista_depositos) > 15:
            lista_texto += f"\n... e mais {len(lista_depositos) - 15} participantes"
        
        embed.add_field(name="📋 Distribuição", value=lista_texto, inline=False)
        
        # Status da distribuição
        if depositos_falha > 0:
            embed.add_field(
                name="⚠️ Atenção", 
                value=f"{depositos_falha} participantes não tinham sala de meta. Peça para criarem usando o comando!",
                inline=False
            )
        
        embed.set_footer(text=f"✅ {depositos_ok} depósitos realizados com sucesso")
        
        # Atualiza a mensagem original
        await interaction.message.edit(embed=embed, view=None)
        
        # Atualiza o painel de ações
        await atualizar_painel_acoes(interaction.guild)
        
        # Confirma para quem usou o botão
        await interaction.followup.send(
            f"✅ **{depositos_ok} depósitos realizados!**\n"
            f"💰 Total distribuído: R$ {formatar_dinheiro(valor_por_pessoa * qtd_participantes)}\n"
            f"👥 Participantes: {qtd_participantes}\n"
            f"💸 Cada um recebeu: R$ {formatar_dinheiro(valor_por_pessoa)}",
            ephemeral=True
        )

# =========================================================
# ================= RELATÓRIO AÇÃO ========================
# =========================================================

async def registrar_relatorio_acao(guild, acao_id):

    async with db.acquire() as conn:

        acao = await conn.fetchrow(
            "SELECT * FROM acoes_semana WHERE id=$1",
            acao_id
        )

        participantes = await conn.fetch(
            "SELECT user_id FROM participantes_acoes WHERE acao_id=$1",
            acao_id
        )

    lista = "\n".join([f"<@{p['user_id']}>" for p in participantes]) or "Nenhum"

    embed = discord.Embed(title="🚨 RELATÓRIO DE AÇÃO", color=0xe74c3c)

    embed.add_field(name="🏦 Ação", value=acao["tipo"], inline=False)
    embed.add_field(name="👥 Participantes", value=lista, inline=False)
    embed.add_field(name="🎯 Resultado", value="⏳ Aguardando...", inline=False)

    canal = guild.get_channel(CANAL_RELATORIO_ACOES_ID)

    if canal:
        await canal.send(embed=embed, view=ResultadoAcaoView(acao_id))

# =========================================================
# ================= PAINEL ================================
# =========================================================

async def enviar_painel_acoes(guild):

    canal = guild.get_channel(CANAL_ESCALACOES_ID)

    if not canal:
        print("❌ Canal ações não encontrado")
        return

    embed = await gerar_embed_acoes()

    await enviar_ou_atualizar_painel(
        "painel_acoes",
        CANAL_ESCALACOES_ID,
        embed,
        PainelAcoesView()
    )

async def atualizar_painel_acoes(guild):
    """Atualiza o painel de ações com os dados mais recentes"""
    await enviar_painel_acoes(guild)

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

# =========================================================
# ================= FIM DO SISTEMA ========================
# =========================================================
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

    
    # =====================================================
    # ================= RESTAURAR PRODUÇÕES ===============
    # =====================================================

    try:
        async with db.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT pid FROM producoes
                WHERE pid NOT LIKE 'TESTE_%'
                AND CAST(fim AS timestamp) > NOW()
                """
            )

        for r in rows:

            pid = r["pid"]

            if pid not in producoes_tasks:

                task = bot.loop.create_task(
                    acompanhar_producao(pid)
                )

                producoes_tasks[pid] = task

        print(f"🏭 Produções restauradas: {len(rows)}")

    except Exception as e:
        print("Erro restaurar produções:", e)

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
