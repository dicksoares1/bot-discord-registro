# =========================================================
# ======================== IMPORTS =========================
# =========================================================

# ================= SISTEMA =================
import os
import json
import gc

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
# ================== FILA GLOBAL DE EDIÇÃO =================
# =========================================================

edit_queue = asyncio.Queue()

async def edit_worker():
    while True:
        coro = await edit_queue.get()
        try:
            await coro

            # 🔥 delay mais seguro
            await asyncio.sleep(1.2)

        except discord.NotFound:
            # mensagem deletada
            pass

        except discord.HTTPException as e:
            if e.status == 429:
                # se bater rate limit, espera mais
                await asyncio.sleep(3)
            else:
                print("Erro HTTP edit_worker:", e)

        except Exception as e:
            print("Erro no edit_worker:", e)

        edit_queue.task_done()
# =========================================================

http_session = None
# =========================================================
# ===================== RELÓGIO GLOBAL ====================
# =========================================================

BRASIL = ZoneInfo("America/Sao_Paulo")

def agora():
    return datetime.now(BRASIL)

# =========================================================
# =============== DATETIME PADRÃO PARA DB =================
# =========================================================

def agora_db():
    """
    Retorna datetime sem timezone para usar no PostgreSQL
    evitando erro de timezone (offset-aware vs offset-naive)
    """
    return agora().replace(tzinfo=None)

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

# ================= BANCO POSTGRES (POOL PROFISSIONAL) =================

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

# 
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

# =========================================================
# ======================== CARGOS ==========================
# =========================================================

# REGISTRO
AGREGADO_ROLE_ID = 1422847202937536532
CONVIDADO_ROLE_ID = 1337382961456353342

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

# garante que o discord.py realmente faça cache dos membros
intents.guilds = True
intents.presences = True

bot = commands.Bot(command_prefix="!", intents=intents)

# trava anti-duplicação de criação de metas
criando_meta = set()

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

# =========================================================
# ======================= REGISTRO =========================
# =========================================================

class RegistroModal(discord.ui.Modal, title="Registro de Entrada"):
    nome = discord.ui.TextInput(label="Nome Completo")
    passaporte = discord.ui.TextInput(label="Passaporte")
    indicado = discord.ui.TextInput(label="Indicado por")
    telefone = discord.ui.TextInput(label="Telefone In Game")

    async def on_submit(self, interaction: discord.Interaction):
        membro = interaction.user
        guild = interaction.guild

        # Atualiza nick
        await membro.edit(nick=f"{self.passaporte.value} - {self.nome.value}")

        agregado = guild.get_role(AGREGADO_ROLE_ID)
        convidado = guild.get_role(CONVIDADO_ROLE_ID)

        if agregado:
            await membro.add_roles(agregado)

        if convidado:
            await membro.remove_roles(convidado)

        canal_log = guild.get_channel(CANAL_LOG_REGISTRO_ID)
        if canal_log:
            embed = discord.Embed(title="📋 Novo Registro", color=0x2ecc71)
            embed.add_field(name="Nome", value=self.nome.value)
            embed.add_field(name="Passaporte", value=self.passaporte.value)
            embed.add_field(name="Indicado por", value=self.indicado.value)
            embed.add_field(name="Telefone", value=self.telefone.value)
            embed.add_field(name="Usuário", value=membro.mention)
            await canal_log.send(embed=embed)

        await interaction.response.defer()


class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="📋 Fazer Registro",
        style=discord.ButtonStyle.success,
        custom_id="registro_fazer"
    )
    async def registro(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistroModal())
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
        row = await conn.fetchrow("SELECT ultimo FROM pedidos WHERE id=1")

        if not row:
            await conn.execute("INSERT INTO pedidos (id, ultimo) VALUES (1, 1)")
            return 1

        novo = row["ultimo"] + 1
        await conn.execute("UPDATE pedidos SET ultimo=$1 WHERE id=1", novo)
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
        rows = await conn.fetch("SELECT * FROM vendas")
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

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="status_pago")
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

        # remove pagamento pendente
        linhas = [l for l in linhas if not l.startswith("⏳")]

        # remove pago antigo
        linhas = [l for l in linhas if not l.startswith("💰")]


        linhas.append(
            f"💰 Pago • Recebido por {user} • {agora_str}"
        )

        embed = self.set_status(embed, idx, linhas)

        # Finaliza pedido → trava botões
        await interaction.message.edit(embed=embed, view=self)
        await interaction.response.defer()

    # =====================================================
    # ================= BOTÃO ENTREGUE ====================
    # =====================================================

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="status_entregue")
    async def entregue(self, interaction: discord.Interaction, button: discord.ui.Button):

        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        if self.pedido_pago(linhas):
            await interaction.response.send_message(
                "⚠️ Este pedido já foi pago.",
                ephemeral=True
            )
            return

        if self.pedido_cancelado(linhas):
            await interaction.response.send_message(
                "⚠️ Este pedido foi cancelado.",
                ephemeral=True
            )
            return

        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        user = interaction.user

        # remove A entregar
        linhas = [l for l in linhas if not l.startswith("📦")]

        # remove entregue antigo
        linhas = [l for l in linhas if not l.startswith("✅")]

        linhas.append(
            f"✅ Entregue por {user.mention} • {agora_str}"
        )

        embed = self.set_status(embed, idx, linhas)

        await interaction.message.edit(embed=embed, view=self)

        # ===============================
        # PEGAR PACOTES DO EMBED
        # ===============================

        pacotes_pt = 0
        pacotes_sub = 0

        for field in embed.fields:

            if field.name == "🔫 PT":
                try:
                    pacotes_pt = int(field.value.split("📦")[1].split()[0])
                except:
                    pass

            if field.name == "🔫 SUB":
                try:
                    pacotes_sub = int(field.value.split("📦")[1].split()[0])
                except:
                    pass

        # ===============================
        # ENVIAR NO BAÚ
        # ===============================

        canal_bau = interaction.guild.get_channel(CANAL_BAU_GALPAO_SUL_ID)

        if canal_bau:
            await canal_bau.send(
                f"📦 **Retirada do Baú**\n\n"
                f"👤 Retirado por: {user.mention}\n"
                f"🔫 PT: {pacotes_pt} pacotes\n"
                f"🔫 SUB: {pacotes_sub} pacotes"
            )

        await interaction.response.defer()

    # =====================================================
    # ================= BOTÃO CANCELADO ===================
    # =====================================================

    @discord.ui.button(label="❌ Pedido cancelado", style=discord.ButtonStyle.danger, custom_id="status_cancelado")
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
        await interaction.message.edit(embed=embed, view=StatusView(disabled=True))
        await interaction.response.defer()

    # =====================================================
    # ================= BOTÃO EDITAR ======================
    # =====================================================

    @discord.ui.button(label="✏️ Editar Venda", style=discord.ButtonStyle.secondary)
    async def editar(self, interaction: discord.Interaction, button: discord.ui.Button):

        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        if self.pedido_cancelado(linhas):
            await interaction.response.send_message(
                "⚠️ Pedido cancelado não pode ser editado.",
                ephemeral=True
            )
            return

        await interaction.response.send_modal(EditarVendaModal(interaction.message))
# =========================================================
# ================= MODAL DE VENDA ========================
# =========================================================

class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(label="Organização")
    qtd_pt = discord.ui.TextInput(label="Quantidade PT")
    qtd_sub = discord.ui.TextInput(label="Quantidade SUB")
    observacoes = discord.ui.TextInput(
        label="Observações",
        style=discord.TextStyle.paragraph,
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):

        try:
            pt = int(self.qtd_pt.value.strip())
            sub = int(self.qtd_sub.value.strip())
        except:
            await interaction.response.send_message("Valores inválidos.", ephemeral=True)
            return

        numero_pedido = await proximo_pedido()

        pacotes_pt = pt // 50
        pacotes_sub = sub // 50
        total = (pt * 50) + (sub * 90)

        # SALVA NO POSTGRES
        await salvar_venda_db(str(interaction.user.id), total, numero_pedido)

        valor_formatado = f"{total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        org_nome = self.organizacao.value.strip().upper()
        config = ORGANIZACOES_CONFIG.get(org_nome, {"emoji": "🏷️", "cor": 0x1e3a8a})

        embed = discord.Embed(
            title=f"📦 NOVA ENCOMENDA • Pedido #{numero_pedido:04d}",
            color=config["cor"]
        )

        embed.add_field(name="👤 Vendedor", value=interaction.user.mention, inline=False)
        embed.add_field(name="🏷 Organização", value=f"{config['emoji']} {org_nome}", inline=False)

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
            embed.add_field(name="📝 Observações", value=self.observacoes.value, inline=False)

        embed.set_footer(text="🛡 Sistema de Encomendas • VDR 442")

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())

        await interaction.response.defer()

# =========================================================
# ================= EDITAR== VENDA ========================
# =========================================================
class EditarVendaModal(discord.ui.Modal, title="Editar Venda"):

    qtd_pt = discord.ui.TextInput(label="Nova Quantidade PT")
    qtd_sub = discord.ui.TextInput(label="Nova Quantidade SUB")

    def __init__(self, message):
        super().__init__()
        self.message = message

    async def on_submit(self, interaction: discord.Interaction):

        try:
            pt = int(self.qtd_pt.value.strip())
            sub = int(self.qtd_sub.value.strip())
        except:
            await interaction.response.send_message("Valores inválidos.", ephemeral=True)
            return

        pacotes_pt = pt // 50
        pacotes_sub = sub // 50
        total = (pt * 50) + (sub * 90)

        valor_formatado = f"{total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        embed = self.message.embeds[0]

        vendedor_id = None
        for field in embed.fields:
            if field.name == "👤 Vendedor":
                vendedor_id = field.value.replace("<@", "").replace(">", "")

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

       
        titulo = embed.title
        pedido_numero = int(titulo.split("#")[1])

        await atualizar_valor_venda_db(pedido_numero, total)
        await self.message.edit(embed=embed)

        canal_log = interaction.guild.get_channel(1478381934026424391)
        if canal_log:
            await canal_log.send(
                f"✏️ **Venda editada**\n"
                f"👤 Editor: {interaction.user.mention}\n"
                f"🧾 Pedido: {embed.title}\n"
                f"💰 Novo valor: R$ {valor_formatado}"
            )

        await interaction.response.send_message("Venda editada com sucesso.", ephemeral=True)
# =========================================================
# ================= MODAL RELATÓRIO =======================
# =========================================================

class RelatorioModal(discord.ui.Modal, title="Gerar Relatório"):
    data_inicio = discord.ui.TextInput(label="Data inicial (dd/mm/aaaa)")
    data_fim = discord.ui.TextInput(label="Data final (dd/mm/aaaa)")

    async def on_submit(self, interaction: discord.Interaction):
        from datetime import datetime

        try:
            d1 = datetime.strptime(self.data_inicio.value, "%d/%m/%Y")
            d2 = datetime.strptime(self.data_fim.value, "%d/%m/%Y")
        except:
            await interaction.response.send_message("Data inválida.", ephemeral=True)
            return

        vendas = await carregar_vendas_db()
        totais = {}

        for v in vendas:
            data_venda = datetime.strptime(v["data"], "%d/%m/%Y")
            if d1 <= data_venda <= d2:
                totais[v["user_id"]] = totais.get(v["user_id"], 0) + v["valor"]

        if not totais:
            await interaction.response.send_message("Nenhuma venda no período.", ephemeral=True)
            return

        texto = "**RELATÓRIO DE VENDAS**\n\n"

        for vendedor, valor in totais.items():
            membro = interaction.guild.get_member(int(vendedor))
            nome = membro.display_name if membro else vendedor
            texto += f"{nome} — R$ {valor:,.0f}\n"

        await interaction.channel.send(texto)
        await interaction.response.send_message("Relatório enviado!", ephemeral=True)


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
        await interaction.response.send_modal(VendaModal())

    @discord.ui.button(
        label="Relatório",
        style=discord.ButtonStyle.success,
        custom_id="calc_relatorio_vendas"
    )
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RelatorioModal())

# =========================================================
# ================= PAINEL ================================
# =========================================================

async def enviar_painel_vendas():
    canal = bot.get_channel(CANAL_VENDAS_ID)

    if not canal:
        print("❌ Canal de vendas não encontrado")
        return

    embed = discord.Embed(
        title="🛒 Painel de Vendas",
        description="Escolha uma opção abaixo.",
        color=0x2ecc71
    )

    # procura painel existente
    async for msg in canal.history(limit=30):
        if (
            msg.author == bot.user
            and msg.embeds
            and msg.embeds[0].title == "🛒 Painel de Vendas"
        ):
            await asyncio.sleep(0.4)  # reduz rate limit
            await edit_queue.put(msg.edit(embed=embed, view=CalculadoraView()))
            print("♻️ Painel de vendas atualizado")
            return

    # cria novo
    await canal.send(embed=embed, view=CalculadoraView())
    print("🛒 Painel de vendas criado")
# =========================================================
# ======================== PRODUÇÃO ========================
# =========================================================

producoes_tasks = {}

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
        "canal_id": int(r["canal_id"])
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
            (pid, galpao, autor, inicio, fim, obs, msg_id, canal_id, segunda_task_user, segunda_task_time)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
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
            segunda_task_time=$10
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
            segunda_time
        )


async def deletar_producao(pid):
    async with db.acquire() as conn:
        await conn.execute(
            "DELETE FROM producoes WHERE pid=$1",
            pid
        )


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
        pid = f"{self.galpao}_{interaction.id}"

        inicio = agora()
        fim = inicio + timedelta(minutes=self.tempo)

        canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)

        msg = await canal.send(
            embed=discord.Embed(
                title="🏭 Produção",
                description=f"Iniciando produção em **{self.galpao}**...",
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
            "msg_id": msg.id,
            "canal_id": CANAL_REGISTRO_GALPAO_ID
        }

        await salvar_producao(pid, dados)

        if pid not in producoes_tasks:
            task = bot.loop.create_task(acompanhar_producao(pid))
            producoes_tasks[pid] = task

        await interaction.response.defer()

# ================= 2ª TASK =================

class SegundaTaskView(discord.ui.View):
    def __init__(self, pid):
        super().__init__(timeout=None)
        self.pid = pid

    @discord.ui.button(
        label="✅ Confirmar 2ª Task",
        style=discord.ButtonStyle.success,
        custom_id="segunda_task_btn"
    )
    async def ok(self, interaction: discord.Interaction, button: discord.ui.Button):

        prod = await carregar_producao(self.pid)
        if not prod:
            await interaction.response.defer()
            return

        prod["segunda_task_confirmada"] = {
            "user": interaction.user.id,
            "time": agora().isoformat()
        }

        await salvar_producao(self.pid, prod)

        await interaction.message.edit(view=None)
        await interaction.response.defer()



# =========================================================
# ================= VIEW FABRICAÇÃO ========================
# =========================================================

class FabricacaoView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🏭 Galpões Norte",
        style=discord.ButtonStyle.primary,
        custom_id="fabricacao_norte"
    )
    async def norte(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(
            ObservacaoProducaoModal("GALPÕES NORTE", 65)
        )


    @discord.ui.button(
        label="🏭 Galpões Sul",
        style=discord.ButtonStyle.secondary,
        custom_id="fabricacao_sul"
    )
    async def sul(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(
            ObservacaoProducaoModal("GALPÕES SUL", 130)
        )


    @discord.ui.button(
        label="🧪 TESTE 3 MIN",
        style=discord.ButtonStyle.secondary,
        custom_id="fabricacao_teste"
    )
    async def teste(self, interaction: discord.Interaction, button: discord.ui.Button):

        inicio = agora()
        fim = inicio + timedelta(minutes=3)

        embed = discord.Embed(
            title="🏭 Produção (TESTE)",
            description="Iniciando produção de teste...",
            color=0x95a5a6
        )

        msg = await interaction.channel.send(embed=embed)

        # PID apenas para controle interno (não vai para o banco)
        pid = f"TESTE_{msg.id}"

        # inicia acompanhamento apenas em memória
        task = bot.loop.create_task(acompanhar_producao(pid))

        producoes_tasks[pid] = task

        await interaction.response.defer()
# =========================================================
# ================= LOOP DE ACOMPANHAMENTO =================
# =========================================================

async def acompanhar_producao(pid):

    print(f"▶ Produção retomada: {pid}")

    msg = None

    while not bot.is_closed():

        prod = await carregar_producao(pid)

        if not prod:
            return

        canal = bot.get_channel(prod["canal_id"])

        if not canal:
            await asyncio.sleep(10)
            continue

        # pega mensagem apenas uma vez
        if msg is None:
            try:
                msg = await canal.fetch_message(prod["msg_id"])
            except:
                await asyncio.sleep(10)
                continue

        inicio = datetime.fromisoformat(prod["inicio"])
        fim = datetime.fromisoformat(prod["fim"])

        total = (fim - inicio).total_seconds()
        restante = max(0, (fim - agora()).total_seconds())

        pct = max(0, min(1, 1 - (restante / total)))
        mins = int(restante // 60)

        desc = (
            f"**Galpão:** {prod['galpao']}\n"
            f"**Iniciado por:** <@{prod['autor']}>\n"
        )

        if prod.get("obs"):
            desc += f"📝 **Obs:** {prod['obs']}\n"

        desc += (
            f"Início: <t:{int(inicio.timestamp())}:t>\n"
            f"Término: <t:{int(fim.timestamp())}:t>\n\n"
            f"⏳ **Restante:** {mins} min\n"
            f"{barra(pct)}"
        )

        if prod.get("segunda_task_confirmada"):
            uid = prod["segunda_task_confirmada"]["user"]
            desc += f"\n\n✅ **Segunda task concluída por:** <@{uid}>"

                # ================= FINALIZA PRODUÇÃO =================

        if restante <= 0:

            desc += "\n\n🔵 **Produção Finalizada**"

            try:
                await edit_queue.put(
                    msg.edit(
                        embed=discord.Embed(
                            title="🏭 Produção",
                            description=desc,
                            color=0x34495e
                        ),
                        view=None
                    )
                )
            except:
                pass

            # remove do banco
            await deletar_producao(pid)

            # remove da lista de tasks
            if pid in producoes_tasks:
                producoes_tasks.pop(pid)

            print(f"🗑️ Produção removida: {pid}")

            return

        # ================= ATUALIZA PROGRESSO =================

        try:
            await edit_queue.put(
                msg.edit(
                    embed=discord.Embed(
                        title="🏭 Produção",
                        description=desc,
                        color=0x34495e
                    )
                )
            )
        except:
            pass

        # espera antes de atualizar novamente
        await asyncio.sleep(75)
# =========================================================
# ================= PAINEL FABRICAÇÃO =====================
# =========================================================

async def enviar_painel_fabricacao():
    try:
        canal = await bot.fetch_channel(CANAL_FABRICACAO_ID)
    except:
        print("❌ Canal de fabricação não encontrado")
        return

    embed = discord.Embed(
        title="🏭 Fabricação",
        description="Selecione Norte, Sul ou Teste para iniciar a produção.",
        color=0x2c3e50
    )

    async for msg in canal.history(limit=20):
        if msg.author == bot.user and msg.embeds:
            if msg.embeds[0].title == "🏭 Fabricação":
                await edit_queue.put(msg.edit(embed=embed, view=FabricacaoView()))
                print("🔁 Painel de fabricação atualizado")
                return

    await canal.send(embed=embed, view=FabricacaoView())
    print("🏭 Painel de fabricação criado")

# =========================================================
# ======================== POLVORAS ========================
# =========================================================

# ================= BANCO =================

async def salvar_polvora_db(user_id, vendedor, qtd, valor):
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO polvoras (user_id, vendedor, quantidade, valor, data) VALUES ($1,$2,$3,$4,$5)",
            str(user_id),
            vendedor,
            qtd,
            valor,
            agora().isoformat()
        )


async def carregar_polvoras_db():
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM polvoras")
        return rows


async def limpar_polvoras_db():
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM polvoras")

# ================= MODAL =================

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
            await interaction.response.send_message("Quantidade inválida.", ephemeral=True)
            return

        valor = qtd * 80
        valor_formatado = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        await salvar_polvora_db(interaction.user.id, self.vendedor.value, qtd, valor)

        canal = interaction.guild.get_channel(CANAL_REGISTRO_POLVORA_ID)

        embed = discord.Embed(
            title="🧨 Registro de Pólvora",
            color=0xe67e22
        )

        embed.add_field(name="Vendedor", value=self.vendedor.value, inline=False)
        embed.add_field(name="Comprado por", value=interaction.user.mention, inline=False)
        embed.add_field(name="Quantidade", value=str(qtd), inline=True)
        embed.add_field(name="Valor", value=f"**R$ {valor_formatado}**", inline=True)

        await canal.send(embed=embed)
        await interaction.response.send_message("Registro feito com sucesso!", ephemeral=True)


# ================= CONFIRMAR PAGAMENTO =================

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
        await interaction.response.defer()


# ================= VIEW =================

class PolvoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Registrar Compra de Pólvora",
        style=discord.ButtonStyle.primary,
        custom_id="polvora_btn"
    )
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PolvoraModal())


# ================= RELATÓRIO SEMANAL =================

@tasks.loop(minutes=1)
async def relatorio_semanal_polvoras():

    agora_br = agora()

    if agora_br.weekday() != 6 or agora_br.hour != 23 or agora_br.minute != 59:
        return

    dados = await carregar_polvoras_db()

    inicio_semana = agora_br - timedelta(days=6)
    inicio_semana = inicio_semana.replace(hour=0, minute=0, second=0, microsecond=0)
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
        user = await bot.fetch_user(int(user_id))

        valor_formatado = f"{total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        await canal.send(
            content=(
                f"🧨 **RELATÓRIO SEMANAL DE PÓLVORA**\n"
                f"📅 Período: {inicio_semana.strftime('%d/%m')} até {fim_semana.strftime('%d/%m')}\n\n"
                f"👤 Comprado por: {user.mention}\n"
                f"💰 Valor a ressarcir: **R$ {valor_formatado}**"
            ),
            view=ConfirmarPagamentoView()
        )


# ================= PAINEL =================

async def enviar_painel_polvoras(bot):
    canal = bot.get_channel(CANAL_CALCULO_POLVORA_ID)

    async for m in canal.history(limit=10):
        if m.author == bot.user:
            return

    embed = discord.Embed(
        title="🧨 Calculadora de Pólvora",
        description="Use o botão abaixo para registrar a compra.",
        color=0xe67e22
    )

    await canal.send(embed=embed, view=PolvoraView())

# =========================================================
# ======================== LAVAGEM =========================
# =========================================================

lavagens_pendentes = {}

@tasks.loop(minutes=15)
async def limpar_lavagens_pendentes():
    lavagens_pendentes.clear()

def formatar_real(valor: int) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ================= BANCO (POSTGRES) =================

async def salvar_lavagem_db(user_id, valor_sujo, taxa, valor_retorno):
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO lavagens (user_id, valor, taxa, liquido, data) VALUES ($1,$2,$3,$4,$5)",
            str(user_id),
            valor_sujo,
            taxa,
            valor_retorno,
            agora().replace(tzinfo=None)  # <-- datetime direto
        )


async def carregar_lavagens_db():
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM lavagens")
        return rows


async def limpar_lavagens_db():
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM lavagens")


# ================= MODAL =================

class LavagemModal(discord.ui.Modal, title="Iniciar Lavagem"):
    valor = discord.ui.TextInput(label="Valor do dinheiro sujo")

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            valor_sujo = int(self.valor.value.replace(".", "").replace(",", ""))
        except:
            await interaction.followup.send("Valor inválido.", ephemeral=True)
            return

        taxa = 20  # %
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
            embed.add_field(name="Valor sujo", value=formatar_real(valor_sujo), inline=True)
            embed.add_field(name="Valor a repassar (80%)", value=formatar_real(valor_retorno), inline=True)
            embed.set_image(url=f"attachment://{arquivo.filename}")

            await canal_destino.send(embed=embed, file=arquivo)

            return

    await bot.process_commands(message)


# ================= PERMISSÃO =================

def pode_gerenciar_lavagem(member: discord.Member):
    cargos_permitidos = [
        CARGO_GERENTE_ID,
        CARGO_01_ID,
        CARGO_02_ID,
        CARGO_GERENTE_GERAL_ID
    ]
    return any(role.id in cargos_permitidos for role in member.roles)


# ================= VIEW =================

class LavagemView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Iniciar Lavagem",
        style=discord.ButtonStyle.primary,
        custom_id="lavagem_iniciar"
    )
    async def iniciar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(LavagemModal())


    @discord.ui.button(
        label="🧹 Limpar Sala",
        style=discord.ButtonStyle.danger,
        custom_id="lavagem_limpar"
    )
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


    @discord.ui.button(
        label="📊 Gerar Relatório",
        style=discord.ButtonStyle.success,
        custom_id="lavagem_relatorio"
    )
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):

        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message("Você não tem permissão.", ephemeral=True)
            return

        dados = await carregar_lavagens_db()
        canal = interaction.guild.get_channel(CANAL_RELATORIO_LAVAGEM_ID)

        for item in dados:
            user = await bot.fetch_user(int(item["user_id"]))

            await canal.send(
                f"{user.mention} - Valor a repassar: {formatar_real(item['liquido'])} "
                f"- Valor sujo: {formatar_real(item['valor'])}"
            )

        await interaction.response.send_message("Relatório enviado!", ephemeral=True)


    @discord.ui.button(
        label="📩 Avisar TODOS no DM",
        style=discord.ButtonStyle.primary,
        custom_id="lavagem_dm"
    )
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

# ================= PAINEL =================

async def enviar_painel_lavagem():
    canal = bot.get_channel(CANAL_INICIAR_LAVAGEM_ID)

    async for m in canal.history(limit=10):
        if m.author == bot.user:
            return

    embed = discord.Embed(
        title="🧼 Sistema de Lavagem",
        description="Use os botões abaixo para registrar a lavagem.",
        color=0x1abc9c
    )

    await canal.send(embed=embed, view=LavagemView())


async def enviar_ou_atualizar_painel(canal_id, titulo_embed, embed, view):
    canal = bot.get_channel(canal_id)
    if not canal:
        return

    async for msg in canal.history(limit=50):
        if (
            msg.author == bot.user
            and msg.embeds
            and msg.embeds[0].title == titulo_embed
        ):
            try:
                await edit_queue.put(msg.edit(embed=embed, view=view))
                return
            except:
                return

    await canal.send(embed=embed, view=view)

# =========================================================
# ========================= LIVES ==========================
# =========================================================

ADM_ID = 467673818375389194


# ================= BANCO =================

async def carregar_lives():
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM lives")

    return {
        r["user_id"]: {
            "link": r["link"],
            "divulgado": r["divulgado"]
        }
        for r in rows
    }


async def salvar_live(user_id, link):
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO lives (user_id, link, divulgado)
            VALUES ($1, $2, false)
            ON CONFLICT (user_id)
            DO UPDATE SET link = $2
            """,
            str(user_id),
            link
        )


async def atualizar_divulgado(user_id, valor):
    async with db.acquire() as conn:
        await conn.execute(
            "UPDATE lives SET divulgado=$1 WHERE user_id=$2",
            valor,
            str(user_id)
        )


async def remover_live_db(user_id):
    async with db.acquire() as conn:
        await conn.execute(
            "DELETE FROM lives WHERE user_id=$1",
            str(user_id)
        )

# ================= CHECAR TWITCH =================

async def checar_se_esta_ao_vivo(canal):

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

                thumbnail = info["thumbnail_url"]\
                    .replace("{width}", "1280")\
                    .replace("{height}", "720")

                return (
                    True,
                    info.get("title"),
                    info.get("game_name"),
                    thumbnail
                )

        return False, None, None, None

    except Exception as e:

        print("Erro Twitch API:", e)

        return False, None, None, None

# ================= DIVULGAÇÃO =================

async def divulgar_live(user_id, link, titulo, jogo, thumbnail):

    try:

        canal = await bot.fetch_channel(CANAL_DIVULGACAO_LIVE_ID)

        user = await bot.fetch_user(int(user_id))

        embed = discord.Embed(
            title="🔴 LIVE AO VIVO NA TWITCH!",
            color=0x9146FF
        )

        embed.description = (
            f"👤 **Streamer:** {user.mention}\n"
            f"🎮 **Jogo:** {jogo or 'Não informado'}\n"
            f"📝 **Título:** {titulo or 'Sem título'}\n\n"
            f"🔗 {link}\n\n"
            f"🚨 **Entrou ao vivo agora!**"
        )

        if thumbnail:
            embed.set_image(url=thumbnail)

        await canal.send(
            content=f"🚨 {user.mention} está ao vivo!",
            embed=embed
        )

        print(f"📢 Live divulgada: {user_id}")

    except Exception as e:

        print("Erro divulgar live:", e)

# ================= LOOP TWITCH =================

@tasks.loop(minutes=2)
async def verificar_lives_twitch():

    print("🔄 Verificando lives na Twitch...")

    try:

        lives = await carregar_lives()

        for user_id, data in lives.items():

            link = data.get("link", "")
            divulgado = data.get("divulgado", False)

            canal = link.lower().strip()

            canal = canal.replace("https://", "")
            canal = canal.replace("http://", "")
            canal = canal.replace("www.", "")
            canal = canal.replace("twitch.tv/", "")
            canal = canal.split("/")[0].strip()

            if not canal:
                continue

            ao_vivo, titulo, jogo, thumbnail = await checar_se_esta_ao_vivo(canal)

            print(f"🎮 {canal} ao vivo? {ao_vivo}")

            # live terminou
            if not ao_vivo and divulgado:

                await atualizar_divulgado(user_id, False)

            # live começou
            if ao_vivo and not divulgado:

                await divulgar_live(user_id, link, titulo, jogo, thumbnail)

                await atualizar_divulgado(user_id, True)

    except Exception as e:

        print("Erro no loop Twitch:", e)

# ================= CADASTRO =================

class CadastrarLiveModal(discord.ui.Modal, title="🎥 Cadastrar Live"):
    link = discord.ui.TextInput(
        label="Cole o link da sua live",
        placeholder="https://twitch.tv/seucanal"
    )

    def extrair_canal_twitch(self, link):
        canal = link.lower().strip()
        canal = canal.replace("https://", "")
        canal = canal.replace("http://", "")
        canal = canal.replace("www.", "")
        canal = canal.replace("twitch.tv/", "")
        canal = canal.split("/")[0].strip()
        return canal

    async def on_submit(self, interaction: discord.Interaction):
        lives = await carregar_lives()

        novo_link = self.link.value.strip()
        novo_canal = self.extrair_canal_twitch(novo_link)

        if not novo_canal:
            await interaction.response.send_message("❌ Link inválido.", ephemeral=True)
            return

        for uid, data in lives.items():
            canal_existente = self.extrair_canal_twitch(data.get("link", ""))

            if canal_existente == novo_canal:
                await interaction.response.send_message("❌ Esse canal já está cadastrado.", ephemeral=True)
                return

        await salvar_live(interaction.user.id, novo_link)

        embed = discord.Embed(
            title="✅ Live cadastrada!",
            description=f"{interaction.user.mention}\n{novo_link}",
            color=0x2ecc71
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)


# ================= VIEW CADASTRO =================

class CadastrarLiveView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🎥 Cadastrar minha Live",
        style=discord.ButtonStyle.primary,
        custom_id="cadastrar_live_btn"
    )
    async def cadastrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(CadastrarLiveModal())


# ================= SELECT REMOVER =================

class SelectRemoverLive(discord.ui.Select):
    def __init__(self):
        self.options_list = []
        super().__init__(placeholder="Carregando...", options=[])

    async def refresh_options(self):
        lives = await carregar_lives()
        self.options = [
            discord.SelectOption(
                label=data["link"][:80],
                description=f"Usuário: {uid}",
                value=uid
            )
            for uid, data in lives.items()
        ] or [discord.SelectOption(label="Nenhuma live", value="none")]

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != ADM_ID:
            await interaction.response.send_message("❌ Apenas ADM.", ephemeral=True)
            return

        uid = self.values[0]
        await remover_live_db(uid)
        await interaction.response.send_message("✅ Live removida.", ephemeral=True)


class RemoverLiveView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)

    async def on_timeout(self):
        self.stop()

    async def interaction_check(self, interaction):
        select = SelectRemoverLive()
        await select.refresh_options()
        self.clear_items()
        self.add_item(select)
        return True


# ================= ADMIN =================

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
            await interaction.response.send_message("❌ Apenas ADM.", ephemeral=True)
            return

        lives = await carregar_lives()

        texto = ""
        for uid, data in lives.items():
            texto += f"👤 <@{uid}>\n🔗 {data['link']}\n\n"

        embed = discord.Embed(
            title="📡 Lives cadastradas",
            description=texto or "Nenhuma.",
            color=0x3498db
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @discord.ui.button(
        label="🗑️ Remover Live",
        style=discord.ButtonStyle.danger,
        custom_id="remover_live_admin_btn"
    )
    async def remover(self, interaction: discord.Interaction, button: discord.ui.Button):
        if interaction.user.id != ADM_ID:
            await interaction.response.send_message("❌ Apenas ADM.", ephemeral=True)
            return

        view = RemoverLiveView()
        await interaction.response.send_message("Escolha:", view=view, ephemeral=True)


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
            await interaction.response.send_message("❌ Apenas ADM.", ephemeral=True)
            return

        await interaction.response.send_message("Painel ADM:", view=GerenciarLivesView(), ephemeral=True)


# ================= PAINÉIS =================

async def enviar_painel_admin_lives():
    embed = discord.Embed(
        title="⚙️ Painel ADM - Lives",
        description="Gerencie todas as lives cadastradas.",
        color=0xe74c3c
    )

    await enviar_ou_atualizar_painel(
        CANAL_CADASTRO_LIVE_ID,
        "⚙️ Painel ADM - Lives",
        embed,
        PainelLivesAdmin()
    )

async def enviar_painel_lives():
    embed = discord.Embed(
        title="🎥 Cadastro de Live",
        description="Clique no botão para cadastrar sua live.",
        color=0x9146FF
    )

    await enviar_ou_atualizar_painel(
        CANAL_CADASTRO_LIVE_ID,
        "🎥 Cadastro de Live",
        embed,
        CadastrarLiveView()
    )

# =========================================================
# ======================== AÇÕES ==========================
# =========================================================

# =========================================================
# AÇÕES DA SEMANA
# =========================================================

ACOES_SEMANA = {
    # Ilimitadas
    "Loja de Armas (Ammunation)": None,
    "Loja de Bebidas": None,
    "Loja de Departamento": None,
    "Mergulhador": None,
    "Grapeseed": None,
    "Companhia de Gás": None,
    "Life Invader": None,
    "Aeroporto de Sucata": None,
    "Carro Forte": None,

    # Limitadas
    "Joalheria": 5,
    "Banco Fleeca": 4,
    "Banco de Paleto": 1,
    "Banco Central": 1,
    "Banco Bahamas": 1,
    "Nióbio": 1,
}

# =========================================================
# ================= FORMATAR DINHEIRO =====================
# =========================================================

def formatar_dinheiro(valor):

    try:
        valor = float(valor)
    except:
        valor = 0

    return f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# =========================================================
# =============== CARGOS PERMITIDOS AÇÃO ==================
# =========================================================

CARGOS_ACAO = [
    AGREGADO_ROLE_ID,
    CARGO_MEMBRO_ID,
    CARGO_SOLDADO_ID,
    CARGO_GERENTE_ID,
    CARGO_GERENTE_GERAL_ID,
    CARGO_01_ID,
    CARGO_02_ID,
    CARGO_RESP_METAS_ID,
    CARGO_RESP_ACAO_ID,
    CARGO_RESP_VENDAS_ID,
    CARGO_RESP_PRODUCAO_ID
]


# =========================================================
# ================= CARREGAR AÇÕES SEMANA =================
# =========================================================

async def carregar_acoes_semana():

    inicio_semana = agora_db() - timedelta(days=agora_db().weekday())

    async with db.acquire() as conn:

        rows = await conn.fetch(
            """
            SELECT tipo, COUNT(*) as total
            FROM acoes_semana
            WHERE data >= $1
            GROUP BY tipo
            """,
            inicio_semana
        )

    return {r["tipo"]: r["total"] for r in rows}


# =========================================================
# ================= EMBED PAINEL ==========================
# =========================================================

async def gerar_embed_acoes():

    feitos = await carregar_acoes_semana()

    linhas_limitadas = []
    linhas_ilimitadas = []

    for acao, limite in ACOES_SEMANA.items():

        qtd = feitos.get(acao, 0)

        # ================= LIMITADAS =================
        if limite is not None:

            if qtd >= limite:
                status = "🟢"
            elif qtd > 0:
                status = "🟡"
            else:
                status = "🔴"

            linhas_limitadas.append(
                f"{status} **{acao}** {qtd}/{limite}"
            )

        # ================= ILIMITADAS =================
        else:

            linhas_ilimitadas.append(
                f"🔹 **{acao}** {qtd} ♾️"
            )

    embed = discord.Embed(
        title="🚨 AÇÕES DA SEMANA",
        color=0xe74c3c
    )

    if linhas_limitadas:
        embed.add_field(
            name="🎯 Ações principais",
            value="\n".join(linhas_limitadas),
            inline=False
        )

    if linhas_ilimitadas:
        embed.add_field(
            name="📦 Ações ilimitadas",
            value="\n".join(linhas_ilimitadas),
            inline=False
        )

    embed.set_footer(text="Reset automático toda segunda às 00:00")

    return embed

# =========================================================
# ================= MODAL EXTERNOS ========================
# =========================================================

class ExternosModal(discord.ui.Modal, title="Participantes externos"):

    nomes = discord.ui.TextInput(
        label="Pessoas de fora (opcional)",
        style=discord.TextStyle.paragraph,
        required=False
    )

    def __init__(self, acao):
        super().__init__()
        self.acao = acao

    async def on_submit(self, interaction: discord.Interaction):

        view = SelecionarMembrosView(self.acao, self.nomes.value)

        await interaction.response.send_message(
            "Selecione os participantes:",
            view=view,
            ephemeral=True
        )


# =========================================================
# ================= SELECIONAR MEMBROS ====================
# =========================================================

class SelecionarMembros(discord.ui.UserSelect):

    def __init__(self, view_ref):

        super().__init__(
            placeholder="Selecione os participantes",
            min_values=1,
            max_values=10
        )

        self.view_ref = view_ref

    async def callback(self, interaction: discord.Interaction):

        membros_validos = []

        for m in self.values:

            if any(role.id in CARGOS_ACAO for role in m.roles):
                membros_validos.append(m)

        if not membros_validos:

            await interaction.response.send_message(
                "Nenhum membro selecionado possui cargo permitido.",
                ephemeral=True
            )
            return

        self.view_ref.membros = membros_validos

        await interaction.response.send_message(
            f"{len(membros_validos)} participantes selecionados.\nClique em **Enviar Escalação** quando terminar.",
            ephemeral=True
        )


class SelecionarMembrosView(discord.ui.View):

    def __init__(self, acao, externos):
        super().__init__(timeout=300)

        self.acao = acao
        self.externos = externos
        self.membros = []

        self.add_item(SelecionarMembros(self))
        self.add_item(EnviarEscalacaoButton(self))

class EnviarEscalacaoButton(discord.ui.Button):

    def __init__(self, view_ref):
        super().__init__(
            label="📤 Enviar Escalação",
            style=discord.ButtonStyle.success
        )

        self.view_ref = view_ref

    async def callback(self, interaction: discord.Interaction):

        membros = self.view_ref.membros

        if not membros:
            await interaction.response.send_message(
                "Selecione os participantes primeiro.",
                ephemeral=True
            )
            return

        async with db.acquire() as conn:

            acao_id = await conn.fetchval(
                """
                INSERT INTO acoes_semana (tipo,data,autor)
                VALUES ($1,$2,$3)
                RETURNING id
                """,
                self.view_ref.acao,
                agora_db(),
                str(interaction.user.id)
            )

            for m in membros:

                await conn.execute(
                    """
                    INSERT INTO participantes_acoes (acao_id,user_id)
                    VALUES ($1,$2)
                    """,
                    acao_id,
                    str(m.id)
                )

            if self.view_ref.externos:

                nomes = [n.strip() for n in self.view_ref.externos.split("\n") if n.strip()]

                for nome in nomes:

                    await conn.execute(
                        """
                        INSERT INTO participantes_acoes (acao_id,nome_externo)
                        VALUES ($1,$2)
                        """,
                        acao_id,
                        nome
                    )

        await registrar_relatorio_acao(interaction.guild, acao_id)

        await atualizar_painel_acoes(interaction.guild)

        await interaction.response.send_message(
            "Escalação enviada com sucesso!",
            ephemeral=True
        )


# =========================================================
# ================= SELECT AÇÃO ===========================
# =========================================================

class AcaoSelect(discord.ui.Select):

    def __init__(self):

        options = [
            discord.SelectOption(label=a)
            for a in ACOES_SEMANA.keys()
        ]

        super().__init__(
            placeholder="Escolha a ação",
            options=options,
            custom_id="acao_select_menu"
        )

    async def callback(self, interaction: discord.Interaction):

        acao = self.values[0]

        await interaction.response.send_modal(ExternosModal(acao))


class PainelAcoesView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

        self.add_item(AcaoSelect())


# =========================================================
# ================= ATUALIZAR PAINEL ======================
# =========================================================

async def atualizar_painel_acoes(guild):

    canal = guild.get_channel(CANAL_ESCALACOES_ID)

    embed = await gerar_embed_acoes()

    async for msg in canal.history(limit=20):

        if msg.author == guild.me and msg.embeds:

            if msg.embeds[0].title == "🚨 AÇÕES DA SEMANA":

                await edit_queue.put(msg.edit(embed=embed, view=PainelAcoesView()))
                return

    await canal.send(embed=embed, view=PainelAcoesView())


# =========================================================
# ================= RESULTADO DA AÇÃO =====================
# =========================================================

class ResultadoAcaoView(discord.ui.View):

    def __init__(self, acao_id):
        super().__init__(timeout=None)
        self.acao_id = acao_id

    @discord.ui.button(label="🏆 Ganhou", style=discord.ButtonStyle.success)
    async def ganhou(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(ResultadoModal(self.acao_id, True))

    @discord.ui.button(label="💀 Perdeu", style=discord.ButtonStyle.danger)
    async def perdeu(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(ResultadoModal(self.acao_id, False))


# =========================================================
# ================= MODAL RESULTADO =======================
# =========================================================

class ResultadoModal(discord.ui.Modal):

    dinheiro = discord.ui.TextInput(label="Dinheiro ganho", required=False)
    ouro = discord.ui.TextInput(label="Ouro ganho", required=False)

    def __init__(self, acao_id, venceu):
        super().__init__(title="Resultado da ação")

        self.acao_id = acao_id
        self.venceu = venceu

    async def on_submit(self, interaction: discord.Interaction):

        resultado = "GANHOU" if self.venceu else "PERDEU"

        dinheiro = 0
        ouro = 0

        try:
            if self.dinheiro.value.strip():
                dinheiro = int(self.dinheiro.value.replace(".", "").replace(",", ""))

            if self.ouro.value:
                ouro = int(self.ouro.value.replace(".", "").replace(",", ""))

        except:
            await interaction.response.send_message(
                "Valor inválido.",
                ephemeral=True
            )
            return

        async with db.acquire() as conn:

            participantes = await conn.fetch(
                """
                SELECT user_id, nome_externo
                FROM participantes_acoes
                WHERE acao_id=$1
                """,
                self.acao_id
            )

        qtd = len(participantes)

        if qtd == 0:
            qtd = 1

        valor_por_pessoa = dinheiro // qtd

        # ==============================
        # DISTRIBUIR NAS METAS
        # ==============================

        for p in participantes:

            uid = p["user_id"]

            if uid not in metas_cache:
                continue

            metas_cache[uid]["acao"] += valor_por_pessoa

            await salvar_meta(
                int(uid),
                metas_cache[uid]["canal_id"],
                metas_cache[uid]["dinheiro"],
                metas_cache[uid]["polvora"],
                metas_cache[uid]["acao"]
            )

            guild = interaction.guild
            membro = guild.get_member(int(uid))

            if membro:
                await atualizar_painel_meta(membro)

        # ==============================
        # EMBED RESULTADO
        # ==============================
     
        participantes_marcados = []
        for p in participantes:
            uid = p.get("user_id")
            nome = p.get("nome_externo")
            if uid:
                participantes_marcados.append(f"<@{uid}>")
            elif nome:
                participantes_marcados.append(nome)

        async with db.acquire() as conn:
            acao = await conn.fetchrow(
                "SELECT tipo FROM acoes_semana WHERE id=$1",
                self.acao_id
            )

        embed = discord.Embed(
            title="📊 Resultado da Ação",
            color=0x2ecc71 if self.venceu else 0xe74c3c
        )

        embed.add_field(
            name="🏦 Ação",
            value=acao["tipo"],
            inline=False
        )

        embed.add_field(
            name="Resultado",
            value=resultado,
            inline=False
        )

        if self.venceu and dinheiro:
            embed.add_field(
                name="💰 Dinheiro",
                value=f"R$ {dinheiro:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                inline=False
            )

        if self.venceu and ouro:
            embed.add_field(
                name="🥇 Ouro",
                value=str(ouro),
                inline=False
            )

        embed.add_field(
            name="👥 Participantes",
            value="\n".join(participantes_marcados),
            inline=False
        )
# =========================================================
# ================= RELATÓRIO AÇÃO ========================
# =========================================================

async def registrar_relatorio_acao(guild, acao_id):

    canal = guild.get_channel(CANAL_RELATORIO_ACOES_ID)

    async with db.acquire() as conn:

        acao = await conn.fetchrow(
            "SELECT tipo FROM acoes_semana WHERE id=$1",
            acao_id
        )

    embed = discord.Embed(
        title="🚨 Nova Ação Registrada",
        description=f"🏦 **Ação:** {acao['tipo']}",
        color=0xf39c12
    )

    await canal.send(
        embed=embed,
        view=ResultadoAcaoView(acao_id)
    )
@tasks.loop(time=time(hour=0, minute=0, tzinfo=ZoneInfo("America/Sao_Paulo")))
async def reset_acoes_segunda():

    agora = datetime.now(ZoneInfo("America/Sao_Paulo"))

    if agora.weekday() == 0:  # segunda

        async with db.acquire() as conn:
            await conn.execute("DELETE FROM acoes_semana")
            await conn.execute("DELETE FROM participantes_acoes")

        print("🔄 Ações da semana resetadas.")

        guild = bot.get_guild(GUILD_ID)
        if guild:
            await atualizar_painel_acoes(guild)
# =========================================================
# =========================== METAS ========================
# =========================================================

metas_cache = {}
criando_meta = set()

# =========================================================
# JSON
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
# META POR CARGO
# =========================================================

def obter_meta_dinheiro(member: discord.Member) -> int:

    roles = [r.id for r in member.roles]

    if "CARGO_MECANICO_ID" in globals() and CARGO_MECANICO_ID in roles:
        return 100_000

    if any(r in roles for r in [
        CARGO_RESP_METAS_ID,
        CARGO_RESP_ACAO_ID,
        CARGO_RESP_VENDAS_ID,
        CARGO_RESP_PRODUCAO_ID
    ]):
        return 150_000

    if CARGO_MEMBRO_ID in roles or CARGO_SOLDADO_ID in roles:
        return 250_000

    return 0

# =========================================================
# MODAL
# =========================================================

class RegistrarValorModal(discord.ui.Modal):

    def __init__(self, tipo, member_id):
        super().__init__(title=f"Registrar {tipo}")
        self.tipo = tipo
        self.member_id = int(member_id)

        self.valor = discord.ui.TextInput(
            label="Valor",
            placeholder="Ex: 50000"
        )

        self.add_item(self.valor)

    async def on_submit(self, interaction: discord.Interaction):

        dados = metas_cache.get(str(self.member_id))
        if not dados:
            await interaction.response.send_message("Meta não encontrada.", ephemeral=True)
            return

        try:
            valor = int(self.valor.value.replace(".", "").replace(",", ""))
        except:
            await interaction.response.send_message("Valor inválido.", ephemeral=True)
            return

        dados[self.tipo] += valor

        await salvar_meta(
            self.member_id,
            dados["canal_id"],
            dados["dinheiro"],
            dados["polvora"],
            dados["acao"]
        )

        membro = interaction.guild.get_member(self.member_id)
        if membro:
            await atualizar_painel_meta(membro)

        await interaction.response.send_message("Registrado.", ephemeral=True)

# =========================================================
# BOTÕES META
# =========================================================

class BotaoDinheiro(discord.ui.Button):
    def __init__(self, member_id):
        super().__init__(
            label="💰 Dinheiro",
            style=discord.ButtonStyle.success,
            custom_id=f"meta_dinheiro_{member_id}"
        )
        self.member_id = int(member_id)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.member_id:
            await interaction.response.send_message("Você não pode registrar meta de outro membro.", ephemeral=True)
            return
        await interaction.response.send_modal(RegistrarValorModal("dinheiro", self.member_id))


class BotaoAcao(discord.ui.Button):
    def __init__(self, member_id):
        super().__init__(
            label="🎯 Ação",
            style=discord.ButtonStyle.secondary,
            custom_id=f"meta_acao_{member_id}"
        )
        self.member_id = int(member_id)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.member_id:
            await interaction.response.send_message("Você não pode registrar meta de outro membro.", ephemeral=True)
            return
        await interaction.response.send_modal(RegistrarValorModal("acao", self.member_id))


class BotaoPolvora(discord.ui.Button):
    def __init__(self, member_id):
        super().__init__(
            label="💣 Pólvora",
            style=discord.ButtonStyle.primary,
            custom_id=f"meta_polvora_{member_id}"
        )
        self.member_id = int(member_id)

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.member_id:
            await interaction.response.send_message("Você não pode registrar meta de outro membro.", ephemeral=True)
            return
        await interaction.response.send_modal(RegistrarValorModal("polvora", self.member_id))


class BotaoReiniciar(discord.ui.Button):
    def __init__(self, member_id):
        super().__init__(
            label="🔄 Reiniciar Quadro",
            style=discord.ButtonStyle.danger,
            custom_id=f"meta_reiniciar_{member_id}"
        )
        self.member_id = int(member_id)

    async def callback(self, interaction: discord.Interaction):

        if not any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles):
            await interaction.response.send_message("Apenas gerentes podem reiniciar.", ephemeral=True)
            return

        dados = metas_cache.get(str(self.member_id))
        if not dados:
            return

        await salvar_meta(self.member_id, dados["canal_id"], 0, 0, 0)

        membro = interaction.guild.get_member(self.member_id)
        if membro:
            await atualizar_painel_meta(membro)

        await interaction.response.send_message("Quadro reiniciado.", ephemeral=True)


class BotaoLimparSala(discord.ui.Button):
    def __init__(self):
        super().__init__(
            label="🧹 Limpar Sala",
            style=discord.ButtonStyle.secondary,
            custom_id="meta_limpar_sala"
        )

    async def callback(self, interaction: discord.Interaction):

        if not any(r.id in [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID] for r in interaction.user.roles):
            await interaction.response.send_message("Apenas gerentes podem limpar.", ephemeral=True)
            return

        await interaction.response.defer(ephemeral=True)

        canal = interaction.channel

        async for msg in canal.history(limit=200):
            if msg.author == interaction.guild.me and msg.embeds:
                if msg.embeds[0].title == "📊 META INDIVIDUAL":
                    continue
            try:
                await msg.delete()
            except:
                pass

        await interaction.followup.send("Sala limpa com sucesso.", ephemeral=True)

# =========================================================
# VIEW META
# =========================================================

class MetaView(discord.ui.View):
    def __init__(self, member_id):
        super().__init__(timeout=None)

        guild = bot.get_guild(GUILD_ID)
        if not guild:
            return

        member = guild.get_member(int(member_id))
        if not member:
            return

        roles = [r.id for r in member.roles]

        if AGREGADO_ROLE_ID in roles and CARGO_MEMBRO_ID not in roles:
            self.add_item(BotaoPolvora(member_id))
        else:
            self.add_item(BotaoDinheiro(member_id))
            self.add_item(BotaoAcao(member_id))

        self.add_item(BotaoReiniciar(member_id))
        self.add_item(BotaoLimparSala())


# =========================================================
# ================= BOTÕES META ===========================
# =========================================================

# ================= DINHEIRO =================

class BotaoDinheiro(discord.ui.Button):

    def __init__(self, member_id):

        super().__init__(
            label="💰 Dinheiro",
            style=discord.ButtonStyle.success,
            custom_id=f"meta_dinheiro_{member_id}"
        )

        self.member_id = member_id

    async def callback(self, interaction: discord.Interaction):

        if interaction.user.id != self.member_id:
            await interaction.response.send_message(
                "Você não pode registrar meta de outro membro.",
                ephemeral=True
            )
            return

        await interaction.response.send_modal(
            RegistrarValorModal("dinheiro", self.member_id)
        )


# ================= AÇÃO =================

class BotaoAcao(discord.ui.Button):

    def __init__(self, member_id):

        super().__init__(
            label="🎯 Ação",
            style=discord.ButtonStyle.secondary,
            custom_id=f"meta_acao_{member_id}"
        )

        self.member_id = member_id

    async def callback(self, interaction: discord.Interaction):

        if interaction.user.id != self.member_id:
            await interaction.response.send_message(
                "Você não pode registrar meta de outro membro.",
                ephemeral=True
            )
            return

        await interaction.response.send_modal(
            RegistrarValorModal("acao", self.member_id)
        )


# ================= PÓLVORA =================

class BotaoPolvora(discord.ui.Button):

    def __init__(self, member_id):

        super().__init__(
            label="💣 Pólvora",
            style=discord.ButtonStyle.primary,
            custom_id=f"meta_polvora_{member_id}"
        )

        self.member_id = member_id

    async def callback(self, interaction: discord.Interaction):

        if interaction.user.id != self.member_id:
            await interaction.response.send_message(
                "Você não pode registrar meta de outro membro.",
                ephemeral=True
            )
            return

        await interaction.response.send_modal(
            RegistrarValorModal("polvora", self.member_id)
        )
# =========================================================
# CRIAR SALA
# =========================================================

async def criar_sala_meta(member: discord.Member):

    if member.id in criando_meta:
        return

    criando_meta.add(member.id)

    try:
        metas = metas_cache
        guild = member.guild

        # Já existe no banco
        if str(member.id) in metas:

            canal = guild.get_channel(metas[str(member.id)]["canal_id"])

            # canal existe
            if canal:
                await atualizar_painel_meta(member)
                criando_meta.discard(member.id)
                return

            # canal foi apagado
            else:
                metas.pop(str(member.id))


        # =================================================
        # VERIFICA SE JÁ EXISTE CANAL ANTIGO
        # =================================================

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

            await atualizar_painel_meta(member)

            criando_meta.discard(member.id)
            return

        # =================================================
        # CRIAR NOVA SALA
        # =================================================

        categoria_id = obter_categoria_meta(member)
        if not categoria_id:
            criando_meta.discard(member.id)
            return

        categoria = guild.get_channel(categoria_id)
        if not categoria:
            criando_meta.discard(member.id)
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

        # =================================================
        # SALVAR META
        # =================================================

        await salvar_meta(
            member.id,
            canal.id,
            0,
            0,
            0
        )

        await atualizar_painel_meta(member)

    except Exception as e:
        print("Erro criar canal meta:", e)

    finally:
        criando_meta.discard(member.id)

# =========================================================
# ATUALIZAR / RECRIAR PAINEL (VERSÃO ESTÁVEL - SEM DUPLICAR)
# =========================================================

async def atualizar_painel_meta(member: discord.Member):

    dados = metas_cache.get(str(member.id))
    if not dados:
        return

    canal = member.guild.get_channel(dados["canal_id"])
    if not canal:
        await criar_sala_meta(member)
        return

    dinheiro = dados["dinheiro"]
    polvora = dados["polvora"]
    acao = dados["acao"]
    meta_alvo = obter_meta_dinheiro(member)

    embed = discord.Embed(
        title="📊 META INDIVIDUAL",
        color=0x2ecc71
    )

    if meta_alvo > 0:
        embed.add_field(
            name="💰 Dinheiro",
            value=f"R$ {dinheiro:,}".replace(",", "."),
            inline=True
        )
        embed.add_field(
            name="🎯 Ação",
            value=f"R$ {acao:,}".replace(",", "."),
            inline=True
        )
        embed.add_field(
            name="📈 Progresso",
            value=f"R$ {dinheiro:,} / {meta_alvo:,}".replace(",", "."),
            inline=False
        )
    else:
        embed.add_field(
            name="💣 Pólvora",
            value=str(polvora),
            inline=True
        )

    view = MetaView(member.id)

    painel_encontrado = None

    # Procura painel já existente (evita duplicação no redeploy)
    async for msg in canal.history(limit=30):
        if msg.author == member.guild.me and msg.embeds:
            if msg.embeds[0].title == "📊 META INDIVIDUAL":
                painel_encontrado = msg
                break

    # Se encontrou → apenas edita (NÃO deleta)
    if painel_encontrado:
        try:
            await asyncio.sleep(0.3)  # 👈 ADICIONE ESTA LINHA
            await edit_queue.put(painel_encontrado.edit(embed=embed, view=view))
            return
        except Exception as e:
            print("Erro ao editar painel meta:", e)

    # Se não encontrou → cria novo
    try:
        await canal.send(embed=embed, view=view)
    except Exception as e:
        print("Erro ao criar painel meta:", e)
# =========================================================
# =========== RECONSTRUIR VIEWS DAS METAS =================
# =========================================================

async def reconstruir_views_metas():

    guild = bot.get_guild(GUILD_ID)

    if not guild:
        return

    for uid, dados in metas_cache.items():

        canal = guild.get_channel(dados["canal_id"])

        if not canal:
            continue

        membro = guild.get_member(int(uid))

        if not membro:
            continue

        async for msg in canal.history(limit=30):

            if msg.author == bot.user and msg.embeds:

                try:
                    await edit_queue.put(msg.edit(view=MetaView(int(uid))))
                except Exception as e:
                    print("Erro restaurando view meta:", e)

# =========================================================
# MOVER CATEGORIA AO SUBIR CARGO
# =========================================================

async def atualizar_categoria_meta(member):
    metas = metas_cache
    if str(member.id) not in metas:
        return

    canal = member.guild.get_channel(metas[str(member.id)]["canal_id"])
    if not canal:
        return

    nova = obter_categoria_meta(member)
    if nova and canal.category_id != nova:
        await edit_queue.put(
            canal.edit(category=member.guild.get_channel(nova))
        )

# =========================================================
# EVENTOS
# =========================================================

@bot.event
async def on_member_update(before, after):

    if after.bot:
        return

    metas = metas_cache

    # recebeu agregado
    tinha_agregado = any(r.id == AGREGADO_ROLE_ID for r in before.roles)
    tem_agregado = any(r.id == AGREGADO_ROLE_ID for r in after.roles)

    # =====================================================
    # NOVO AGREGADO → CRIAR META
    # =====================================================

    if not tinha_agregado and tem_agregado:

        await asyncio.sleep(2)

        # anti duplicação
        if str(after.id) not in metas:
            await criar_sala_meta(after)

        return

    # =====================================================
    # MEMBRO JÁ TEM META → ATUALIZA
    # =====================================================

    if str(after.id) in metas:

        await asyncio.sleep(1)

        await atualizar_categoria_meta(after)
        await atualizar_painel_meta(after)

    # agregado sempre mantém painel atualizado
    if tem_agregado:
        await atualizar_categoria_meta(after)
        await atualizar_painel_meta(after)


# =========================================================
# NOVO MEMBRO
# =========================================================

@bot.event
async def on_member_join(member):

    if member.bot:
        return

    if any(r.id == AGREGADO_ROLE_ID for r in member.roles):

        await asyncio.sleep(2)

        if str(member.id) not in metas_cache:
            await criar_sala_meta(member)


# =========================================================
# DETECTA EXCLUSÃO DE CANAL DE META
# =========================================================

@bot.event
async def on_guild_channel_delete(channel):

    metas = metas_cache

    for uid, dados in list(metas.items()):

        if dados["canal_id"] == channel.id:

            print(f"🗑️ Canal de meta apagado: {uid}")

            metas.pop(uid)

            try:
                async with db.acquire() as conn:
                    await conn.execute(
                        "DELETE FROM metas WHERE user_id=$1",
                        uid
                    )
            except Exception as e:
                print("Erro removendo meta do banco:", e)

            break


# =========================================================
# RECRIAR PAINEL SE APAGAREM A MENSAGEM
# =========================================================

@bot.event
async def on_raw_message_delete(payload):

    metas = metas_cache

    if payload.guild_id != GUILD_ID:
        return

    guild = bot.get_guild(payload.guild_id)
    if not guild:
        return

    for uid, dados in metas.items():

        if dados["canal_id"] != payload.channel_id:
            continue

        member = guild.get_member(int(uid))
        if not member:
            return

        canal = guild.get_channel(payload.channel_id)
        if not canal:
            return

        encontrou = False

        try:

            async for msg in canal.history(limit=30):

                if msg.author == guild.me and msg.embeds:
                    encontrou = True
                    break

        except:
            return

        # painel foi apagado
        if not encontrou:

            print(f"♻️ Painel recriado automaticamente para {member.display_name}")

            try:
                await atualizar_painel_meta(member)
            except Exception as e:
                print("Erro recriando painel:", e)

        break
# =========================================================
# VARREDURA (ATUALIZA TODOS QUE TÊM SALA)
# =========================================================

@tasks.loop(minutes=15)
async def varrer_agregados_sem_sala():
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return

    metas = metas_cache

    for member in guild.members:
        if member.bot:
            continue

        if not any(r.id == AGREGADO_ROLE_ID for r in member.roles):
            continue

        if str(member.id) not in metas:
            await criar_sala_meta(member)
            continue

        canal = guild.get_channel(metas[str(member.id)]["canal_id"])
        if not canal:
            await criar_sala_meta(member)
            continue

        await atualizar_categoria_meta(member)
        await atualizar_painel_meta(member)

# =========================================================
# RELATÓRIO + RESET
# =========================================================

async def enviar_relatorio_semanal():

    canal = bot.get_channel(RESULTADOS_METAS_ID)
    guild = bot.get_guild(GUILD_ID)
    metas = metas_cache

    linhas = []
    total = 0
    zerados = 0

    for uid, dados in metas.items():

        m = guild.get_member(int(uid))
        if not m:
            continue

        dinheiro = dados["dinheiro"]
        polvora = dados["polvora"]
        acao = dados["acao"]

        meta = obter_meta_dinheiro(m)

        total += dinheiro

        # ================= STATUS DA META =================

        if dinheiro == 0 and polvora == 0 and acao == 0:

            zerados += 1
            status = "❌ Não pagou"

        elif meta > 0 and dinheiro >= meta:

            status = "✅ META BATIDA"

        else:

            status = "⚠️ Pagou parcialmente"

        linhas.append(
            f"**{m.display_name}**\n"
            f"{status}\n"
            f"💰 {dinheiro}\n"
            f"💣 {polvora}\n"
            f"🎯 {acao}\n"
        )

    embed = discord.Embed(
        title="📊 RELATÓRIO SEMANAL",
        description="\n".join(linhas[:25]),
        color=0x2ecc71
    )

    embed.add_field(
        name="💰 Total Facção",
        value=f"R$ {total:,}".replace(",", "."),
        inline=False
    )

    embed.add_field(
        name="📉 Não pagaram",
        value=str(zerados),
        inline=True
    )

    await canal.send(embed=embed)

    embed.add_field(name="💰 Total Facção", value=f"R$ {total:,}".replace(",", "."), inline=False)
    embed.add_field(name="📉 Zerados", value=str(zerados))

    
@tasks.loop(time=time(hour=12, minute=0, tzinfo=ZoneInfo("America/Sao_Paulo")))
async def relatorio_semanal_task():
    agora = datetime.now(ZoneInfo("America/Sao_Paulo"))

    if agora.weekday() == 5:
        await enviar_relatorio_semanal()

        metas = metas_cache

        for uid, dados in metas.items():
            await salvar_meta(
                int(uid),
                dados["canal_id"],
                0,
                0,
                0
            )

        print("Metas resetadas após relatório semanal.")

# =========================================================
# =============== SOLICITAR SALA MANUAL ===================
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
                "Você já possui uma sala de meta.",
                ephemeral=True
            )
            return

        await interaction.response.defer(ephemeral=True)

        await criar_sala_meta(interaction.user)

        await interaction.followup.send(
            "Sua sala foi criada com sucesso!",
            ephemeral=True
        )

async def enviar_painel_solicitar_sala():

    canal = bot.get_channel(1337374500366450741)

    if not canal:
        return

    embed = discord.Embed(
        title="📂 Solicitar Sala de Meta",
        description="Clique no botão abaixo para criar sua sala manualmente.",
        color=0x2ecc71
    )

    async for msg in canal.history(limit=20):
        if msg.author == bot.user and msg.embeds:
            if msg.embeds[0].title == "📂 Solicitar Sala de Meta":
                await edit_queue.put(msg.edit(embed=embed, view=SolicitarSalaView()))
                return

    await canal.send(embed=embed, view=SolicitarSalaView())

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

    global db
    global http_session

    print("🔄 Iniciando configuração do bot...")

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

    # Registrar MetaView individual
    for uid in metas_cache.keys():
        try:
            bot.add_view(MetaView(int(uid)))
        except Exception as e:
            print("Erro registrando MetaView:", e)

    # Registrar SolicitarSalaView
    try:
        bot.add_view(SolicitarSalaView())
    except Exception as e:
        print("Erro registrando SolicitarSalaView:", e)

    # Registrar outras views
    outras_views = [
        RegistroView,
        CadastrarLiveView,
        PainelLivesAdmin,
        GerenciarLivesView,
        PolvoraView,
        ConfirmarPagamentoView,
        LavagemView,
        FabricacaoView,
        PainelAcoesView,
    ]

    for view_class in outras_views:
        try:
            bot.add_view(view_class())
        except Exception as e:
            print(f"Erro ao registrar view {view_class.__name__}:", e)

    # =====================================================
    # ================= RESTAURAR VIEWS METAS =============
    # =====================================================
    try:
        bot.loop.create_task(reconstruir_views_metas())
    except Exception as e:
        print("Erro reconstruindo views metas:", e)

    # =====================================================
    # ================= INICIAR LOOPS =====================
    # =====================================================

    try:
        if not verificar_lives_twitch.is_running():
            verificar_lives_twitch.start()
    except Exception as e:
        print("Erro loop twitch:", e)

    try:
        if not relatorio_semanal_polvoras.is_running():
            relatorio_semanal_polvoras.start()
    except Exception as e:
        print("Erro loop polvora:", e)

    try:
        if not relatorio_semanal_task.is_running():
            relatorio_semanal_task.start()
    except Exception as e:
        print("Erro loop metas:", e)

    try:
        if not varrer_agregados_sem_sala.is_running():
            varrer_agregados_sem_sala.start()

        # força primeira execução
        bot.loop.create_task(varrer_agregados_sem_sala())
    except Exception as e:
        print("Erro varredura metas:", e)

    try:
        if not limpar_lavagens_pendentes.is_running():
            limpar_lavagens_pendentes.start()
    except Exception as e:
        print("Erro loop limpeza lavagens:", e)

    try:
        if not reset_acoes_segunda.is_running():
            reset_acoes_segunda.start()
    except Exception as e:
        print("Erro reset ações semana:", e)

    # =====================================================
    # ================= RESTAURAR PRODUÇÕES ===============
    # =====================================================
    try:
        async with db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT pid FROM producoes WHERE pid NOT LIKE 'TESTE_%'"
            )

        for r in rows:
            pid = r["pid"]

            if pid not in producoes_tasks:
                task = bot.loop.create_task(acompanhar_producao(pid))
                producoes_tasks[pid] = task

        print(f"🏭 Produções restauradas: {len(rows)}")

    except Exception as e:
        print("Erro restaurar produções:", e)

    # =====================================================
    # ================= ENVIAR PAINÉIS ====================
    # =====================================================

    try:

        painel_tasks = [
            enviar_painel_fabricacao(),
            enviar_painel_lives(),
            enviar_painel_admin_lives(),
            enviar_painel_polvoras(bot),
            enviar_painel_lavagem(),
            enviar_painel_vendas(),
            atualizar_painel_acoes(bot.get_guild(GUILD_ID)),
            enviar_painel_solicitar_sala()
        ]

        for task in painel_tasks:
            try:
                await task
                await asyncio.sleep(0.7)  # delay anti rate limit
            except Exception as e:
                print("Erro em painel específico:", e)

        print("🖥️ Painéis verificados/enviados.")

    except Exception as e:
        print("Erro geral ao enviar painéis:", e)

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






