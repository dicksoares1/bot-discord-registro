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
# ================ ANTI ERRO DE INTERAÇÃO ==================

async def responder_interacao(interaction: discord.Interaction, *, defer=False, ephemeral=False):

    try:

        if interaction.response.is_done():
            return

        if defer:
            await interaction.response.defer(ephemeral=ephemeral)
        else:
            await responder_interacao(interaction, defer=True, ephemeral=True)

    except discord.errors.HTTPException:

        # interação já foi respondida ou expirou
        pass

    except Exception as e:

        print("Erro responder_interacao:", e)
        
# ==================== HTTP SESSÃO=========================

http_session = None

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
CANAL_HELICRASH_ID = 1478919637260435498

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
    indicado = discord.ui.TextInput(label="Indicado por", required=False)
    telefone = discord.ui.TextInput(label="Telefone In Game")

    async def on_submit(self, interaction: discord.Interaction):
        membro = interaction.user
        guild = interaction.guild

        # Atualiza nick
        await membro.edit(nick=f"{self.passaporte.value} - {self.nome.value}")

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
            embed = discord.Embed(title="📋 Novo Registro", color=0x2ecc71)

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


class TipoRegistroView(discord.ui.View):

    def __init__(self, nome, passaporte, indicado, telefone):
        super().__init__(timeout=300)
        self.add_item(
            TipoRegistroSelect(nome, passaporte, indicado, telefone)
        )


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

        linhas = [l for l in linhas if not l.startswith("⏳")]
        linhas = [l for l in linhas if not l.startswith("💰")]

        linhas.append(
            f"💰 Pago • Recebido por {user} • {agora_str}"
        )

        embed = self.set_status(embed, idx, linhas)

        # ================= FINALIZA VENDA =================

        finalizado = any(l.startswith("💰") for l in linhas) and any(l.startswith("✅") for l in linhas)

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

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="status_entregue")
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

        finalizado = any(l.startswith("💰") for l in linhas) and any(l.startswith("✅") for l in linhas)

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
                            pacotes_pt = int(l.replace("📦", "").replace("pacotes", "").strip())
                except:
                    pass

            if field.name == "🔫 SUB":
                try:
                    linhas = field.value.split("\n")
                    for l in linhas:
                        if "📦" in l:
                            pacotes_sub = int(l.replace("📦", "").replace("pacotes", "").strip())
                except:
                    pass

        # ===============================
        # ENVIAR NO BAÚ
        # ===============================

        canal_bau = interaction.guild.get_channel(CANAL_BAU_GALPAO_SUL_ID)

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
        valor_novo = total

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

        await responder_interacao(interaction, defer=True)

        try:

            inicio = datetime.strptime(self.data_inicio.value, "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value, "%d/%m/%Y")

        except:
            await interaction.followup.send(
                "Formato de data inválido. Use DD/MM/AAAA.",
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
                f"👤 <@{r['user_id']}> • R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            )

        total_fmt = f"{total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

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

        await interaction.followup.send(embed=embed, ephemeral=True)

# =========================================================
# ================= EDITAR== VENDA ========================
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
            await interaction.response.send_message("Valores inválidos.", ephemeral=True)
            return

        pacotes_pt = pt // 50
        pacotes_sub = sub // 50
        total = (pt * 50) + (sub * 90)
        valor_novo = total

        valor_formatado = f"{total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

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
                        field.value.replace("**R$ ", "")
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

            if field.name == "📝 Observações" and self.observacao.value:
                embed.set_field_at(
                    i,
                    name="📝 Observações",
                    value=self.observacao.value.strip(),
                    inline=False
                )

        titulo = embed.title
        pedido_numero = int(titulo.split("#")[1])

        await atualizar_valor_venda_db(pedido_numero, total)
        await self.message.edit(embed=embed)

        # ===============================
        # DETECTAR ALTERAÇÕES
        # ===============================

        alteracoes = []

        if pt_antigo != pt:
            alteracoes.append(f"PT: {pt_antigo} → {pt}")

        if sub_antigo != sub:
            alteracoes.append(f"SUB: {sub_antigo} → {sub}")

        if valor_antigo != valor_novo:
            valor_antigo_fmt = f"{valor_antigo:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
            alteracoes.append(f"Valor: R$ {valor_antigo_fmt} → R$ {valor_formatado}")

        if self.organizacao.value:
            alteracoes.append(f"Organização alterada")

        if self.observacao.value:
            alteracoes.append("Observação alterada")

        alteracao_texto = "\n".join(alteracoes) if alteracoes else "Nenhuma alteração detectada"

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
                name="🔧 Alterações",
                value=alteracao_texto,
                inline=False
            )

            await canal_log.send(embed=embed_log)

        await interaction.response.send_message("Venda editada com sucesso.", ephemeral=True)
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

    
# ==============================
# VARIÁVEIS GLOBAIS
# ==============================

producoes_tasks = {}
galpoes_ativos = set()
edit_queue = asyncio.Queue()
producoes_ativas = set()


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

        # pequena pausa para garantir consistência
        await asyncio.sleep(1)

        if pid not in producoes_tasks:

            task = bot.loop.create_task(
                acompanhar_producao(pid)
            )

            producoes_tasks[pid] = task

        await responder_interacao(interaction, defer=True)

# ================= 2ª TASK =================

class SegundaTaskView(discord.ui.View):

    def __init__(self, pid):
        super().__init__(timeout=None)

        self.pid = pid

        botao = discord.ui.Button(
            label="✅ Confirmar 2ª Task",
            style=discord.ButtonStyle.success,
            custom_id=f"segunda_task_{pid}"
        )

        botao.callback = self.confirmar

        self.add_item(botao)


    async def confirmar(self, interaction: discord.Interaction):

    if not interaction.response.is_done():
        await responder_interacao(interaction, defer=True)

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

        # responde rápido para não expirar interação
        await responder_interacao(interaction, defer=True)

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
            tempo_real = int(self.tempo * (polvora / 400))
            fim = inicio + timedelta(minutes=tempo_real)

            canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)

            if not canal:
                await interaction.followup.send(
                    "Canal de produção não encontrado.",
                    ephemeral=True
                )
                return

            # ================= EMBED COMPLETO =================

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
                task = asyncio.create_task(acompanhar_producao(pid))
                producoes_tasks[pid] = task

        except Exception as e:
            print("ERRO PRODUÇÃO:", e)


# =========================================================
# ================= VIEW FABRICAÇÃO ========================
# =========================================================

class FabricacaoView(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    # ================= GALPÕES NORTE =================

    @discord.ui.button(
        label="🏭 Galpões Norte",
        style=discord.ButtonStyle.primary,
        custom_id="fabricacao_norte"
    )
    async def norte(self, interaction: discord.Interaction, button: discord.ui.Button):

        async with db.acquire() as conn:

            ativo = await conn.fetchval(
                """
                SELECT 1
                FROM producoes
                WHERE galpao=$1
                AND CAST(fim AS timestamp) > NOW()
                """,
                "GALPÕES NORTE"
            )

        if ativo:

            await interaction.response.send_message(
                "Este galpão já está em produção.",
                ephemeral=True
            )
            return

        galpoes_ativos.add("GALPÕES NORTE")

        try:

            await interaction.response.send_modal(
                PolvoraProducaoModal("GALPÕES NORTE", 65)
            )

        except Exception:

            galpoes_ativos.discard("GALPÕES NORTE")


    # ================= GALPÕES SUL =================

    @discord.ui.button(
        label="🏭 Galpões Sul",
        style=discord.ButtonStyle.secondary,
        custom_id="fabricacao_sul"
    )
    async def sul(self, interaction: discord.Interaction, button: discord.ui.Button):

        async with db.acquire() as conn:

            ativo = await conn.fetchval(
                """
                SELECT 1
                FROM producoes
                WHERE galpao=$1
                AND CAST(fim AS timestamp) > NOW()
                """,
                "GALPÕES SUL"
            )

        if ativo:

            await interaction.response.send_message(
                "Este galpão já está em produção.",
                ephemeral=True
            )
            return

        galpoes_ativos.add("GALPÕES SUL")

        try:

            await interaction.response.send_modal(
                PolvoraProducaoModal("GALPÕES SUL", 130)
            )

        except Exception:

            galpoes_ativos.discard("GALPÕES SUL")


    # ================= BAHAMAS =================

    @discord.ui.button(
        label="🏭 Bahamas",
        style=discord.ButtonStyle.primary,
        custom_id="fabricacao_bahamas"
    )
    async def bahamas(self, interaction: discord.Interaction, button: discord.ui.Button):

        async with db.acquire() as conn:

            ativo = await conn.fetchval(
                """
                SELECT 1
                FROM producoes
                WHERE galpao=$1
                AND CAST(fim AS timestamp) > NOW()
                """,
                "BAHAMAS"
            )

        if ativo:

            await interaction.response.send_message(
                "Este galpão já está em produção.",
                ephemeral=True
            )
            return

        galpoes_ativos.add("BAHAMAS")

        try:

            await interaction.response.send_modal(
                PolvoraProducaoModal("BAHAMAS", 65)
            )

        except Exception:

            galpoes_ativos.discard("BAHAMAS")


    # ================= TESTE =================

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

        pid = f"TESTE_{msg.id}"

        if pid in producoes_tasks:
            return

        dados = {
            "galpao": "TESTE",
            "autor": interaction.user.id,
            "inicio": inicio.isoformat(),
            "fim": fim.isoformat(),
            "obs": "Teste do sistema",
            "polvora": 400,
            "msg_id": msg.id,
            "canal_id": interaction.channel.id
        }

        await salvar_producao(pid, dados)

        task = bot.loop.create_task(acompanhar_producao(pid))
        producoes_tasks[pid] = task


# =========================================================
# ================= LOOP DE ACOMPANHAMENTO =================
# =========================================================

async def acompanhar_producao(pid):

    if pid in producoes_ativas:
        return

    producoes_ativas.add(pid)

    print(f"▶ Produção retomada: {pid}")

    msg = None

    while not bot.is_closed():

        prod = await carregar_producao(pid)

        if not prod:
            producoes_ativas.discard(pid)
            return

        canal = pegar_canal(prod["canal_id"])

        if not canal:
            await asyncio.sleep(10)
            continue

        if msg is None:
            try:
                msg = await canal.fetch_message(int(prod["msg_id"]))

            except discord.NotFound:

                print(f"⚠️ Mensagem da produção não encontrada: {pid}")

                await deletar_producao(pid)

                galpoes_ativos.discard(prod["galpao"])

                if pid in producoes_tasks:
                    producoes_tasks.pop(pid)

                producoes_ativas.discard(pid)
                return

            except Exception as e:

                print("Erro buscar mensagem produção:", e)
                await asyncio.sleep(5)
                continue

        inicio = datetime.fromisoformat(prod["inicio"])
        fim = datetime.fromisoformat(prod["fim"])

        total = (fim - inicio).total_seconds()
        restante = max(0, (fim - agora()).total_seconds())

        if total <= 0:
            total = 1

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

        # ================= FINALIZAÇÃO =================

        if restante <= 0:

            polvora = prod.get("polvora", 400)
            segunda = prod.get("segunda_task_confirmada")

            base = 0

            if prod["galpao"] == "GALPÕES NORTE":
                base = 1777 if segunda else 1688

            if prod["galpao"] == "GALPÕES SUL":
                base = 1618 if segunda else 1608

            if prod["galpao"] == "BAHAMAS":
                base = 1777 if segunda else 1688

            capsulas = (base * polvora) // 400
            peso = capsulas * 0.05

            desc += (
                "\n\n🔵 **Produção Finalizada**"
                f"\n\n🧪 Produziu **{capsulas} cápsulas**"
                f"\n⚖️ Peso total: **{peso:.2f} kg**"
                f"\n💣 Pólvora utilizada: **{polvora}**"
            )

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

            await deletar_producao(pid)

            galpoes_ativos.discard(prod["galpao"])

            if pid in producoes_tasks:
                producoes_tasks.pop(pid)

            producoes_ativas.discard(pid)

            print(f"🗑️ Produção removida: {pid}")

            return

        # ================= ATUALIZA EMBED =================

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

        await asyncio.sleep(75)
# =========================================================
# ================= PAINEL FABRICAÇÃO =====================
# =========================================================

async def enviar_painel_fabricacao():

    canal = pegar_canal(CANAL_FABRICACAO_ID)

    if not canal:
        print("❌ Canal de fabricação não encontrado")
        return

    embed = discord.Embed(
        title="🏭 Fabricação",
        description="Selecione Norte ou Sul para iniciar a produção.",
        color=0x2c3e50
    )

    await enviar_ou_atualizar_painel(
        "painel_fabricacao",
        CANAL_FABRICACAO_ID,
        embed,
        FabricacaoView()
    )

    print("🏭 Painel de fabricação verificado/atualizado")
    
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
        await responder_interacao(interaction, defer=True)


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

    canal = pegar_canal(CANAL_REGISTRO_POLVORA_ID)

    for user_id, total in resumo.items():
        user = await pegar_usuario(int(user_id))

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
        await responder_interacao(interaction, defer=True)

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


# ================= BANCO =================

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
        return partes[1].replace("@", "")

    return None


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

                thumbnail = info["thumbnail_url"]\
                    .replace("{width}", "1280")\
                    .replace("{height}", "720")

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

    try:

        url = f"https://www.tiktok.com/@{username}/live"

        headers = {"User-Agent": "Mozilla/5.0"}

        async with http_session.get(url, headers=headers) as r:

            html = await r.text()

        if '"isLiveBroadcast":true' not in html:
            return False, None, None, None

        titulo = "Live no TikTok"
        thumb = None

        if '"title":"' in html:
            try:
                titulo = html.split('"title":"')[1].split('"')[0]
            except:
                pass

        if '"cover":"' in html:
            try:
                thumb = html.split('"cover":"')[1].split('"')[0]
                thumb = thumb.replace("\\u002F", "/")
            except:
                pass

        return True, titulo, None, thumb

    except:

        return False, None, None, None


# =========================================================
# ================= DIVULGAÇÃO ============================
# =========================================================

async def divulgar_live(user_id, link, titulo, jogo, thumbnail):

    try:

        canal = bot.get_channel(CANAL_DIVULGACAO_LIVE_ID)

        user = await pegar_usuario(int(user_id))

        plataforma = detectar_plataforma(link)

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

        embed = discord.Embed(
            title="🔴 LIVE AO VIVO!",
            color=cores.get(plataforma, 0xffffff)
        )

        embed.description = (
            f"👤 **Streamer:** {user.mention}\n"
            f"📺 **Plataforma:** {nomes.get(plataforma)}\n"
            f"🎮 **Jogo:** {jogo or 'Não informado'}\n"
            f"📝 **Título:** {titulo or 'Sem título'}\n\n"
            f"🔗 {link}"
        )

        if thumbnail:
            embed.set_image(url=thumbnail)

        await canal.send(
            content="@everyone 🔴 Live iniciada!",
            embed=embed,
            allowed_mentions=discord.AllowedMentions(everyone=True)
        )

    except Exception as e:

        print("Erro divulgar live:", e)


# =========================================================
# ================= LOOP =========================
# =========================================================

@tasks.loop(minutes=2)
async def verificar_lives():

    print("🔄 Verificando lives...")

    try:

        lives = await carregar_lives()

        for user_id, lista_lives in lives.items():

            for data in lista_lives:

                link = data.get("link", "")
                divulgado = data.get("divulgado", False)

                plataforma = detectar_plataforma(link)
                canal = extrair_canal(link)

                if not plataforma or not canal:
                    continue

                if plataforma == "twitch":

                    ao_vivo, titulo, jogo, thumbnail = await checar_twitch(canal)

                elif plataforma == "kick":

                    ao_vivo, titulo, jogo, thumbnail = await checar_kick(canal)

                elif plataforma == "tiktok":

                    ao_vivo, titulo, jogo, thumbnail = await checar_tiktok(canal)

                else:
                    continue

                if not ao_vivo and divulgado:

                    await atualizar_divulgado(link, False)

                if ao_vivo and not divulgado:

                    await divulgar_live(user_id, link, titulo, jogo, thumbnail)

                    await atualizar_divulgado(link, True)

    except Exception as e:

        print("Erro no loop de lives:", e)


# ================= CADASTRO =================

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
            await interaction.response.send_message("❌ Link inválido.", ephemeral=True)
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

    async def setup(self):

        select = SelectRemoverLive()
        await select.refresh_options()

        self.clear_items()
        self.add_item(select)

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

        for uid, lista_lives in lives.items():

            for data in lista_lives:

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
# =============== SISTEMA GLOBAL DE PAINÉIS ===============
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

        # =====================================================
        # TENTA EDITAR PAINEL EXISTENTE
        # =====================================================

        if row:

            try:

                canal_salvo = bot.get_channel(int(row["canal_id"])) or canal

                msg = await canal_salvo.fetch_message(int(row["mensagem_id"]))

                await edit_queue.put(
                    msg.edit(embed=embed, view=view)
                )

                return

            except discord.NotFound:
                print(f"⚠️ Painel perdido: {nome}, recriando...")

            except Exception as e:
                print(f"Erro atualizar painel {nome}:", e)

        # =====================================================
        # CRIA NOVO PAINEL
        # =====================================================

        msg = await canal.send(embed=embed, view=view)

        await conn.execute(
            """
            INSERT INTO paineis (nome, canal_id, mensagem_id)
            VALUES ($1,$2,$3)
            ON CONFLICT (nome)
            DO UPDATE SET
            canal_id=$2,
            mensagem_id=$3
            """,
            nome,
            str(canal_id),
            str(msg.id)
        )
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

    @discord.ui.button(
        label="📊 Gerar Relatório",
        style=discord.ButtonStyle.secondary,
        custom_id="acoes_relatorio_btn"
    )
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):

        await interaction.response.send_modal(
            RelatorioAcoesModal()
        )


# =========================================================
# ================= ATUALIZAR PAINEL AÇÕES =================
# =========================================================

async def atualizar_painel_acoes(guild):

    embed = await gerar_embed_acoes()

    await enviar_ou_atualizar_painel(
        "painel_acoes",
        CANAL_ESCALACOES_ID,
        embed,
        PainelAcoesView()
    )
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

    def __init__(self, acao_id, venceu):
        super().__init__(title="Resultado da ação")

        self.acao_id = acao_id
        self.venceu = venceu

    async def on_submit(self, interaction: discord.Interaction):

        if getattr(self, "ja_processado", False):
            return

        self.ja_processado = True

        try:

            resultado = "GANHOU" if self.venceu else "PERDEU"

            dinheiro = 0

            try:

                valor_input = (self.dinheiro.value or "").strip()

                if valor_input:
                    valor_input = valor_input.replace(".", "").replace(",", "")
                    dinheiro = int(valor_input)

            except Exception:

                await interaction.response.send_message(
                    "Valor inválido.",
                    ephemeral=True
                )
                return

            async with db.acquire() as conn:

                ja_finalizada = await conn.fetchval(
                    "SELECT resultado FROM acoes_semana WHERE id=$1",
                    self.acao_id
                )

                if ja_finalizada:

                    await interaction.response.send_message(
                        "Esta ação já teve resultado registrado.",
                        ephemeral=True
                    )
                    return

                participantes = await conn.fetch(
                    """
                    SELECT user_id, nome_externo
                    FROM participantes_acoes
                    WHERE acao_id=$1
                    """,
                    self.acao_id
                )

                acao = await conn.fetchrow(
                    "SELECT tipo, autor FROM acoes_semana WHERE id=$1",
                    self.acao_id
                )

                await conn.execute(
                    "UPDATE acoes_semana SET resultado=$1 WHERE id=$2",
                    resultado,
                    self.acao_id
                )

            # =================================================
            # CALCULAR VALOR POR PESSOA
            # =================================================

            qtd = len(participantes)

            if qtd == 0:
                qtd = 1

            valor_por_pessoa = dinheiro // qtd if dinheiro > 0 else 0

            # =================================================
            # ENVIAR RESULTADO NA SALA DE CADA PARTICIPANTE
            # =================================================

            for p in participantes:

                uid = p["user_id"]

                if not uid:
                    continue

                uid = str(uid)

                if uid not in metas_cache:
                    continue

                canal_id = metas_cache[uid]["canal_id"]

                canal = interaction.guild.get_channel(canal_id)

                if not canal:
                    continue

                try:

                    embed_user = discord.Embed(
                        title="🚨 Resultado da Ação",
                        color=0x2ecc71 if self.venceu else 0xe74c3c
                    )

                    embed_user.add_field(
                        name="🏦 Ação",
                        value=acao["tipo"],
                        inline=False
                    )

                    embed_user.add_field(
                        name="🎯 Resultado",
                        value="🟢 GANHOU" if self.venceu else "🔴 PERDEU",
                        inline=False
                    )

                    if valor_por_pessoa > 0:
                        valor_fmt = f"R$ {valor_por_pessoa:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
                        embed_user.add_field(
                            name="💰 Sua parte",
                            value=valor_fmt,
                            inline=False
                        )

                    embed_user.set_footer(text="Sistema de Ações • VDR 442")

                    await canal.send(embed=embed_user)

                except Exception as e:
                    print("Erro enviar resultado na sala:", e)

            # =================================================
            # EMBED NO RELATÓRIO DA AÇÃO
            # =================================================

            participantes_marcados = []

            for p in participantes:

                uid = p["user_id"]
                nome = p["nome_externo"]

                if uid:
                    participantes_marcados.append(f"<@{uid}>")

                elif nome and nome.strip() != "0":
                    participantes_marcados.append(nome)

            cor = 0x2ecc71 if self.venceu else 0xe74c3c
            status = "🟢 **AÇÃO GANHA**" if self.venceu else "🔴 **AÇÃO PERDIDA**"

            embed = discord.Embed(
                title="🚨 RELATÓRIO DE AÇÃO",
                color=cor
            )

            embed.description = "━━━━━━━━━━━━━━━━━━━━━━"

            embed.add_field(
                name="🏦 Ação",
                value=f"```{acao['tipo']}```",
                inline=False
            )

            embed.add_field(
                name="📋 Escalação feita por",
                value=f"<@{acao['autor']}>",
                inline=False
            )

            embed.add_field(
                name="🎯 Resultado",
                value=status,
                inline=False
            )

            if self.venceu and dinheiro:

                embed.add_field(
                    name="💰 Dinheiro Recuperado",
                    value=f"R$ {dinheiro:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
                    inline=False
                )

            participantes_texto = "\n".join(participantes_marcados) if participantes_marcados else "Nenhum registrado"

            embed.add_field(
                name=f"👥 Participantes ({len(participantes_marcados)})",
                value=participantes_texto,
                inline=False
            )

            embed.set_footer(text="Sistema de Ações • VDR 442")

            embed.description += "\n━━━━━━━━━━━━━━━━━━━━━━"

            await interaction.message.edit(embed=embed, view=None)

            await responder_interacao(interaction, defer=True)

        except Exception as e:
            import traceback
            print("ERRO NO RESULTADO DA AÇÃO:")
            traceback.print_exc()
            await responder_interacao(interaction, defer=True)
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


class RelatorioAcoesModal(discord.ui.Modal, title="📊 Relatório de Ações"):

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

            inicio = datetime.strptime(self.data_inicio.value, "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value, "%d/%m/%Y")

        except Exception:

            await interaction.followup.send(
                "Formato inválido. Use **DD/MM/AAAA**",
                ephemeral=True
            )
            return

        async with db.acquire() as conn:

            rows = await conn.fetch(
                """
                SELECT
                    p.user_id,
                    COUNT(a.id) as total_acoes
                FROM participantes_acoes p
                JOIN acoes_semana a ON a.id = p.acao_id
                WHERE a.resultado = 'GANHOU'
                AND a.data BETWEEN $1 AND $2
                AND p.user_id IS NOT NULL
                GROUP BY p.user_id
                """,
                inicio,
                fim
            )

        if not rows:

            await interaction.followup.send(
                "Nenhuma ação encontrada no período.",
                ephemeral=True
            )
            return

        total_geral = 0
        linhas = []

        for r in rows:

            uid = r["user_id"]
            qtd = r["total_acoes"]

            valor = qtd * 0  # placeholder caso queira calcular valores futuramente

            total_geral += valor

            linhas.append(
                f"👤 <@{uid}> — {qtd} ações"
            )

        embed = discord.Embed(
            title="📊 Relatório de Ações",
            color=0x3498db
        )

        embed.add_field(
            name="Participações",
            value="\n".join(linhas),
            inline=False
        )

        embed.set_footer(
            text=f"Período: {self.data_inicio.value} até {self.data_fim.value}"
        )

        canal = interaction.guild.get_channel(CANAL_RELATORIO_ACOES_ID)

        if canal:
            await canal.send(embed=embed)

        await interaction.followup.send(
            "Relatório enviado no canal de ações.",
            ephemeral=True
        )

# =========================================================
# ====================== HELICRASH ========================
# =========================================================

CANAL_HELICRASH_ID = 1478919637260435498

# =========================================================
# VIEW DO HELICRASH
# =========================================================

class HelicrashView(discord.ui.View):

    def __init__(self, hid):
        super().__init__(timeout=None)
        self.hid = hid

    @discord.ui.button(
        label="Entrar no Helicrash",
        style=discord.ButtonStyle.success,
        custom_id="helicrash_entrar"
    )
    async def entrar(self, interaction: discord.Interaction, button: discord.ui.Button):

        async with db.acquire() as conn:

            row = await conn.fetchrow(
                "SELECT participantes FROM helicrash WHERE id=$1",
                self.hid
            )

            if not row:
                await interaction.response.send_message(
                    "Evento não encontrado.",
                    ephemeral=True
                )
                return

            # cria lista segura
            lista = [x for x in (row["participantes"] or "").split(",") if x]

            if str(interaction.user.id) in lista:
                await interaction.response.send_message(
                    "Você já entrou.",
                    ephemeral=True
                )
                return

            if len(lista) >= 10:
                await interaction.response.send_message(
                    "Escalação cheia.",
                    ephemeral=True
                )
                return

            lista.append(str(interaction.user.id))

            await conn.execute(
                "UPDATE helicrash SET participantes=$1 WHERE id=$2",
                ",".join(lista),
                self.hid
            )

        await atualizar_embed_helicrash(self.hid)

        await responder_interacao(interaction, defer=True)

# =========================================================
# ATUALIZA EMBED DO HELICRASH
# =========================================================

async def atualizar_embed_helicrash(hid):

    async with db.acquire() as conn:

        row = await conn.fetchrow(
            "SELECT * FROM helicrash WHERE id=$1",
            hid
        )

    if not row:
        return

    canal = bot.get_channel(int(row["canal_id"]))

    if not canal:
        return

    try:
        msg = await canal.fetch_message(int(row["msg_id"]))
    except:
        return

    participantes = [x for x in (row["participantes"] or "").split(",") if x]

    vagas_restantes = 10 - len(participantes)

    horario = row["horario"].replace(tzinfo=BRASIL)

    agora_br = agora()

    restante = horario - agora_br

    segundos = int(restante.total_seconds())

    if segundos < 0:
        segundos = 0

    minutos = segundos // 60
    segundos = segundos % 60

    lista = "\n".join(f"<@{uid}>" for uid in participantes) if participantes else "Ninguém ainda"

    embed = discord.Embed(
        title=f"🚁 HELICRASH {horario.strftime('%H:%M')}",
        color=0xe74c3c
    )

    embed.add_field(
        name="⏳ Começa em",
        value=f"{minutos}m {segundos}s",
        inline=True
    )

    embed.add_field(
        name="👥 Participantes",
        value=f"{len(participantes)}/10",
        inline=True
    )
    
    embed.add_field(
        name="Participantes",
        value=lista,
        inline=False
    )

    await msg.edit(embed=embed, view=HelicrashView(hid))


# =========================================================
# LOOP DO HELICRASH
# =========================================================

async def acompanhar_helicrash(hid):

    while True:

        await asyncio.sleep(10)

        async with db.acquire() as conn:

            row = await conn.fetchrow(
                "SELECT * FROM helicrash WHERE id=$1",
                hid
            )

        if not row:
            return

        horario = row["horario"].replace(tzinfo=BRASIL)

        if agora() >= horario:

            await finalizar_helicrash(hid)

            return

        await atualizar_embed_helicrash(hid)


# =========================================================
# FINALIZA HELICRASH
# =========================================================

async def finalizar_helicrash(hid):

    async with db.acquire() as conn:

        row = await conn.fetchrow(
            "SELECT * FROM helicrash WHERE id=$1",
            hid
        )

    if not row:
        return

    participantes = [x for x in (row["participantes"] or "").split(",") if x]

    canal = bot.get_channel(CANAL_RELATORIO_ACOES_ID)

    lista = "\n".join(f"<@{uid}>" for uid in participantes) if participantes else "Sem participantes"

    embed = discord.Embed(
        title=f"🚁 HELICRASH {row['horario'].strftime('%H:%M')}",
        description=lista,
        color=0xe74c3c
    )

    if canal:
        await canal.send(embed=embed)

    canal_evento = bot.get_channel(int(row["canal_id"]))

    if canal_evento:

        try:
            msg = await canal_evento.fetch_message(int(row["msg_id"]))
            await msg.delete()
        except:
            pass

    async with db.acquire() as conn:
        await conn.execute("DELETE FROM helicrash WHERE id=$1", hid)


# =========================================================
# MODAL PARA CRIAR HELICRASH
# =========================================================

class HelicrashModal(discord.ui.Modal, title="Criar Helicrash"):

    hora = discord.ui.TextInput(
        label="Hora do helicrash (HH:MM)",
        placeholder="Ex: 21:30"
    )

    async def on_submit(self, interaction: discord.Interaction):

        try:

            h, m = self.hora.value.split(":")
            h = int(h)
            m = int(m)

            agora_br = agora()

            horario = agora_br.replace(
                hour=h,
                minute=m,
                second=0,
                microsecond=0
            )

            if horario < agora_br:
                horario += timedelta(days=1)

            canal = interaction.channel

            embed = discord.Embed(
                title=f"🚁 HELICRASH {horario.strftime('%H:%M')}",
                description="Aguardando participantes...",
                color=0xe74c3c
            )

            embed.add_field(name="⏳ Começa em", value="Calculando...", inline=True)
            embed.add_field(name="👥 Participantes", value="0/10", inline=True)
            embed.add_field(name="Participantes", value="Ninguém ainda", inline=False)

            msg = await canal.send(
                content="@everyone 🚁 Helicrash disponível! Clique para participar.",
                embed=embed,
                allowed_mentions=discord.AllowedMentions(everyone=True)
            )

            hid = None

            try:

                async with db.acquire() as conn:

                    hid = await conn.fetchval(
                        """
                        INSERT INTO helicrash (horario, canal_id, msg_id, participantes)
                        VALUES ($1,$2,$3,$4)
                        RETURNING id
                        """,
                        horario.replace(tzinfo=None),
                        str(canal.id),
                        str(msg.id),
                        ""
                    )

            except Exception as e:
                print("Erro banco helicrash:", e)

            # garante botão mesmo se banco falhar
            await msg.edit(view=HelicrashView(hid or 0))

            bot.loop.create_task(acompanhar_helicrash(hid))

            await interaction.response.send_message(
                f"Helicrash criado para **{horario.strftime('%H:%M')}**",
                ephemeral=True
            )

        except Exception as e:

            print("Erro criar helicrash:", e)

            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "Erro ao criar helicrash.",
                    ephemeral=True
                )
# =========================================================
# BOTÃO PARA CRIAR HELICRASH
# =========================================================

class HelicrashPainel(discord.ui.View):

    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🚁 Escalar Helicrash",
        style=discord.ButtonStyle.primary,
        custom_id="helicrash_criar"
    )
    async def criar(self, interaction: discord.Interaction, button: discord.ui.Button):

        try:
            await interaction.response.send_modal(HelicrashModal())

        except Exception as e:

            print("Erro abrir modal helicrash:", e)

            if not interaction.response.is_done():
                await interaction.response.send_message(
                    "Erro ao abrir o modal.",
                    ephemeral=True
                )
# =========================================================
# ENVIAR PAINEL HELICRASH
# =========================================================

async def enviar_painel_helicrash():

    canal = bot.get_channel(CANAL_HELICRASH_ID)

    if not canal:
        print("❌ Canal helicrash não encontrado")
        return

    embed = discord.Embed(
        title="🚁 Sistema de Helicrash",
        description="Clique no botão abaixo para escalar um helicrash.",
        color=0xe74c3c
    )

    await enviar_ou_atualizar_painel(
        "painel_helicrash",
        CANAL_HELICRASH_ID,
        embed,
        HelicrashPainel()
    )
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

    canal = member.guild.get_channel(metas_cache[str(member.id)]["canal_id"])

    if not canal:
        return

    nova = obter_categoria_meta(member)

    if nova and canal.category_id != nova:
        await canal.edit(category=member.guild.get_channel(nova))


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

#=================== SEGUNDA TASK =========================

@bot.event
async def on_interaction(interaction: discord.Interaction):

    try:

        if interaction.type != discord.InteractionType.component:
            return

        cid = interaction.data.get("custom_id")

        if not cid:
            return

        if cid.startswith("segunda_task_"):

            pid = cid.replace("segunda_task_", "")

            view = SegundaTaskView(pid)

            await view.confirmar(interaction)

    except discord.errors.HTTPException as e:

        if e.code in (40060, 10062):
            return

        print("Erro HTTP interaction:", e)

    except Exception as e:

        print("Erro geral interaction:", e)
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
        CalculadoraView,
        StatusView,
        HelicrashPainel
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
                """
                SELECT pid FROM producoes
                WHERE pid NOT LIKE 'TESTE_%'
                AND CAST(fim AS timestamp) > NOW()
                """
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

        painel_tasks = [
            enviar_painel_fabricacao(),
            enviar_painel_lives(),
            enviar_painel_admin_lives(),
            enviar_painel_polvoras(),
            enviar_painel_lavagem(),
            enviar_painel_vendas(),
            atualizar_painel_acoes(bot.get_guild(GUILD_ID)),
            enviar_painel_solicitar_sala(),
            enviar_painel_helicrash()
        ]

        results = await asyncio.gather(*painel_tasks, return_exceptions=True)

        for r in results:
            if isinstance(r, Exception):
                print("Erro em painel específico:", r)

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


