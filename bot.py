import os
import json
import asyncio
import aiohttp
import discord
from discord.ext import commands
from datetime import datetime, timedelta

# ================= CONFIG =================

TOKEN = os.environ.get("TOKEN")

intents = discord.Intents.default()
intents.members = True
intents.message_content = True

AGREGADO_ROLE_ID = 1422847202937536532
CONVIDADO_ROLE_ID = 1337382961456353342

CANAL_REGISTRO_ID = 1229556030397218878
CANAL_LOG_REGISTRO_ID = 1462457604939841851
CANAL_CALCULADORA_ID = 1460984821458272347
CANAL_ENCOMENDAS_ID = 1460980984811098294

# PRODUÇÃO
CANAL_FABRICACAO_ID = 1466421612566810634
CANAL_REGISTRO_GALPAO_ID = 1356174712337862819
ARQUIVO_PRODUCOES = "producoes.json"

# LIVES
CANAL_CADASTRO_LIVE_ID = 1466464557215256790  # TROQUE
CANAL_DIVULGACAO_LIVE_ID = 1243325102917943335  # TROQUE
ARQUIVO_LIVES = "lives.json"

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")

GUILD_ID = 1229526644193099880
GUILD = discord.Object(id=GUILD_ID)

bot = commands.Bot(command_prefix="!", intents=intents)

# =========================================================
# ================= REGISTRO (ORIGINAL) ===================
# =========================================================

class RegistroModal(discord.ui.Modal, title="Registro de Entrada"):
    nome = discord.ui.TextInput(label="Nome Completo")
    passaporte = discord.ui.TextInput(label="Passaporte")
    indicado = discord.ui.TextInput(label="Indicado por")
    telefone = discord.ui.TextInput(label="Telefone In Game")

    async def on_submit(self, interaction: discord.Interaction):
        membro = interaction.user
        guild = interaction.guild

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

    @discord.ui.button(label="📋 Fazer Registro", style=discord.ButtonStyle.success, custom_id="registro_fazer")
    async def registro(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistroModal())

# =========================================================
# ================= VENDAS (ORIGINAL) =====================
# =========================================================

class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def toggle_status(self, interaction, label):
        embed = interaction.message.embeds[0]
        campo = embed.fields[-1]
        status_atual = campo.value.split("\n")

        if label.startswith("✅ Entregue"):
            agora = datetime.now().strftime("%d/%m/%Y %H:%M")
            label = f"✅ Entregue • {agora}"

        if label in status_atual:
            status_atual.remove(label)
        else:
            status_atual.append(label)

        if not status_atual:
            status_atual = ["⏳ Pagamento pendente"]

        embed.set_field_at(-1, name="📌 Status", value="\n".join(status_atual), inline=False)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="status_entregue")
    async def entregue(self, interaction, button):
        await self.toggle_status(interaction, "✅ Entregue")

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="status_pago")
    async def pago(self, interaction, button):
        await self.toggle_status(interaction, "💰 Pago")

    @discord.ui.button(label="📦 A entregar", style=discord.ButtonStyle.secondary, custom_id="status_a_entregar")
    async def a_entregar(self, interaction, button):
        await self.toggle_status(interaction, "📦 A entregar")

    @discord.ui.button(label="⏳ Pagamento pendente", style=discord.ButtonStyle.danger, custom_id="status_pendente")
    async def pendente(self, interaction, button):
        await self.toggle_status(interaction, "⏳ Pagamento pendente")

class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(label="Organização")
    qtd_pt = discord.ui.TextInput(label="Quantidade PT (R$50)")
    qtd_sub = discord.ui.TextInput(label="Quantidade SUB (R$90)")
    observacoes = discord.ui.TextInput(label="Observações", style=discord.TextStyle.paragraph, required=False)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            pt = int(self.qtd_pt.value.strip())
            sub = int(self.qtd_sub.value.strip())
        except ValueError:
            await interaction.response.defer()
            return

        pacotes_pt = pt // 50
        pacotes_sub = sub // 50
        total = (pt * 50) + (sub * 90)

        embed = discord.Embed(title="📦 NOVA ENCOMENDA", color=0x1e3a8a)
        embed.add_field(name="👤 Vendedor", value=interaction.user.mention, inline=False)
        embed.add_field(name="🏷 Organização", value=self.organizacao.value, inline=False)
        embed.add_field(name="🔫 PT", value=f"{pt} munições\n📦 {pacotes_pt} pacotes", inline=True)
        embed.add_field(name="🔫 SUB", value=f"{sub} munições\n📦 {pacotes_sub} pacotes", inline=True)
        embed.add_field(name="💰 Total", value=f"R$ {total}", inline=False)
        embed.add_field(name="📌 Status", value="📦 A entregar", inline=False)
        embed.set_footer(text="🛡 Sistema de Encomendas • VDR 442")

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())
        await interaction.response.defer()

class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🧮 Registrar Venda", style=discord.ButtonStyle.primary, custom_id="calculadora_registrar")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(VendaModal())

# =========================================================
# ================= PRODUÇÃO ===============================
# =========================================================

def carregar_producoes():
    if not os.path.exists(ARQUIVO_PRODUCOES):
        return {}
    try:
        with open(ARQUIVO_PRODUCOES, "r") as f:
            return json.load(f)
    except:
        return {}

def salvar_producoes(dados):
    with open(ARQUIVO_PRODUCOES, "w") as f:
        json.dump(dados, f, indent=4)

def barra(pct, size=20):
    cheio = int(pct * size)
    return "▓" * cheio + "░" * (size - cheio)

class SegundaTaskView(discord.ui.View):
    def __init__(self, pid):
        super().__init__(timeout=None)
        self.pid = pid

    @discord.ui.button(label="✅ 2º Task Feita", style=discord.ButtonStyle.success, custom_id="segunda_task_feita")
    async def ok(self, interaction: discord.Interaction, button: discord.ui.Button):
        producoes = carregar_producoes()
        if self.pid in producoes:
            producoes[self.pid]["segunda_task_confirmada"] = {
                "user": interaction.user.id,
                "time": datetime.utcnow().isoformat()
            }
            salvar_producoes(producoes)
        await interaction.response.defer()

class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def iniciar(self, interaction, galpao, total_min, segunda_task_faltando_min):
        producoes = carregar_producoes()
        pid = f"{galpao}_{interaction.id}"
        inicio = datetime.utcnow()
        fim = inicio + timedelta(minutes=total_min)

        canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)
        msg = await canal.send(embed=discord.Embed(title="🏭 Produção", description="Iniciando...", color=0x3498db))

        producoes[pid] = {
            "galpao": galpao,
            "autor": interaction.user.id,
            "inicio": inicio.isoformat(),
            "fim": fim.isoformat(),
            "segunda_task_em": (total_min - segunda_task_faltando_min) * 60,
            "segunda_task": False,
            "msg_id": msg.id,
            "canal_id": CANAL_REGISTRO_GALPAO_ID
        }

        salvar_producoes(producoes)
        bot.loop.create_task(acompanhar_producao(pid))
        await interaction.response.defer()

    @discord.ui.button(label="🏭 Galpões Sul", style=discord.ButtonStyle.primary, custom_id="fabricacao_sul")
    async def sul(self, interaction, button):
        await self.iniciar(interaction, "Sul", 130, 80)

    @discord.ui.button(label="🏭 Galpões Norte", style=discord.ButtonStyle.secondary, custom_id="fabricacao_norte")
    async def norte(self, interaction, button):
        await self.iniciar(interaction, "Norte", 65, 40)

async def acompanhar_producao(pid):
    while True:
        producoes = carregar_producoes()
        if pid not in producoes:
            return

        prod = producoes[pid]
        canal = bot.get_channel(prod["canal_id"])

        try:
            msg = await canal.fetch_message(prod["msg_id"])
        except:
            return

        inicio = datetime.fromisoformat(prod["inicio"])
        fim = datetime.fromisoformat(prod["fim"])

        total = (fim - inicio).total_seconds()
        restante = max(0, (fim - datetime.utcnow()).total_seconds())
        pct = max(0, min(1, 1 - (restante / total)))
        mins = int(restante // 60)

        desc = (
            f"**Galpão:** {prod['galpao']}\n"
            f"**Iniciado por:** <@{prod['autor']}>\n"
            f"Início: <t:{int(inicio.timestamp())}:t>\n"
            f"Término: <t:{int(fim.timestamp())}:t>\n\n"
            f"**Restante:** {mins} min\n"
            f"{barra(pct)}"
        )

        view = None

        if not prod["segunda_task"] and (total - restante) >= prod["segunda_task_em"]:
            prod["segunda_task"] = True
            salvar_producoes(producoes)
            desc += "\n\n🟡 **2ª Task Liberada**"
            view = SegundaTaskView(pid)

        if restante <= 0:
            desc += "\n\n🟢 **Produção Finalizada**"
            del producoes[pid]
            salvar_producoes(producoes)

        await msg.edit(embed=discord.Embed(title="🏭 Produção", description=desc, color=0x34495e), view=view)

        if restante <= 0:
            return

        await asyncio.sleep(180)

async def enviar_painel_fabricacao():
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    async for m in canal.history(limit=10):
        if m.author == bot.user and m.embeds and m.embeds[0].title == "🏭 Fabricação":
            return

    await canal.send(
        embed=discord.Embed(title="🏭 Fabricação", description="Selecione o galpão.", color=0x2c3e50),
        view=FabricacaoView()
    )

# =========================================================
# ================= LIVES (BOTÃO) =========================
# =========================================================

def carregar_lives():
    if not os.path.exists(ARQUIVO_LIVES):
        return {}
    try:
        with open(ARQUIVO_LIVES, "r") as f:
            return json.load(f)
    except:
        return {}

def salvar_lives(dados):
    with open(ARQUIVO_LIVES, "w") as f:
        json.dump(dados, f, indent=4)

class CadastrarLiveModal(discord.ui.Modal, title="🎥 Cadastrar Live"):
    link = discord.ui.TextInput(label="Cole o link da sua live (Twitch)", placeholder="https://twitch.tv/seucanal")

    async def on_submit(self, interaction: discord.Interaction):
        lives = carregar_lives()
        lives[str(interaction.user.id)] = {"link": self.link.value.strip(), "divulgado": False}
        salvar_lives(lives)

        embed = discord.Embed(
            title="✅ Live cadastrada!",
            description=f"{interaction.user.mention}\n{self.link.value}",
            color=0x2ecc71
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

class CadastrarLiveView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎥 Cadastrar minha Live", style=discord.ButtonStyle.primary, custom_id="cadastrar_live_btn")
    async def cadastrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(CadastrarLiveModal())

async def enviar_painel_lives():
    canal = bot.get_channel(CANAL_CADASTRO_LIVE_ID)
    if not canal:
        return

    async for m in canal.history(limit=10):
        if m.author == bot.user and m.embeds and m.embeds[0].title == "🎥 Cadastro de Live":
            return

    embed = discord.Embed(
        title="🎥 Cadastro de Live",
        description="Clique no botão para cadastrar sua live.",
        color=0x9146FF
    )

    await canal.send(embed=embed, view=CadastrarLiveView())

# =========================================================
# ================= EVENTS ================================
# =========================================================

@bot.event
async def on_ready():
    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    bot.add_view(StatusView())
    bot.add_view(FabricacaoView())
    bot.add_view(CadastrarLiveView())

    for pid in carregar_producoes():
        bot.loop.create_task(acompanhar_producao(pid))

    await enviar_painel_fabricacao()
    await enviar_painel_lives()

    print("✅ Bot online com Registro + Vendas + Produção + Lives")

bot.run(TOKEN)
