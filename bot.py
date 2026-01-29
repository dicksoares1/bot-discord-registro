import os
import json
import asyncio
import discord
from discord.ext import commands
from datetime import datetime, timedelta

# ================= CONFIG =================

TOKEN = os.environ.get("TOKEN")

intents = discord.Intents.default()
intents.members = True

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

GUILD_ID = 1229526644193099880
GUILD = discord.Object(id=GUILD_ID)

bot = commands.Bot(command_prefix="!", intents=intents)

# ================= UTIL =================

def carregar_producoes():
    if not os.path.exists(ARQUIVO_PRODUCOES):
        return {}
    try:
        with open(ARQUIVO_PRODUCOES, "r") as f:
            return json.load(f)
    except json.JSONDecodeError:
        return {}

def salvar_producoes(dados):
    with open(ARQUIVO_PRODUCOES, "w") as f:
        json.dump(dados, f, indent=4)

def formatar_tempo(segundos):
    m, s = divmod(segundos, 60)
    return f"{m:02d}:{s:02d}"

def barra_progresso(restante, total, tamanho=14):
    progresso = 1 - (restante / total)
    preenchido = int(tamanho * progresso)
    return "█" * preenchido + "░" * (tamanho - preenchido)

# ================= REGISTRO =================

class RegistroModal(discord.ui.Modal, title="Registro de Entrada"):
    nome = discord.ui.TextInput(label="Nome Completo")
    passaporte = discord.ui.TextInput(label="Passaporte")
    indicado = discord.ui.TextInput(label="Indicado por")
    telefone = discord.ui.TextInput(label="Telefone In Game")

    async def on_submit(self, interaction):
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

        await interaction.response.send_message("✅ Registro concluído!", ephemeral=True)

class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📋 Fazer Registro", style=discord.ButtonStyle.success, custom_id="registro_fazer")
    async def registrar(self, interaction, button):
        await interaction.response.send_modal(RegistroModal())

# ================= VENDAS =================

class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="status_entregue")
    async def entregue(self, interaction, button):
        await self.set_status(interaction, "✅ Entregue")

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="status_pago")
    async def pago(self, interaction, button):
        await self.set_status(interaction, "💰 Pago")

    async def set_status(self, interaction, texto):
        embed = interaction.message.embeds[0]
        embed.set_field_at(-1, name="📌 Status", value=texto, inline=False)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(label="Organização")
    qtd_pt = discord.ui.TextInput(label="Quantidade PT")
    qtd_sub = discord.ui.TextInput(label="Quantidade SUB")

    async def on_submit(self, interaction):
        pt = int(self.qtd_pt.value)
        sub = int(self.qtd_sub.value)
        total = pt * 50 + sub * 90

        embed = discord.Embed(title="📦 NOVA ENCOMENDA", color=0x1e3a8a)
        embed.add_field(name="👤 Vendedor", value=interaction.user.mention)
        embed.add_field(name="🏷 Organização", value=self.organizacao.value)
        embed.add_field(name="💰 Total", value=f"R$ {total}")
        embed.add_field(name="📌 Status", value="⏳ Pagamento pendente")

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())

        await interaction.response.send_message("✅ Venda registrada!", ephemeral=True)

class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🧮 Registrar Venda", style=discord.ButtonStyle.primary, custom_id="registrar_venda")
    async def registrar(self, interaction, button):
        await interaction.response.send_modal(VendaModal())

# ================= PRODUÇÃO =================

class SegundaTaskView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="✅ 2º Task Feita", style=discord.ButtonStyle.success, custom_id="segunda_task")
    async def confirmar(self, interaction, button):
        await interaction.response.send_message("🟢 Segunda task confirmada!", ephemeral=True)

async def acompanhar_producao(pid):
    while True:
        producoes = carregar_producoes()
        if pid not in producoes:
            return

        prod = producoes[pid]
        canal = bot.get_channel(CANAL_REGISTRO_GALPAO_ID)

        fim = datetime.fromisoformat(prod["fim"])
        restante = int((fim - datetime.utcnow()).total_seconds())

        if restante <= 0:
            del producoes[pid]
            salvar_producoes(producoes)
            return

        progresso = barra_progresso(restante, prod["duracao"])

        try:
            msg = await canal.fetch_message(prod["mensagem_id"])
            embed = msg.embeds[0]
            embed.set_field_at(2, name="⏳ Tempo restante", value=formatar_tempo(restante))
            embed.set_field_at(3, name="📊 Progresso", value=progresso)
            await msg.edit(embed=embed)
        except:
            pass

        if not prod["segunda_task"] and restante <= prod["segunda_task_em"]:
            await canal.send(
                f"🟡 **2º Task disponível** — {formatar_tempo(restante)}",
                view=SegundaTaskView()
            )
            prod["segunda_task"] = True
            salvar_producoes(producoes)

        await asyncio.sleep(30)

class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def iniciar(self, interaction, galpao, duracao, segunda_task_em):
        producoes = carregar_producoes()
        pid = f"{interaction.id}_{galpao}"

        inicio = datetime.utcnow()
        fim = inicio + timedelta(seconds=duracao)

        embed = discord.Embed(title="🏭 Produção Iniciada", color=0x3498db)
        embed.add_field(name="🏗 Galpão", value=galpao)
        embed.add_field(name="👤 Iniciado por", value=interaction.user.mention)
        embed.add_field(name="⏳ Tempo restante", value=formatar_tempo(duracao))
        embed.add_field(name="📊 Progresso", value=barra_progresso(duracao, duracao))

        canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)
        msg = await canal.send(embed=embed)

        producoes[pid] = {
            "galpao": galpao,
            "inicio": inicio.isoformat(),
            "fim": fim.isoformat(),
            "duracao": duracao,
            "segunda_task_em": segunda_task_em,
            "segunda_task": False,
            "mensagem_id": msg.id
        }

        salvar_producoes(producoes)
        bot.loop.create_task(acompanhar_producao(pid))

        await interaction.response.send_message("✅ Produção iniciada!", ephemeral=True)

    @discord.ui.button(label="🏭 Galpões Sul", style=discord.ButtonStyle.primary, custom_id="fab_sul")
    async def sul(self, i, b):
        await self.iniciar(i, "Sul", 3900, 1500)

    @discord.ui.button(label="🏭 Galpões Norte", style=discord.ButtonStyle.secondary, custom_id="fab_norte")
    async def norte(self, i, b):
        await self.iniciar(i, "Norte", 7800, 1200)

# ================= SLASH COMMANDS =================

@bot.tree.command(name="setup_registro", description="Configura o painel de registro", guild=GUILD)
async def setup_registro(interaction):
    canal = interaction.guild.get_channel(CANAL_REGISTRO_ID)
    embed = discord.Embed(title="📋 Registro", description="Clique abaixo para se registrar.")
    await canal.send(embed=embed, view=RegistroView())
    await interaction.response.send_message("✅ Registro configurado", ephemeral=True)

@bot.tree.command(name="setup_calculadora", description="Configura o painel de vendas", guild=GUILD)
async def setup_calculadora(interaction):
    canal = interaction.guild.get_channel(CANAL_CALCULADORA_ID)
    embed = discord.Embed(title="🧮 Calculadora de Vendas", description="Registrar vendas")
    await canal.send(embed=embed, view=CalculadoraView())
    await interaction.response.send_message("✅ Calculadora configurada", ephemeral=True)

# ================= EVENTS =================

@bot.event
async def on_ready():
    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    bot.add_view(StatusView())
    bot.add_view(FabricacaoView())
    bot.add_view(SegundaTaskView())

    bot.tree.copy_global_to(guild=GUILD)
    await bot.tree.sync(guild=GUILD)

    for pid in carregar_producoes():
        bot.loop.create_task(acompanhar_producao(pid))

    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    if canal:
        embed = discord.Embed(title="🏭 Fabricação", description="Escolha o galpão")
        await canal.send(embed=embed, view=FabricacaoView())

    print("✅ Bot online — Registro, Vendas e Produção funcionando")

bot.run(TOKEN)
