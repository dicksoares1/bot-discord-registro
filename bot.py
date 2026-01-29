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

# ================= UTIL PRODUÇÃO =================

def carregar_producoes():
    if not os.path.exists(ARQUIVO_PRODUCOES):
        return {}
    with open(ARQUIVO_PRODUCOES, "r") as f:
        return json.load(f)

def salvar_producoes(dados):
    with open(ARQUIVO_PRODUCOES, "w") as f:
        json.dump(dados, f, indent=4)

# ================= PRODUÇÃO =================

class SegundaTaskView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="✅ 2º Task Feita",
        style=discord.ButtonStyle.success,
        custom_id="segunda_task_feita"
    )
    async def segunda_task(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            "🟢 Segunda task confirmada!",
            ephemeral=True
        )

async def acompanhar_producao(producao_id):
    while True:
        producoes = carregar_producoes()
        if producao_id not in producoes:
            return

        prod = producoes[producao_id]
        agora = datetime.utcnow()
        fim = datetime.fromisoformat(prod["fim"])
        restante = fim - agora

        if restante.total_seconds() <= 0:
            del producoes[producao_id]
            salvar_producoes(producoes)
            return

        if not prod["segunda_task"] and restante <= timedelta(seconds=prod["tempo_segunda_task"]):
            canal = bot.get_channel(CANAL_REGISTRO_GALPAO_ID)
            embed = discord.Embed(
                title="📦 Produção",
                description=(
                    f"**Galpão:** {prod['galpao']}\n"
                    f"**Iniciado por:** <@{prod['autor']}>\n"
                    f"🟡 **2º Task disponível**"
                ),
                color=0xf1c40f
            )
            await canal.send(embed=embed, view=SegundaTaskView())
            prod["segunda_task"] = True
            producoes[producao_id] = prod
            salvar_producoes(producoes)

        await asyncio.sleep(30)

class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def iniciar(self, interaction, galpao, duracao, segunda_task_em):
        producoes = carregar_producoes()

        producao_id = f"{galpao}_{interaction.id}"
        inicio = datetime.utcnow()
        fim = inicio + timedelta(seconds=duracao)

        producoes[producao_id] = {
            "galpao": galpao,
            "autor": interaction.user.id,
            "inicio": inicio.isoformat(),
            "fim": fim.isoformat(),
            "tempo_segunda_task": segunda_task_em,
            "segunda_task": False
        }

        salvar_producoes(producoes)

        canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)

        embed = discord.Embed(
            title="🏭 Produção Iniciada",
            description=(
                f"**Galpão:** {galpao}\n"
                f"**Iniciado por:** {interaction.user.mention}\n"
                f"⏳ **Duração:** {duracao // 60} minutos"
            ),
            color=0x3498db
        )

        await canal.send(embed=embed)
        await interaction.response.send_message("✅ Produção iniciada!", ephemeral=True)

        bot.loop.create_task(acompanhar_producao(producao_id))

    @discord.ui.button(
        label="🏭 Galpões Sul",
        style=discord.ButtonStyle.primary,
        custom_id="fabricacao_sul"
    )
    async def sul(self, interaction, button):
        await self.iniciar(interaction, "Sul", 3900, 1500)

    @discord.ui.button(
        label="🏭 Galpões Norte",
        style=discord.ButtonStyle.secondary,
        custom_id="fabricacao_norte"
    )
    async def norte(self, interaction, button):
        await self.iniciar(interaction, "Norte", 7800, 1200)

# ================= REGISTRO =================

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

        await interaction.response.send_message("✅ Registro concluído!", ephemeral=True)

class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📋 Fazer Registro", style=discord.ButtonStyle.success, custom_id="registro_fazer")
    async def registro(self, interaction, button):
        await interaction.response.send_modal(RegistroModal())

# ================= VENDAS =================

class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(label="Organização")
    qtd_pt = discord.ui.TextInput(label="Quantidade PT (R$50)")
    qtd_sub = discord.ui.TextInput(label="Quantidade SUB (R$90)")
    observacoes = discord.ui.TextInput(label="Observações", style=discord.TextStyle.paragraph, required=False)

    async def on_submit(self, interaction: discord.Interaction):
        pt = int(self.qtd_pt.value)
        sub = int(self.qtd_sub.value)

        total = (pt * 50) + (sub * 90)

        embed = discord.Embed(title="📦 NOVA ENCOMENDA • FACÇÃO", color=0x1e3a8a)
        embed.add_field(name="👤 Vendedor", value=interaction.user.mention)
        embed.add_field(name="🏷 Organização", value=self.organizacao.value)
        embed.add_field(name="💰 Total", value=f"R$ {total:,}".replace(",", "."))

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed)
        await interaction.response.send_message("✅ Venda registrada!", ephemeral=True)

class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🧮 Registrar Venda", style=discord.ButtonStyle.primary, custom_id="calculadora_registrar")
    async def registrar(self, interaction, button):
        await interaction.response.send_modal(VendaModal())

# ================= EVENTS =================

@bot.event
async def on_ready():
    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    bot.add_view(FabricacaoView())
    bot.add_view(SegundaTaskView())

    producoes = carregar_producoes()
    for pid in producoes:
        bot.loop.create_task(acompanhar_producao(pid))

    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    if canal:
        embed = discord.Embed(
            title="🏭 Fabricação",
            description="Selecione o galpão para iniciar a produção.",
            color=0x2c3e50
        )
        await canal.send(embed=embed, view=FabricacaoView())

    print("✅ Bot online e produção restaurada!")

bot.run(TOKEN)
