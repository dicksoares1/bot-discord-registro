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
    try:
        with open(ARQUIVO_PRODUCOES, "r") as f:
            return json.load(f)
    except:
        return {}

def salvar_producoes(dados):
    with open(ARQUIVO_PRODUCOES, "w") as f:
        json.dump(dados, f, indent=4)

def formatar_tempo(seg):
    h = seg // 3600
    m = (seg % 3600) // 60
    s = seg % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

def barra_progresso(atual, total, tam=20):
    pct = atual / total
    cheio = int(pct * tam)
    return "▓" * cheio + "░" * (tam - cheio)

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

        await membro.add_roles(guild.get_role(AGREGADO_ROLE_ID))
        await membro.remove_roles(guild.get_role(CONVIDADO_ROLE_ID))

        canal = guild.get_channel(CANAL_LOG_REGISTRO_ID)
        embed = discord.Embed(title="📋 Novo Registro", color=0x2ecc71)
        embed.add_field(name="Nome", value=self.nome.value)
        embed.add_field(name="Passaporte", value=self.passaporte.value)
        embed.add_field(name="Indicado por", value=self.indicado.value)
        embed.add_field(name="Telefone", value=self.telefone.value)
        embed.add_field(name="Usuário", value=membro.mention)
        await canal.send(embed=embed)

        await interaction.response.send_message("✅ Registro concluído!", ephemeral=True)

class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📋 Fazer Registro", style=discord.ButtonStyle.success, custom_id="registro_btn")
    async def abrir(self, interaction, button):
        await interaction.response.send_modal(RegistroModal())

# ================= VENDAS =================

class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def toggle(self, interaction, label):
        embed = interaction.message.embeds[0]
        campo = embed.fields[-1]
        lista = campo.value.split("\n")

        if label.startswith("✅ Entregue"):
            label = f"✅ Entregue • {datetime.now().strftime('%d/%m %H:%M')}"

        if label in lista:
            lista.remove(label)
        else:
            lista.append(label)

        embed.set_field_at(-1, name="📌 Status", value="\n".join(lista), inline=False)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="st_entregue")
    async def entregue(self, i, b): await self.toggle(i, "✅ Entregue")

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="st_pago")
    async def pago(self, i, b): await self.toggle(i, "💰 Pago")

    @discord.ui.button(label="📦 A entregar", style=discord.ButtonStyle.secondary, custom_id="st_aentregar")
    async def aentregar(self, i, b): await self.toggle(i, "📦 A entregar")

class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(label="Organização")
    qtd_pt = discord.ui.TextInput(label="Quantidade PT")
    qtd_sub = discord.ui.TextInput(label="Quantidade SUB")

    async def on_submit(self, interaction):
        pt = int(self.qtd_pt.value)
        sub = int(self.qtd_sub.value)
        total = pt * 50 + sub * 90

        embed = discord.Embed(title="📦 NOVA ENCOMENDA", color=0x1e3a8a)
        embed.add_field(name="Vendedor", value=interaction.user.mention)
        embed.add_field(name="Organização", value=self.organizacao.value)
        embed.add_field(name="💰 Total", value=f"R$ {total:,}".replace(",", "."))
        embed.add_field(name="📌 Status", value="⏳ Pagamento pendente", inline=False)

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())
        await interaction.response.send_message("✅ Venda registrada!", ephemeral=True)

class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🧮 Registrar Venda", style=discord.ButtonStyle.primary, custom_id="calc_btn")
    async def abrir(self, interaction, button):
        await interaction.response.send_modal(VendaModal())

# ================= PRODUÇÃO =================

class SegundaTaskView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="✅ 2º Task Feita", style=discord.ButtonStyle.success, custom_id="task2_btn")
    async def ok(self, interaction, button):
        await interaction.response.send_message("🟢 2º task confirmada!", ephemeral=True)

async def acompanhar(pid):
    while True:
        producoes = carregar_producoes()
        if pid not in producoes:
            return

        prod = producoes[pid]
        canal = bot.get_channel(CANAL_REGISTRO_GALPAO_ID)
        msg = await canal.fetch_message(prod["msg"])

        inicio = datetime.fromisoformat(prod["inicio"])
        fim = datetime.fromisoformat(prod["fim"])
        agora = datetime.utcnow()

        total = int((fim - inicio).total_seconds())
        rest = int((fim - agora).total_seconds())
        dec = total - rest

        embed = msg.embeds[0]
        embed.set_field_at(1, name="⏱ Tempo", value=formatar_tempo(max(0, rest)), inline=False)
        embed.set_field_at(2, name="📊 Progresso", value=barra_progresso(dec, total), inline=False)

        await msg.edit(embed=embed)

        if rest <= 0:
            producoes.pop(pid)
            salvar_producoes(producoes)
            return

        await asyncio.sleep(30)

class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def iniciar(self, interaction, galpao, duracao, task2):
        inicio = datetime.utcnow()
        fim = inicio + timedelta(seconds=duracao)

        embed = discord.Embed(
            title="🏭 Produção em andamento",
            description=f"Galpão **{galpao}**\n{interaction.user.mention}",
            color=0x3498db
        )
        embed.add_field(name="⏱ Tempo", value=formatar_tempo(duracao), inline=False)
        embed.add_field(name="📊 Progresso", value="░" * 20, inline=False)

        canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)
        msg = await canal.send(embed=embed)

        dados = carregar_producoes()
        pid = f"{galpao}_{msg.id}"
        dados[pid] = {
            "inicio": inicio.isoformat(),
            "fim": fim.isoformat(),
            "msg": msg.id
        }
        salvar_producoes(dados)

        bot.loop.create_task(acompanhar(pid))
        await interaction.response.send_message("✅ Produção iniciada!", ephemeral=True)

    @discord.ui.button(label="🏭 Galpões Sul", style=discord.ButtonStyle.primary, custom_id="fab_sul")
    async def sul(self, interaction, button):
        await self.iniciar(interaction, "Sul", 7800, 1500)

    @discord.ui.button(label="🏭 Galpões Norte", style=discord.ButtonStyle.secondary, custom_id="fab_norte")
    async def norte(self, interaction, button):
        await self.iniciar(interaction, "Norte", 3900, 1200)

# ================= EVENTS =================

@bot.event
async def on_ready():
    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    bot.add_view(StatusView())
    bot.add_view(FabricacaoView())
    bot.add_view(SegundaTaskView())

    for pid in carregar_producoes():
        bot.loop.create_task(acompanhar(pid))

    print("✅ Bot online • tudo funcionando")

bot.run(TOKEN)
