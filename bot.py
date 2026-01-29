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

GUILD_ID = 1229526644193099880
GUILD = discord.Object(id=GUILD_ID)

bot = commands.Bot(command_prefix="!", intents=intents)

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

        await interaction.response.send_message("✅ Registro concluído com sucesso!", ephemeral=True)


class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📋 Fazer Registro", style=discord.ButtonStyle.success, custom_id="registro_fazer")
    async def registro(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistroModal())


# ================= STATUS VIEW =================

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


# ================= VENDAS =================

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
            await interaction.response.send_message("❌ Quantidades inválidas.", ephemeral=True)
            return

        pacotes_pt = pt // 50
        pacotes_sub = sub // 50
        total = (pt * 50) + (sub * 90)

        embed = discord.Embed(title="📦 NOVA ENCOMENDA • FACÇÃO", color=0x1e3a8a)
        embed.add_field(name="👤 Vendedor", value=interaction.user.mention, inline=False)
        embed.add_field(name="🏷 Organização", value=self.organizacao.value, inline=False)
        embed.add_field(name="🔫 PT", value=f"{pt}\n📦 {pacotes_pt}", inline=True)
        embed.add_field(name="🔫 SUB", value=f"{sub}\n📦 {pacotes_sub}", inline=True)
        embed.add_field(name="💰 Total", value=f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."), inline=False)
        embed.add_field(name="📝 Observações", value=self.observacoes.value or "Nenhuma", inline=False)
        embed.add_field(name="📌 Status", value="⏳ Pagamento pendente", inline=False)

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())
        await interaction.response.send_message("✅ Venda registrada com sucesso!", ephemeral=True)


class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🧮 Registrar Venda", style=discord.ButtonStyle.primary, custom_id="calculadora_registrar")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(VendaModal())


# ================= PRODUÇÃO =================

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
    return "🟩" * cheio + "⬜" * (size - cheio)

class SegundaTaskView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="✅ 2º Task Feita", style=discord.ButtonStyle.success, custom_id="segunda_task_feita")
    async def ok(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message("🟢 Segunda task confirmada!", ephemeral=True)

class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def iniciar(self, interaction, galpao, duracao, segunda_task_em):
        producoes = carregar_producoes()
        pid = f"{galpao}_{interaction.id}"
        inicio = datetime.utcnow()
        fim = inicio + timedelta(seconds=duracao)

        producoes[pid] = {
            "galpao": galpao,
            "autor": interaction.user.id,
            "inicio": inicio.isoformat(),
            "fim": fim.isoformat(),
            "segunda_task_em": segunda_task_em,
            "segunda_task": False
        }

        salvar_producoes(producoes)

        canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)
        await canal.send(embed=discord.Embed(
            title="🏭 Produção Iniciada",
            description=f"**Galpão:** {galpao}\n**Iniciado por:** {interaction.user.mention}",
            color=0x3498db
        ))

        bot.loop.create_task(acompanhar_producao(pid))
        await interaction.response.send_message("✅ Produção iniciada!", ephemeral=True)

    @discord.ui.button(label="🏭 Galpões Sul", style=discord.ButtonStyle.primary, custom_id="fabricacao_sul")
    async def sul(self, interaction, button):
        await self.iniciar(interaction, "Sul", 130*60, 40*60)

    @discord.ui.button(label="🏭 Galpões Norte", style=discord.ButtonStyle.secondary, custom_id="fabricacao_norte")
    async def norte(self, interaction, button):
        await self.iniciar(interaction, "Norte", 65*60, 40*60)

async def acompanhar_producao(pid):
    while True:
        producoes = carregar_producoes()
        if pid not in producoes:
            return

        prod = producoes[pid]
        inicio = datetime.fromisoformat(prod["inicio"])
        fim = datetime.fromisoformat(prod["fim"])
        total = (fim - inicio).total_seconds()
        restante = (fim - datetime.utcnow()).total_seconds()

        pct = max(0, min(1, 1 - (restante / total)))

        if not prod["segunda_task"] and (total - restante) >= prod["segunda_task_em"]:
            canal = bot.get_channel(CANAL_REGISTRO_GALPAO_ID)
            await canal.send(embed=discord.Embed(
                title="🟡 2ª Task Liberada",
                description=f"**Galpão:** {prod['galpao']}\n<@{prod['autor']}>",
                color=0xf1c40f
            ), view=SegundaTaskView())
            prod["segunda_task"] = True
            salvar_producoes(producoes)

        if restante <= 0:
            del producoes[pid]
            salvar_producoes(producoes)
            return

        await asyncio.sleep(30)

async def enviar_painel_fabricacao():
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    if not canal:
        return

    async for m in canal.history(limit=10):
        if m.author == bot.user and m.embeds and m.embeds[0].title == "🏭 Fabricação":
            return

    await canal.send(
        embed=discord.Embed(title="🏭 Fabricação", description="Selecione o galpão.", color=0x2c3e50),
        view=FabricacaoView()
    )

# ================= EVENTS =================

@bot.event
async def on_ready():
    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    bot.add_view(StatusView())
    bot.add_view(FabricacaoView())
    bot.add_view(SegundaTaskView())

    for pid in carregar_producoes():
        bot.loop.create_task(acompanhar_producao(pid))

    await enviar_painel_fabricacao()

    bot.tree.copy_global_to(guild=GUILD)
    await bot.tree.sync(guild=GUILD)

    print("✅ Bot online com Registro, Vendas e Produção")

@bot.event
async def on_member_join(member):
    cargo = member.guild.get_role(CONVIDADO_ROLE_ID)
    if cargo:
        await member.add_roles(cargo)

# ================= COMMANDS =================

@bot.tree.command(name="setup_registro", description="Configura o painel de registro", guild=GUILD)
@commands.has_permissions(administrator=True)
async def setup_registro(interaction: discord.Interaction):
    canal = interaction.guild.get_channel(CANAL_REGISTRO_ID)
    await canal.send(embed=discord.Embed(title="📋 Registro", color=0x2ecc71), view=RegistroView())
    await interaction.response.send_message("✅ Registro configurado.", ephemeral=True)

@bot.tree.command(name="setup_calculadora", description="Configura a calculadora de vendas", guild=GUILD)
@commands.has_permissions(administrator=True)
async def setup_calculadora(interaction: discord.Interaction):
    canal = interaction.guild.get_channel(CANAL_CALCULADORA_ID)
    await canal.send(embed=discord.Embed(title="🧮 Calculadora de Vendas", color=0x1e3a8a), view=CalculadoraView())
    await interaction.response.send_message("✅ Calculadora configurada.", ephemeral=True)

bot.run(TOKEN)
