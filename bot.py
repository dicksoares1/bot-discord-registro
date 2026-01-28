import os
import discord
from discord.ext import commands

TOKEN = os.environ.get("TOKEN")

intents = discord.Intents.default()
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)

# =========================
# CONFIGURAÇÕES – REGISTRO
# =========================
AGREGADO_ROLE_ID = 1422847202937536532
CONVIDADO_ROLE_ID = 1337382961456353342
CANAL_REGISTRO_ID = 1229556030397218878
CANAL_LOG_REGISTRO_ID = 1462457604939841851

# =========================
# CONFIGURAÇÕES – CALCULADORA
# =========================
CANAL_CALCULADORA_ID = 123456789012345678  # 🔁 troque
CANAL_ENCOMENDAS_ID = 123456789012345678   # 🔁 troque

PRECO_PT = 50
PRECO_SUB = 90


# ==================================================
# ================= REGISTRO ========================
# ==================================================
class RegistroModal(discord.ui.Modal, title="📋 Registro de Entrada"):
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

        await interaction.response.send_message(
            "✅ Registro concluído com sucesso!",
            ephemeral=True
        )


class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="📋 Fazer Registro",
        style=discord.ButtonStyle.green,
        custom_id="registro_botao"
    )
    async def registro(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistroModal())


@bot.tree.command(name="setup_registro")
@commands.has_permissions(administrator=True)
async def setup_registro(interaction: discord.Interaction):
    canal = interaction.guild.get_channel(CANAL_REGISTRO_ID)

    embed = discord.Embed(
        title="📋 Registro",
        description="Clique no botão abaixo para se registrar.",
        color=0x2ecc71
    )

    await canal.send(embed=embed, view=RegistroView())
    await interaction.response.send_message("✅ Registro configurado!", ephemeral=True)


# ==================================================
# ============== CALCULADORA =======================
# ==================================================
class CalculadoraModal(discord.ui.Modal, title="🧮 Calculadora de Vendas"):
    obs = discord.ui.TextInput(label="Observações gerais", required=False)
    qtd_pt = discord.ui.TextInput(label="Quantidade PT (R$ 50)", required=True)
    qtd_sub = discord.ui.TextInput(label="Quantidade SUB (R$ 90)", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            pt = int(self.qtd_pt.value)
            sub = int(self.qtd_sub.value)
        except ValueError:
            await interaction.response.send_message("❌ Use apenas números.", ephemeral=True)
            return

        total = pt * PRECO_PT + sub * PRECO_SUB

        embed = discord.Embed(title="📦 Nova Encomenda", color=0x3498db)
        embed.add_field(name="📝 Observações", value=self.obs.value or "Nenhuma", inline=False)
        embed.add_field(name="🔫 PT", value=f"{pt} x R$ 50,00", inline=True)
        embed.add_field(name="🔫 SUB", value=f"{sub} x R$ 90,00", inline=True)
        embed.add_field(
            name="💰 Total",
            value=f"**R$ {total:,.2f}**".replace(",", "X").replace(".", ",").replace("X", "."),
            inline=False
        )
        embed.add_field(name="📌 Status", value="*(nenhum)*", inline=False)

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())

        await interaction.response.send_message("✅ Venda registrada!", ephemeral=True)


class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🧮 Nova Venda",
        style=discord.ButtonStyle.green,
        custom_id="botao_calculadora"
    )
    async def abrir(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(CalculadoraModal())


class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.status = set()

    async def atualizar(self, interaction):
        embed = interaction.message.embeds[0]
        texto = "\n".join(self.status) if self.status else "*(nenhum)*"
        embed.set_field_at(4, name="📌 Status", value=texto, inline=False)
        await interaction.message.edit(embed=embed, view=self)

    async def toggle(self, interaction, texto):
        if texto in self.status:
            self.status.remove(texto)
        else:
            self.status.add(texto)
        await self.atualizar(interaction)
        await interaction.response.defer()

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="pago")
    async def pago(self, i, b): await self.toggle(i, "💰 Pago")

    @discord.ui.button(label="📦 Entregue", style=discord.ButtonStyle.success, custom_id="entregue")
    async def entregue(self, i, b): await self.toggle(i, "📦 Entregue")

    @discord.ui.button(label="⏳ Pagamento pendente", style=discord.ButtonStyle.secondary, custom_id="pendente")
    async def pendente(self, i, b): await self.toggle(i, "⏳ Pagamento pendente")

    @discord.ui.button(label="🚚 Falta entregar", style=discord.ButtonStyle.danger, custom_id="falta")
    async def falta(self, i, b): await self.toggle(i, "🚚 Falta entregar")


@bot.tree.command(name="setup_calculadora")
@commands.has_permissions(administrator=True)
async def setup_calculadora(interaction: discord.Interaction):
    canal = interaction.guild.get_channel(CANAL_CALCULADORA_ID)

    embed = discord.Embed(
        title="🧮 Calculadora de Vendas",
        description="Clique abaixo para registrar uma venda.",
        color=0x2ecc71
    )

    await canal.send(embed=embed, view=CalculadoraView())
    await interaction.response.send_message("✅ Calculadora configurada!", ephemeral=True)


# =========================
# EVENTOS GERAIS
# =========================
@bot.event
async def on_member_join(member):
    cargo = member.guild.get_role(CONVIDADO_ROLE_ID)
    if cargo:
        await member.add_roles(cargo)


@bot.event
async def on_ready():
    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    print("✅ Bot online com REGISTRO + CALCULADORA!")


bot.run(TOKEN)
