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
CANAL_CALCULADORA_ID = 1460984821458272347  # 🔁 troque
CANAL_ENCOMENDAS_ID = 1460980984811098294   # 🔁 troque

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
class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(
        label="Organização",
        placeholder="Ex: VDR 442",
        required=True
    )

    qtd_pt = discord.ui.TextInput(
        label="Quantidade PT (R$50)",
        placeholder="Somente números",
        required=True
    )

    qtd_sub = discord.ui.TextInput(
        label="Quantidade SUB (R$90)",
        placeholder="Somente números",
        required=True
    )

    observacoes = discord.ui.TextInput(
        label="Observações gerais",
        style=discord.TextStyle.paragraph,
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            pt = int(self.qtd_pt.value)
            sub = int(self.qtd_sub.value)
        except ValueError:
            await interaction.response.send_message(
                "❌ Quantidades inválidas. Use apenas números.",
                ephemeral=True
            )
            return

        total_pt = pt * 50
        total_sub = sub * 90
        total = total_pt + total_sub

        embed = discord.Embed(
            title="📦 Nova Encomenda",
            color=0x1e3a8a
        )

        embed.add_field(
            name="👤 Vendedor",
            value=interaction.user.mention,
            inline=False
        )

        embed.add_field(
            name="🏷 Organização",
            value=self.organizacao.value,
            inline=False
        )

        embed.add_field(
            name="🔫 PT",
            value=f"{pt} x R$50 = R${total_pt}",
            inline=True
        )

        embed.add_field(
            name="🔫 SUB",
            value=f"{sub} x R$90 = R${total_sub}",
            inline=True
        )

        embed.add_field(
            name="💰 Total",
            value=f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            inline=False
        )

        embed.add_field(
            name="📝 Observações",
            value=self.observacoes.value or "Nenhuma",
            inline=False
        )

        embed.add_field(
            name="📌 Status",
            value="⏳ Pagamento pendente",
            inline=False
        )

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())

        await interaction.response.send_message(
            "✅ Venda registrada com sucesso!",
            ephemeral=True
        )

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


