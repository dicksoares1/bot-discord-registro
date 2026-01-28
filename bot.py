import os
import discord
from discord.ext import commands

print("TODAS AS VARIÁVEIS:", os.environ)

TOKEN = os.environ.get("TOKEN")
print("TOKEN LIDO:", TOKEN)

intents = discord.Intents.default()
intents.members = True

# IDs (SUBSTITUA PELOS REAIS – SEM ASPAS)
AGREGADO_ROLE_ID = 1422847202937536532
CONVIDADO_ROLE_ID = 1337382961456353342
CANAL_REGISTRO_ID = 1229556030397218878
CANAL_LOG_REGISTRO_ID = 1462457604939841851
CANAL_CALCULADORA_ID = 1460984821458272347  # calculadora-de-vendas
CANAL_ENCOMENDAS_ID = 1460980984811098294  # encomendas
GUILD_ID = 1229526644193099880


bot = commands.Bot(command_prefix="!", intents=intents)

GUILD_ID = 1229526644193099880
GUILD = discord.Object(id=GUILD_ID)

class RegistroModal(discord.ui.Modal, title="Registro de Entrada"):
    nome = discord.ui.TextInput(label="Nome Completo")
    passaporte = discord.ui.TextInput(label="Passaporte")
    indicado = discord.ui.TextInput(label="Indicado por")
    telefone = discord.ui.TextInput(label="Telefone In Game")

    async def on_submit(self, interaction: discord.Interaction):
        membro = interaction.user
        guild = interaction.guild

        apelido = f"{self.passaporte.value} - {self.nome.value}"
        await membro.edit(nick=apelido)

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
class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(label="Organização")
    qtd_pt = discord.ui.TextInput(label="Quantidade PT (R$50)")
    qtd_sub = discord.ui.TextInput(label="Quantidade SUB (R$90)")
    observacoes = discord.ui.TextInput(
        label="Observações",
        style=discord.TextStyle.paragraph,
        required=False
    )

    async def on_submit(self, interaction: discord.Interaction):
        pt = int(self.qtd_pt.value)
        sub = int(self.qtd_sub.value)

        except ValueError:
    await interaction.response.send_message(
        "❌ Use apenas números nas quantidades.",
        ephemeral=True
    )
    return

        total_pt = pt * 50
        total_sub = sub * 90
        total = total_pt + total_sub

        embed = discord.Embed(title="📦 Nova Encomenda", color=0x1e3a8a)
        embed.add_field(name="👤 Vendedor", value=interaction.user.mention, inline=False)
        embed.add_field(name="🏷 Organização", value=self.organizacao.value, inline=False)
        embed.add_field(name="🔫 PT", value=f"{pt} x R$50 = R${total_pt}", inline=True)
        embed.add_field(name="🔫 SUB", value=f"{sub} x R$90 = R${total_sub}", inline=True)
        embed.add_field(name="💰 Total", value=f"R${total}", inline=False)
        embed.add_field(name="📝 Observações", value=self.observacoes.value or "Nenhuma", inline=False)
        embed.add_field(name="📌 Status", value="⏳ Pagamento pendente", inline=False)

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())

        await interaction.response.send_message(
            "✅ Venda registrada com sucesso!",
            ephemeral=True
        )

class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def atualizar_status(self, interaction, status):
        embed = interaction.message.embeds[0]
        embed.set_field_at(-1, name="📌 Status", value=status, inline=False)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success)
    async def entregue(self, interaction, button):
        await self.atualizar_status(interaction, "✅ Entregue")

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary)
    async def pago(self, interaction, button):
        await self.atualizar_status(interaction, "💰 Pago")

    @discord.ui.button(label="📦 A entregar", style=discord.ButtonStyle.secondary)
    async def a_entregar(self, interaction, button):
        await self.atualizar_status(interaction, "📦 A entregar")

    @discord.ui.button(label="⏳ Pagamento pendente", style=discord.ButtonStyle.danger)
    async def pendente(self, interaction, button):
        await self.atualizar_status(interaction, "⏳ Pagamento pendente")

class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🧮 Registrar Venda",
        style=discord.ButtonStyle.primary,
        custom_id="botao_venda"
    )
    async def registrar_venda(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(VendaModal())


class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="📋 Fazer Registro",
        style=discord.ButtonStyle.success,
        custom_id="registro_botao"
    )
    async def registro(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistroModal())


@bot.event
async def on_member_join(member):
    cargo = member.guild.get_role(CONVIDADO_ROLE_ID)
    if cargo:
        await member.add_roles(cargo)


@bot.event
async def on_ready():
    guild = discord.Object(id=GUILD_ID)

    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    bot.add_view(StatusView())

    bot.tree.copy_global_to(guild=guild)
   await bot.tree.sync(guild=GUILD)
print("🔄 Comandos sincronizados no servidor!")


@bot.tree.command(
    name="setup_registro",
    description="Configura o painel de registro",
    guild=GUILD
)
@commands.has_permissions(administrator=True)
async def setup_registro(interaction: discord.Interaction):

    canal = interaction.guild.get_channel(CANAL_REGISTRO_ID)

    embed = discord.Embed(
        title="📋 Registro",
        description="Clique no botão abaixo para se registrar.",
        color=0x2ecc71
    )

    await canal.send(embed=embed, view=RegistroView())
    await interaction.response.send_message(
        "✅ Registro configurado.",
        ephemeral=True
    )

@bot.tree.command(
    name="setup_calculadora",
    description="Configura a calculadora de vendas",
    guild=GUILD
)
@commands.has_permissions(administrator=True)
async def setup_calculadora(interaction: discord.Interaction):

    canal = interaction.guild.get_channel(CANAL_CALCULADORA_ID)

    embed = discord.Embed(
        title="🧮 Calculadora de Vendas",
        description="Clique no botão abaixo para registrar uma venda.",
        color=0x1e3a8a
    )

    await canal.send(embed=embed, view=CalculadoraView())
    await interaction.response.send_message(
        "✅ Calculadora configurada.",
        ephemeral=True
    )

bot.run(TOKEN)








