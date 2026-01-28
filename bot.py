import os
import discord
from discord.ext import commands

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

        await interaction.response.send_message(
            "✅ Registro concluído com sucesso!",
            ephemeral=True
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

# ================= VENDAS =================

def formatar_dinheiro(valor: int) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

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
        try:
            pt = int(self.qtd_pt.value)
            sub = int(self.qtd_sub.value)
        except ValueError:
            await interaction.response.send_message(
                "❌ Use apenas números nas quantidades.",
                ephemeral=True
            )
            return

        total = (pt * 50) + (sub * 90)

        embed = discord.Embed(title="📦 Nova Encomenda", color=0x1e3a8a)
        embed.add_field(name="👤 Vendedor", value=interaction.user.mention, inline=False)
        embed.add_field(name="🏷 Organização", value=self.organizacao.value, inline=False)
        embed.add_field(name="🔫 PT", value=f"{pt} x R$50", inline=True)
        embed.add_field(name="🔫 SUB", value=f"{sub} x R$90", inline=True)
        embed.add_field(name="💰 Total", value=formatar_dinheiro(total), inline=False)
        embed.add_field(
            name="📝 Observações",
            value=self.observacoes.value or "Nenhuma",
            inline=False
        )
        embed.add_field(
            name="📌 Status",
            value="—",
            inline=False
        )

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())

        await interaction.response.send_message(
            "✅ Venda registrada com sucesso!",
            ephemeral=True
        )

# ================= STATUS =================

class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def atualizar(self, interaction, novo_status):
        embed = interaction.message.embeds[0]
        campo = embed.fields[-1]

        status_atual = campo.value.split("\n") if campo.value != "—" else []

        if novo_status not in status_atual:
            status_atual.append(novo_status)

        embed.set_field_at(
            -1,
            name="📌 Status",
            value="\n".join(status_atual),
            inline=False
        )

        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="status_entregue")
    async def entregue(self, interaction, button):
        await self.atualizar(interaction, "✅ Entregue")

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="status_pago")
    async def pago(self, interaction, button):
        await self.atualizar(interaction, "💰 Pago")

    @discord.ui.button(label="📦 A entregar", style=discord.ButtonStyle.secondary, custom_id="status_entregar")
    async def a_entregar(self, interaction, button):
        await self.atualizar(interaction, "📦 A entregar")

    @discord.ui.button(label="⏳ Pagamento pendente", style=discord.ButtonStyle.danger, custom_id="status_pendente")
    async def pendente(self, interaction, button):
        await self.atualizar(interaction, "⏳ Pagamento pendente")

class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🧮 Registrar Venda",
        style=discord.ButtonStyle.primary,
        custom_id="calculadora_registrar"
    )
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(VendaModal())

# ================= EVENTS =================

@bot.event
async def on_ready():
    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    bot.add_view(StatusView())

    bot.tree.copy_global_to(guild=GUILD)
    await bot.tree.sync(guild=GUILD)

    print("✅ Bot online e comandos sincronizados!")

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

    embed = discord.Embed(
        title="📋 Registro",
        description="Clique no botão abaixo para se registrar.",
        color=0x2ecc71
    )

    await canal.send(embed=embed, view=RegistroView())
    await interaction.response.send_message("✅ Registro configurado.", ephemeral=True)

@bot.tree.command(name="setup_calculadora", description="Configura a calculadora de vendas", guild=GUILD)
@commands.has_permissions(administrator=True)
async def setup_calculadora(interaction: discord.Interaction):
    canal = interaction.guild.get_channel(CANAL_CALCULADORA_ID)

    embed = discord.Embed(
        title="🧮 Calculadora de Vendas",
        description="Clique no botão abaixo para registrar uma venda.",
        color=0x1e3a8a
    )

    await canal.send(embed=embed, view=CalculadoraView())
    await interaction.response.send_message("✅ Calculadora configurada.", ephemeral=True)

bot.run(TOKEN)
