import os
import discord
from discord.ext import commands

TOKEN = os.getenv("TOKEN")

# IDs (SUBSTITUA PELOS REAIS – SEM ASPAS)
AGREGADO_ROLE_ID = 1422847202937536532
CONVIDADO_ROLE_ID = 1337382961456353342
CANAL_REGISTRO_ID = 1229556030397218878
CANAL_LOG_REGISTRO_ID = 1462457604939841851

intents = discord.Intents.default()
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents)


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


@bot.event
async def on_member_join(member):
    cargo = member.guild.get_role(CONVIDADO_ROLE_ID)
    if cargo:
        await member.add_roles(cargo)


@bot.event
async def on_ready():
    bot.add_view(RegistroView())
    print("✅ Bot online!")


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
    await interaction.response.send_message(
        "✅ Registro configurado.",
        ephemeral=True
    )


bot.run(TOKEN)


