import os
import discord
from discord.ext import commands, tasks
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
CANAL_PRODUCAO_ID = 1463000000000000000  # 🔴 coloque o ID do canal de produção

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

        await interaction.response.send_message("✅ Registro concluído!", ephemeral=True)


class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📋 Fazer Registro", style=discord.ButtonStyle.success, custom_id="registro_btn")
    async def registro(self, interaction, button):
        await interaction.response.send_modal(RegistroModal())


# ================= STATUS =================

class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def toggle_status(self, interaction, label):
        embed = interaction.message.embeds[0]
        campo = embed.fields[-1]
        status = campo.value.split("\n")

        if label.startswith("✅ Entregue"):
            agora = datetime.now().strftime("%d/%m/%Y %H:%M")
            label = f"✅ Entregue • {agora}"

        if label in status:
            status.remove(label)
        else:
            status.append(label)

        embed.set_field_at(-1, name="📌 Status", value="\n".join(status), inline=False)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="status_entregue")
    async def entregue(self, i, b): await self.toggle_status(i, "✅ Entregue")

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="status_pago")
    async def pago(self, i, b): await self.toggle_status(i, "💰 Pago")

    @discord.ui.button(label="📦 A entregar", style=discord.ButtonStyle.secondary, custom_id="status_entregar")
    async def entregar(self, i, b): await self.toggle_status(i, "📦 A entregar")


# ================= VENDAS =================

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
        embed.add_field(name="Total", value=f"R$ {total:,}".replace(",", "."))
        embed.add_field(name="📌 Status", value="⏳ Pagamento pendente", inline=False)

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())
        await interaction.response.send_message("✅ Venda registrada!", ephemeral=True)


class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🧮 Registrar Venda", style=discord.ButtonStyle.primary, custom_id="calc_btn")
    async def registrar(self, interaction, button):
        await interaction.response.send_modal(VendaModal())


# ================= PRODUÇÃO =================

producoes = {}

def barra(p):
    total = 10
    cheio = int((p / 100) * total)
    return "█" * cheio + "░" * (total - cheio)

class ProducaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🏭 Iniciar Produção", style=discord.ButtonStyle.success, custom_id="prod_start")
    async def iniciar(self, interaction, button):
        fim = datetime.utcnow() + timedelta(minutes=30)
        producoes[interaction.user.id] = fim

        embed = discord.Embed(title="🏭 Produção em andamento", color=0xf1c40f)
        embed.add_field(name="Tempo restante", value="30:00")
        embed.add_field(name="Progresso", value=barra(0))

        msg = await interaction.channel.send(embed=embed)
        await interaction.response.defer()

        @tasks.loop(seconds=60)
        async def atualizar():
            restante = fim - datetime.utcnow()
            if restante.total_seconds() <= 0:
                atualizar.cancel()
                embed.color = 0x2ecc71
                embed.set_field_at(0, name="Tempo restante", value="✅ Concluído")
                embed.set_field_at(1, name="Progresso", value=barra(100))
                await msg.edit(embed=embed)
                return

            minutos = int(restante.total_seconds() // 60)
            perc = int((30 - minutos) / 30 * 100)

            embed.set_field_at(0, name="Tempo restante", value=f"{minutos} min")
            embed.set_field_at(1, name="Progresso", value=barra(perc))
            await msg.edit(embed=embed)

        atualizar.start()


# ================= EVENTS =================

@bot.event
async def on_ready():
    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    bot.add_view(StatusView())
    bot.add_view(ProducaoView())

    guild = bot.get_guild(GUILD_ID)

    async def painel(canal_id, titulo, desc, view):
        canal = guild.get_channel(canal_id)
        async for m in canal.history(limit=20):
            if m.author == bot.user:
                return
        embed = discord.Embed(title=titulo, description=desc)
        await canal.send(embed=embed, view=view)

    await painel(CANAL_REGISTRO_ID, "📋 Registro", "Clique para se registrar", RegistroView())
    await painel(CANAL_CALCULADORA_ID, "🧮 Calculadora", "Registrar venda", CalculadoraView())
    await painel(CANAL_PRODUCAO_ID, "🏭 Produção", "Gerencie produções", ProducaoView())

    print("✅ Bot online e funcional!")


@bot.event
async def on_member_join(member):
    cargo = member.guild.get_role(CONVIDADO_ROLE_ID)
    if cargo:
        await member.add_roles(cargo)


bot.run(TOKEN)
