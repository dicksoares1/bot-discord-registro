import os
import json
import discord
from discord.ext import commands, tasks
from datetime import datetime

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

# ================= UTIL =================

def proximo_pedido():
    if not os.path.exists("pedido_id.txt"):
        with open("pedido_id.txt", "w") as f:
            f.write("0")

    with open("pedido_id.txt", "r") as f:
        conteudo = f.read().strip()
        numero = int(conteudo) if conteudo.isdigit() else 0

    numero += 1

    with open("pedido_id.txt", "w") as f:
        f.write(str(numero))

    return f"#{numero:04d}"


def registrar_venda_dia(pt, sub, total):
    if os.path.exists("vendas_hoje.json"):
        with open("vendas_hoje.json", "r") as f:
            dados = json.load(f)
    else:
        dados = {"pt": 0, "sub": 0, "total": 0, "pedidos": 0}

    dados["pt"] += pt
    dados["sub"] += sub
    dados["total"] += total
    dados["pedidos"] += 1

    with open("vendas_hoje.json", "w") as f:
        json.dump(dados, f)

# ================= REGISTRO =================

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
            embed = discord.Embed(title="📋 NOVO REGISTRO", color=0x22c55e)
            embed.add_field(name="👤 Nome", value=self.nome.value, inline=False)
            embed.add_field(name="🆔 Passaporte", value=self.passaporte.value)
            embed.add_field(name="🤝 Indicado por", value=self.indicado.value)
            embed.add_field(name="📞 Telefone", value=self.telefone.value)
            embed.add_field(name="🔗 Usuário", value=membro.mention)
            await canal_log.send(embed=embed)

        await interaction.response.send_message("✅ Registro concluído!", ephemeral=True)


class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="📋 Fazer Registro",
        style=discord.ButtonStyle.success,
        custom_id="registro_fazer"
    )
    async def registro(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(RegistroModal())

# ================= STATUS =================

class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def toggle(self, interaction: discord.Interaction, texto: str):
        embed = interaction.message.embeds[0]
        campo = embed.fields[-1]
        lista = campo.value.split("\n") if campo.value else []

        if texto == "✅ Entregue":
            texto = f"✅ Entregue • {datetime.now().strftime('%d/%m/%Y %H:%M')}"

        base = texto.split("•")[0]
        existente = [l for l in lista if l.startswith(base)]

        if existente:
            for e in existente:
                lista.remove(e)
        else:
            lista.append(texto)

        embed.set_field_at(-1, name="📌 STATUS", value="\n".join(lista), inline=False)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="status_entregue")
    async def entregue(self, interaction, button):
        await self.toggle(interaction, "✅ Entregue")

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="status_pago")
    async def pago(self, interaction, button):
        await self.toggle(interaction, "💰 Pago")

    @discord.ui.button(label="📦 A entregar", style=discord.ButtonStyle.secondary, custom_id="status_a_entregar")
    async def a_entregar(self, interaction, button):
        await self.toggle(interaction, "📦 A entregar")

    @discord.ui.button(label="⏳ Pagamento pendente", style=discord.ButtonStyle.danger, custom_id="status_pendente")
    async def pendente(self, interaction, button):
        await self.toggle(interaction, "⏳ Pagamento pendente")

# ================= VENDAS =================

class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(label="Organização")
    qtd_pt = discord.ui.TextInput(label="Quantidade PT (munição)")
    qtd_sub = discord.ui.TextInput(label="Quantidade SUB (munição)")
    observacoes = discord.ui.TextInput(label="Observações", style=discord.TextStyle.paragraph, required=False)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            pt = int(self.qtd_pt.value)
            sub = int(self.qtd_sub.value)
        except ValueError:
            await interaction.response.send_message("❌ Use apenas números.", ephemeral=True)
            return

        pedido_id = proximo_pedido()

        pac_pt = pt // 50
        pac_sub = sub // 50

        total = (pt * 50) + (sub * 90)
        registrar_venda_dia(pt, sub, total)

        embed = discord.Embed(
            title=f"📦 ENCOMENDA {pedido_id}",
            color=0x1e3a8a
        )

        embed.add_field(name="👤 Vendedor", value=interaction.user.mention, inline=False)
        embed.add_field(name="🏷 Organização", value=self.organizacao.value, inline=False)
        embed.add_field(name="🔫 PT", value=f"{pt} muni\n📦 {pac_pt} pacotes", inline=True)
        embed.add_field(name="🔫 SUB", value=f"{sub} muni\n📦 {pac_sub} pacotes", inline=True)
        embed.add_field(
            name="💰 TOTAL",
            value=f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            inline=False
        )
        embed.add_field(name="📝 Observações", value=self.observacoes.value or "Nenhuma", inline=False)
        embed.add_field(name="📌 STATUS", value="⏳ Pagamento pendente", inline=False)
        embed.set_footer(text="🛡 Sistema de Encomendas • VDR 442")

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())

        await interaction.response.send_message("✅ Venda registrada!", ephemeral=True)


class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🧮 Registrar Venda",
        style=discord.ButtonStyle.primary,
        custom_id="calc_registrar_venda"
    )
    async def registrar(self, interaction, button):
        await interaction.response.send_modal(VendaModal())

# ================= RESUMO DIÁRIO =================

@tasks.loop(minutes=1)
async def resumo_diario():
    if datetime.now().strftime("%H:%M") != "23:59":
        return

    if not os.path.exists("vendas_hoje.json"):
        return

    with open("vendas_hoje.json", "r") as f:
        dados = json.load(f)

    canal = bot.get_channel(CANAL_ENCOMENDAS_ID)

    embed = discord.Embed(title="📊 RESUMO DIÁRIO DE VENDAS", color=0x16a34a)
    embed.add_field(name="📦 Pedidos", value=dados["pedidos"])
    embed.add_field(name="🔫 PT", value=dados["pt"])
    embed.add_field(name="🔫 SUB", value=dados["sub"])
    embed.add_field(
        name="💰 Total",
        value=f"R$ {dados['total']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        inline=False
    )
    embed.set_footer(text="🛡 Sistema VDR 442")

    await canal.send(embed=embed)
    os.remove("vendas_hoje.json")

# ================= EVENTS =================

@bot.event
async def on_ready():
    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    bot.add_view(StatusView())

    bot.tree.copy_global_to(guild=GUILD)
    await bot.tree.sync(guild=GUILD)

    resumo_diario.start()
    print("✅ Bot online, estável e operacional!")

@bot.event
async def on_member_join(member):
    cargo = member.guild.get_role(CONVIDADO_ROLE_ID)
    if cargo:
        await member.add_roles(cargo)

# ================= COMMANDS =================

@bot.tree.command(name="setup_registro", guild=GUILD)
async def setup_registro(interaction: discord.Interaction):
    canal = interaction.guild.get_channel(CANAL_REGISTRO_ID)
    embed = discord.Embed(title="📋 REGISTRO", description="Clique abaixo para se registrar.", color=0x22c55e)
    await canal.send(embed=embed, view=RegistroView())
    await interaction.response.send_message("✅ Registro configurado.", ephemeral=True)

@bot.tree.command(name="setup_calculadora", guild=GUILD)
async def setup_calculadora(interaction: discord.Interaction):
    canal = interaction.guild.get_channel(CANAL_CALCULADORA_ID)
    embed = discord.Embed(
        title="🧮 CALCULADORA DE VENDAS",
        description="Clique abaixo para registrar uma encomenda.",
        color=0x1e3a8a
    )
    await canal.send(embed=embed, view=CalculadoraView())
    await interaction.response.send_message("✅ Calculadora configurada.", ephemeral=True)

bot.run(TOKEN)
