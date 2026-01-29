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
        numero = int(f.read())

    numero += 1

    with open("pedido_id.txt", "w") as f:
        f.write(str(numero))

    return f"#{numero:04d}"


def registrar_venda_dia(pt, sub, total):
    dados = {"pt": 0, "sub": 0, "total": 0, "pedidos": 0}

    if os.path.exists("vendas_hoje.json"):
        with open("vendas_hoje.json", "r") as f:
            dados = json.load(f)

    dados["pt"] += pt
    dados["sub"] += sub
    dados["total"] += total
    dados["pedidos"] += 1

    with open("vendas_hoje.json", "w") as f:
        json.dump(dados, f)


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

    @discord.ui.button(label="📋 Fazer Registro", style=discord.ButtonStyle.success)
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

        if texto.startswith("✅ Entregue"):
            texto = f"✅ Entregue • {datetime.now().strftime('%d/%m/%Y %H:%M')}"

        existente = [l for l in lista if l.startswith(texto.split("•")[0])]
        if existente:
            for e in existente:
                lista.remove(e)
        else:
            lista.append(texto)

        embed.set_field_at(-1, name="📌 Status", value="\n".join(lista), inline=False)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success)
    async def entregue(self, interaction, button):
        await self.toggle(interaction, "✅ Entregue")

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary)
    async def pago(self, interaction, button):
        await self.toggle(interaction, "💰 Pago")

    @discord.ui.button(label="📦 A entregar", style=discord.ButtonStyle.secondary)
    async def a_entregar(self, interaction, button):
        await self.toggle(interaction, "📦 A entregar")

    @discord.ui.button(label="⏳ Pagamento pendente", style=discord.ButtonStyle.danger)
    async def pendente(self, interaction, button):
        await self.toggle(interaction, "⏳ Pagamento pendente")


# ================= VENDAS =================

class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(label="Organização")
    qtd_pt = discord.ui.TextInput(label="Quantidade PT (R$50)")
    qtd_sub = discord.ui.TextInput(label="Quantidade SUB (R$90)")
    observacoes = discord.ui.TextInput(label="Observações", required=False)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)

        pt = int(self.qtd_pt.value)
        sub = int(self.qtd_sub.value)

        pac_pt = pt // 50
        pac_sub = sub // 50
        total = (pt * 50) + (sub * 90)

        registrar_venda_dia(pt, sub, total)
        pedido_id = proximo_pedido()

        embed = discord.Embed(
            title=f"📦 ENCOMENDA {pedido_id}",
            color=0x1e3a8a
        )

        embed.add_field(name="👤 Vendedor", value=interaction.user.mention, inline=False)
        embed.add_field(name="🏷 Organização", value=self.organizacao.value, inline=False)
        embed.add_field(name="🔫 PT", value=f"{pt} muni\n📦 {pac_pt} pacotes", inline=True)
        embed.add_field(name="🔫 SUB", value=f"{sub} muni\n📦 {pac_sub} pacotes", inline=True)
        embed.add_field(
            name="💰 Total",
            value=f"R$ {total:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
            inline=False
        )
        embed.add_field(name="📝 Observações", value=self.observacoes.value or "Nenhuma", inline=False)
        embed.add_field(name="📌 Status", value="", inline=False)
        embed.set_footer(text="🛡 Sistema de Encomendas • VDR 442")

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())

        await interaction.followup.send("✅ Venda registrada com sucesso!", ephemeral=True)


class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🧮 Registrar Venda", style=discord.ButtonStyle.primary)
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

    embed = discord.Embed(title="📊 RESUMO DIÁRIO", color=0x16a34a)
    embed.add_field(name="📦 Pedidos", value=dados["pedidos"])
    embed.add_field(name="🔫 PT", value=dados["pt"])
    embed.add_field(name="🔫 SUB", value=dados["sub"])
    embed.add_field(
        name="💰 Total",
        value=f"R$ {dados['total']:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."),
        inline=False
    )

    await canal.send(embed=embed)
    os.remove("vendas_hoje.json")


# ================= READY =================

@bot.event
async def on_ready():
    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    bot.add_view(StatusView())

    resumo_diario.start()
    print("✅ Bot online e operacional!")


bot.run(TOKEN)
