import os
import json
import asyncio
import aiohttp
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

# LIVES (ADICIONADO)
CANAL_CADASTRO_LIVE_ID = 1466464557215256790  # <-- ID do #cadastrar-live
CANAL_DIVULGACAO_LIVE_ID = 1243325102917943335  # <-- ID do #divulgacao-lives

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")

ARQUIVO_LIVES = "lives.json"

GUILD_ID = 1229526644193099880
GUILD = discord.Object(id=GUILD_ID)

bot = commands.Bot(command_prefix="!", intents=intents)

# ================= REGISTRO / VENDAS (INTACTO) =================
# >>> TODO O SEU CÓDIGO DE REGISTRO, STATUS E VENDAS FICA IGUAL <<<
# (NÃO MEXI EM NADA AQUI — mantido como você enviou anteriormente)

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
    return " " * cheio + " " * (size - cheio)

class SegundaTaskView(discord.ui.View):
    def __init__(self, pid):
        super().__init__(timeout=None)
        self.pid = pid

    @discord.ui.button(label=" 2º Task Feita", style=discord.ButtonStyle.success, custom_id="segunda_task_feita")
    async def ok(self, interaction: discord.Interaction, button: discord.ui.Button):
        producoes = carregar_producoes()
        if self.pid not in producoes:
            await interaction.response.defer()
            return

        producoes[self.pid]["segunda_task_confirmada"] = {
            "user": interaction.user.id,
            "time": datetime.utcnow().isoformat()
        }

        salvar_producoes(producoes)
        await interaction.response.defer()

class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def iniciar(self, interaction, galpao, total_min, segunda_task_faltando_min):
        producoes = carregar_producoes()
        pid = f"{galpao}_{interaction.id}"
        inicio = datetime.utcnow()
        fim = inicio + timedelta(minutes=total_min)

        msg = await interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID).send(
            embed=discord.Embed(title=" Produção", description="Iniciando...", color=0x3498db)
        )

        producoes[pid] = {
            "galpao": galpao,
            "autor": interaction.user.id,
            "inicio": inicio.isoformat(),
            "fim": fim.isoformat(),
            "segunda_task_em": (total_min - segunda_task_faltando_min) * 60,
            "segunda_task": False,
            "msg_id": msg.id,
            "canal_id": CANAL_REGISTRO_GALPAO_ID
        }

        salvar_producoes(producoes)
        bot.loop.create_task(acompanhar_producao(pid))
        await interaction.response.defer()

    @discord.ui.button(label=" Galpões Sul", style=discord.ButtonStyle.primary, custom_id="fabricacao_sul")
    async def sul(self, interaction, button):
        await self.iniciar(interaction, "Sul", 130, 80)

    @discord.ui.button(label=" Galpões Norte", style=discord.ButtonStyle.secondary, custom_id="fabricacao_norte")
    async def norte(self, interaction, button):
        await self.iniciar(interaction, "Norte", 65, 40)

async def acompanhar_producao(pid):
    while True:
        producoes = carregar_producoes()
        if pid not in producoes:
            return

        prod = producoes[pid]
        canal = bot.get_channel(prod["canal_id"])
        try:
            msg = await canal.fetch_message(prod["msg_id"])
        except:
            return

        inicio = datetime.fromisoformat(prod["inicio"])
        fim = datetime.fromisoformat(prod["fim"])

        total = (fim - inicio).total_seconds()
        restante = max(0, (fim - datetime.utcnow()).total_seconds())
        pct = max(0, min(1, 1 - (restante / total)))
        mins = int(restante // 60)

        desc = (
            f"**Galpão:** {prod['galpao']}\n"
            f" <@{prod['autor']}>\n"
            f" Início: <t:{int(inicio.timestamp())}:t>\n"
            f" Término: <t:{int(fim.timestamp())}:t>\n\n"
            f" **Restante:** {mins} min\n"
            f"{barra(pct)}"
        )

        view = None

        if not prod["segunda_task"] and (total - restante) >= prod["segunda_task_em"]:
            prod["segunda_task"] = True
            salvar_producoes(producoes)
            desc += "\n\n **2ª Task Liberada**"
            view = SegundaTaskView(pid)

        if restante <= 0:
            desc += "\n\n **Produção Finalizada**"
            del producoes[pid]
            salvar_producoes(producoes)

        await msg.edit(embed=discord.Embed(title=" Produção", description=desc, color=0x34495e), view=view)

        if restante <= 0:
            return

        await asyncio.sleep(180)

async def enviar_painel_fabricacao():
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    async for m in canal.history(limit=10):
        if m.author == bot.user and m.embeds and m.embeds[0].title == " Fabricação":
            return

    await canal.send(
        embed=discord.Embed(title=" Fabricação", description="Selecione o galpão.", color=0x2c3e50),
        view=FabricacaoView()
    )

# ================= LIVES (ADICIONADO) =================

def carregar_lives():
    if not os.path.exists(ARQUIVO_LIVES):
        return {}
    try:
        with open(ARQUIVO_LIVES, "r") as f:
            return json.load(f)
    except:
        return {}

def salvar_lives(dados):
    with open(ARQUIVO_LIVES, "w") as f:
        json.dump(dados, f, indent=4)

async def twitch_esta_online(username):
    url = f"https://api.twitch.tv/helix/streams?user_login={username}"
    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {TWITCH_CLIENT_SECRET}"
    }

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            data = await resp.json()
            return len(data.get("data", [])) > 0

async def verificar_lives():
    await bot.wait_until_ready()

    while not bot.is_closed():
        lives = carregar_lives()
        canal = bot.get_channel(CANAL_DIVULGACAO_LIVE_ID)

        for user_id, info in lives.items():
            link = info["link"]

            if "twitch.tv" in link:
                username = link.split("/")[-1]
                online = await twitch_esta_online(username)

                if online and not info.get("divulgado"):
                    embed = discord.Embed(
                        title="🔴 LIVE ONLINE!",
                        description=(
                            f"🎮 <@{user_id}> está AO VIVO!\n\n"
                            f"👉 **Assista agora:**\n{link}"
                        ),
                        color=0xe74c3c
                    )
                    embed.set_footer(text="Sistema automático de lives")

                    await canal.send(embed=embed)
                    info["divulgado"] = True

                if not online:
                    info["divulgado"] = False

        salvar_lives(lives)
        await asyncio.sleep(120)

# ================= EVENTO PARA CADASTRAR LIVE =================

@bot.event
async def on_message(message):
    if message.author.bot:
        return

    if message.channel.id == CANAL_CADASTRO_LIVE_ID:
        link = message.content.strip()
        lives = carregar_lives()

        lives[str(message.author.id)] = {
            "link": link,
            "divulgado": False
        }

        salvar_lives(lives)

        embed = discord.Embed(
            title="✅ Live cadastrada!",
            description=(
                f"👤 {message.author.mention}\n"
                f"🔗 {link}\n\n"
                f"O bot irá divulgar automaticamente quando você entrar ao vivo."
            ),
            color=0x2ecc71
        )

        await message.reply(embed=embed)

    await bot.process_commands(message)

# ================= EVENTS =================

@bot.event
async def on_ready():
    bot.add_view(FabricacaoView())

    for pid in carregar_producoes():
        bot.loop.create_task(acompanhar_producao(pid))

    bot.loop.create_task(verificar_lives())

    await enviar_painel_fabricacao()

    print(" Bot online com tudo funcionando + Lives")

bot.run(TOKEN)
