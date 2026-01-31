import os
import json
import asyncio
import aiohttp
import discord
from discord.ext import commands, tasks
from datetime import datetime, timedelta
from discord.utils import escape_markdown
from datetime import datetime
from zoneinfo import ZoneInfo

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
ARQUIVO_PEDIDOS = "pedidos.json"

# LIVES
CANAL_CADASTRO_LIVE_ID = 1466464557215256790
CANAL_DIVULGACAO_LIVE_ID = 1243325102917943335
ARQUIVO_LIVES = "lives.json"

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")

# ================= TWITCH API =================

twitch_access_token = None

async def obter_token_twitch():
    global twitch_access_token

    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": TWITCH_CLIENT_ID,
        "client_secret": TWITCH_CLIENT_SECRET,
        "grant_type": "client_credentials"
    }

    async with aiohttp.ClientSession() as session:
        async with session.post(url, params=params) as resp:
            data = await resp.json()
            twitch_access_token = data.get("access_token")
            print("🎮 Token Twitch obtido com sucesso!")

async def checar_se_esta_ao_vivo(canal_twitch):
    global twitch_access_token

    if not twitch_access_token:
        await obter_token_twitch()

    headers = {
        "Client-ID": TWITCH_CLIENT_ID,
        "Authorization": f"Bearer {twitch_access_token}"
    }

    url = f"https://api.twitch.tv/helix/streams?user_login={canal_twitch}"

    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                print("❌ Erro Twitch API:", resp.status)
                return False, None, None, None

            data = await resp.json()
            streams = data.get("data", [])

            if not streams:
                return False, None, None, None

            stream = streams[0]

            titulo = stream.get("title")
            jogo = stream.get("game_name")

            # Thumbnail (trocar {width} e {height})
            thumbnail = stream.get("thumbnail_url")
            if thumbnail:
                thumbnail = thumbnail.replace("{width}", "640").replace("{height}", "360")

            return True, titulo, jogo, thumbnail

GUILD_ID = 1229526644193099880
GUILD = discord.Object(id=GUILD_ID)

bot = commands.Bot(command_prefix="!", intents=intents)

# =========================================================
# ================= REGISTRO ===============================
# =========================================================

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

        await interaction.response.defer()

class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📋 Fazer Registro", style=discord.ButtonStyle.success, custom_id="registro_fazer")
    async def registro(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistroModal())

# =========================================================
# ================= VENDAS ================================
# =========================================================

ORGANIZACOES_CONFIG = {
    "VDR": {"emoji": "🔥", "cor": 0xe74c3c},
    "POLICIA": {"emoji": "🚓", "cor": 0x3498db},
    "EXERCITO": {"emoji": "🪖", "cor": 0x2ecc71},
    "MAFIA": {"emoji": "💀", "cor": 0x8e44ad},
    "CIVIL": {"emoji": "👤", "cor": 0x95a5a6},
}

# ================= CONTROLE DE PEDIDOS =================

def carregar_pedidos():
    if not os.path.exists(ARQUIVO_PEDIDOS):
        return {"ultimo": 0}
    try:
        with open(ARQUIVO_PEDIDOS, "r") as f:
            return json.load(f)
    except:
        return {"ultimo": 0}


def proximo_pedido():
    dados = carregar_pedidos()
    dados["ultimo"] += 1

    with open(ARQUIVO_PEDIDOS, "w") as f:
        json.dump(dados, f, indent=4)

    return dados["ultimo"]

# ================= STATUS DOS BOTÕES =================

class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    def get_status(self, embed):
        for i, field in enumerate(embed.fields):
            if field.name == "📌 Status":
                return i, field.value.split("\n")
        return None, []

    def set_status(self, embed, idx, linhas):
        if not linhas:
            linhas = ["⏳ Pagamento pendente"]

        embed.set_field_at(
            idx,
            name="📌 Status",
            value="\n".join(linhas),
            inline=False
        )
        return embed

    def toggle_linha(self, linhas, prefixo, nova_linha):
        for l in linhas:
            if l.startswith(prefixo):
                linhas.remove(l)
                return linhas  # toggle OFF
                
        linhas.append(nova_linha)  # toggle ON
        return linhas

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="status_pago")
    async def pago(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        agora = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M")
        user = interaction.user.mention

        # Remove pagamento pendente quando paga
        linhas = [l for l in linhas if not l.startswith("⏳")]

        linhas = self.toggle_linha(linhas, "💰", f"💰 Pago • Recebido por {user} • {agora}")

        embed = self.set_status(embed, idx, linhas)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="status_entregue")
    async def entregue(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        agora = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%d/%m/%Y %H:%M")
        user = interaction.user.mention

        # Remove "A entregar" quando entrega
        linhas = [l for l in linhas if not l.startswith("📦")]

        linhas = self.toggle_linha(linhas, "✅", f"✅ Entregue por {user} • {agora}")

        embed = self.set_status(embed, idx, linhas)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

    @discord.ui.button(label="⏳ Pagamento pendente", style=discord.ButtonStyle.danger, custom_id="status_pendente")
    async def pendente(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        linhas = self.toggle_linha(linhas, "⏳", "⏳ Pagamento pendente")

        embed = self.set_status(embed, idx, linhas)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()


# ================= MODAL DE VENDA =================

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
            pt = int(self.qtd_pt.value.strip())
            sub = int(self.qtd_sub.value.strip())
        except ValueError:
            await interaction.response.defer()
            return
        
        numero_pedido = proximo_pedido()

        pacotes_pt = pt // 50
        pacotes_sub = sub // 50
        total = (pt * 50) + (sub * 90)

        valor_formatado = f"{total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        org_nome = self.organizacao.value.strip().upper()
        config = ORGANIZACOES_CONFIG.get(org_nome, {"emoji": "🏷️", "cor": 0x1e3a8a})

        embed = discord.Embed(
    title=f"📦 NOVA ENCOMENDA • Pedido #{numero_pedido:04d}",
    color=config["cor"]
)
        embed.add_field(name="👤 Vendedor", value=interaction.user.mention, inline=False)
        embed.add_field(
            name="🏷 Organização",
            value=f"**{config['emoji']} {org_nome}**",
            inline=False
        )
        embed.add_field(
            name="🔫 PT",
            value=f"{pt} munições\n📦 {pacotes_pt} pacotes",
            inline=True
        )
        embed.add_field(
            name="🔫 SUB",
            value=f"{sub} munições\n📦 {pacotes_sub} pacotes",
            inline=True
        )
        embed.add_field(
            name="💰 Total",
            value=f"**R$ {valor_formatado}**",
            inline=False
        )

        # STATUS INICIAL
        embed.add_field(
            name="📌 Status",
            value="📦 A entregar",
            inline=False
        )

        # OBSERVAÇÕES (AGORA NÃO SOMEM MAIS)
        if self.observacoes.value:
            embed.add_field(
                name="📝 Observações",
                value=self.observacoes.value,
                inline=False
            )

        embed.set_footer(text="🛡 Sistema de Encomendas • VDR 442")

        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())
        await interaction.response.defer()


# ================= BOTÃO PARA ABRIR MODAL =================

class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🧮 Registrar Venda", style=discord.ButtonStyle.primary, custom_id="calculadora_registrar")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(VendaModal())



# =========================================================
# ================= PRODUÇÃO ===============================
# =========================================================

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

    # CORES POR PROGRESSO
    if pct <= 0.35:
        cor = "🟢"
    elif pct <= 0.70:
        cor = "🟡"
    elif pct < 1:
        cor = "🔴"
    else:
        cor = "🔵"

    return cor + " " + ("▓" * cheio) + ("░" * (size - cheio))

class SegundaTaskView(discord.ui.View):
    def __init__(self, pid):
        super().__init__(timeout=None)
        self.pid = pid

    @discord.ui.button(label="✅ Confirmar 2ª Task", style=discord.ButtonStyle.success, custom_id="segunda_task_feita")
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

        # REMOVE BOTÃO APÓS CONFIRMAR
        await interaction.message.edit(view=None)
        await interaction.response.defer()

class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def iniciar(self, interaction, galpao, total_min, segunda_task_faltando_min):
        producoes = carregar_producoes()
        pid = f"{galpao}_{interaction.id}"
        inicio = datetime.utcnow()
        fim = inicio + timedelta(minutes=total_min)

        canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)

        msg = await canal.send(
            embed=discord.Embed(title="🏭 Produção", description="Iniciando...", color=0x3498db)
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

    @discord.ui.button(label="🏭 Galpões Sul", style=discord.ButtonStyle.primary, custom_id="fabricacao_sul")
    async def sul(self, interaction, button):
        await self.iniciar(interaction, "Sul", 130, 80)

    @discord.ui.button(label="🏭 Galpões Norte", style=discord.ButtonStyle.secondary, custom_id="fabricacao_norte")
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
            f"**Iniciado por:** <@{prod['autor']}>\n"
            f"Início: <t:{int(inicio.timestamp())}:t>\n"
            f"Término: <t:{int(fim.timestamp())}:t>\n\n"
            f"⏳ **Restante:** {mins} min\n"
            f"{barra(pct)}"
        )

        view = None

        # 🟡 LIBERA 2ª TASK QUANDO FALTAR 15 MINUTOS
        if not prod["segunda_task"] and mins <= 15:
            prod["segunda_task"] = True
            salvar_producoes(producoes)

            desc += "\n\n🟡 **2ª Task Disponível**"
            view = SegundaTaskView(pid)

        # ✅ MOSTRA QUEM CONFIRMOU A 2ª TASK
        if "segunda_task_confirmada" in prod:
            uid = prod["segunda_task_confirmada"]["user"]
            desc += f"\n\n✅ **2ª Task OK por:** <@{uid}>"

        # 🟢 FINALIZA PRODUÇÃO
        if restante <= 0:
            desc += "\n\n🔵 **Produção Finalizada**"
            del producoes[pid]
            salvar_producoes(producoes)

        await msg.edit(
            embed=discord.Embed(
                title="🏭 Produção",
                description=desc,
                color=0x34495e
            ),
            view=view
        )

        if restante <= 0:
            return

        await asyncio.sleep(60)

async def enviar_painel_fabricacao():
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    async for m in canal.history(limit=10):
        if m.author == bot.user and m.embeds and m.embeds[0].title == "🏭 Fabricação":
            return

    await canal.send(
        embed=discord.Embed(title="🏭 Fabricação", description="Selecione o galpão.", color=0x2c3e50),
        view=FabricacaoView()
    )

# =========================================================
# ================= LIVES ================================
# =========================================================

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

# ================= DIVULGAÇÃO =================

async def divulgar_live(user_id, link, titulo, jogo, thumbnail):
    try:
        canal = await bot.fetch_channel(CANAL_DIVULGACAO_LIVE_ID)
    except Exception as e:
        print("❌ Erro ao buscar canal divulgação:", e)
        return

    user = await bot.fetch_user(int(user_id))

    # 🔒 Escapa markdown para não quebrar _
    link_formatado = escape_markdown(link)

    descricao = (
        f"👤 Streamer: {user.mention}\n"
        f"🎮 Jogo: **{jogo or 'Não informado'}**\n"
        f"📝 Título: **{titulo or 'Sem título'}**\n\n"
        f"🔗 {link_formatado}\n\n"
        f"🚨 Está AO VIVO agora! Corre lá!"
    )

    embed = discord.Embed(
        title="🔴 LIVE AO VIVO NA TWITCH!",
        description=descricao,
        color=0x9146FF
    )

    # 🖼️ THUMBNAIL DA LIVE
    if thumbnail:
        embed.set_image(url=thumbnail)

    await canal.send(content="@everyone", embed=embed)

# ================= LOOP TWITCH =================

@tasks.loop(minutes=2)
async def verificar_lives_twitch():
    print("🔄 Verificando lives na Twitch...")

    lives = carregar_lives()
    alterado = False

    for user_id, data in lives.items():
        link = data.get("link", "")
        divulgado = data.get("divulgado", False)

        # 🔧 LIMPA LINK DA TWITCH (À PROVA DE ERRO)
        canal_twitch = link.lower().strip()
        canal_twitch = canal_twitch.replace("https://", "")
        canal_twitch = canal_twitch.replace("http://", "")
        canal_twitch = canal_twitch.replace("www.", "")
        canal_twitch = canal_twitch.replace("twitch.tv/", "")
        canal_twitch = canal_twitch.split("/")[0].strip()

        if not canal_twitch:
            continue

        ao_vivo, titulo, jogo, thumbnail = await checar_se_esta_ao_vivo(canal_twitch)

        print(f"🎮 {canal_twitch} ao vivo? {ao_vivo}")

        # 🔴 FICOU OFFLINE → RESETAR
        if not ao_vivo and divulgado:
            print(f"🔁 {canal_twitch} ficou OFFLINE. Resetando divulgado.")
            lives[user_id]["divulgado"] = False
            alterado = True

        # 🟢 FICOU AO VIVO → DIVULGAR
        if ao_vivo and not divulgado:
            await divulgar_live(user_id, link, titulo, jogo, thumbnail)
            lives[user_id]["divulgado"] = True
            alterado = True

    if alterado:
        salvar_lives(lives)

# ================= CADASTRO =================

class CadastrarLiveModal(discord.ui.Modal, title="🎥 Cadastrar Live"):
    link = discord.ui.TextInput(
        label="Cole o link da sua live",
        placeholder="https://twitch.tv/seucanal"
    )

    def extrair_canal_twitch(self, link):
        canal = link.lower().strip()
        canal = canal.replace("https://", "")
        canal = canal.replace("http://", "")
        canal = canal.replace("www.", "")
        canal = canal.replace("twitch.tv/", "")
        canal = canal.split("/")[0].strip()
        return canal

    async def on_submit(self, interaction: discord.Interaction):
        lives = carregar_lives()

        novo_link = self.link.value.strip()
        novo_canal = self.extrair_canal_twitch(novo_link)

        if not novo_canal:
            await interaction.response.send_message(
                "❌ Link da Twitch inválido. Envie no formato:\nhttps://twitch.tv/seucanal",
                ephemeral=True
            )
            return

        # 🚫 BLOQUEAR CANAL JÁ CADASTRADO
        for uid, data in lives.items():
            canal_existente = self.extrair_canal_twitch(data.get("link", ""))

            if canal_existente == novo_canal:
                await interaction.response.send_message(
                    f"❌ Esse canal da Twitch já está cadastrado por outro usuário.\n"
                    f"Canal: **{escape_markdown(novo_canal)}**",
                    ephemeral=True
                )
                return

        # ✅ CADASTRAR NORMALMENTE
        lives[str(interaction.user.id)] = {
            "link": novo_link,
            "divulgado": False
        }

        salvar_lives(lives)

        # 🔒 Escapa markdown no embed
        link_formatado = escape_markdown(novo_link)

        embed = discord.Embed(
            title="✅ Live cadastrada!",
            description=(
                f"{interaction.user.mention}\n"
                f"{link_formatado}\n\n"
                f"📡 A live será divulgada automaticamente quando você entrar AO VIVO."
            ),
            color=0x2ecc71
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)

# ================= VIEW + PAINEL =================

class CadastrarLiveView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎥 Cadastrar minha Live", style=discord.ButtonStyle.primary, custom_id="cadastrar_live_btn")
    async def cadastrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(CadastrarLiveModal())

async def enviar_painel_lives():
    canal = bot.get_channel(CANAL_CADASTRO_LIVE_ID)
    if not canal:
        return

    async for m in canal.history(limit=10):
        if m.author == bot.user and m.embeds and m.embeds[0].title == "🎥 Cadastro de Live":
            return

    embed = discord.Embed(
        title="🎥 Cadastro de Live",
        description="Clique no botão para cadastrar sua live.",
        color=0x9146FF
    )

    await canal.send(embed=embed, view=CadastrarLiveView())

# =========================================================
# ================= METAS ================================
# =========================================================

# CANAIS / CATEGORIAS
CANAL_SOLICITAR_SALA_ID = 1337374500366450741

CATEGORIA_META_GERENTE_ID = 1337374002422743122
CATEGORIA_META_RESPONSAVEIS_ID = 1462810826992783422
CATEGORIA_META_SOLDADO_ID = 1461335635519475894
CATEGORIA_META_MEMBRO_ID = 1461335697209163900
CATEGORIA_META_AGREGADO_ID = 1461335748870541323

# CARGOS
CARGO_GERENTE_ID = 1324499473296134154
CARGO_RESP_METAS_ID = 1337407399656423485
CARGO_RESP_ACAO_ID = 1337379517274259509
CARGO_RESP_VENDAS_ID = 1337379530586980352
CARGO_RESP_PRODUCAO_ID = 1337379524949573662
CARGO_SOLDADO_ID = 1422845498863259700
CARGO_MEMBRO_ID = 1422847198789369926
CARGO_AGREGADO_ID = 1422847202937536532

ARQUIVO_METAS = "metas.json"

# ================= UTIL METAS =================

def carregar_metas():
    if not os.path.exists(ARQUIVO_METAS):
        return {}
    try:
        with open(ARQUIVO_METAS, "r") as f:
            return json.load(f)
    except:
        return {}

def salvar_metas(dados):
    with open(ARQUIVO_METAS, "w") as f:
        json.dump(dados, f, indent=4)

def obter_categoria_meta(member: discord.Member):
    roles = [r.id for r in member.roles]

    # PRIORIDADE (MAIS ALTO PRIMEIRO)
    if CARGO_GERENTE_ID in roles:
        return CATEGORIA_META_GERENTE_ID
    if any(r in roles for r in [CARGO_RESP_METAS_ID, CARGO_RESP_ACAO_ID, CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID]):
        return CATEGORIA_META_RESPONSAVEIS_ID
    if CARGO_SOLDADO_ID in roles:
        return CATEGORIA_META_SOLDADO_ID
    if CARGO_MEMBRO_ID in roles:
        return CATEGORIA_META_MEMBRO_ID
    if CARGO_AGREGADO_ID in roles:
        return CATEGORIA_META_AGREGADO_ID

    return None

def tem_meta(member_id):
    metas = carregar_metas()
    return str(member_id) in metas

# ================= BOTÕES =================

class MetaView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📁 Solicitar Sala de Meta", style=discord.ButtonStyle.primary, custom_id="meta_criar")
    async def criar_meta(self, interaction: discord.Interaction, button: discord.ui.Button):
        member = interaction.user
        guild = interaction.guild

        if tem_meta(member.id):
            await interaction.response.send_message("❌ Você já possui uma sala de meta.", ephemeral=True)
            return

        categoria_id = obter_categoria_meta(member)
        if not categoria_id:
            await interaction.response.send_message("❌ Você não possui cargo autorizado para metas.", ephemeral=True)
            return

        categoria = guild.get_channel(categoria_id)

        nome_canal = f"📁・{member.display_name}".lower().replace(" ", "-")

        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            member: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
        }

        gerente_role = guild.get_role(CARGO_GERENTE_ID)
        if gerente_role:
            overwrites[gerente_role] = discord.PermissionOverwrite(
                view_channel=True, send_messages=True, manage_channels=True
            )

        canal = await guild.create_text_channel(
            name=nome_canal,
            category=categoria,
            overwrites=overwrites
        )

        metas = carregar_metas()
        metas[str(member.id)] = {
            "canal_id": canal.id
        }
        salvar_metas(metas)

        cargo_resp_metas = guild.get_role(CARGO_RESP_METAS_ID)

        aviso = await canal.send(
            f"📢 **ALGUNS AVISOS IMPORTANTES SOBRE ESSA SALA** 📢\n\n"
            f"📌 Apenas você {member.mention} e a Gerência tem acesso a essa sala.\n"
            f"📌 Aqui explica tudo como funciona a meta da Fac ⁠📢・faq-meta\n"
            f"📌 Responsável por cuidar de metas: {cargo_resp_metas.mention if cargo_resp_metas else '@RESP | Metas'}",
            view=MetaFecharView(member.id)
        )
        await aviso.pin()

        await interaction.response.send_message("✅ Sua sala de meta foi criada!", ephemeral=True)

class MetaFecharView(discord.ui.View):
    def __init__(self, member_id):
        super().__init__(timeout=None)
        self.member_id = member_id

    @discord.ui.button(label="🧹 Fechar Sala", style=discord.ButtonStyle.danger, custom_id="meta_fechar")
    async def fechar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not any(r.id == CARGO_GERENTE_ID for r in interaction.user.roles):
            await interaction.response.send_message("❌ Apenas Gerentes podem fechar esta sala.", ephemeral=True)
            return

        metas = carregar_metas()
        data = metas.get(str(self.member_id))

        if not data:
            await interaction.response.defer()
            return

        canal = interaction.guild.get_channel(data["canal_id"])
        if canal:
            await canal.delete()

        del metas[str(self.member_id)]
        salvar_metas(metas)

        await interaction.response.send_message("🧹 Sala de meta fechada com sucesso.", ephemeral=True)

# ================= EVENTOS METAS =================

@bot.event
async def on_member_remove(member):
    metas = carregar_metas()
    data = metas.get(str(member.id))

    if data:
        canal = member.guild.get_channel(data["canal_id"])
        if canal:
            try:
                await canal.send("⚠️ Usuário saiu do servidor. Sala aguardando exclusão por um Gerente.")
            except:
                pass

# ================= ATUALIZA META AO TROCAR CARGO =================

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    if before.roles == after.roles:
        return

    metas = carregar_metas()
    data = metas.get(str(after.id))
    if not data:
        return

    nova_categoria_id = obter_categoria_meta(after)
    if not nova_categoria_id:
        return

    canal = after.guild.get_channel(data["canal_id"])
    nova_categoria = after.guild.get_channel(nova_categoria_id)

    if not canal or not nova_categoria:
        return

    if canal.category_id == nova_categoria_id:
        return

    try:
        await canal.edit(category=nova_categoria)
        await canal.send("🔁 Sala movida automaticamente para a categoria correta devido à mudança de cargo.")
    except Exception as e:
        print(f"❌ Erro ao mover sala de meta: {e}")

# ================= PAINEL METAS =================

async def enviar_painel_metas():
    canal = bot.get_channel(CANAL_SOLICITAR_SALA_ID)
    if not canal:
        return

    async for m in canal.history(limit=10):
        if m.author == bot.user and m.embeds and m.embeds[0].title == "📁 Solicitação de Sala de Meta":
            return

    embed = discord.Embed(
        title="📁 Solicitação de Sala de Meta",
        description="Clique no botão abaixo para criar sua sala de meta.",
        color=0xf1c40f
    )

    await canal.send(embed=embed, view=MetaView())

# =========================================================
# ================= EVENTS (FINAL) ========================
# =========================================================

@bot.event
async def on_ready():
    # Views persistentes
    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    bot.add_view(StatusView())
    bot.add_view(FabricacaoView())
    bot.add_view(CadastrarLiveView())
    bot.add_view(MetaView())
    bot.add_view(MetaFecharView(0))  # 🔥 IMPORTANTE

    # 🔴 INICIA VERIFICAÇÃO TWITCH
    if not verificar_lives_twitch.is_running():
        verificar_lives_twitch.start()

    # Restaura produções ativas
    for pid in carregar_producoes():
        bot.loop.create_task(acompanhar_producao(pid))

    # Envia painéis automáticos
    await enviar_painel_fabricacao()
    await enviar_painel_lives()
    await enviar_painel_metas()

    print("✅ Bot online com Registro + Vendas + Produção + Lives + Metas")

# =========================================================
# ================= START BOT =============================
# =========================================================

bot.run(TOKEN)






















