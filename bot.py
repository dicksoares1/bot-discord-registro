# =========================================================
# ====================== BOT COMPLETO =====================
# =========================================================
# 
# ORGANIZADO POR SEÇÕES - TODAS AS FUNÇÕES COMPLETAS
#
# =========================================================

import os
import json
import gc
import re
import asyncio
import aiohttp
import asyncpg
import discord
import tweepy
import time as time_module
from discord.ext import commands, tasks
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# =========================================================
# =================== CONFIGURAÇÕES =======================
# =========================================================

TOKEN = os.environ.get("TOKEN")
TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")

# Twitter
auth = tweepy.OAuth1UserHandler(
    os.environ.get("API_KEY"),
    os.environ.get("API_SECRET"),
    os.environ.get("ACCESS_TOKEN"),
    os.environ.get("ACCESS_SECRET")
)
api = tweepy.API(auth)

# Banco
DATABASE_URL = os.environ.get("DATABASE_URL")

# Tempo
BRASIL = ZoneInfo("America/Sao_Paulo")

# IDs
GUILD_ID = 1229526644193099880
ADM_ID = 467673818375389194

# ================= CANAIS =================
CANAL_REGISTRO_ID = 1229556030397218878
CANAL_LOG_REGISTRO_ID = 1462457604939841851
CANAL_VENDAS_ID = 1460984821458272347
CANAL_ENCOMENDAS_ID = 1460980984811098294
CANAL_FABRICACAO_ID = 1466421612566810634
CANAL_REGISTRO_GALPAO_ID = 1356174712337862819
CANAL_BAU_GALPAO_ID = 1448561598384963747
CANAL_BAU_GALPAO_SUL_ID = 1356174937764794521
CANAL_CALCULO_POLVORA_ID = 1462834441968943157
CANAL_REGISTRO_POLVORA_ID = 1448570795101261846
CANAL_CADASTRO_LIVE_ID = 1466464557215256790
CANAL_DIVULGACAO_LIVE_ID = 1243325102917943335
CANAL_INICIAR_LAVAGEM_ID = 1467152989499293768
CANAL_LAVAGEM_MEMBROS_ID = 1467159346923311216
CANAL_RELATORIO_LAVAGEM_ID = 1467150805273546878
CANAL_SOLICITAR_SALA_ID = 1337374500366450741
RESULTADOS_METAS_ID = 1341403574483288125
CANAL_ESCALACOES_ID = 1241406819545514064
CANAL_RELATORIO_ACOES_ID = 1477308788531921019
CANAL_GERENCIA_ID = 1237393478414241854
CANAL_CLIPES_ID = 1229526645837271134
CANAL_BOTAO_AUSENCIA_ID = 1491427870277374162
CANAL_REGISTRO_AUSENCIA_ID = 1313854772545196032
CANAL_REGISTRAR_COMPRA_ID = 1498668853465448560
CANAL_COMPRAS_REGISTRADAS_ID = 1270467793363669053
CANAL_POSTAGEM_X = 1486353689680547900

# ================= CARGOS =================
AGREGADO_ROLE_ID = 1422847202937536532
CONVIDADO_ROLE_ID = 1337382961456353342
CARGO_01_ID = 1258753233355014144
CARGO_02_ID = 1258753479082512394
CARGO_GERENTE_ID = 1324499473296134154
CARGO_GERENTE_GERAL_ID = 1462804425163935796
CARGO_RESP_METAS_ID = 1337407399656423485
CARGO_RESP_ACAO_ID = 1337379517274259509
CARGO_RESP_VENDAS_ID = 1337379530586980352
CARGO_RESP_PRODUCAO_ID = 1337379524949573662
CARGO_SOLDADO_ID = 1422845498863259700
CARGO_MEMBRO_ID = 1422847198789369926
CARGO_AUSENTE_ID = 1337420032212336823

# ================= CATEGORIAS =================
CATEGORIA_META_GERENTE_ID = 1337374002422743122
CATEGORIA_META_RESPONSAVEIS_ID = 1462810826992783422
CATEGORIA_META_SOLDADO_ID = 1461335635519475894
CATEGORIA_META_MEMBRO_ID = 1461335697209163900
CATEGORIA_META_AGREGADO_ID = 1461335748870541323

# ================= CONFIGURAÇÕES =================
ORGANIZACOES_CONFIG = {
    "VDR": {"emoji": "🔥", "cor": 0xe74c3c},
    "POLICIA": {"emoji": "🚓", "cor": 0x3498db},
    "EXERCITO": {"emoji": "🪖", "cor": 0x2ecc71},
    "MAFIA": {"emoji": "💀", "cor": 0x8e44ad},
    "CIVIL": {"emoji": "👤", "cor": 0x95a5a6},
}

ACOES_SEMANA = {
    "Joalheria": 5,
    "Banco Fleeca": 4,
    "Banco de Paleto": 1,
    "Banco Central": 1,
    "Nióbio": 1,
    "Loja de Armas (Ammunation)": None,
    "Loja de Bebidas": None,
    "Lan House - (Bahamas)": None,
    "Loja de Departamento": None,
    "Mergulhador": None,
    "Grapeseed": None,
    "Companhia de Gás": None,
    "Life Invader": None,
    "Aeroporto de Sucata": None,
    "Carro Forte": None,
    "Banco Bahamas": None,
    "🚁 Helicrash (13h)": None,
    "🚁 Helicrash (15h)": None,
    "🚁 Helicrash (22h)": None,
    "🚁 Helicrash (02h)": None,
}

CARGOS_PERMITIDOS_ESCALACAO = [
    AGREGADO_ROLE_ID, CARGO_MEMBRO_ID, CARGO_SOLDADO_ID,
    CARGO_01_ID, CARGO_02_ID, CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID
]

CARGOS_PERMITIDOS_REMOVER = [
    CARGO_GERENTE_ID, CARGO_01_ID, CARGO_02_ID, CARGO_GERENTE_GERAL_ID
]

PRECO_POLVORA = 80
EMOJI_APROVACAO = "✅"

# =========================================================
# =================== UTILIDADES ==========================
# =========================================================

def agora():
    return datetime.now(BRASIL)

def agora_db():
    return datetime.now(BRASIL).replace(tzinfo=None)

def str_para_datetime(data_str):
    if not data_str:
        return None
    if isinstance(data_str, datetime):
        if data_str.tzinfo is None:
            return data_str.replace(tzinfo=BRASIL)
        return data_str
    dt = datetime.fromisoformat(data_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=BRASIL)
    return dt

def para_db_naive(dt):
    if dt.tzinfo is not None:
        dt = dt.astimezone(BRASIL)
    return dt.replace(tzinfo=None)

def formatar_dinheiro(valor):
    try:
        valor = float(valor)
    except:
        valor = 0
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

def fmt_num(valor):
    return f"{valor:,.0f}".replace(",", ".")

def barra(pct, size=20):
    cheio = int(pct * size)
    if pct <= 0.35:
        cor = "🟢"
    elif pct <= 0.70:
        cor = "🟡"
    elif pct < 1:
        cor = "🔴"
    else:
        cor = "🔵"
    return cor + " " + ("▓" * cheio) + ("░" * (size - cheio))

def tem_cargo(member, cargo_id):
    return member and any(r.id == cargo_id for r in member.roles)

def tem_algum_cargo(member, cargos_ids):
    return member and any(r.id in cargos_ids for r in member.roles)

channel_cache = {}
user_cache = {}

def pegar_canal(bot, cid):
    if cid in channel_cache:
        return channel_cache[cid]
    canal = bot.get_channel(cid)
    if canal:
        channel_cache[cid] = canal
    return canal

async def pegar_usuario(bot, uid):
    if uid in user_cache:
        return user_cache[uid]
    try:
        user = await bot.fetch_user(uid)
        user_cache[uid] = user
        return user
    except:
        return None

# =========================================================
# =================== BANCO DE DADOS ======================
# =========================================================

db = None

async def conectar_db():
    global db
    if not DATABASE_URL:
        print("❌ DATABASE_URL não encontrada!")
        return
    if not db:
        db = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=5,
            command_timeout=60
        )
        print("🟢 Pool PostgreSQL conectado!")
    return db

def get_db():
    return db

# =========================================================
# =================== FUNÇÕES AUXILIARES ==================
# =========================================================

async def responder_interacao(interaction, *, defer=False, ephemeral=False):
    try:
        if interaction.response.is_done():
            return
        if defer:
            await interaction.response.defer(ephemeral=ephemeral)
        else:
            await interaction.response.defer(ephemeral=True)
    except:
        pass

async def enviar_ou_atualizar_painel(nome, canal_id, embed, view):
    canal = bot.get_channel(canal_id)
    if not canal:
        return
    async for msg in canal.history(limit=20):
        if msg.author == bot.user and msg.embeds:
            if msg.embeds[0].title == embed.title:
                try:
                    await msg.edit(embed=embed, view=view)
                    return
                except:
                    pass
    await canal.send(embed=embed, view=view)

# =========================================================
# =================== SISTEMA REGISTRO ====================
# =========================================================

class RegistroModal(discord.ui.Modal, title="📋 Registro de Entrada"):
    nome = discord.ui.TextInput(label="Nome Completo", required=True)
    passaporte = discord.ui.TextInput(label="Passaporte", required=True)
    indicado = discord.ui.TextInput(label="Indicado por", required=False)
    telefone = discord.ui.TextInput(label="Telefone In Game", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.user.edit(nick=f"{self.passaporte.value} - {self.nome.value}")
        view = TipoRegistroView(self.nome.value, self.passaporte.value, self.indicado.value, self.telefone.value)
        await interaction.response.send_message("Selecione o tipo de entrada:", view=view, ephemeral=True)

class TipoRegistroSelect(discord.ui.Select):
    def __init__(self, nome, passaporte, indicado, telefone):
        self.nome, self.passaporte, self.indicado, self.telefone = nome, passaporte, indicado, telefone
        options = [
            discord.SelectOption(label="Agregado", description="Membro da fac", emoji="🕴️"),
            discord.SelectOption(label="Amigos", description="Visita", emoji="🤝")
        ]
        super().__init__(placeholder="Escolha o tipo", options=options)

    async def callback(self, interaction: discord.Interaction):
        guild, member = interaction.guild, interaction.user
        agregado = guild.get_role(AGREGADO_ROLE_ID)
        amigos = guild.get_role(1309121290241704046)
        convidado = guild.get_role(CONVIDADO_ROLE_ID)
        
        if self.values[0] == "Agregado" and agregado:
            await member.add_roles(agregado)
        elif self.values[0] == "Amigos" and amigos:
            await member.add_roles(amigos)
        if convidado:
            await member.remove_roles(convidado)
        
        canal_log = guild.get_channel(CANAL_LOG_REGISTRO_ID)
        if canal_log:
            embed = discord.Embed(title="📋 Novo Registro", color=0x2ecc71)
            embed.add_field(name="Nome", value=self.nome)
            embed.add_field(name="Passaporte", value=self.passaporte)
            embed.add_field(name="Indicado", value=self.indicado or "Ninguém")
            embed.add_field(name="Telefone", value=self.telefone)
            embed.add_field(name="Tipo", value=self.values[0])
            embed.add_field(name="Usuário", value=member.mention)
            await canal_log.send(embed=embed)
        
        await interaction.response.send_message(f"✅ Registro concluído como **{self.values[0]}**", ephemeral=True)

class TipoRegistroView(discord.ui.View):
    def __init__(self, nome, passaporte, indicado, telefone):
        super().__init__(timeout=300)
        self.add_item(TipoRegistroSelect(nome, passaporte, indicado, telefone))

class RegistroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📋 Fazer Registro", style=discord.ButtonStyle.success, custom_id="registro_fazer")
    async def registro(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(RegistroModal())

# =========================================================
# =================== SISTEMA VENDAS ======================
# =========================================================

async def proximo_pedido():
    db = get_db()
    async with db.acquire() as conn:
        row = await conn.fetchrow("SELECT ultimo FROM pedidos WHERE id=1")
        if not row:
            await conn.execute("INSERT INTO pedidos (id, ultimo) VALUES (1, 1)")
            return 1
        novo = row["ultimo"] + 1
        await conn.execute("UPDATE pedidos SET ultimo=$1 WHERE id=1", novo)
        return novo

async def salvar_venda_db(vendedor_id, valor, pedido_numero):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO vendas (user_id, valor, data, pedido_numero) VALUES ($1, $2, $3, $4)",
            vendedor_id, valor, agora().strftime("%d/%m/%Y"), pedido_numero
        )

async def atualizar_valor_venda_db(pedido_numero, valor):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute("UPDATE vendas SET valor = $1 WHERE pedido_numero = $2", valor, pedido_numero)

async def carregar_vendas_db():
    db = get_db()
    async with db.acquire() as conn:
        return await conn.fetch("SELECT * FROM vendas")

class StatusView(discord.ui.View):
    def __init__(self, disabled: bool = False):
        super().__init__(timeout=None)
        if disabled:
            for item in self.children:
                item.disabled = True

    def get_status(self, embed):
        for i, field in enumerate(embed.fields):
            if field.name == "📌 Status":
                return i, field.value.split("\n")
        return None, []

    def set_status(self, embed, idx, linhas):
        if not linhas:
            linhas = ["📦 A entregar"]
        embed.set_field_at(idx, name="📌 Status", value="\n".join(linhas), inline=False)
        return embed

    def pedido_pago(self, linhas):
        return any(l.startswith("💰") for l in linhas)

    def pedido_cancelado(self, linhas):
        return any(l.startswith("❌") for l in linhas)

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="status_pago")
    async def pago(self, interaction: discord.Interaction, button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)
        if self.pedido_cancelado(linhas) or self.pedido_pago(linhas):
            await interaction.response.send_message("⚠️ Este pedido não pode ser pago.", ephemeral=True)
            return
        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        user = interaction.user.mention
        linhas = [l for l in linhas if not l.startswith("⏳") and not l.startswith("💰")]
        linhas.append(f"💰 Pago • Recebido por {user} • {agora_str}")
        embed = self.set_status(embed, idx, linhas)
        finalizado = any(l.startswith("💰") for l in linhas) and any(l.startswith("✅") for l in linhas)
        if finalizado:
            embed.color = 0x2ecc71
            embed.title = "🎉 VENDA CONCLUÍDA"
            await interaction.message.edit(embed=embed, view=StatusView(disabled=True))
        else:
            await interaction.message.edit(embed=embed, view=self)
        await responder_interacao(interaction, defer=True)

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="status_entregue")
    async def entregue(self, interaction: discord.Interaction, button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)
        if self.pedido_cancelado(linhas):
            await interaction.response.send_message("⚠️ Este pedido foi cancelado.", ephemeral=True)
            return
        # Verifica estoque
        pacotes_pt = 0
        pacotes_sub = 0
        for field in embed.fields:
            if field.name == "🔫 PT":
                try:
                    pacotes_pt = int(field.value.split("📦")[1].replace("pacotes", "").strip())
                except:
                    pass
            if field.name == "🔫 SUB":
                try:
                    pacotes_sub = int(field.value.split("📦")[1].replace("pacotes", "").strip())
                except:
                    pass
        if pacotes_pt > 0 and not await verificar_estoque_suficiente("PT", pacotes_pt):
            estoque = await carregar_estoque()
            await interaction.response.send_message(
                f"❌ **ESTOQUE INSUFICIENTE!**\n🔫 PT: {pacotes_pt} pacotes necessários\n📦 Estoque atual: {estoque['PT']} pacotes",
                ephemeral=True
            )
            return
        if pacotes_sub > 0 and not await verificar_estoque_suficiente("SUB", pacotes_sub):
            estoque = await carregar_estoque()
            await interaction.response.send_message(
                f"❌ **ESTOQUE INSUFICIENTE!**\n🔫 SUB: {pacotes_sub} pacotes necessários\n📦 Estoque atual: {estoque['SUB']} pacotes",
                ephemeral=True
            )
            return
        # Registra saída
        titulo = embed.title
        pedido_numero = int(titulo.split("#")[1]) if "#" in titulo else 0
        if pacotes_pt > 0:
            await registrar_saida_estoque(pedido_numero, "PT", pacotes_pt, interaction.user.id)
        if pacotes_sub > 0:
            await registrar_saida_estoque(pedido_numero, "SUB", pacotes_sub, interaction.user.id)
        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        linhas = [l for l in linhas if not l.startswith("📦") and not l.startswith("✅")]
        linhas.append(f"✅ Entregue por {interaction.user.mention} • {agora_str}")
        embed = self.set_status(embed, idx, linhas)
        finalizado = any(l.startswith("💰") for l in linhas) and any(l.startswith("✅") for l in linhas)
        if finalizado:
            embed.color = 0x2ecc71
            embed.title = "🎉 VENDA CONCLUÍDA"
            await interaction.message.edit(embed=embed, view=StatusView(disabled=True))
        else:
            await interaction.message.edit(embed=embed, view=self)
        await responder_interacao(interaction, defer=True)

    @discord.ui.button(label="❌ Pedido cancelado", style=discord.ButtonStyle.danger, custom_id="status_cancelado")
    async def cancelado(self, interaction: discord.Interaction, button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)
        if self.pedido_pago(linhas):
            await interaction.response.send_message("⚠️ Este pedido já foi pago.", ephemeral=True)
            return
        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        linhas = [f"❌ Pedido cancelado por {interaction.user.mention} • {agora_str}"]
        embed = self.set_status(embed, idx, linhas)
        await interaction.message.edit(embed=embed, view=StatusView(disabled=True))
        await responder_interacao(interaction, defer=True)

class VendaModal(discord.ui.Modal, title="🧮 Registro de Venda"):
    organizacao = discord.ui.TextInput(label="Organização", required=True)
    qtd_pt = discord.ui.TextInput(label="Quantidade PT", required=True)
    qtd_sub = discord.ui.TextInput(label="Quantidade SUB", required=True)
    observacoes = discord.ui.TextInput(label="Observ", style=discord.TextStyle.paragraph, required=False)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            pt = int(self.qtd_pt.value.strip())
            sub = int(self.qtd_sub.value.strip())
        except:
            await interaction.response.send_message("Valores inválidos.", ephemeral=True)
            return
        numero_pedido = await proximo_pedido()
        pacotes_pt = pt // 50
        pacotes_sub = sub // 50
        total = (pt * 50) + (sub * 90)
        await salvar_venda_db(str(interaction.user.id), total, numero_pedido)
        valor_formatado = f"{total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        org_nome = self.organizacao.value.strip().upper()
        config = ORGANIZACOES_CONFIG.get(org_nome, {"emoji": "🏷️", "cor": 0x1e3a8a})
        embed = discord.Embed(title=f"📦 NOVA ENCOMENDA • Pedido #{numero_pedido:04d}", color=config["cor"])
        embed.add_field(name="👤 Vendedor", value=interaction.user.mention, inline=False)
        embed.add_field(name="🏷 Organização", value=f"{config['emoji']} {org_nome}", inline=False)
        embed.add_field(name="🔫 PT", value=f"{pt} munições\n📦 {pacotes_pt} pacotes", inline=True)
        embed.add_field(name="🔫 SUB", value=f"{sub} munições\n📦 {pacotes_sub} pacotes", inline=True)
        embed.add_field(name="💰 Total", value=f"**R$ {valor_formatado}**", inline=False)
        embed.add_field(name="📌 Status", value="📦 A Entregar\n⏳ Pagamento pendente", inline=False)
        if self.observacoes.value:
            embed.add_field(name="📝 Observ", value=self.observacoes.value, inline=False)
        embed.set_footer(text="🛡 Sistema de Encomendas • VDR 442")
        canal = interaction.guild.get_channel(CANAL_ENCOMENDAS_ID)
        await canal.send(embed=embed, view=StatusView())
        await responder_interacao(interaction, defer=True)

class RelatorioModal(discord.ui.Modal, title="📊 Relatório de Vendas"):
    data_inicio = discord.ui.TextInput(label="Data inicial", placeholder="01/03/2026", required=True)
    data_fim = discord.ui.TextInput(label="Data final", placeholder="17/03/2026", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            inicio = datetime.strptime(self.data_inicio.value, "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value, "%d/%m/%Y") + timedelta(days=1)
        except:
            await interaction.followup.send("Formato inválido. Use **DD/MM/AAAA**", ephemeral=True)
            return
        db = get_db()
        async with db.acquire() as conn:
            rows = await conn.fetch(
                "SELECT user_id, SUM(valor) as total FROM vendas WHERE TO_DATE(data, 'DD/MM/YYYY') BETWEEN $1 AND $2 GROUP BY user_id",
                inicio, fim
            )
        if not rows:
            await interaction.followup.send("Nenhuma venda no período.", ephemeral=True)
            return
        total = 0
        linhas = []
        for r in rows:
            valor = r["total"]
            total += valor
            linhas.append(f"👤 <@{r['user_id']}> • R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", "."))
        total_fmt = f"{total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        embed = discord.Embed(title="📊 Relatório de Vendas", color=0x2ecc71)
        embed.add_field(name="💰 Total Vendido", value=f"R$ {total_fmt}", inline=False)
        embed.add_field(name="👥 Por vendedor", value="\n".join(linhas), inline=False)
        canal = interaction.guild.get_channel(1365372467723501723)
        if canal:
            await canal.send(embed=embed)
        await interaction.followup.send("Relatório enviado no canal de vendas.", ephemeral=True)

class CalculadoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Registrar Venda", style=discord.ButtonStyle.primary, custom_id="calc_registrar_venda")
    async def registrar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(VendaModal())

    @discord.ui.button(label="Relatório", style=discord.ButtonStyle.success, custom_id="calc_relatorio_vendas")
    async def relatorio(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(RelatorioModal())

# =========================================================
# =================== SISTEMA PRODUÇÃO ====================
# =========================================================

producoes_tasks = {}
galpoes_ativos = set()

# Estoques
async def carregar_estoque():
    db = get_db()
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT tipo, quantidade FROM estoque_municoes")
    estoque = {"PT": 0, "SUB": 0}
    for row in rows:
        estoque[row["tipo"]] = row["quantidade"]
    return estoque

async def carregar_estoque_insumos():
    db = get_db()
    async with db.acquire() as conn:
        capsulas = await conn.fetchval("SELECT quantidade FROM estoque_capsulas WHERE id = 1") or 0
        embalagens = await conn.fetchval("SELECT quantidade FROM estoque_embalagens WHERE id = 1") or 0
    return {"capsulas": capsulas, "embalagens": embalagens}

async def atualizar_estoque(tipo, quantidade, operacao="adicionar"):
    db = get_db()
    async with db.acquire() as conn:
        if operacao == "adicionar":
            await conn.execute(
                "UPDATE estoque_municoes SET quantidade = quantidade + $1, ultima_atualizacao = NOW() WHERE tipo = $2",
                quantidade, tipo
            )
        else:
            await conn.execute(
                "UPDATE estoque_municoes SET quantidade = quantidade - $1, ultima_atualizacao = NOW() WHERE tipo = $2 AND quantidade >= $1",
                quantidade, tipo
            )

async def atualizar_estoque_capsulas(quantidade, operacao="adicionar"):
    db = get_db()
    async with db.acquire() as conn:
        if operacao == "adicionar":
            await conn.execute("UPDATE estoque_capsulas SET quantidade = quantidade + $1, ultima_atualizacao = NOW() WHERE id = 1", quantidade)
        else:
            await conn.execute("UPDATE estoque_capsulas SET quantidade = quantidade - $1, ultima_atualizacao = NOW() WHERE id = 1 AND quantidade >= $1", quantidade)

async def atualizar_estoque_embalagens(quantidade, operacao="adicionar"):
    db = get_db()
    async with db.acquire() as conn:
        if operacao == "adicionar":
            await conn.execute("UPDATE estoque_embalagens SET quantidade = quantidade + $1, ultima_atualizacao = NOW() WHERE id = 1", quantidade)
        else:
            await conn.execute("UPDATE estoque_embalagens SET quantidade = quantidade - $1, ultima_atualizacao = NOW() WHERE id = 1 AND quantidade >= $1", quantidade)

async def registrar_entrada_insumos(tipo, quantidade, registrado_por, obs=""):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO entrada_insumos (tipo, quantidade, registrado_por, obs) VALUES ($1, $2, $3, $4)",
            tipo, quantidade, str(registrado_por), obs
        )
        if tipo == "capsulas":
            await atualizar_estoque_capsulas(quantidade, "adicionar")
        elif tipo == "embalagens":
            await atualizar_estoque_embalagens(quantidade, "adicionar")

async def verificar_insumos_producao(tipo, pacotes):
    estoque = await carregar_estoque_insumos()
    if tipo == "PT":
        capsulas_necessarias = pacotes * 25
        embalagens_necessarias = pacotes * 5
    else:
        capsulas_necessarias = pacotes * 65
        embalagens_necessarias = pacotes * 25
    return {
        "suficiente": estoque["capsulas"] >= capsulas_necessarias and estoque["embalagens"] >= embalagens_necessarias,
        "capsulas_necessarias": capsulas_necessarias,
        "embalagens_necessarias": embalagens_necessarias,
        "capsulas_disponiveis": estoque["capsulas"],
        "embalagens_disponiveis": estoque["embalagens"]
    }

async def consumir_insumos_producao(tipo, pacotes):
    if tipo == "PT":
        capsulas_consumir = pacotes * 25
        embalagens_consumir = pacotes * 5
    else:
        capsulas_consumir = pacotes * 65
        embalagens_consumir = pacotes * 25
    await atualizar_estoque_capsulas(capsulas_consumir, "remover")
    await atualizar_estoque_embalagens(embalagens_consumir, "remover")
    return capsulas_consumir, embalagens_consumir

async def registrar_producao_municao(tipo, pacotes, produzido_por, obs=""):
    municoes = pacotes * 50
    capsulas_consumidas, embalagens_consumidas = await consumir_insumos_producao(tipo, pacotes)
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO producao_municao (tipo, pacotes, municoes, produzido_por, obs, capsulas_consumidas, embalagens_consumidas) VALUES ($1, $2, $3, $4, $5, $6, $7)",
            tipo, pacotes, municoes, str(produzido_por), obs, capsulas_consumidas, embalagens_consumidas
        )
        await atualizar_estoque(tipo, pacotes, "adicionar")

async def registrar_saida_estoque(pedido_numero, tipo, pacotes, retirado_por):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO saida_estoque (pedido_numero, tipo, pacotes, retirado_por, data) VALUES ($1, $2, $3, $4, NOW())",
            pedido_numero, tipo, pacotes, str(retirado_por)
        )
        await atualizar_estoque(tipo, pacotes, "remover")

async def verificar_estoque_suficiente(tipo, pacotes_necessarios):
    estoque = await carregar_estoque()
    return estoque.get(tipo, 0) >= pacotes_necessarios

# Produção
async def carregar_producao(pid):
    db = get_db()
    async with db.acquire() as conn:
        r = await conn.fetchrow("SELECT * FROM producoes WHERE pid=$1", pid)
    if not r:
        return None
    dados = {
        "galpao": r["galpao"],
        "autor": int(r["autor"]),
        "inicio": r["inicio"],
        "fim": r["fim"],
        "obs": r["obs"],
        "msg_id": int(r["msg_id"]),
        "canal_id": int(r["canal_id"]),
        "polvora": r["polvora"] or 400,
        "qtd_galpoes": r.get("qtd_galpoes", 1) if r.get("qtd_galpoes") else 1,
        "polvora_por_galpao": r.get("polvora_por_galpao", 400) if r.get("polvora_por_galpao") else 400
    }
    if r["segunda_task_user"]:
        dados["segunda_task_confirmada"] = {
            "user": int(r["segunda_task_user"]),
            "time": r["segunda_task_time"]
        }
    return dados

async def salvar_producao(pid, dados):
    db = get_db()
    segunda_user = None
    segunda_time = None
    qtd_galpoes = dados.get("qtd_galpoes", 1)
    polvora_por_galpao = dados.get("polvora_por_galpao", dados.get("polvora", 400) // qtd_galpoes if qtd_galpoes > 0 else 400)
    if "segunda_task_confirmada" in dados:
        segunda_user = str(dados["segunda_task_confirmada"]["user"])
        segunda_time = dados["segunda_task_confirmada"]["time"]
    inicio = para_db_naive(dados["inicio"]) if isinstance(dados["inicio"], datetime) else dados["inicio"]
    fim = para_db_naive(dados["fim"]) if isinstance(dados["fim"], datetime) else dados["fim"]
    async with db.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO producoes (pid, galpao, autor, inicio, fim, obs, msg_id, canal_id, segunda_task_user, segunda_task_time, polvora, qtd_galpoes, polvora_por_galpao)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
            ON CONFLICT (pid) DO UPDATE SET
            galpao=$2, autor=$3, inicio=$4, fim=$5, obs=$6, msg_id=$7, canal_id=$8,
            segunda_task_user=$9, segunda_task_time=$10, polvora=$11, qtd_galpoes=$12, polvora_por_galpao=$13
            """,
            pid, dados["galpao"], str(dados["autor"]), inicio, fim, dados["obs"],
            str(dados["msg_id"]), str(dados["canal_id"]), segunda_user, segunda_time,
            dados.get("polvora", 400), qtd_galpoes, polvora_por_galpao
        )

async def deletar_producao(pid):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM producoes WHERE pid=$1", pid)

class SegundaTaskView(discord.ui.View):
    def __init__(self, pid):
        super().__init__(timeout=None)
        self.pid = pid

    @discord.ui.button(label="✅ Confirmar 2ª Task", style=discord.ButtonStyle.success, custom_id="segunda_task_btn")
    async def confirmar(self, interaction: discord.Interaction, button):
        await interaction.response.defer(ephemeral=True)
        prod = await carregar_producao(self.pid)
        if not prod:
            await interaction.followup.send("❌ Produção não encontrada!", ephemeral=True)
            return
        if prod.get("segunda_task_confirmada"):
            await interaction.followup.send("⚠️ A segunda task já foi confirmada!", ephemeral=True)
            return
        fim = prod["fim"]
        if isinstance(fim, str):
            fim = str_para_datetime(fim)
        if agora() >= fim:
            await interaction.followup.send("⚠️ Esta produção já terminou!", ephemeral=True)
            return
        prod["segunda_task_confirmada"] = {"user": interaction.user.id, "time": agora().isoformat()}
        await salvar_producao(self.pid, prod)
        await interaction.message.edit(view=None)
        await interaction.followup.send("✅ Segunda task confirmada com sucesso!", ephemeral=True)

class ProducaoModal(discord.ui.Modal, title="🏭 Iniciar Produção"):
    qtd_galpoes = discord.ui.TextInput(label="📊 Quantos galpões?", placeholder="1, 2 ou 3", required=True, max_length=1)
    polvora_por_galpao = discord.ui.TextInput(label="💣 Pólvora por galpão", placeholder="400", required=True)
    obs = discord.ui.TextInput(label="📝 Observação", style=discord.TextStyle.paragraph, required=False)

    def __init__(self, galpao, tempo_base):
        super().__init__()
        self.galpao = galpao
        self.tempo_base = tempo_base

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            qtd = int(self.qtd_galpoes.value)
            if qtd not in [1, 2, 3]:
                raise ValueError
        except:
            await interaction.followup.send("❌ Quantidade inválida! Digite 1, 2 ou 3.", ephemeral=True)
            return
        try:
            polvora_por_galpao = int(self.polvora_por_galpao.value)
            if polvora_por_galpao <= 0:
                raise ValueError
        except:
            await interaction.followup.send("❌ Pólvora inválida!", ephemeral=True)
            return
        polvora_total = polvora_por_galpao * qtd
        tempo_real = max(1, int(self.tempo_base * (polvora_por_galpao / 400)))
        pid = f"{self.galpao}_{qtd}g_{interaction.id}_{int(time_module.time())}"
        inicio = agora()
        fim = inicio + timedelta(minutes=tempo_real)
        canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)
        if not canal:
            await interaction.followup.send("❌ Canal não encontrado.", ephemeral=True)
            return
        desc = f"**Galpão:** {self.galpao}\n**Quantidade:** {qtd}\n**Iniciado por:** {interaction.user.mention}\n"
        if self.obs.value:
            desc += f"📝 **Obs:** {self.obs.value}\n"
        desc += f"**Pólvora por galpão:** {polvora_por_galpao}\n**Pólvora total:** {polvora_total}\nInício: <t:{int(inicio.timestamp())}:t>\nTérmino: <t:{int(fim.timestamp())}:t>\n\n⏳ **Restante:** {tempo_real} min\n{barra(0)}"
        msg = await canal.send(embed=discord.Embed(title=f"🏭 Produção - {qtd} Galpão(ões)", description=desc, color=0x3498db), view=SegundaTaskView(pid))
        dados = {
            "galpao": f"{self.galpao} ({qtd} galpões)",
            "autor": interaction.user.id,
            "inicio": inicio,
            "fim": fim,
            "obs": self.obs.value,
            "polvora": polvora_total,
            "qtd_galpoes": qtd,
            "polvora_por_galpao": polvora_por_galpao,
            "msg_id": msg.id,
            "canal_id": CANAL_REGISTRO_GALPAO_ID
        }
        await salvar_producao(pid, dados)
        if pid not in producoes_tasks:
            task = asyncio.create_task(acompanhar_producao(pid))
            producoes_tasks[pid] = task
        await interaction.followup.send(
            f"✅ **Produção iniciada!**\n🏭 {self.galpao}\n📊 {qtd} galpão(ões)\n💣 Pólvora: {fmt_num(polvora_total)}\n⏰ Término: <t:{int(fim.timestamp())}:t>",
            ephemeral=True
        )

class RegistrarCapsulasModal(discord.ui.Modal, title="📦 Registrar Cápsulas"):
    quantidade = discord.ui.TextInput(label="Quantidade", placeholder="1000", required=True)
    observacao = discord.ui.TextInput(label="Observação", style=discord.TextStyle.paragraph, required=False)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            quantidade = int(self.quantidade.value.replace(".", "").replace(",", ""))
        except:
            await interaction.followup.send("❌ Quantidade inválida!", ephemeral=True)
            return
        await registrar_entrada_insumos("capsulas", quantidade, interaction.user.id, self.observacao.value)
        estoque = await carregar_estoque_insumos()
        await interaction.followup.send(f"✅ {fmt_num(quantidade)} cápsulas adicionadas! Estoque: {fmt_num(estoque['capsulas'])}", ephemeral=True)
        await atualizar_painel_fabricacao()

class RegistrarEmbalagensModal(discord.ui.Modal, title="📦 Registrar Embalagens"):
    quantidade = discord.ui.TextInput(label="Quantidade", placeholder="500", required=True)
    observacao = discord.ui.TextInput(label="Observação", style=discord.TextStyle.paragraph, required=False)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            quantidade = int(self.quantidade.value.replace(".", "").replace(",", ""))
        except:
            await interaction.followup.send("❌ Quantidade inválida!", ephemeral=True)
            return
        await registrar_entrada_insumos("embalagens", quantidade, interaction.user.id, self.observacao.value)
        estoque = await carregar_estoque_insumos()
        await interaction.followup.send(f"✅ {fmt_num(quantidade)} embalagens adicionadas! Estoque: {fmt_num(estoque['embalagens'])}", ephemeral=True)
        await atualizar_painel_fabricacao()

class ProducaoMunicaoModal(discord.ui.Modal, title="🎯 Produzir Munição"):
    tipo_municao = discord.ui.TextInput(label="Tipo", placeholder="PT ou SUB", required=True, max_length=3)
    quantidade_pacotes = discord.ui.TextInput(label="Pacotes", placeholder="100", required=True)
    observacao = discord.ui.TextInput(label="Observação", style=discord.TextStyle.paragraph, required=False)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        tipo = self.tipo_municao.value.strip().upper()
        if tipo not in ["PT", "SUB"]:
            await interaction.followup.send("❌ Tipo inválido! Use PT ou SUB.", ephemeral=True)
            return
        try:
            pacotes = int(self.quantidade_pacotes.value.replace(".", "").replace(",", ""))
        except:
            await interaction.followup.send("❌ Quantidade inválida!", ephemeral=True)
            return
        verificacao = await verificar_insumos_producao(tipo, pacotes)
        if not verificacao["suficiente"]:
            await interaction.followup.send(
                f"❌ **INSUMOS INSUFICIENTES!**\nPrecisa de {fmt_num(verificacao['capsulas_necessarias'])} cápsulas e {fmt_num(verificacao['embalagens_necessarias'])} embalagens",
                ephemeral=True
            )
            return
        await registrar_producao_municao(tipo, pacotes, interaction.user.id, self.observacao.value)
        estoque = await carregar_estoque()
        await interaction.followup.send(f"✅ {fmt_num(pacotes)} pacotes de {tipo} produzidos! Estoque PT: {fmt_num(estoque['PT'])}, SUB: {fmt_num(estoque['SUB'])}", ephemeral=True)
        await atualizar_painel_fabricacao()

class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🏭 Galpões Norte", style=discord.ButtonStyle.primary, custom_id="fabricacao_norte")
    async def norte(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(ProducaoModal("GALPÕES NORTE", 65))

    @discord.ui.button(label="🏭 Galpões Sul", style=discord.ButtonStyle.secondary, custom_id="fabricacao_sul")
    async def sul(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(ProducaoModal("GALPÕES SUL", 130))

    @discord.ui.button(label="💊 Registrar Cápsulas", style=discord.ButtonStyle.primary, custom_id="registrar_capsulas")
    async def registrar_capsulas(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(RegistrarCapsulasModal())

    @discord.ui.button(label="📦 Registrar Embalagens", style=discord.ButtonStyle.primary, custom_id="registrar_embalagens")
    async def registrar_embalagens(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(RegistrarEmbalagensModal())

    @discord.ui.button(label="🔫 Produzir Munição", style=discord.ButtonStyle.success, custom_id="fabricacao_municao")
    async def produzir_municao(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(ProducaoMunicaoModal())

    @discord.ui.button(label="📊 Estoque", style=discord.ButtonStyle.secondary, custom_id="ver_estoque_completo")
    async def ver_estoque(self, interaction: discord.Interaction, button):
        await interaction.response.defer(ephemeral=True)
        estoque = await carregar_estoque()
        insumos = await carregar_estoque_insumos()
        embed = discord.Embed(title="📊 ESTOQUE", color=0x3498db)
        embed.add_field(name="🔫 MUNIÇÕES", value=f"PT: {fmt_num(estoque['PT'])} pacotes\nSUB: {fmt_num(estoque['SUB'])} pacotes", inline=False)
        embed.add_field(name="💊 INSUMOS", value=f"Cápsulas: {fmt_num(insumos['capsulas'])}\nEmbalagens: {fmt_num(insumos['embalagens'])}", inline=False)
        await interaction.followup.send(embed=embed, ephemeral=True)

async def acompanhar_producao(pid):
    print(f"▶ Produção iniciada: {pid}")
    msg = None
    while not bot.is_closed():
        prod = await carregar_producao(pid)
        if not prod:
            print(f"❌ Produção {pid} não encontrada")
            return
        canal = bot.get_channel(prod["canal_id"])
        if not canal:
            await asyncio.sleep(10)
            continue
        if msg is None:
            try:
                msg = await canal.fetch_message(prod["msg_id"])
            except:
                await asyncio.sleep(5)
                continue
        inicio = str_para_datetime(prod["inicio"])
        fim = str_para_datetime(prod["fim"])
        agora_dt = agora()
        if agora_dt >= fim:
            await finalizar_producao(pid, msg, prod)
            return
        total = (fim - inicio).total_seconds()
        restante = (fim - agora_dt).total_seconds()
        restante = max(0, restante)
        if total <= 0:
            total = 1
        pct = 1 - (restante / total)
        pct = max(0, min(1, pct))
        mins = int(restante // 60)
        segundos = int(restante % 60)
        desc = f"**Galpão:** {prod['galpao']}\n**Iniciado por:** <@{prod['autor']}>\n"
        if prod.get("obs"):
            desc += f"📝 **Obs:** {prod['obs']}\n"
        desc += f"Início: <t:{int(inicio.timestamp())}:t>\nTérmino: <t:{int(fim.timestamp())}:t>\n\n⏳ **Restante:** {mins}m {segundos}s\n{barra(pct)}"
        if prod.get("segunda_task_confirmada"):
            uid = prod["segunda_task_confirmada"]["user"]
            desc += f"\n\n✅ **Segunda task concluída por:** <@{uid}>"
        try:
            await msg.edit(embed=discord.Embed(title="🏭 Produção", description=desc, color=0x34495e))
        except:
            pass
        await asyncio.sleep(5)

async def finalizar_producao(pid, msg, prod):
    print(f"🔵 FINALIZANDO produção {pid}")
    polvora_total = prod.get("polvora", 400)
    segunda = prod.get("segunda_task_confirmada")
    galpao = prod["galpao"]
    qtd_galpoes = prod.get("qtd_galpoes", 1)
    polvora_por_galpao = prod.get("polvora_por_galpao", polvora_total // qtd_galpoes if qtd_galpoes > 0 else polvora_total)
    if "NORTE" in galpao.upper():
        base_por_galpao = 1777 if segunda else 1688
    elif "SUL" in galpao.upper():
        base_por_galpao = 1618 if segunda else 1608
    else:
        base_por_galpao = 1777 if segunda else 1688
    capsulas_por_galpao = (base_por_galpao * polvora_por_galpao) // 400
    capsulas_total = capsulas_por_galpao * qtd_galpoes
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO producoes_finalizadas (user_id, capsulas, data, polvora, galpao) VALUES ($1, $2, $3, $4, $5)",
            str(prod["autor"]), capsulas_total, agora_db(), polvora_total, f"{galpao} ({qtd_galpoes} galpões)"
        )
        await conn.execute("UPDATE estoque_capsulas SET quantidade = quantidade + $1, ultima_atualizacao = NOW() WHERE id = 1", capsulas_total)
    if msg:
        try:
            desc = msg.embeds[0].description if msg.embeds else ""
            linhas = [l for l in desc.split("\n") if not l.startswith("⏳") and not "▓" in l and not "░" in l and l.strip()]
            desc = "\n".join(linhas)
            desc += f"\n\n🔵 **Produção Finalizada**\n🧪 Produziu **{fmt_num(capsulas_total)}** cápsulas\n📦 Por galpão: {fmt_num(capsulas_por_galpao)} cápsulas\n🏭 {qtd_galpoes} galpão(ões)\n💣 Pólvora: {polvora_total}\n💊 Cápsulas adicionadas ao estoque!"
            await msg.edit(embed=discord.Embed(title="🏭 Produção", description=desc, color=0x34495e), view=None)
        except:
            pass
    await deletar_producao(pid)
    if pid in producoes_tasks:
        del producoes_tasks[pid]
    await atualizar_painel_fabricacao()
    print(f"✅ Produção {pid} finalizada com {capsulas_total} cápsulas")

@tasks.loop(minutes=1)
async def verificar_producoes_orfas():
    try:
        db = get_db()
        if not db:
            return
        async with db.acquire() as conn:
            rows = await conn.fetch("SELECT pid FROM producoes WHERE CAST(fim AS timestamp) > NOW()")
        for r in rows:
            pid = r["pid"]
            if pid not in producoes_tasks:
                print(f"🔧 Produção órfã: {pid}, restaurando...")
                task = bot.loop.create_task(acompanhar_producao(pid))
                producoes_tasks[pid] = task
    except Exception as e:
        print(f"Erro verificar produções: {e}")

async def enviar_painel_fabricacao():
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    if not canal:
        return
    estoque = await carregar_estoque()
    insumos = await carregar_estoque_insumos()
    embed = discord.Embed(title="🏭 PAINEL DE FABRICAÇÃO", color=0x2ecc71)
    embed.add_field(name="📦 MUNIÇÃO", value=f"PT: {fmt_num(estoque['PT'])} pacotes\nSUB: {fmt_num(estoque['SUB'])} pacotes", inline=False)
    embed.add_field(name="💊 INSUMOS", value=f"Cápsulas: {fmt_num(insumos['capsulas'])}\nEmbalagens: {fmt_num(insumos['embalagens'])}", inline=False)
    async for msg in canal.history(limit=10):
        if msg.author == bot.user and msg.embeds and msg.embeds[0].title == "🏭 PAINEL DE FABRICAÇÃO":
            try:
                await msg.delete()
            except:
                pass
    await canal.send(embed=embed, view=FabricacaoView())

async def atualizar_painel_fabricacao():
    await enviar_painel_fabricacao()

# =========================================================
# =================== SISTEMA PÓLVORA =====================
# =========================================================

async def salvar_polvora_db(user_id, vendedor, qtd, valor):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO polvoras (user_id, vendedor, quantidade, valor, data) VALUES ($1,$2,$3,$4,$5)",
            str(user_id), vendedor, qtd, valor, agora().isoformat()
        )

async def carregar_polvoras_db():
    db = get_db()
    async with db.acquire() as conn:
        return await conn.fetch("SELECT * FROM polvoras")

class PolvoraModal(discord.ui.Modal, title="🧨 Registro de Pólvora"):
    vendedor = discord.ui.TextInput(label="Vendedor", placeholder="Nome do vendedor", required=True)
    quantidade = discord.ui.TextInput(label="Quantidade", placeholder="100", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value)
        except:
            await interaction.response.send_message("Quantidade inválida.", ephemeral=True)
            return
        valor = qtd * 80
        valor_formatado = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        await salvar_polvora_db(interaction.user.id, self.vendedor.value, qtd, valor)
        canal = interaction.guild.get_channel(CANAL_REGISTRO_POLVORA_ID)
        embed = discord.Embed(title="🧨 Registro de Pólvora", color=0xe67e22)
        embed.add_field(name="Vendedor", value=self.vendedor.value, inline=False)
        embed.add_field(name="Comprado por", value=interaction.user.mention, inline=False)
        embed.add_field(name="Quantidade", value=str(qtd), inline=True)
        embed.add_field(name="Valor", value=f"**R$ {valor_formatado}**", inline=True)
        await canal.send(embed=embed)
        await interaction.response.send_message("Registro feito com sucesso!", ephemeral=True)

class PolvoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Registrar Compra", style=discord.ButtonStyle.primary, custom_id="polvora_btn")
    async def registrar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(PolvoraModal())

# =========================================================
# =================== SISTEMA LAVAGEM =====================
# =========================================================

lavagens_pendentes = {}

async def salvar_lavagem_db(user_id, valor_sujo, taxa, valor_retorno):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO lavagens (user_id, valor, taxa, liquido, data) VALUES ($1,$2,$3,$4,$5)",
            str(user_id), valor_sujo, taxa, valor_retorno, agora().replace(tzinfo=None)
        )

async def carregar_lavagens_db():
    db = get_db()
    async with db.acquire() as conn:
        return await conn.fetch("SELECT * FROM lavagens")

async def limpar_lavagens_db():
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM lavagens")

def pode_gerenciar_lavagem(member):
    return any(r.id in [CARGO_GERENTE_ID, CARGO_01_ID, CARGO_02_ID, CARGO_GERENTE_GERAL_ID] for r in member.roles)

class LavagemModal(discord.ui.Modal, title="🧼 Iniciar Lavagem"):
    valor = discord.ui.TextInput(label="Valor do dinheiro sujo", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await responder_interacao(interaction, defer=True)
        try:
            valor_sujo = int(self.valor.value.replace(".", "").replace(",", ""))
        except:
            await interaction.followup.send("Valor inválido.", ephemeral=True)
            return
        taxa = 20
        valor_retorno = int(valor_sujo * 0.8)
        msg_info = await interaction.channel.send(f"{interaction.user.mention} envie agora o PRINT da tela.")
        lavagens_pendentes[interaction.user.id] = {
            "sujo": valor_sujo, "retorno": valor_retorno, "taxa": taxa, "msg_info": msg_info
        }

class LavagemView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Iniciar Lavagem", style=discord.ButtonStyle.primary, custom_id="lavagem_iniciar")
    async def iniciar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(LavagemModal())

    @discord.ui.button(label="🧹 Limpar Sala", style=discord.ButtonStyle.danger, custom_id="lavagem_limpar")
    async def limpar(self, interaction: discord.Interaction, button):
        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
        canal = interaction.guild.get_channel(CANAL_LAVAGEM_MEMBROS_ID)
        async for msg in canal.history(limit=200):
            try:
                await msg.delete()
            except:
                pass
        await limpar_lavagens_db()
        await interaction.response.send_message("Sala limpa!", ephemeral=True)

    @discord.ui.button(label="📊 Relatório", style=discord.ButtonStyle.success, custom_id="lavagem_relatorio")
    async def relatorio(self, interaction: discord.Interaction, button):
        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message("Sem permissão.", ephemeral=True)
            return
        dados = await carregar_lavagens_db()
        canal = interaction.guild.get_channel(CANAL_RELATORIO_LAVAGEM_ID)
        for item in dados:
            user = await bot.fetch_user(int(item["user_id"]))
            await canal.send(f"{user.mention} - {formatar_dinheiro(item['liquido'])}")
        await interaction.response.send_message("Relatório enviado!", ephemeral=True)

async def handle_lavagem_message(message):
    if message.channel.id == CANAL_INICIAR_LAVAGEM_ID and message.attachments and message.author.id in lavagens_pendentes:
        dados_temp = lavagens_pendentes.pop(message.author.id)
        canal_destino = bot.get_channel(CANAL_LAVAGEM_MEMBROS_ID)
        arquivo = await message.attachments[0].to_file()
        try:
            await message.delete()
        except:
            pass
        try:
            await dados_temp["msg_info"].delete()
        except:
            pass
        await salvar_lavagem_db(message.author.id, dados_temp["sujo"], dados_temp["taxa"], dados_temp["retorno"])
        embed = discord.Embed(title="🧼 Nova Lavagem", color=0x1abc9c)
        embed.add_field(name="Membro", value=message.author.mention, inline=False)
        embed.add_field(name="Valor sujo", value=formatar_dinheiro(dados_temp["sujo"]), inline=True)
        embed.add_field(name="Valor a repassar", value=formatar_dinheiro(dados_temp["retorno"]), inline=True)
        embed.set_image(url=f"attachment://{arquivo.filename}")
        await canal_destino.send(embed=embed, file=arquivo)
        return True
    return False

# =========================================================
# =================== SISTEMA LIVES =======================
# =========================================================

twitch_token = None
twitch_token_expira = 0

async def obter_token_twitch():
    global twitch_token, twitch_token_expira
    agora_ts = time_module.time()
    if twitch_token and agora_ts < twitch_token_expira:
        return twitch_token
    url = "https://id.twitch.tv/oauth2/token"
    params = {"client_id": TWITCH_CLIENT_ID, "client_secret": TWITCH_CLIENT_SECRET, "grant_type": "client_credentials"}
    async with aiohttp.ClientSession() as session:
        async with session.post(url, params=params) as r:
            data = await r.json()
            if "access_token" not in data:
                return None
            twitch_token = data["access_token"]
            twitch_token_expira = agora_ts + data["expires_in"] - 100
            return twitch_token

def detectar_plataforma(link):
    link = link.lower()
    if "twitch.tv" in link:
        return "twitch"
    if "kick.com" in link:
        return "kick"
    if "tiktok.com" in link:
        return "tiktok"
    return None

def extrair_canal(link):
    link = link.lower().strip().replace("https://", "").replace("http://", "").replace("www.", "").split("?")[0].rstrip("/")
    partes = link.split("/")
    if "twitch.tv" in link:
        return partes[1] if len(partes) > 1 else None
    if "kick.com" in link:
        if len(partes) > 1:
            canal = partes[1]
            if canal == "live" and len(partes) > 2:
                return partes[2]
            return canal
        return None
    if "tiktok.com" in link:
        return partes[1].replace("@", "") if len(partes) > 1 else None
    return None

async def checar_twitch(canal):
    try:
        token = await obter_token_twitch()
        headers = {"Client-ID": TWITCH_CLIENT_ID, "Authorization": f"Bearer {token}"}
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://api.twitch.tv/helix/streams?user_login={canal}", headers=headers) as r:
                data = await r.json()
                if data.get("data"):
                    info = data["data"][0]
                    thumbnail = info["thumbnail_url"].replace("{width}", "1280").replace("{height}", "720")
                    return True, info.get("title"), info.get("game_name"), thumbnail
        return False, None, None, None
    except:
        return False, None, None, None

async def checar_kick(canal):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://kick.com/api/v2/channels/{canal}") as r:
                if r.status == 200:
                    data = await r.json()
                    if data.get("livestream"):
                        livestream = data["livestream"]
                        titulo = livestream.get("session_title", f"Live na Kick - {canal}")
                        return True, titulo, None, None
        return False, None, None, None
    except:
        return False, None, None, None

async def checar_tiktok(username):
    try:
        username = username.lower().replace("@", "").strip()
        headers = {"User-Agent": "Mozilla/5.0"}
        async with aiohttp.ClientSession() as session:
            async with session.get(f"https://www.tiktok.com/@{username}", headers=headers) as r:
                html = await r.text()
        if re.search(r'"isLive":\s*true', html, re.IGNORECASE):
            return True, f"Live no TikTok - @{username}", "TikTok", None
        return False, None, None, None
    except:
        return False, None, None, None

async def carregar_lives():
    db = get_db()
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM lives")
    lives = {}
    for r in rows:
        uid = r["user_id"]
        if uid not in lives:
            lives[uid] = []
        lives[uid].append({"link": r["link"], "divulgado": r["divulgado"]})
    return lives

async def salvar_live(user_id, link):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute("INSERT INTO lives (user_id, link, divulgado) VALUES ($1, $2, false)", str(user_id), link)

async def atualizar_divulgado(link, valor):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute("UPDATE lives SET divulgado=$1 WHERE link=$2", valor, link)

async def remover_live_db(user_id):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM lives WHERE user_id=$1", str(user_id))

async def divulgar_live(user_id, link, titulo, jogo, thumbnail):
    try:
        canal = bot.get_channel(CANAL_DIVULGACAO_LIVE_ID)
        if not canal:
            return False
        user = await pegar_usuario(bot, int(user_id))
        if not user:
            return False
        plataforma = detectar_plataforma(link)
        cores = {"twitch": 0x9146FF, "kick": 0x53FC18, "tiktok": 0x000000}
        nomes = {"twitch": "Twitch", "kick": "Kick", "tiktok": "TikTok"}
        icones = {"twitch": "🟣", "kick": "🟢", "tiktok": "📱"}
        embed = discord.Embed(title=f"{icones.get(plataforma, '🔴')} LIVE AO VIVO!", color=cores.get(plataforma, 0xff0000))
        embed.description = f"👤 **Streamer:** {user.mention}\n📺 **Plataforma:** {nomes.get(plataforma, plataforma.upper())}\n"
        if jogo and jogo != "TikTok" and jogo != "None":
            embed.description += f"🎮 **Jogo:** {jogo}\n"
        embed.description += f"📝 **Título:** {titulo or 'Sem título'}\n\n🔗 **Assistir:** {link}"
        if thumbnail and thumbnail != "None":
            embed.set_image(url=thumbnail)
        await canal.send(content="@everyone 🔴 **LIVE INICIADA!**", embed=embed, allowed_mentions=discord.AllowedMentions(everyone=True))
        return True
    except Exception as e:
        print(f"Erro divulgar live: {e}")
        return False

@tasks.loop(minutes=2)
async def verificar_lives():
    try:
        lives = await carregar_lives()
        if not lives:
            return
        for user_id, lista_lives in lives.items():
            for data in lista_lives:
                link = data.get("link", "")
                divulgado = data.get("divulgado", False)
                if not link:
                    continue
                plataforma = detectar_plataforma(link)
                canal_name = extrair_canal(link)
                if not plataforma or not canal_name:
                    continue
                ao_vivo = False
                titulo = jogo = thumbnail = None
                if plataforma == "twitch":
                    ao_vivo, titulo, jogo, thumbnail = await checar_twitch(canal_name)
                elif plataforma == "kick":
                    ao_vivo, titulo, jogo, thumbnail = await checar_kick(canal_name)
                elif plataforma == "tiktok":
                    ao_vivo, titulo, jogo, thumbnail = await checar_tiktok(canal_name)
                if not ao_vivo and divulgado:
                    await atualizar_divulgado(link, False)
                if ao_vivo and not divulgado:
                    if await divulgar_live(user_id, link, titulo, jogo, thumbnail):
                        await atualizar_divulgado(link, True)
    except Exception as e:
        print(f"Erro verificar lives: {e}")

class CadastrarLiveModal(discord.ui.Modal, title="🎥 Cadastrar Live"):
    link = discord.ui.TextInput(label="Link da sua live", placeholder="https://kick.com/seucanal", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        novo_link = self.link.value.strip().lower().split("?")[0].rstrip("/")
        plataforma = detectar_plataforma(novo_link)
        novo_canal = extrair_canal(novo_link)
        if not plataforma or not novo_canal:
            await interaction.response.send_message("❌ Link inválido.", ephemeral=True)
            return
        lives = await carregar_lives()
        for uid, lista_lives in lives.items():
            if str(uid) != str(interaction.user.id):
                continue
            for live in lista_lives:
                if extrair_canal(live["link"]) == novo_canal and detectar_plataforma(live["link"]) == plataforma:
                    await interaction.response.send_message(f"❌ Você já cadastrou {novo_canal} na {plataforma}!", ephemeral=True)
                    return
        await salvar_live(interaction.user.id, novo_link)
        await interaction.response.send_message(f"✅ Live cadastrada! {plataforma.upper()} - {novo_link}", ephemeral=True)

class CadastrarLiveView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎥 Cadastrar minha Live", style=discord.ButtonStyle.primary, custom_id="cadastrar_live_btn")
    async def cadastrar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(CadastrarLiveModal())

# =========================================================
# =================== SISTEMA METAS =======================
# =========================================================

metas_cache = {}

async def carregar_metas_cache():
    global metas_cache
    db = get_db()
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM metas")
    metas_cache = {str(r["user_id"]): {"canal_id": int(r["canal_id"]), "dinheiro": r["dinheiro"], "polvora": r["polvora"], "acao": r["acao"]} for r in rows}

async def salvar_meta(user_id, canal_id, dinheiro, polvora, acao):
    metas_cache[str(user_id)] = {"canal_id": canal_id, "dinheiro": dinheiro, "polvora": polvora, "acao": acao}
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO metas (user_id, canal_id, dinheiro, polvora, acao) VALUES ($1,$2,$3,$4,$5) ON CONFLICT (user_id) DO UPDATE SET canal_id=$2, dinheiro=$3, polvora=$4, acao=$5",
            str(user_id), str(canal_id), dinheiro, polvora, acao
        )

def obter_categoria_meta(member):
    roles = [r.id for r in member.roles]
    if CARGO_GERENTE_ID in roles:
        return CATEGORIA_META_GERENTE_ID
    if any(r in roles for r in [CARGO_RESP_METAS_ID, CARGO_RESP_ACAO_ID, CARGO_RESP_VENDAS_ID, CARGO_RESP_PRODUCAO_ID]):
        return CATEGORIA_META_RESPONSAVEIS_ID
    if CARGO_SOLDADO_ID in roles:
        return CATEGORIA_META_SOLDADO_ID
    if CARGO_MEMBRO_ID in roles:
        return CATEGORIA_META_MEMBRO_ID
    if AGREGADO_ROLE_ID in roles:
        return CATEGORIA_META_AGREGADO_ID
    return None

async def criar_sala_meta(member):
    guild = member.guild
    nome_canal = f"📁-{member.display_name}".lower()
    for canal in guild.text_channels:
        if canal.name == nome_canal:
            return canal
    canal_encontrado = None
    for cat in guild.categories:
        if not cat.name.lower().startswith("metas"):
            continue
        for c in cat.channels:
            if isinstance(c, discord.TextChannel) and c.overwrites_for(member).view_channel:
                canal_encontrado = c
                break
        if canal_encontrado:
            break
    if canal_encontrado:
        await salvar_meta(member.id, canal_encontrado.id, 0, 0, 0)
        return canal_encontrado
    categoria_id = obter_categoria_meta(member)
    if not categoria_id:
        return
    categoria = guild.get_channel(categoria_id)
    if not categoria:
        return
    nick = member.display_name.lower().replace(" ", "-")
    nome_canal = f"📁・{nick}"
    overwrites = {guild.default_role: discord.PermissionOverwrite(view_channel=False), member: discord.PermissionOverwrite(view_channel=True, send_messages=True)}
    gerente = guild.get_role(CARGO_GERENTE_ID)
    if gerente:
        overwrites[gerente] = discord.PermissionOverwrite(view_channel=True)
    canal = await guild.create_text_channel(nome_canal, category=categoria, overwrites=overwrites)
    await salvar_meta(member.id, canal.id, 0, 0, 0)
    return canal

class SolicitarSalaView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="➕ Criar Minha Sala", style=discord.ButtonStyle.success, custom_id="criar_sala_manual")
    async def criar(self, interaction: discord.Interaction, button):
        if str(interaction.user.id) in metas_cache:
            await interaction.response.send_message("Você já possui uma sala.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        await criar_sala_meta(interaction.user)
        await interaction.followup.send("Sua sala foi criada com sucesso!", ephemeral=True)

# =========================================================
# =================== SISTEMA AÇÕES =======================
# =========================================================

async def gerar_embed_acoes():
    db = get_db()
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT tipo, COUNT(*) as qtd FROM acoes_semana WHERE status = 'concluida' AND (resultado = 'ganhou' OR resultado = 'perdeu' OR resultado = 'concluida') GROUP BY tipo")
    feitas = {r["tipo"]: r["qtd"] for r in rows}
    linhas = []
    total_feitas = 0
    total_meta = 0
    for nome, limite in ACOES_SEMANA.items():
        qtd = feitas.get(nome, 0)
        total_feitas += qtd
        if limite is None:
            linhas.append(f"• {nome}: {qtd}")
        else:
            restante = max(limite - qtd, 0)
            linhas.append(f"• {nome}: {qtd}/{limite} (restam {restante})" if qtd < limite else f"• {nome}: ✅ {qtd}/{limite} (COMPLETO)")
            total_meta += limite
    if total_meta > 0:
        porcentagem = int((total_feitas / total_meta) * 100)
        barra_progresso = "▓" * (porcentagem // 5) + "░" * (20 - (porcentagem // 5))
        status_texto = f"📊 Progresso: {porcentagem}% {barra_progresso}\n\n"
    else:
        status_texto = ""
    embed = discord.Embed(title="📊 AÇÕES DA SEMANA", color=0x2ecc71)
    embed.add_field(name="📌 AÇÕES REALIZADAS", value=status_texto + "\n".join(linhas), inline=False)
    embed.add_field(name="📊 TOTAL", value=f"{total_feitas}/{total_meta} ações realizadas" if total_meta > 0 else f"{total_feitas} ações realizadas", inline=False)
    embed.set_footer(text=f"Atualizado em {agora().strftime('%d/%m/%Y %H:%M')}")
    return embed

class FecharButton(discord.ui.Button):
    def __init__(self):
        super().__init__(label="❌ Fechar", style=discord.ButtonStyle.danger)
    async def callback(self, interaction: discord.Interaction):
        await interaction.message.delete()

class SelecionarAcaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=60)
        options = []
        for nome, limite in ACOES_SEMANA.items():
            desc = f"Limite: {limite}/semana" if limite else "Ilimitado"
            options.append(discord.SelectOption(label=nome, description=desc))
        select = discord.ui.Select(placeholder="Escolha a ação", options=options)
        select.callback = self.select_callback
        self.add_item(select)
        self.add_item(FecharButton())

    async def select_callback(self, interaction: discord.Interaction):
        acao_tipo = interaction.data["values"][0]
        await interaction.response.defer(ephemeral=True)
        limite = ACOES_SEMANA.get(acao_tipo)
        if limite and limite is not None:
            db = get_db()
            async with db.acquire() as conn:
                qtd = await conn.fetchval("SELECT COUNT(*) FROM acoes_semana WHERE tipo=$1 AND status='concluida' AND (resultado='ganhou' OR resultado='perdeu')", acao_tipo)
                if qtd >= limite:
                    await interaction.followup.send(f"❌ Ação **{acao_tipo}** já atingiu o limite de {limite}!", ephemeral=True)
                    return
        db = get_db()
        async with db.acquire() as conn:
            acao_id = await conn.fetchval("INSERT INTO acoes_semana (tipo, data, autor, status) VALUES ($1, $2, $3, 'aberta') RETURNING id", acao_tipo, agora_db(), str(interaction.user.id))
        cor = 0xe67e22 if "Helicrash" in acao_tipo else 0x3498db
        embed = discord.Embed(title=f"{'🚁' if 'Helicrash' in acao_tipo else '🎯'} {acao_tipo}", color=cor)
        embed.description = "**Clique no botão ✅ PARTICIPAR para se inscrever!**"
        embed.add_field(name="👥 Participantes (0)", value="Nenhum participante ainda.", inline=False)
        embed.add_field(name="👤 Criado por", value=interaction.user.mention, inline=True)
        embed.add_field(name="📅 Data", value=agora().strftime('%d/%m/%Y %H:%M'), inline=True)
        embed.set_footer(text=f"ID: {acao_id}")
        canal = interaction.guild.get_channel(CANAL_ESCALACOES_ID)
        await canal.send(embed=embed, view=AcaoCheckinView(acao_id, interaction.user.id))
        await interaction.followup.send(f"✅ Ação **{acao_tipo}** criada!", ephemeral=True)

class AcaoCheckinView(discord.ui.View):
    def __init__(self, acao_id, criador_id):
        super().__init__(timeout=None)
        self.acao_id = acao_id
        self.criador_id = criador_id

    @discord.ui.button(label="✅ Participar", style=discord.ButtonStyle.success, custom_id="acao_participar")
    async def participar(self, interaction: discord.Interaction, button):
        if not tem_algum_cargo(interaction.user, CARGOS_PERMITIDOS_ESCALACAO):
            await interaction.response.send_message("❌ Sem permissão!", ephemeral=True)
            return
        db = get_db()
        async with db.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.response.send_message("❌ Ação já concluída!", ephemeral=True)
                return
            ja_participa = await conn.fetchval("SELECT 1 FROM participantes_acoes WHERE acao_id=$1 AND user_id=$2", self.acao_id, str(interaction.user.id))
            if ja_participa:
                await interaction.response.send_message("⚠️ Já está participando!", ephemeral=True)
                return
            await conn.execute("INSERT INTO participantes_acoes (acao_id, user_id) VALUES ($1, $2)", self.acao_id, str(interaction.user.id))
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            acao = await conn.fetchrow("SELECT tipo FROM acoes_semana WHERE id=$1", self.acao_id)
        embed = interaction.message.embeds[0]
        for i, field in enumerate(embed.fields):
            if field.name.startswith("👥 Participantes"):
                lista = "\n".join([f"<@{p['user_id']}>" for p in participantes]) or "Nenhum participante ainda."
                embed.set_field_at(i, name=f"👥 Participantes ({len(participantes)})", value=lista, inline=False)
                break
        await interaction.message.edit(embed=embed)
        await interaction.response.send_message(f"✅ Inscrito na ação!", ephemeral=True)

    @discord.ui.button(label="📤 Concluir", style=discord.ButtonStyle.primary, custom_id="acao_concluir")
    async def concluir(self, interaction: discord.Interaction, button):
        is_criador = interaction.user.id == self.criador_id
        is_gerente = tem_algum_cargo(interaction.user, [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID])
        if not is_criador and not is_gerente:
            await interaction.response.send_message("❌ Sem permissão!", ephemeral=True)
            return
        db = get_db()
        async with db.acquire() as conn:
            status = await conn.fetchval("SELECT status FROM acoes_semana WHERE id=$1", self.acao_id)
            if status != "aberta":
                await interaction.response.send_message("❌ Já concluída!", ephemeral=True)
                return
            acao = await conn.fetchrow("SELECT tipo, autor FROM acoes_semana WHERE id=$1", self.acao_id)
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
            if not participantes:
                await interaction.response.send_message("⚠️ Nenhum participante!", ephemeral=True)
                return
            await conn.execute("UPDATE acoes_semana SET status='concluida' WHERE id=$1", self.acao_id)
        lista = "\n".join([f"<@{p['user_id']}>" for p in participantes])
        embed = discord.Embed(title="🚨 RELATÓRIO DE AÇÃO", color=0xe74c3c)
        embed.add_field(name="🏦 Ação", value=acao["tipo"], inline=False)
        embed.add_field(name="👥 Participantes", value=lista, inline=False)
        embed.add_field(name="🎯 Resultado", value="⏳ Aguardando...", inline=False)
        canal = interaction.guild.get_channel(CANAL_RELATORIO_ACOES_ID)
        msg = await canal.send(embed=embed)
        await msg.edit(view=ResultadoAcaoView(self.acao_id, msg))
        await interaction.message.delete()
        await interaction.response.send_message(f"✅ Escalação concluída!", ephemeral=True)

class ResultadoAcaoView(discord.ui.View):
    def __init__(self, acao_id, mensagem):
        super().__init__(timeout=None)
        self.acao_id = acao_id
        self.mensagem = mensagem

    @discord.ui.button(label="🏆 Ganhou", style=discord.ButtonStyle.success)
    async def ganhou(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(ResultadoGanhouModal(self.acao_id, self.mensagem))

    @discord.ui.button(label="💀 Perdeu", style=discord.ButtonStyle.danger)
    async def perdeu(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(ResultadoPerdeuModal(self.acao_id, self.mensagem))

class ResultadoGanhouModal(discord.ui.Modal, title="🎉 GANHOU"):
    dinheiro = discord.ui.TextInput(label="Valor total", placeholder="50000", required=True)

    def __init__(self, acao_id, mensagem):
        super().__init__()
        self.acao_id = acao_id
        self.mensagem = mensagem

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            valor_total = int(self.dinheiro.value.replace(".", "").replace(",", ""))
        except:
            await interaction.followup.send("❌ Valor inválido!", ephemeral=True)
            return
        db = get_db()
        async with db.acquire() as conn:
            acao = await conn.fetchrow("SELECT tipo FROM acoes_semana WHERE id=$1", self.acao_id)
            await conn.execute("UPDATE acoes_semana SET valor=$1, resultado='ganhou' WHERE id=$2", valor_total, self.acao_id)
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
        ids = [str(p["user_id"]) for p in participantes]
        qtd = len(ids)
        if qtd == 0:
            await interaction.followup.send("⚠️ Nenhum participante!", ephemeral=True)
            return
        valor_por_pessoa = valor_total // qtd
        resto = valor_total % qtd
        for uid in ids:
            await depositar_na_meta(int(uid), valor_por_pessoa, f"Ação: {acao['tipo']}")
        if resto > 0:
            await depositar_na_meta(int(ids[0]), resto, f"Ação: {acao['tipo']} (Restante)")
        lista = "\n".join([f"<@{uid}>" for uid in ids])
        embed = discord.Embed(title="🎉 RESULTADO - GANHOU!", color=0x2ecc71)
        embed.add_field(name="🎯 Ação", value=acao["tipo"], inline=False)
        embed.add_field(name="💰 Total", value=formatar_dinheiro(valor_total), inline=False)
        embed.add_field(name="👥 Participantes", value=lista, inline=False)
        embed.add_field(name="💸 Valor por pessoa", value=formatar_dinheiro(valor_por_pessoa), inline=True)
        await self.mensagem.edit(embed=embed, view=None)
        await interaction.followup.send("✅ Depósitos realizados!", ephemeral=True)

class ResultadoPerdeuModal(discord.ui.Modal, title="💀 PERDEU"):
    confirmacao = discord.ui.TextInput(label="Digite CONFIRMAR", required=True)

    def __init__(self, acao_id, mensagem):
        super().__init__()
        self.acao_id = acao_id
        self.mensagem = mensagem

    async def on_submit(self, interaction: discord.Interaction):
        if self.confirmacao.value.strip().upper() != "CONFIRMAR":
            await interaction.response.send_message("❌ Confirmação incorreta!", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True)
        db = get_db()
        async with db.acquire() as conn:
            acao = await conn.fetchrow("SELECT tipo FROM acoes_semana WHERE id=$1", self.acao_id)
            await conn.execute("UPDATE acoes_semana SET valor=0, resultado='perdeu' WHERE id=$1", self.acao_id)
            participantes = await conn.fetch("SELECT user_id FROM participantes_acoes WHERE acao_id=$1", self.acao_id)
        lista = "\n".join([f"<@{p['user_id']}>" for p in participantes]) or "Ninguém"
        embed = discord.Embed(title="💀 RESULTADO - PERDEU!", color=0xe74c3c)
        embed.add_field(name="🎯 Ação", value=acao["tipo"], inline=False)
        embed.add_field(name="👥 Participantes", value=lista, inline=False)
        embed.add_field(name="💰 Total", value="R$ 0,00", inline=True)
        await self.mensagem.edit(embed=embed, view=None)
        await interaction.followup.send("✅ Registrado como PERDEU!", ephemeral=True)

class PainelAcoesView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🎯 Criar Nova Ação", style=discord.ButtonStyle.success, custom_id="criar_acao")
    async def criar(self, interaction: discord.Interaction, button):
        await interaction.response.defer(ephemeral=True)
        await interaction.followup.send("**Selecione o tipo:**", view=SelecionarAcaoView(), ephemeral=True)

async def enviar_painel_acoes(guild):
    canal = guild.get_channel(CANAL_ESCALACOES_ID)
    if not canal:
        return
    embed = await gerar_embed_acoes()
    await enviar_ou_atualizar_painel("painel_acoes", CANAL_ESCALACOES_ID, embed, PainelAcoesView())

# =========================================================
# =================== SISTEMA AUSÊNCIA ====================
# =========================================================

async def salvar_ausencia_db(user_id, nome, motivo, data_inicio, data_fim):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO ausencias (user_id, nome, motivo, data_inicio, data_fim, ativo) VALUES ($1, $2, $3, $4, $5, true)",
            str(user_id), nome, motivo, data_inicio, data_fim
        )

async def buscar_ausencias_ativas():
    db = get_db()
    async with db.acquire() as conn:
        return await conn.fetch("SELECT * FROM ausencias WHERE ativo = true AND data_fim > NOW() ORDER BY data_fim ASC")

async def buscar_ausencia_por_user(user_id):
    db = get_db()
    async with db.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM ausencias WHERE user_id = $1 AND ativo = true AND data_fim > NOW()", str(user_id))

async def desativar_ausencia(user_id):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute("UPDATE ausencias SET ativo = false WHERE user_id = $1 AND ativo = true", str(user_id))

async def remover_ausencias_expiradas():
    db = get_db()
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM ausencias WHERE ativo = true AND data_fim <= NOW()")
        for row in rows:
            await conn.execute("UPDATE ausencias SET ativo = false WHERE user_id = $1", row["user_id"])
        return [row["user_id"] for row in rows]

class AusenciaModal(discord.ui.Modal, title="📝 Solicitar Ausência"):
    nome = discord.ui.TextInput(label="Nome completo", required=True)
    data_inicio = discord.ui.TextInput(label="Data INÍCIO (DD/MM/AAAA)", placeholder="10/04/2026", required=True)
    data_fim = discord.ui.TextInput(label="Data RETORNO (DD/MM/AAAA)", placeholder="15/04/2026", required=True)
    motivo = discord.ui.TextInput(label="Motivo", style=discord.TextStyle.paragraph, required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            data_inicio_dt = datetime.strptime(self.data_inicio.value.strip(), "%d/%m/%Y")
            data_fim_dt = datetime.strptime(self.data_fim.value.strip(), "%d/%m/%Y")
            data_inicio_naive = data_inicio_dt.replace(hour=0, minute=0, second=0)
            data_fim_naive = data_fim_dt.replace(hour=23, minute=59, second=59)
        except:
            await interaction.followup.send("❌ Formato de data inválido! Use DD/MM/AAAA", ephemeral=True)
            return
        if data_fim_naive <= data_inicio_naive:
            await interaction.followup.send("❌ Data de retorno deve ser depois da data de início!", ephemeral=True)
            return
        if await buscar_ausencia_por_user(interaction.user.id):
            await interaction.followup.send("❌ Você já possui uma ausência ativa!", ephemeral=True)
            return
        dias_ausencia = (data_fim_naive - data_inicio_naive).days + 1
        if dias_ausencia >= 15:
            canal_gerencia = interaction.guild.get_channel(CANAL_GERENCIA_ID)
            if canal_gerencia:
                embed = discord.Embed(title="⚠️ AUSÊNCIA PROLONGADA", description=f"{interaction.user.mention} - {dias_ausencia} dias!", color=0xe74c3c)
                embed.add_field(name="Nome", value=self.nome.value, inline=True)
                embed.add_field(name="Período", value=f"{self.data_inicio.value} a {self.data_fim.value}", inline=True)
                embed.add_field(name="Motivo", value=self.motivo.value[:100], inline=False)
                await canal_gerencia.send(embed=embed)
        await salvar_ausencia_db(interaction.user.id, self.nome.value, self.motivo.value, data_inicio_naive, data_fim_naive)
        cargo = interaction.guild.get_role(CARGO_AUSENTE_ID)
        if cargo:
            await interaction.user.add_roles(cargo)
        canal_registro = interaction.guild.get_channel(CANAL_REGISTRO_AUSENCIA_ID)
        if canal_registro:
            embed = discord.Embed(title="📋 AUSÊNCIA REGISTRADA", description=f"{interaction.user.mention} está ausente!", color=0xe67e22)
            embed.add_field(name="Nome", value=self.nome.value, inline=True)
            embed.add_field(name="Período", value=f"{self.data_inicio.value} a {self.data_fim.value}", inline=True)
            embed.add_field(name="Motivo", value=self.motivo.value, inline=False)
            await canal_registro.send(embed=embed)
        await interaction.followup.send("✅ Ausência registrada!", ephemeral=True)

class AusenciaBotaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📝 Solicitar Ausência", style=discord.ButtonStyle.primary, custom_id="ausencia_solicitar_botao")
    async def solicitar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(AusenciaModal())

@tasks.loop(minutes=60)
async def verificar_ausencias_expiradas():
    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return
    cargo_ausente = guild.get_role(CARGO_AUSENTE_ID)
    if not cargo_ausente:
        return
    users = await remover_ausencias_expiradas()
    for user_id in users:
        member = guild.get_member(int(user_id))
        if member and cargo_ausente in member.roles:
            await member.remove_roles(cargo_ausente)
            canal = guild.get_channel(CANAL_REGISTRO_AUSENCIA_ID)
            if canal:
                await canal.send(embed=discord.Embed(title="🎉 RETORNO", description=f"{member.mention} retornou!", color=0x2ecc71))

# =========================================================
# =================== SISTEMA ALUGUEL =====================
# =========================================================

alugueis_ativos = {}

async def salvar_aluguel_db(galpao, user_id, data_fim, dias):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute(
            "INSERT INTO alugueis (galpao, user_id, data_inicio, data_fim, dias, ativo) VALUES ($1, $2, $3, $4, $5, true) ON CONFLICT (galpao) DO UPDATE SET user_id=$2, data_fim=$4, dias=$5, ativo=true",
            galpao, str(user_id), agora_db(), data_fim, dias
        )

async def carregar_alugueis_db():
    global alugueis_ativos
    db = get_db()
    async with db.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM alugueis WHERE ativo = true AND data_fim > NOW()")
    alugueis_ativos.clear()
    for row in rows:
        data_fim = row["data_fim"]
        if data_fim.tzinfo is None:
            data_fim = data_fim.replace(tzinfo=BRASIL)
        alugueis_ativos[row["galpao"]] = {"user_id": int(row["user_id"]), "fim": data_fim, "dias": row["dias"]}

async def desativar_aluguel_db(galpao):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute("UPDATE alugueis SET ativo = false WHERE galpao = $1", galpao)
    if galpao in alugueis_ativos:
        del alugueis_ativos[galpao]

async def renovar_aluguel_db(galpao, user_id, data_fim, dias):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute("UPDATE alugueis SET user_id = $2, data_fim = $3, dias = $4, ativo = true WHERE galpao = $1", galpao, str(user_id), data_fim, dias)

def formatar_tempo_detalhado(data_fim):
    agora_br = agora()
    if not data_fim or agora_br >= data_fim:
        return "⚠️ **EXPIRADO**"
    diff = data_fim - agora_br
    dias = diff.days
    horas = diff.seconds // 3600
    minutos = (diff.seconds % 3600) // 60
    if dias > 0:
        return f"**{dias} dia(s)** e **{horas}h** {minutos}m"
    elif horas > 0:
        return f"**{horas}h** {minutos}m"
    else:
        return f"**{minutos}m**"

def calcular_barra_progresso(data_fim, dias_totais):
    agora_br = agora()
    if not data_fim or agora_br >= data_fim:
        return "❌ EXPIRADO"
    data_inicio = data_fim - timedelta(days=dias_totais)
    tempo_total = (data_fim - data_inicio).total_seconds()
    tempo_restante = (data_fim - agora_br).total_seconds()
    if tempo_total <= 0:
        pct = 0
    else:
        pct = (tempo_restante / tempo_total) * 100
        pct = max(0, min(100, pct))
    preenchidos = int((pct / 100) * 20)
    cor = "🟢" if pct > 66 else "🟡" if pct > 33 else "🔴"
    return f"{cor} `{'█' * preenchidos}{'░' * (20 - preenchidos)}` {pct:.0f}%"

class AluguelModal(discord.ui.Modal, title="💰 Alugar Galpão"):
    dias = discord.ui.TextInput(label="Quantos dias?", placeholder="15, 30, 7", required=True)

    def __init__(self, galpao, editando=False, user_id=None):
        super().__init__()
        self.galpao = galpao
        self.editando = editando
        self.user_id = user_id

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            dias_aluguel = int(self.dias.value)
        except:
            await interaction.followup.send("❌ Número inválido!", ephemeral=True)
            return
        data_fim = agora() + timedelta(days=dias_aluguel)
        if self.editando:
            await renovar_aluguel_db(self.galpao, self.user_id, data_fim.replace(tzinfo=None), dias_aluguel)
            alugueis_ativos[self.galpao] = {"user_id": self.user_id, "fim": data_fim, "dias": dias_aluguel}
            await interaction.followup.send(f"✅ {self.galpao} atualizado para {dias_aluguel} dias!", ephemeral=True)
        else:
            if self.galpao in alugueis_ativos:
                await interaction.followup.send(f"❌ {self.galpao} já está alugado!", ephemeral=True)
                return
            await salvar_aluguel_db(self.galpao, interaction.user.id, data_fim.replace(tzinfo=None), dias_aluguel)
            alugueis_ativos[self.galpao] = {"user_id": interaction.user.id, "fim": data_fim, "dias": dias_aluguel}
            await interaction.followup.send(f"✅ {self.galpao} alugado por {dias_aluguel} dias!", ephemeral=True)
        await atualizar_painel_alugueis()

class AlugarButton(discord.ui.Button):
    def __init__(self, galpao, label, custom_id):
        super().__init__(label=label, style=discord.ButtonStyle.success, custom_id=custom_id)
        self.galpao = galpao

    async def callback(self, interaction: discord.Interaction):
        await interaction.response.send_modal(AluguelModal(self.galpao))

class EditarAluguelButton(discord.ui.Button):
    def __init__(self, galpao, user_id, dias_atuais):
        super().__init__(label="✏️ Editar", style=discord.ButtonStyle.secondary, custom_id=f"editar_{galpao}")
        self.galpao = galpao
        self.user_id = user_id
        self.dias_atuais = dias_atuais

    async def callback(self, interaction: discord.Interaction):
        if interaction.user.id != self.user_id and not tem_algum_cargo(interaction.user, [CARGO_GERENTE_ID, CARGO_GERENTE_GERAL_ID]):
            await interaction.response.send_message("❌ Sem permissão!", ephemeral=True)
            return
        await interaction.response.send_modal(AluguelModal(self.galpao, editando=True, user_id=self.user_id))

class AluguelView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)
        self.add_item(AlugarButton("GALPÕES NORTE", "💰 Alugar Galpões Norte", "aluguel_norte_btn"))
        self.add_item(AlugarButton("GALPÕES SUL", "💰 Alugar Galpões Sul", "aluguel_sul_btn"))

    def adicionar_edicao(self, galpao, user_id, dias):
        self.add_item(EditarAluguelButton(galpao, user_id, dias))

async def enviar_painel_alugueis():
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    if not canal:
        return
    await carregar_alugueis_db()
    embed = discord.Embed(title="🏭💰 CONTROLE DE ALUGUEL", color=0x3498db)
    view = AluguelView()
    for galpao in ["GALPÕES NORTE", "GALPÕES SUL"]:
        if galpao in alugueis_ativos:
            dados = alugueis_ativos[galpao]
            data_fim = dados["fim"]
            embed.add_field(
                name=f"🏭 {galpao}",
                value=(
                    f"**STATUS:** 🔵 ALUGADO\n"
                    f"👤 <@{dados['user_id']}>\n"
                    f"📅 {dados['dias']} dias\n"
                    f"⏰ {formatar_tempo_detalhado(data_fim)}\n"
                    f"{calcular_barra_progresso(data_fim, dados['dias'])}"
                ),
                inline=False
            )
            view.adicionar_edicao(galpao, dados['user_id'], dados['dias'])
        else:
            embed.add_field(
                name=f"🏭 {galpao}",
                value="**STATUS:** 🟢 DISPONÍVEL\n💰 Clique no botão para alugar",
                inline=False
            )
    await enviar_ou_atualizar_painel("painel_alugueis", CANAL_FABRICACAO_ID, embed, view)

async def atualizar_painel_alugueis():
    await enviar_painel_alugueis()

@tasks.loop(minutes=1)
async def atualizar_contagem_alugueis():
    if alugueis_ativos:
        await atualizar_painel_alugueis()

@tasks.loop(minutes=5)
async def verificar_alugueis_expirados():
    for galpao, dados in list(alugueis_ativos.items()):
        if agora() >= dados["fim"]:
            await desativar_aluguel_db(galpao)
            canal = bot.get_channel(CANAL_FABRICACAO_ID)
            if canal:
                await canal.send(f"⚠️ **{galpao}** EXPIRou! Disponível para novo aluguel.")
    await atualizar_painel_alugueis()

# =========================================================
# =================== SISTEMA COMPRAS =====================
# =========================================================

async def salvar_compra_db(produto, valor, comprado_por):
    db = get_db()
    async with db.acquire() as conn:
        await conn.execute("INSERT INTO compras (produto, valor, comprado_por, data) VALUES ($1, $2, $3, $4)", produto, valor, str(comprado_por), agora_db())

class RegistrarCompraModal(discord.ui.Modal, title="📝 Registrar Compra"):
    produto = discord.ui.TextInput(label="Produto", placeholder="Pólvora", required=True)
    valor = discord.ui.TextInput(label="Valor", placeholder="50000", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        produto = self.produto.value.strip()
        try:
            valor_compra = int(self.valor.value.replace(".", "").replace(",", ""))
        except:
            await interaction.followup.send("❌ Valor inválido!", ephemeral=True)
            return
        await salvar_compra_db(produto, valor_compra, interaction.user.id)
        await interaction.followup.send(f"✅ Compra registrada: {produto} - {formatar_dinheiro(valor_compra)}", ephemeral=True)

class RegistrarCompraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📝 Registrar Nova Compra", style=discord.ButtonStyle.success, custom_id="registrar_compra_btn")
    async def registrar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(RegistrarCompraModal())

# =========================================================
# =================== SISTEMA CLIPES ======================
# =========================================================

clips_postados = set()
fila_clipes = None

async def postar_clipe_x(message):
    canal = bot.get_channel(CANAL_POSTAGEM_X)
    if not canal:
        return
    link = message.content if message.content else "Sem link"
    texto = f"🚀 **CLIPE APROVADO**\n👤 {message.author.mention}\n🔗 {link}\n\n🔥 Olha esse clipe!\n{link}\n#fivem #clips #gaming"
    await canal.send(texto)
    await message.reply("📤 Enviado para postagem!")

async def worker_clipes():
    while True:
        message = await fila_clipes.get()
        try:
            await postar_clipe_x(message)
        except Exception as e:
            print("Erro worker:", e)
        await asyncio.sleep(5)
        fila_clipes.task_done()

async def handle_clipe_reaction(bot, reaction, user):
    if user.bot or reaction.message.channel.id != CANAL_CLIPES_ID:
        return
    if str(reaction.emoji) != EMOJI_APROVACAO or reaction.message.id in clips_postados:
        return
    tem_video = False
    tem_link = False
    if reaction.message.attachments:
        att = reaction.message.attachments[0]
        if att.filename.endswith((".mp4", ".mov")):
            tem_video = True
    if reaction.message.content and "http" in reaction.message.content:
        tem_link = True
    if not tem_video and not tem_link:
        await reaction.message.reply("❌ Precisa ter vídeo ou link.")
        return
    clips_postados.add(reaction.message.id)
    await fila_clipes.put(reaction.message)
    await reaction.message.reply("🚀 Vai pro X!")

# =========================================================
# =================== SISTEMA FINANCEIRO ==================
# =========================================================

class RelatorioFinanceiroModal(discord.ui.Modal, title="📊 RELATÓRIO FINANCEIRO"):
    data_inicio = discord.ui.TextInput(label="Data INÍCIO", placeholder="01/04/2026", required=True)
    data_fim = discord.ui.TextInput(label="Data FIM", placeholder="30/04/2026", required=True)

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            inicio = datetime.strptime(self.data_inicio.value.strip(), "%d/%m/%Y")
            fim = datetime.strptime(self.data_fim.value.strip(), "%d/%m/%Y")
            inicio_dt = inicio.replace(hour=0, minute=0, second=0)
            fim_dt = fim.replace(hour=23, minute=59, second=59)
        except:
            await interaction.followup.send("❌ Formato inválido! Use DD/MM/AAAA", ephemeral=True)
            return
        db = get_db()
        async with db.acquire() as conn:
            polvora_row = await conn.fetchrow("SELECT COALESCE(SUM(polvora), 0) as total_polvora FROM producoes_finalizadas WHERE data >= $1 AND data <= $2", inicio_dt, fim_dt)
            vendas_row = await conn.fetchrow("SELECT COALESCE(SUM(valor), 0) as total_vendas FROM vendas WHERE TO_DATE(data, 'DD/MM/YYYY') BETWEEN $1 AND $2", inicio.date(), fim.date())
            compras_row = await conn.fetch("SELECT produto, valor FROM compras WHERE data >= $1 AND data <= $2 ORDER BY data DESC", inicio_dt, fim_dt)
        total_polvora = polvora_row["total_polvora"] or 0
        total_gasto_polvora = total_polvora * 80
        total_vendas = vendas_row["total_vendas"] or 0
        total_gasto_compras = sum(c["valor"] for c in compras_row) if compras_row else 0
        total_gastos = total_gasto_polvora + total_gasto_compras
        saldo = total_vendas - total_gastos
        embed = discord.Embed(title="📊 RELATÓRIO FINANCEIRO", description=f"📅 {self.data_inicio.value} a {self.data_fim.value}", color=0x1abc9c)
        embed.add_field(name="💣 PÓLVORA", value=f"Usada: {fmt_num(total_polvora)} unidades\nGasto: {formatar_dinheiro(total_gasto_polvora)}", inline=False)
        embed.add_field(name="🛒 VENDAS", value=f"Total: {formatar_dinheiro(total_vendas)}", inline=False)
        embed.add_field(name="📦 OUTRAS COMPRAS", value=f"Total: {formatar_dinheiro(total_gasto_compras)}", inline=False)
        embed.add_field(name="📊 RESUMO", value=f"Vendas: {formatar_dinheiro(total_vendas)}\nGastos: {formatar_dinheiro(total_gastos)}\n{'🟢' if saldo >= 0 else '🔴'} SALDO: {formatar_dinheiro(saldo)}", inline=False)
        canal = interaction.guild.get_channel(1498664038559776768)
        if canal:
            await canal.send(embed=embed)
        await interaction.followup.send("✅ Relatório enviado!", ephemeral=True)

class RelatorioFinanceiroView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📊 Gerar Relatório", style=discord.ButtonStyle.success, custom_id="relatorio_financeiro_btn")
    async def gerar(self, interaction: discord.Interaction, button):
        await interaction.response.send_modal(RelatorioFinanceiroModal())

# =========================================================
# =================== EVENTOS GLOBAIS =====================
# =========================================================

@bot.event
async def on_member_remove(member):
    if member.bot:
        return
    await asyncio.sleep(2)
    try:
        embed = discord.Embed(title="📤 NOTIFICAÇÃO DE SAÍDA", description=MENSAGEM_SAIDA["mensagem"].format(nome=member.display_name), color=0xe74c3c)
        embed.set_footer(text=f"ID: {member.id}")
        await member.send(embed=embed)
        dm_ok = True
    except:
        dm_ok = False
    canal = bot.get_channel(CANAL_GERENCIA_ID)
    if canal:
        embed = discord.Embed(title="📤 USUÁRIO SAIU", description=f"{member.mention} ({member.name})", color=0xe74c3c)
        embed.add_field(name="DM", value="✅" if dm_ok else "❌")
        await canal.send(embed=embed)

@bot.event
async def on_message(message):
    if message.author.bot:
        return
    if await handle_lavagem_message(message):
        return
    await bot.process_commands(message)

@bot.event
async def on_reaction_add(reaction, user):
    await handle_clipe_reaction(bot, reaction, user)

# =========================================================
# =================== ON_READY ============================
# =========================================================

@bot.event
async def on_ready():
    if hasattr(bot, "ja_iniciado"):
        return
    bot.ja_iniciado = True
    
    global fila_clipes, db
    
    print(f"✅ Logado como {bot.user}")
    print(f"🕒 Horário: {agora().strftime('%d/%m/%Y %H:%M:%S')}")
    
    await conectar_db()
    
    fila_clipes = asyncio.Queue()
    bot.loop.create_task(worker_clipes())
    
    # Registra views
    for view in [RegistroView, CalculadoraView, FabricacaoView, PolvoraView, 
                 LavagemView, CadastrarLiveView, SolicitarSalaView, PainelAcoesView,
                 AusenciaBotaoView, AluguelView, RelatorioFinanceiroView, RegistrarCompraView]:
        bot.add_view(view())
    
    # Inicia loops
    if not verificar_producoes_orfas.is_running():
        verificar_producoes_orfas.start()
    if not verificar_lives.is_running():
        verificar_lives.start()
    if not verificar_ausencias_expiradas.is_running():
        verificar_ausencias_expiradas.start()
    if not atualizar_contagem_alugueis.is_running():
        atualizar_contagem_alugueis.start()
    if not verificar_alugueis_expirados.is_running():
        verificar_alugueis_expirados.start()
    
    # Restaura produções
    db = get_db()
    if db:
        async with db.acquire() as conn:
            rows = await conn.fetch("SELECT pid FROM producoes WHERE CAST(fim AS timestamp) > NOW()")
        for r in rows:
            pid = r["pid"]
            task = bot.loop.create_task(acompanhar_producao(pid))
            producoes_tasks[pid] = task
            print(f"✅ Produção restaurada: {pid}")
    
    # Carrega aluguéis
    await carregar_alugueis_db()
    
    # Envia painéis
    guild = bot.get_guild(GUILD_ID)
    if guild:
        await enviar_painel_acoes(guild)
    
    paineis = [
        (CANAL_REGISTRO_ID, "📋 Registro", RegistroView()),
        (CANAL_VENDAS_ID, "🛒 Painel de Vendas", CalculadoraView()),
        (CANAL_FABRICACAO_ID, "🏭 PAINEL DE FABRICAÇÃO", FabricacaoView()),
        (CANAL_CALCULO_POLVORA_ID, "💣 Registro de Pólvora", PolvoraView()),
        (CANAL_INICIAR_LAVAGEM_ID, "🧼 Lavagem de Dinheiro", LavagemView()),
        (CANAL_CADASTRO_LIVE_ID, "🎥 CADASTRO DE LIVE", CadastrarLiveView()),
        (CANAL_SOLICITAR_SALA_ID, "📂 Solicitar Sala", SolicitarSalaView()),
        (CANAL_ESCALACOES_ID, "📊 AÇÕES DA SEMANA", PainelAcoesView()),
        (CANAL_BOTAO_AUSENCIA_ID, "📋 Solicitar Ausência", AusenciaBotaoView()),
        (CANAL_REGISTRAR_COMPRA_ID, "💰 REGISTRAR COMPRA", RegistrarCompraView()),
    ]
    
    for canal_id, titulo, view in paineis:
        try:
            canal = bot.get_channel(canal_id)
            if canal:
                embed = discord.Embed(title=titulo, color=0x2ecc71)
                await enviar_ou_atualizar_painel(f"painel_{canal_id}", canal_id, embed, view)
        except Exception as e:
            print(f"Erro painel {titulo}: {e}")
    
    await atualizar_painel_fabricacao()
    await enviar_painel_alugueis()
    
    print("✅ BOT PRONTO!")

# =========================================================
# =================== START ===============================
# =========================================================

if __name__ == "__main__":
    print("🚀 Iniciando bot...")
    bot.run(TOKEN)
