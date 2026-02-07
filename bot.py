# =========================================================
# ======================== IMPORTS =========================
# =========================================================

import os
import json
import asyncio
import aiohttp
import discord

from discord.ext import commands, tasks
from discord.utils import escape_markdown

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# =========================================================
# ===================== RELÓGIO GLOBAL ====================
# =========================================================
# 🔥 TODAS AS HORAS DO BOT AGORA USAM BRASÍLIA

BRASIL = ZoneInfo("America/Sao_Paulo")

def agora():
    return datetime.now(BRASIL)


# =========================================================
# ======================== CONFIG ==========================
# =========================================================

TOKEN = os.environ.get("TOKEN")

TWITCH_CLIENT_ID = os.environ.get("TWITCH_CLIENT_ID")
TWITCH_CLIENT_SECRET = os.environ.get("TWITCH_CLIENT_SECRET")


# =========================================================
# ======================== ARQUIVOS ========================
# =========================================================

ARQUIVO_PRODUCOES = "producoes.json"
ARQUIVO_PEDIDOS = "pedidos.json"
ARQUIVO_POLVORAS = "polvoras.json"
ARQUIVO_LIVES = "lives.json"
ARQUIVO_LAVAGENS = "lavagens.json"
ARQUIVO_PONTO = "ponto.json"
ARQUIVO_METAS = "metas.json"


# =========================================================
# ======================== GUILD ===========================
# =========================================================

GUILD_ID = 1229526644193099880
GUILD = discord.Object(id=GUILD_ID)


# =========================================================
# ======================== CANAIS ==========================
# =========================================================

# REGISTRO
CANAL_REGISTRO_ID = 1229556030397218878
CANAL_LOG_REGISTRO_ID = 1462457604939841851

# VENDAS
CANAL_CALCULADORA_ID = 1460984821458272347
CANAL_ENCOMENDAS_ID = 1460980984811098294

# PRODUÇÃO
CANAL_FABRICACAO_ID = 1466421612566810634
CANAL_REGISTRO_GALPAO_ID = 1356174712337862819

# POLVORA
CANAL_CALCULO_POLVORA_ID = 1462834441968943157
CANAL_REGISTRO_POLVORA_ID = 1448570795101261846

# LIVES
CANAL_CADASTRO_LIVE_ID = 1466464557215256790
CANAL_DIVULGACAO_LIVE_ID = 1243325102917943335

# LAVAGEM
CANAL_INICIAR_LAVAGEM_ID = 1467152989499293768
CANAL_LAVAGEM_MEMBROS_ID = 1467159346923311216
CANAL_RELATORIO_LAVAGEM_ID = 1467150805273546878

# PONTO
CANAL_PONTO_ID = 1468941297162391715

# METAS
CANAL_SOLICITAR_SALA_ID = 1337374500366450741


# =========================================================
# ======================== CARGOS ==========================
# =========================================================

# REGISTRO
AGREGADO_ROLE_ID = 1422847202937536532
CONVIDADO_ROLE_ID = 1337382961456353342

# METAS / LAVAGEM / PONTO
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
CARGO_AGREGADO_ID = 1422847202937536532


# =========================================================
# ===================== CATEGORIAS =========================
# =========================================================

CATEGORIA_META_GERENTE_ID = 1337374002422743122
CATEGORIA_META_RESPONSAVEIS_ID = 1462810826992783422
CATEGORIA_META_SOLDADO_ID = 1461335635519475894
CATEGORIA_META_MEMBRO_ID = 1461335697209163900
CATEGORIA_META_AGREGADO_ID = 1461335748870541323


# =========================================================
# ======================== BOT SETUP =======================
# =========================================================

intents = discord.Intents.default()
intents.members = True
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)
# =========================================================
# ======================= REGISTRO =========================
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

            # 🔥 CRIA SALA DE META AUTOMATICAMENTE (CORRIGIDO)
            await criar_sala_meta(membro)

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

    @discord.ui.button(
        label="📋 Fazer Registro",
        style=discord.ButtonStyle.success,
        custom_id="registro_fazer"
    )
    async def registro(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(RegistroModal())
# =========================================================
# ======================== VENDAS ==========================
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
                return linhas
        linhas.append(nova_linha)
        return linhas

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="status_pago")
    async def pago(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        user = interaction.user.mention

        linhas = [l for l in linhas if not l.startswith("⏳")]
        linhas = self.toggle_linha(linhas, "💰", f"💰 Pago • Recebido por {user} • {agora_str}")

        embed = self.set_status(embed, idx, linhas)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="status_entregue")
    async def entregue(self, interaction: discord.Interaction, button: discord.ui.Button):
        embed = interaction.message.embeds[0]
        idx, linhas = self.get_status(embed)

        agora_str = agora().strftime("%d/%m/%Y %H:%M")
        user = interaction.user.mention

        linhas = [l for l in linhas if not l.startswith("📦")]
        linhas = self.toggle_linha(linhas, "✅", f"✅ Entregue por {user} • {agora_str}")

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

        embed.add_field(
            name="📌 Status",
            value="📦 A entregar",
            inline=False
        )

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

    @discord.ui.button(
        label="🧮 Registrar Venda",
        style=discord.ButtonStyle.primary,
        custom_id="calculadora_registrar"
    )
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(VendaModal())
# =========================================================
# ======================== PRODUÇÃO ========================
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

    if pct <= 0.35:
        cor = "🟢"
    elif pct <= 0.70:
        cor = "🟡"
    elif pct < 1:
        cor = "🔴"
    else:
        cor = "🔵"

    return cor + " " + ("▓" * cheio) + ("░" * (size - cheio))


# ================= 2ª TASK =================

class SegundaTaskView(discord.ui.View):
    def __init__(self, pid):
        super().__init__(timeout=None)
        self.pid = pid

    @discord.ui.button(
        label="✅ Confirmar 2ª Task",
        style=discord.ButtonStyle.success,
        custom_id="segunda_task_btn"
    )
    async def ok(self, interaction: discord.Interaction, button: discord.ui.Button):
        producoes = carregar_producoes()

        if self.pid not in producoes:
            await interaction.response.defer()
            return

        producoes[self.pid]["segunda_task_confirmada"] = {
            "user": interaction.user.id,
            "time": agora().isoformat()
        }

        salvar_producoes(producoes)

        await interaction.message.edit(view=None)
        await interaction.response.defer()


# ================= MODAL OBSERVAÇÃO =================

class ObservacaoProducaoModal(discord.ui.Modal, title="Iniciar Produção"):
    obs = discord.ui.TextInput(
        label="Observação inicial",
        placeholder="Ex: Galpões todos produzindo / 1 e 2 ativos / 3 com HC",
        style=discord.TextStyle.paragraph,
        required=False
    )

    def __init__(self, galpao, tempo):
        super().__init__()
        self.galpao = galpao
        self.tempo = tempo

    async def on_submit(self, interaction: discord.Interaction):
        producoes = carregar_producoes()
        pid = f"{self.galpao}_{interaction.id}"

        inicio = agora()
        fim = inicio + timedelta(minutes=self.tempo)

        canal = interaction.guild.get_channel(CANAL_REGISTRO_GALPAO_ID)

        msg = await canal.send(
            embed=discord.Embed(
                title="🏭 Produção",
                description=f"Iniciando produção em **{self.galpao}**...",
                color=0x3498db
            ),
            view=SegundaTaskView(pid)
        )

        producoes[pid] = {
            "galpao": self.galpao,
            "autor": interaction.user.id,
            "inicio": inicio.isoformat(),
            "fim": fim.isoformat(),
            "obs": self.obs.value,
            "msg_id": msg.id,
            "canal_id": CANAL_REGISTRO_GALPAO_ID
        }

        salvar_producoes(producoes)
        bot.loop.create_task(acompanhar_producao(pid))

        await interaction.response.defer()


# ================= VIEW FABRICAÇÃO =================

class FabricacaoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="🏭 Galpões Norte",
        style=discord.ButtonStyle.primary,
        custom_id="fab_norte_btn"
    )
    async def norte(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            ObservacaoProducaoModal("GALPÕES NORTE", 65)
        )

    @discord.ui.button(
        label="🏭 Galpões Sul",
        style=discord.ButtonStyle.secondary,
        custom_id="fab_sul_btn"
    )
    async def sul(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            ObservacaoProducaoModal("GALPÕES SUL", 130)
        )


# ================= LOOP DE ACOMPANHAMENTO =================

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
        restante = max(0, (fim - agora()).total_seconds())
        pct = max(0, min(1, 1 - (restante / total)))
        mins = int(restante // 60)

        desc = (
            f"**Galpão:** {prod['galpao']}\n"
            f"**Iniciado por:** <@{prod['autor']}>\n"
        )

        if prod.get("obs"):
            desc += f"📝 **Obs:** {prod['obs']}\n"

        desc += (
            f"Início: <t:{int(inicio.timestamp())}:t>\n"
            f"Término: <t:{int(fim.timestamp())}:t>\n\n"
            f"⏳ **Restante:** {mins} min\n"
            f"{barra(pct)}"
        )

        if "segunda_task_confirmada" in prod:
            uid = prod["segunda_task_confirmada"]["user"]
            desc += f"\n\n✅ **Segunda task concluída por:** <@{uid}>"

        if restante <= 0:
            desc += "\n\n🔵 **Produção Finalizada**"
            del producoes[pid]
            salvar_producoes(producoes)

        await msg.edit(
            embed=discord.Embed(
                title="🏭 Produção",
                description=desc,
                color=0x34495e
            )
        )

        if restante <= 0:
            return

        await asyncio.sleep(60)


# ================= PAINEL =================

async def enviar_painel_fabricacao():
    canal = bot.get_channel(CANAL_FABRICACAO_ID)
    async for m in canal.history(limit=10):
        if m.author == bot.user and m.embeds and m.embeds[0].title == "🏭 Fabricação":
            return

    await canal.send(
        embed=discord.Embed(
            title="🏭 Fabricação",
            description="Selecione Norte ou Sul para iniciar a produção.",
            color=0x2c3e50
        ),
        view=FabricacaoView()
    )

# =========================================================
# ======================== POLVORAS ========================
# =========================================================

# ================= ARQUIVO =================

def carregar_polvoras():
    if not os.path.exists(ARQUIVO_POLVORAS):
        return []
    try:
        with open(ARQUIVO_POLVORAS, "r") as f:
            return json.load(f)
    except:
        return []

def salvar_polvoras(dados):
    with open(ARQUIVO_POLVORAS, "w") as f:
        json.dump(dados, f, indent=4)


# ================= MODAL =================

class PolvoraModal(discord.ui.Modal, title="Registro de Compra de Pólvora"):
    vendedor = discord.ui.TextInput(
        label="Vendedor (preencha o nome)",
        placeholder="Digite o nome do vendedor"
    )

    quantidade = discord.ui.TextInput(
        label="Quantidade",
        placeholder="Ex: 100"
    )

    async def on_submit(self, interaction: discord.Interaction):
        try:
            qtd = int(self.quantidade.value)
        except:
            await interaction.response.send_message("Quantidade inválida.", ephemeral=True)
            return

        valor = qtd * 80
        valor_formatado = f"{valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        # 🔥 HORÁRIO BRASÍLIA
        agora_br = agora().isoformat()

        dados = carregar_polvoras()
        dados.append({
            "user": interaction.user.id,
            "vendedor": self.vendedor.value,
            "quantidade": qtd,
            "valor": valor,
            "data": agora_br
        })
        salvar_polvoras(dados)

        canal = interaction.guild.get_channel(CANAL_REGISTRO_POLVORA_ID)

        embed = discord.Embed(
            title="🧨 Registro de Pólvora",
            color=0xe67e22
        )

        embed.add_field(name="Vendedor", value=self.vendedor.value, inline=False)
        embed.add_field(name="Comprado por", value=interaction.user.mention, inline=False)
        embed.add_field(name="Quantidade", value=str(qtd), inline=True)
        embed.add_field(name="Valor", value=f"**R$ {valor_formatado}**", inline=True)

        await canal.send(embed=embed)
        await interaction.response.send_message("Registro feito com sucesso!", ephemeral=True)


# ================= CONFIRMAR PAGAMENTO =================

class ConfirmarPagamentoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Confirmar pagamento", style=discord.ButtonStyle.success, custom_id="confirmar_pagamento")
    async def confirmar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.message.edit(
            content=interaction.message.content + "\n\n✅ **PAGO**",
            view=None
        )
        await interaction.response.defer()


# ================= VIEW =================

class PolvoraView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Registrar Compra de Pólvora", style=discord.ButtonStyle.primary, custom_id="polvora_btn")
    async def registrar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(PolvoraModal())


# ================= RELATÓRIO SEMANAL AUTOMÁTICO =================

@tasks.loop(minutes=1)
async def relatorio_semanal_polvoras():

    # 🔥 HORÁRIO BRASÍLIA
    agora_br = agora()

    # Domingo 23:59
    if agora_br.weekday() != 6 or agora_br.hour != 23 or agora_br.minute != 59:
        return

    dados = carregar_polvoras()

    inicio_semana = agora_br - timedelta(days=6)
    inicio_semana = inicio_semana.replace(hour=0, minute=0, second=0, microsecond=0)

    fim_semana = agora_br.replace(hour=23, minute=59, second=59)

    resumo = {}

    for item in dados:
        data_item = datetime.fromisoformat(item["data"])

        if inicio_semana <= data_item <= fim_semana:
            resumo.setdefault(item["user"], 0)
            resumo[item["user"]] += item["valor"]

    if not resumo:
        return

    canal = bot.get_channel(CANAL_REGISTRO_POLVORA_ID)

    for user_id, total in resumo.items():
        user = await bot.fetch_user(user_id)

        valor_formatado = f"{total:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        await canal.send(
            content=(
                f"🧨 **RELATÓRIO SEMANAL DE PÓLVORA**\n"
                f"📅 Período: {inicio_semana.strftime('%d/%m')} até {fim_semana.strftime('%d/%m')}\n\n"
                f"👤 Comprado por: {user.mention}\n"
                f"💰 Valor a ressarcir: **R$ {valor_formatado}**"
            ),
            view=ConfirmarPagamentoView()
        )


# ================= PAINEL =================

async def enviar_painel_polvoras(bot):
    canal = bot.get_channel(CANAL_CALCULO_POLVORA_ID)

    async for m in canal.history(limit=10):
        if m.author == bot.user:
            return

    embed = discord.Embed(
        title="🧨 Calculadora de Pólvora",
        description="Use o botão abaixo para registrar a compra.",
        color=0xe67e22
    )

    await canal.send(embed=embed, view=PolvoraView())
# =========================================================
# ======================== LAVAGEM =========================
# =========================================================

lavagens_pendentes = {}


def formatar_real(valor: int) -> str:
    return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


# ================= ARQUIVO =================

def carregar_lavagens():
    if not os.path.exists(ARQUIVO_LAVAGENS):
        return []
    try:
        with open(ARQUIVO_LAVAGENS, "r") as f:
            return json.load(f)
    except:
        return []

def salvar_lavagens(dados):
    with open(ARQUIVO_LAVAGENS, "w") as f:
        json.dump(dados, f, indent=4)


# ================= MODAL =================

class LavagemModal(discord.ui.Modal, title="Iniciar Lavagem"):
    valor = discord.ui.TextInput(label="Valor do dinheiro sujo")

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer()

        try:
            valor_sujo = int(self.valor.value.replace(".", "").replace(",", ""))
        except:
            await interaction.response.send_message("Valor inválido.", ephemeral=True)
            return

        valor_retorno = int(valor_sujo * 0.8)

        msg_info = await interaction.channel.send(
            f"{interaction.user.mention} envie agora o PRINT da tela."
        )

        lavagens_pendentes[interaction.user.id] = {
            "sujo": valor_sujo,
            "retorno": valor_retorno,
            "msg_info": msg_info
        }


# =========================================================
# =========== CAPTURA AUTOMÁTICA DO PRINT =================
# =========================================================

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    if message.channel.id != CANAL_INICIAR_LAVAGEM_ID:
        return

    if not message.attachments:
        return

    if message.author.id not in lavagens_pendentes:
        return

    dados_temp = lavagens_pendentes.pop(message.author.id)

    valor_sujo = dados_temp["sujo"]
    valor_retorno = dados_temp["retorno"]

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

    dados = carregar_lavagens()
    dados.append({
        "user": message.author.id,
        "sujo": valor_sujo,
        "retorno": valor_retorno
    })
    salvar_lavagens(dados)

    embed = discord.Embed(title="🧼 Nova Lavagem", color=0x1abc9c)
    embed.add_field(name="Membro", value=message.author.mention, inline=False)
    embed.add_field(name="Valor sujo", value=formatar_real(valor_sujo), inline=True)
    embed.add_field(name="Valor a repassar (80%)", value=formatar_real(valor_retorno), inline=True)
    embed.set_image(url=f"attachment://{arquivo.filename}")

    await canal_destino.send(embed=embed, file=arquivo)


# ================= PERMISSÃO =================

def pode_gerenciar_lavagem(member: discord.Member):
    cargos_permitidos = [
        CARGO_GERENTE_ID,
        CARGO_01_ID,
        CARGO_02_ID,
        CARGO_GERENTE_GERAL_ID
    ]
    return any(role.id in cargos_permitidos for role in member.roles)


# ================= VIEW =================

class LavagemView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="Iniciar Lavagem", style=discord.ButtonStyle.primary, custom_id="lavagem_btn")
    async def iniciar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(LavagemModal())

    @discord.ui.button(label="🧹 Limpar Sala", style=discord.ButtonStyle.danger, custom_id="lavagem_limpar")
    async def limpar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message("Você não tem permissão.", ephemeral=True)
            return

        canal = interaction.guild.get_channel(CANAL_LAVAGEM_MEMBROS_ID)

        async for msg in canal.history(limit=None):
            try:
                await msg.delete()
            except:
                pass

        salvar_lavagens([])
        await interaction.response.send_message("Sala limpa!", ephemeral=True)

    @discord.ui.button(label="📊 Gerar Relatório", style=discord.ButtonStyle.success, custom_id="lavagem_relatorio")
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message("Você não tem permissão.", ephemeral=True)
            return

        dados = carregar_lavagens()
        canal = interaction.guild.get_channel(CANAL_RELATORIO_LAVAGEM_ID)

        for item in dados:
            user = await bot.fetch_user(item["user"])
            await canal.send(
                f"{user.mention} - Valor a repassar: {formatar_real(item['retorno'])} "
                f"- Valor sujo: {formatar_real(item['sujo'])}"
            )

        await interaction.response.send_message("Relatório enviado!", ephemeral=True)

    @discord.ui.button(label="📩 Avisar TODOS no DM", style=discord.ButtonStyle.primary, custom_id="lavagem_dm_todos")
    async def avisar_todos(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not pode_gerenciar_lavagem(interaction.user):
            await interaction.response.send_message("Você não tem permissão.", ephemeral=True)
            return

        dados = carregar_lavagens()

        enviados = 0
        falhas = 0

        for item in dados:
            try:
                user = await bot.fetch_user(item["user"])
                await user.send(
                    f"🧼 **Seu dinheiro foi lavado com sucesso!**\n\n"
                    f"💵 Dinheiro informado: {formatar_real(item['sujo'])}\n"
                    f"💰 Valor repassado: {formatar_real(item['retorno'])}"
                )
                enviados += 1
            except:
                falhas += 1

        await interaction.response.send_message(
            f"DM enviada para {enviados} membros.\nFalhas: {falhas}",
            ephemeral=True
        )


# ================= PAINEL =================

async def enviar_painel_lavagem():
    canal = bot.get_channel(CANAL_INICIAR_LAVAGEM_ID)

    async for m in canal.history(limit=10):
        if m.author == bot.user:
            return

    embed = discord.Embed(
        title="🧼 Sistema de Lavagem",
        description="Use os botões abaixo para registrar a lavagem.",
        color=0x1abc9c
    )

    await canal.send(embed=embed, view=LavagemView())
# =========================================================
# ========================= LIVES ==========================
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

        if not ao_vivo and divulgado:
            print(f"🔁 {canal_twitch} ficou OFFLINE. Resetando divulgado.")
            lives[user_id]["divulgado"] = False
            alterado = True

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

        # EVITA DUPLICAR CANAL
        for uid, data in lives.items():
            canal_existente = self.extrair_canal_twitch(data.get("link", ""))

            if canal_existente == novo_canal:
                await interaction.response.send_message(
                    f"❌ Esse canal da Twitch já está cadastrado por outro usuário.\n"
                    f"Canal: **{escape_markdown(novo_canal)}**",
                    ephemeral=True
                )
                return

        lives[str(interaction.user.id)] = {
            "link": novo_link,
            "divulgado": False
        }

        salvar_lives(lives)

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
# ====================== PONTO ELETRÔNICO =================
# =========================================================

# ================= UTIL =================

def carregar_ponto():
    if not os.path.exists(ARQUIVO_PONTO):
        return {}
    try:
        with open(ARQUIVO_PONTO, "r") as f:
            return json.load(f)
    except:
        return {}

def salvar_ponto(dados):
    with open(ARQUIVO_PONTO, "w") as f:
        json.dump(dados, f, indent=4)

def hoje_str():
    return agora().strftime("%d/%m/%Y")

def hora_str():
    return agora().strftime("%H:%M")


# ================= VIEW =================

class PontoView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="🟢 Abrir Ponto", style=discord.ButtonStyle.success, custom_id="abrir_ponto")
    async def abrir(self, interaction: discord.Interaction, button: discord.ui.Button):
        ponto = carregar_ponto()
        uid = str(interaction.user.id)
        hoje = hoje_str()

        ponto.setdefault(uid, {}).setdefault(hoje, [])

        for r in ponto[uid][hoje]:
            if "saida" not in r:
                await interaction.response.defer()
                return

        ponto[uid][hoje].append({"entrada": hora_str()})
        salvar_ponto(ponto)

        await atualizar_painel_ponto(interaction.guild)
        await interaction.response.defer()

    @discord.ui.button(label="🔴 Fechar Ponto", style=discord.ButtonStyle.danger, custom_id="fechar_ponto")
    async def fechar(self, interaction: discord.Interaction, button: discord.ui.Button):
        ponto = carregar_ponto()
        uid = str(interaction.user.id)
        hoje = hoje_str()

        if uid in ponto and hoje in ponto[uid]:
            for r in reversed(ponto[uid][hoje]):
                if "saida" not in r:
                    r["saida"] = hora_str()
                    break

        salvar_ponto(ponto)
        await atualizar_painel_ponto(interaction.guild)
        await interaction.response.defer()

    @discord.ui.button(label="📊 Relatório Semanal", style=discord.ButtonStyle.primary, custom_id="relatorio_ponto")
    async def relatorio(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not any(r.id == CARGO_GERENTE_ID for r in interaction.user.roles):
            await interaction.response.defer()
            return

        ponto = carregar_ponto()
        desc = ""

        for uid, dias in ponto.items():
            membro = interaction.guild.get_member(int(uid))
            if not membro:
                continue

            total_min = 0

            for dia, registros in dias.items():
                data = datetime.strptime(dia, "%d/%m/%Y")
                if data.weekday() >= 5:
                    continue

                for r in registros:
                    if "saida" in r:
                        ent = datetime.strptime(r["entrada"], "%H:%M")
                        sai = datetime.strptime(r["saida"], "%H:%M")
                        total_min += int((sai - ent).total_seconds() / 60)

            if total_min > 0:
                horas = total_min // 60
                minutos = total_min % 60
                desc += f"👤 **{membro.display_name}** — {horas}h {minutos}min\n"

        embed = discord.Embed(
            title="📊 Relatório Semanal de Horas",
            description=desc or "Sem dados.",
            color=0x3498db
        )

        await interaction.response.send_message(embed=embed, ephemeral=True)


# ================= PAINEL BONITO =================

async def atualizar_painel_ponto(guild):
    canal = guild.get_channel(CANAL_PONTO_ID)
    if not canal:
        return

    ponto = carregar_ponto()
    hoje = hoje_str()

    em_servico = ""
    registros = ""
    total_servico = 0

    for uid, dias in ponto.items():
        membro = guild.get_member(int(uid))
        if not membro:
            continue

        if hoje in dias:
            for r in dias[hoje]:
                if "saida" not in r:
                    total_servico += 1
                    em_servico += (
                        f"🟢 **{membro.display_name}**\n"
                        f"└ ⏰ Entrou às **{r['entrada']}**\n\n"
                    )
                else:
                    registros += (
                        f"👤 **{membro.display_name}**\n"
                        f"└ 🕓 {r['entrada']} ➜ {r['saida']}\n\n"
                    )

    if not em_servico:
        em_servico = "🛑 Ninguém em serviço."
    if not registros:
        registros = "📭 Nenhum registro finalizado hoje."

    if total_servico == 0:
        cor = 0xe74c3c
    elif total_servico <= 2:
        cor = 0xf1c40f
    else:
        cor = 0x2ecc71

    embed = discord.Embed(
        title="🛠️ PAINEL DE PONTO — MECÂNICA",
        description=f"📅 {hoje}\n━━━━━━━━━━━━━━━━━━━━━━",
        color=cor
    )

    embed.add_field(name="👷 EM SERVIÇO AGORA", value=em_servico, inline=False)
    embed.add_field(name="📋 REGISTROS FINALIZADOS HOJE", value=registros, inline=False)

    async for m in canal.history(limit=10):
        if m.author == guild.me and m.embeds and "PAINEL DE PONTO" in m.embeds[0].title:
            await m.edit(embed=embed, view=PontoView())
            return

    await canal.send(embed=embed, view=PontoView())


# ================= PAINEL INICIAL =================

async def enviar_painel_ponto():
    canal = bot.get_channel(CANAL_PONTO_ID)
    if canal:
        await atualizar_painel_ponto(canal.guild)

# =========================================================
# =============== CALCULADORA MECÂNICA PRO ================
# =========================================================

CANAL_CALCULADORA_MEC_ID = 1448564716598202408

ITENS = {
    "kit_reparo": ("🧰 Kit Reparo", 600, "serv"),
    "pneu_serv": ("🛞 Pneu Serviço", 275, "serv"),
    "vidro": ("🔨 Repor Vidro", 600, "serv"),
    "kit_serv": ("👨‍🔧 Kit Reposição", 600, "serv"),
    "porta": ("🚪 Porta/Capô", 200, "serv"),

    "chamado": ("📞 Chamado Externo", 1500, "ext"),
    "explodido": ("💥 Veículo Explodido", 1500, "ext"),

    "pneu_venda": ("🛞 Pneu Venda", 400, "vend"),
    "kit_venda": ("🧰 Kit Venda", 700, "vend"),
    "pe": ("🔧 Pé de Cabra", 1000, "vend"),
    "chave": ("🔩 Chave Inglesa", 1000, "vend"),
    "elevador": ("📐 Elevador", 1000, "vend"),
    "guincho": ("🚗 Guincho", 8000, "vend"),
    "bolsa": ("🎒 Bolsa Mecânica", 18000, "vend"),
}

cont = {k: 0 for k in ITENS}


def br(valor):
    return f"R$ {valor:,.0f}".replace(",", ".")


def embed_calc():
    total_serv = 0
    total_ext = 0
    total_vend = 0

    linhas = []

    for k, (nome, valor, tipo) in ITENS.items():
        qtd = cont[k]
        if qtd > 0:
            subtotal = qtd * valor
            linhas.append(f"{nome} x{qtd} = {br(subtotal)}")

            if tipo == "serv":
                total_serv += subtotal
            elif tipo == "ext":
                total_ext += subtotal
            else:
                total_vend += subtotal

    total = total_serv + total_ext + total_vend

    embed = discord.Embed(
        title="🔧 CALCULADORA MECÂNICA",
        description="\n".join(linhas) if linhas else "Nenhum item adicionado.",
        color=0xff9900
    )

    embed.add_field(name="Serviços", value=br(total_serv))
    embed.add_field(name="Externo", value=br(total_ext))
    embed.add_field(name="Vendas", value=br(total_vend))
    embed.add_field(name="TOTAL", value=br(total), inline=False)

    return embed


class CalcView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def refresh(self, interaction):
        await interaction.message.edit(embed=embed_calc(), view=self)

    # ========= + =========

    @discord.ui.button(label="+ Kit Reparo", style=discord.ButtonStyle.primary)
    async def a1(self, i, b):
        cont["kit_reparo"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Pneu Serv", style=discord.ButtonStyle.primary)
    async def a2(self, i, b):
        cont["pneu_serv"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Vidro", style=discord.ButtonStyle.primary)
    async def a3(self, i, b):
        cont["vidro"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Kit Serv", style=discord.ButtonStyle.primary)
    async def a4(self, i, b):
        cont["kit_serv"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Porta", style=discord.ButtonStyle.primary)
    async def a5(self, i, b):
        cont["porta"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Chamado", style=discord.ButtonStyle.success)
    async def a6(self, i, b):
        cont["chamado"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Explodido", style=discord.ButtonStyle.success)
    async def a7(self, i, b):
        cont["explodido"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Pneu Venda", style=discord.ButtonStyle.secondary)
    async def a8(self, i, b):
        cont["pneu_venda"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Kit Venda", style=discord.ButtonStyle.secondary)
    async def a9(self, i, b):
        cont["kit_venda"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Pé de Cabra", style=discord.ButtonStyle.secondary)
    async def a10(self, i, b):
        cont["pe"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Chave", style=discord.ButtonStyle.secondary)
    async def a11(self, i, b):
        cont["chave"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Elevador", style=discord.ButtonStyle.secondary)
    async def a12(self, i, b):
        cont["elevador"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Guincho", style=discord.ButtonStyle.secondary)
    async def a13(self, i, b):
        cont["guincho"] += 1
        await self.refresh(i)

    @discord.ui.button(label="+ Bolsa", style=discord.ButtonStyle.secondary)
    async def a14(self, i, b):
        cont["bolsa"] += 1
        await self.refresh(i)

    # ========= - =========

    @discord.ui.button(label="-1 Geral", style=discord.ButtonStyle.danger, row=4)
    async def menos(self, i, b):
        for k in cont:
            if cont[k] > 0:
                cont[k] -= 1
                break
        await self.refresh(i)

    @discord.ui.button(label="🧹 Limpar", style=discord.ButtonStyle.danger, row=4)
    async def limpar(self, i, b):
        for k in cont:
            cont[k] = 0
        await self.refresh(i)


async def painel_calc():
    canal = bot.get_channel(CANAL_CALCULADORA_MEC_ID)

    async for m in canal.history(limit=10):
        if m.author == bot.user:
            return

    await canal.send(embed=embed_calc(), view=CalcView())

# =========================================================
# ========================== METAS =========================
# =========================================================

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

def tem_meta(member_id):
    metas = carregar_metas()
    return str(member_id) in metas


# =========================================================
# ============ FUNÇÃO CENTRAL (CRIA META) =================
# =========================================================

async def criar_sala_meta(member: discord.Member):
    metas = carregar_metas()

    if str(member.id) in metas:
        return

    categoria_id = obter_categoria_meta(member)
    if not categoria_id:
        return

    guild = member.guild
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

    metas[str(member.id)] = {"canal_id": canal.id}
    salvar_metas(metas)

    cargo_resp_metas = guild.get_role(CARGO_RESP_METAS_ID)

    aviso = await canal.send(
        f"📢 **ALGUNS AVISOS IMPORTANTES SOBRE ESSA SALA** 📢\n\n"
        f"📌 Apenas você {member.mention} e a Gerência tem acesso.\n"
        f"📌 Leia o canal ⁠📢・faq-meta\n"
        f"📌 Responsável por metas: {cargo_resp_metas.mention if cargo_resp_metas else '@RESP | Metas'}",
        view=MetaFecharView(member.id)
    )
    await aviso.pin()


# =========================================================
# ================= BOTÕES ================================
# =========================================================

class MetaView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(label="📁 Solicitar Sala de Meta", style=discord.ButtonStyle.primary, custom_id="meta_criar")
    async def criar_meta(self, interaction: discord.Interaction, button: discord.ui.Button):
        if tem_meta(interaction.user.id):
            await interaction.response.send_message("❌ Você já possui uma sala de meta.", ephemeral=True)
            return

        await criar_sala_meta(interaction.user)
        await interaction.response.send_message("✅ Sua sala de meta foi criada!", ephemeral=True)


class MetaFecharView(discord.ui.View):
    def __init__(self, member_id):
        super().__init__(timeout=None)
        self.member_id = member_id

    @discord.ui.button(label="🧹 Fechar Sala", style=discord.ButtonStyle.danger, custom_id="meta_fechar")
    async def fechar(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not any(r.id == CARGO_GERENTE_ID for r in interaction.user.roles):
            await interaction.response.send_message("❌ Apenas Gerentes podem fechar.", ephemeral=True)
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

        await interaction.response.send_message("🧹 Sala fechada.", ephemeral=True)


# =========================================================
# ============= SAIU DO DISCORD = FECHA META ==============
# =========================================================

@bot.event
async def on_member_remove(member):
    metas = carregar_metas()
    data = metas.get(str(member.id))

    if not data:
        return

    canal = member.guild.get_channel(data["canal_id"])

    if canal:
        try:
            await canal.delete()
        except:
            pass

    del metas[str(member.id)]
    salvar_metas(metas)


# =========================================================
# ========= GANHOU/MUDOU CARGO = CRIA/MOVE META ===========
# =========================================================

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    if before.roles == after.roles:
        return

    metas = carregar_metas()

    before_roles = {r.id for r in before.roles}
    after_roles = {r.id for r in after.roles}

    # GANHOU AGREGADO
    virou_agregado = (
        AGREGADO_ROLE_ID in after_roles and
        AGREGADO_ROLE_ID not in before_roles
    )

    if virou_agregado and str(after.id) not in metas:
        await criar_sala_meta(after)
        return

    # MOVE DE CATEGORIA
    data = metas.get(str(after.id))
    if not data:
        return

    canal = after.guild.get_channel(data["canal_id"])
    if not canal:
        return

    nova_categoria_id = obter_categoria_meta(after)
    if not nova_categoria_id:
        return

    if canal.category_id == nova_categoria_id:
        return

    nova_categoria = after.guild.get_channel(nova_categoria_id)
    if not nova_categoria:
        return

    try:
        await canal.edit(category=nova_categoria)
        await canal.send(
            f"🔁 Sala movida automaticamente para **{nova_categoria.name}** devido à atualização de cargo."
        )
    except Exception as e:
        print(f"Erro ao mover meta: {e}")


# =========================================================
# ========= VERIFICAÇÃO AUTOMÁTICA AO INICIAR =============
# =========================================================

async def verificar_metas_automaticas():
    print("🔎 Verificando metas automáticas...")

    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return

    metas = carregar_metas()

    for member in guild.members:
        if member.bot:
            continue

        # Só quem tem agregado
        tem_agregado = any(r.id == AGREGADO_ROLE_ID for r in member.roles)
        if not tem_agregado:
            continue

        # Já tem registro no JSON?
        if str(member.id) in metas:
            canal_id = metas[str(member.id)]["canal_id"]
            canal = guild.get_channel(canal_id)

            # Canal ainda existe = NÃO recria
            if canal:
                continue

            # Canal foi apagado manualmente
            print(f"⚠️ Canal de meta sumiu para {member.display_name}, recriando...")

        # Não tem meta registrada → cria
        await criar_sala_meta(member)

# =========================================================
# ================= LIMPEZA AUTOMÁTICA METAS ==============
# =========================================================

async def reconstruir_metas():
    print("🧹 Reconstruindo metas.json baseado nos canais reais...")

    guild = bot.get_guild(GUILD_ID)
    if not guild:
        return

    categorias_ids = [
        CATEGORIA_META_GERENTE_ID,
        CATEGORIA_META_RESPONSAVEIS_ID,
        CATEGORIA_META_SOLDADO_ID,
        CATEGORIA_META_MEMBRO_ID,
        CATEGORIA_META_AGREGADO_ID
    ]

    novo_metas = {}

    for cat_id in categorias_ids:
        categoria = guild.get_channel(cat_id)
        if not categoria:
            continue

        for canal in categoria.channels:
            nome = canal.name.replace("📁・", "").replace("-", " ").strip()

            for membro in guild.members:
                if membro.bot:
                    continue

                nome_membro = membro.display_name.lower().strip()

                if nome_membro in nome:
                    novo_metas[str(membro.id)] = {
                        "canal_id": canal.id
                    }

    salvar_metas(novo_metas)
    print(f"✅ metas.json reconstruído com {len(novo_metas)} registros.")


# =========================================================
# ================= PAINEL METAS ==========================
# =========================================================

async def enviar_painel_metas():
    canal = bot.get_channel(CANAL_SOLICITAR_SALA_ID)
    if not canal:
        return

    async for m in canal.history(limit=10):
        if m.author == bot.user and m.embeds and m.embeds[0].title == "📁 Solicitação de Sala de Meta":
            return

    embed = discord.Embed(
        title="📁 Solicitação de Sala de Meta",
        description="Clique no botão abaixo para criar sua sala.",
        color=0xf1c40f
    )

    await canal.send(embed=embed, view=MetaView())
# =========================================================
# ========================= ON READY ======================
# =========================================================

@bot.event
async def on_ready():

    if hasattr(bot, "ready_once"):
        return
    bot.ready_once = True

    print("🔄 Iniciando configuração do bot...")

    # ================= RELÓGIO GLOBAL =================
    print(f"🕒 Horário atual (Brasília): {agora().strftime('%d/%m/%Y %H:%M:%S')}")

    # ================= VIEWS PERSISTENTES =================
    bot.add_view(RegistroView())
    bot.add_view(CalculadoraView())
    bot.add_view(StatusView())
    bot.add_view(CadastrarLiveView())
    bot.add_view(MetaView())
    bot.add_view(MetaFecharView(0))
    bot.add_view(PolvoraView())
    bot.add_view(ConfirmarPagamentoView())
    bot.add_view(LavagemView())
    bot.add_view(PontoView())
    bot.add_view(CalcView())
    bot.add_view(FabricacaoView())

    # ================= LOOPS =================
    if not verificar_lives_twitch.is_running():
        verificar_lives_twitch.start()

    if not relatorio_semanal_polvoras.is_running():
        relatorio_semanal_polvoras.start()

    # ================= RESTAURAR PRODUÇÕES =================
    for pid in carregar_producoes():
        bot.loop.create_task(acompanhar_producao(pid))

    # ================= ENVIAR PAINÉIS =================
    await enviar_painel_fabricacao()
    await enviar_painel_lives()
    await enviar_painel_metas()
    await enviar_painel_polvoras(bot)
    await enviar_painel_lavagem()
    await enviar_painel_ponto()
    await painel_calc()

    # ================= VERIFICAÇÕES AUTOMÁTICAS =================
    await verificar_metas_automaticas()
    await reconstruir_metas()

    print("✅ Bot online com todos os sistemas ativos")
    print("🕒 Todos os horários agora estão em Brasília")

# =========================================================
# ========================= START BOT =====================
# =========================================================

bot.run(TOKEN)



