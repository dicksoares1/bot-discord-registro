import os
import discord
from discord.ext import commands
from datetime import datetime

# ================= CONFIG =================

TOKEN = os.environ.get("TOKEN")  # ou coloque direto: "SEU_TOKEN_AQUI"

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(command_prefix="!", intents=intents)

# ================= VIEW =================

class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    def _get_status_atual(self, embed: discord.Embed):
        for field in embed.fields:
            if field.name == "📌 Status":
                if field.value.strip() == "—":
                    return []
                return field.value.split("\n")
        return []

    def _set_status(self, embed: discord.Embed, status_lista):
        valor = "\n".join(status_lista) if status_lista else "—"

        for i, field in enumerate(embed.fields):
            if field.name == "📌 Status":
                embed.set_field_at(
                    i,
                    name="📌 Status",
                    value=valor,
                    inline=False
                )
                return

        embed.add_field(
            name="📌 Status",
            value=valor,
            inline=False
        )

    async def _get_embed(self, interaction: discord.Interaction):
        if not interaction.message.embeds:
            await interaction.response.send_message(
                "❌ Essa mensagem não possui embed.",
                ephemeral=True
            )
            return None
        return interaction.message.embeds[0]

    async def toggle_simples(self, interaction: discord.Interaction, texto_status: str):
        await interaction.response.defer()

        embed = await self._get_embed(interaction)
        if not embed:
            return

        status_lista = self._get_status_atual(embed)

        if texto_status in status_lista:
            status_lista.remove(texto_status)
        else:
            status_lista.append(texto_status)

        self._set_status(embed, status_lista)
        await interaction.message.edit(embed=embed)

    async def toggle_entregue(self, interaction: discord.Interaction):
        await interaction.response.defer()

        embed = await self._get_embed(interaction)
        if not embed:
            return

        status_lista = [
            s for s in self._get_status_atual(embed)
            if not s.startswith("✅ Entregue")
        ]

        agora = datetime.now().strftime("%d/%m/%Y às %H:%M")
        status_lista.append(f"✅ Entregue — {agora}")

        self._set_status(embed, status_lista)
        await interaction.message.edit(embed=embed)

    @discord.ui.button(
        label="💰 Pago",
        style=discord.ButtonStyle.primary,
        custom_id="status_pago"
    )
    async def pago(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.toggle_simples(interaction, "💰 Pago")

    @discord.ui.button(
        label="✅ Entregue",
        style=discord.ButtonStyle.success,
        custom_id="status_entregue"
    )
    async def entregue(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.toggle_entregue(interaction)

    @discord.ui.button(
        label="📦 A entregar",
        style=discord.ButtonStyle.secondary,
        custom_id="status_a_entregar"
    )
    async def a_entregar(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.toggle_simples(interaction, "📦 A entregar")

    @discord.ui.button(
        label="⏳ Pagamento pendente",
        style=discord.ButtonStyle.danger,
        custom_id="status_pendente"
    )
    async def pendente(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.toggle_simples(interaction, "⏳ Pagamento pendente")

# ================= COMANDO =================

@bot.command()
async def status(ctx):
    embed = discord.Embed(
        title="📦 Pedido",
        description="Controle de status do pedido",
        color=discord.Color.blurple()
    )

    embed.add_field(
        name="📌 Status",
        value="—",
        inline=False
    )

    await ctx.send(embed=embed, view=StatusView())

# ================= EVENTOS =================

@bot.event
async def on_ready():
    bot.add_view(StatusView())  # registra view persistente
    print(f"✅ Bot online como {bot.user}")

# ================= START =================

bot.run(TOKEN)
