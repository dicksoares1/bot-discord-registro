import os
import discord
from discord.ext import commands
from datetime import datetime

TOKEN = os.environ["TOKEN"]

intents = discord.Intents.default()
intents.message_content = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    help_command=None
)

# ================= VIEW =================

class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    def _get_status(self, embed):
        for field in embed.fields:
            if field.name == "📌 Status":
                return [] if field.value == "—" else field.value.split("\n")
        return []

    def _set_status(self, embed, status):
        valor = "\n".join(status) if status else "—"

        for i, field in enumerate(embed.fields):
            if field.name == "📌 Status":
                embed.set_field_at(i, name="📌 Status", value=valor, inline=False)
                return

        embed.add_field(name="📌 Status", value=valor, inline=False)

    async def _get_embed(self, interaction):
        if not interaction.message.embeds:
            await interaction.response.send_message(
                "❌ Mensagem sem embed.",
                ephemeral=True
            )
            return None
        return interaction.message.embeds[0]

    async def toggle(self, interaction, texto):
        await interaction.response.defer()

        embed = await self._get_embed(interaction)
        if not embed:
            return

        status = self._get_status(embed)

        if texto in status:
            status.remove(texto)
        else:
            status.append(texto)

        self._set_status(embed, status)
        await interaction.message.edit(embed=embed)

    async def marcar_entregue(self, interaction):
        await interaction.response.defer()

        embed = await self._get_embed(interaction)
        if not embed:
            return

        status = [s for s in self._get_status(embed) if not s.startswith("✅ Entregue")]
        agora = datetime.now().strftime("%d/%m/%Y às %H:%M")
        status.append(f"✅ Entregue — {agora}")

        self._set_status(embed, status)
        await interaction.message.edit(embed=embed)

    @discord.ui.button(label="💰 Pago", style=discord.ButtonStyle.primary, custom_id="status_pago")
    async def pago(self, interaction, button):
        await self.toggle(interaction, "💰 Pago")

    @discord.ui.button(label="📦 A entregar", style=discord.ButtonStyle.secondary, custom_id="status_entregar")
    async def entregar(self, interaction, button):
        await self.toggle(interaction, "📦 A entregar")

    @discord.ui.button(label="⏳ Pagamento pendente", style=discord.ButtonStyle.danger, custom_id="status_pendente")
    async def pendente(self, interaction, button):
        await self.toggle(interaction, "⏳ Pagamento pendente")

    @discord.ui.button(label="✅ Entregue", style=discord.ButtonStyle.success, custom_id="status_entregue")
    async def entregue(self, interaction, button):
        await self.marcar_entregue(interaction)

# ================= COMANDO =================

@bot.command()
async def status(ctx):
    embed = discord.Embed(
        title="📦 Pedido",
        description="Controle de status",
        color=discord.Color.blue()
    )
    embed.add_field(name="📌 Status", value="—", inline=False)
    await ctx.send(embed=embed, view=StatusView())

# ================= READY =================

@bot.event
async def on_ready():
    bot.add_view(StatusView())
    print("✅ Bot online no Railway")

# ================= START =================

bot.run(TOKEN)
