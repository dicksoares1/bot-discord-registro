from datetime import datetime

class StatusView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    def _get_status_atual(self, embed):
        for field in embed.fields:
            if field.name == "📌 Status":
                if field.value == "—":
                    return []
                return field.value.split("\n")
        return []

    def _set_status(self, embed, status_lista):
        valor = "\n".join(status_lista) if status_lista else "—"

        for i, field in enumerate(embed.fields):
            if field.name == "📌 Status":
                embed.set_field_at(i, name="📌 Status", value=valor, inline=False)
                return

        embed.add_field(name="📌 Status", value=valor, inline=False)

    async def toggle_simples(self, interaction, texto_status):
        embed = interaction.message.embeds[0]
        status_lista = self._get_status_atual(embed)

        if texto_status in status_lista:
            status_lista.remove(texto_status)
        else:
            status_lista.append(texto_status)

        self._set_status(embed, status_lista)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

    async def toggle_entregue(self, interaction):
        embed = interaction.message.embeds[0]
        status_lista = self._get_status_atual(embed)

        # remove qualquer status "Entregue"
        status_lista = [
            s for s in status_lista
            if not s.startswith("✅ Entregue")
        ]

        # se não tinha entregue antes, adiciona com data/hora
        agora = datetime.now().strftime("%d/%m/%Y às %H:%M")
        status_lista.append(f"✅ Entregue — {agora}")

        self._set_status(embed, status_lista)
        await interaction.message.edit(embed=embed)
        await interaction.response.defer()

    @discord.ui.button(
        label="💰 Pago",
        style=discord.ButtonStyle.primary,
        custom_id="status_pago"
    )
    async def pago(self, interaction, button):
        await self.toggle_simples(interaction, "💰 Pago")

    @discord.ui.button(
        label="✅ Entregue",
        style=discord.ButtonStyle.success,
        custom_id="status_entregue"
    )
    async def entregue(self, interaction, button):
        await self.toggle_entregue(interaction)

    @discord.ui.button(
        label="📦 A entregar",
        style=discord.ButtonStyle.secondary,
        custom_id="status_a_entregar"
    )
    async def a_entregar(self, interaction, button):
        await self.toggle_simples(interaction, "📦 A entregar")

    @discord.ui.button(
        label="⏳ Pagamento pendente",
        style=discord.ButtonStyle.danger,
        custom_id="status_pendente"
    )
    async def pendente(self, interaction, button):
        await self.toggle_simples(interaction, "⏳ Pagamento pendente")
