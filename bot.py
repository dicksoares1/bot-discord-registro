@bot.command(name="cache_stats")
@commands.has_permissions(administrator=True)
async def cmd_cache_stats(ctx):
    st = cache.stats()
    embed = discord.Embed(title="📦 CACHE STATS", color=0x3498DB, timestamp=agora())
    embed.add_field(name="📊 Tamanho", value=f"{st['size']}/200", inline=True)
    embed.add_field(name="✅ Hits", value=f"{st['hits']}", inline=True)
    embed.add_field(name="❌ Misses", value=f"{st['misses']}", inline=True)
    embed.add_field(name="📈 Hit Rate", value=f"{st['hit_rate']}", inline=True)
    await ctx.send(embed=embed)

# ────────────────────────────────────────────────────────────────────────────────
# COMANDO: !limpar_cache
# ────────────────────────────────────────────────────────────────────────────────
# ────────────────────────────────────────────────────────────────────────────────
# FUNÇÃO: cmd_limpar_cache
# ────────────────────────────────────────────────────────────────────────────────
@bot.command(name="limpar_cache")
@commands.has_permissions(administrator=True)
async def cmd_limpar_cache(ctx):
    await cache.clear()
    gc.collect()
    await ctx.send("✅ Cache limpo e GC executado!")


# ════════════════════════════════════════════════════════════════════════════════
# BLINDAGEM GLOBAL — ESCUDO ANTI-CRASH E ANTI "INTERACTION FAILED"
# ════════════════════════════════════════════════════════════════════════════════
# ────────────────────────────────────────────────────────────────────────────────
# FUNÇÃO: on_app_command_error
# ────────────────────────────────────────────────────────────────────────────────
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: discord.app_commands.AppCommandError):
    logger.error(f"❌ Erro em comando slash: {error}")
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message("❌ Ocorreu um erro interno. A gerência foi notificada.", ephemeral=True)
    except:
        pass

# ────────────────────────────────────────────────────────────────────────────────
# FUNÇÃO: on_error
# ────────────────────────────────────────────────────────────────────────────────
@bot.event
async def on_error(event, *args, **kwargs):
    import traceback
    logger.error(f"❌ Erro não tratado no evento '{event}':\n{traceback.format_exc()}")

# ────────────────────────────────────────────────────────────────────────────────
# FUNÇÃO: _safe_view_error_handler
# ────────────────────────────────────────────────────────────────────────────────
async def _safe_view_error_handler(interaction: discord.Interaction, error: Exception):
    logger.error(f"❌ Erro em interação ({interaction.data.get('custom_id', '?')}): {error}")
    try:
        if not interaction.response.is_done():
            await interaction.response.send_message("❌ Ocorreu um erro interno. A gerência foi notificada.", ephemeral=True)
        else:
            await interaction.followup.send("❌ Ocorreu um erro interno. A gerência foi notificada.", ephemeral=True)
    except:
        pass

for name, obj in list(globals().items()):
    if isinstance(obj, type):
        if issubclass(obj, (discord.ui.View, discord.ui.Modal)) and obj not in (discord.ui.View, discord.ui.Modal):
            original_on_error = getattr(obj, "on_error", None)
            
            # ────────────────────────────────────────────────────────────────────────────────
            # MÉTODO: new_on_error
            # ────────────────────────────────────────────────────────────────────────────────
            async def new_on_error(self, interaction, error, _original=original_on_error):
                if _original:
                    try: await _original(interaction, error)
                    except: pass
                await _safe_view_error_handler(interaction, error)

            if not original_on_error or original_on_error == discord.ui.View.on_error or original_on_error == discord.ui.Modal.on_error:
                obj.on_error = new_on_error



# ════════════════════════════════════════════════════════════════════════════════
# ║  BLOCO FINAL — INICIALIZAÇÃO DO BOT                                         ║
# ════════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    logger.info("🚀 Iniciando bot v8.0 COMPLETO...")
    try:
        import signal
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop = asyncio.get_running_loop()
            except RuntimeError:
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))
                break
            except RuntimeError:
                pass
    except Exception:
        pass
    try:
        bot.run(TOKEN, reconnect=True)
    except discord.LoginFailure:
        logger.critical("❌ Falha no login! TOKEN inválido?")
        sys.exit(1)
    except Exception as e:
        logger.critical(f"❌ Erro fatal: {e}")
        sys.exit(1)
