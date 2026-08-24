FROM python:3.12-slim

WORKDIR /app

# Instalar FFmpeg e dependências
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements.txt e instalar dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar o resto do código
COPY . .

# Comando para iniciar o bot
CMD ["python", "bot.py"]
