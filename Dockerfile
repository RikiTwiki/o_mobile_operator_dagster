FROM python:3.10-slim

RUN apt-get update && apt-get install -y \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

RUN pip install \
    dagster \
    dagster-postgres \
    dagster-docker \
    psycopg2-binary \
    sqlalchemy \
    python-dotenv \
    pandas

RUN apt-get update && apt-get install -y --no-install-recommends \
    libnss3 libnspr4 libatk1.0-0 libatk-bridge2.0-0 libdrm2 libxkbcommon0 \
    libxcomposite1 libxdamage1 libxfixes3 libxrandr2 libxrender1 libxss1 libxtst6 \
    libcups2 libasound2 libwayland-client0 libwayland-cursor0 libwayland-egl1 \
    libdbus-1-3 libatspi2.0-0 libglib2.0-0 libpango-1.0-0 libpangocairo-1.0-0 \
    libgdk-pixbuf-2.0-0 libgbm1 libgtk-3-0 \
    fonts-liberation fonts-unifont fonts-dejavu-core fonts-noto-core \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster/app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install playwright && playwright install

COPY ./user_code/ ./


EXPOSE 4000

CMD ["dagster", "code-server", "start", "-h", "0.0.0.0", "-p", "4000", "-f", "./definitions.py"]